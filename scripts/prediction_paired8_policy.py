"""One fixed T1 hypothesis and its control, paired at the parent fill instant.

Variants veto one common candidate; none search for a replacement trade.
Coverage is outcome-independent and finalized even when quotes stop arriving.
"""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
import math

from scripts.prediction_value9_parent_frozen import next5_valid, value9_step

PAIRED8_VERSION = 'vol-shadow-v1-fixed600'
SIGMA_FLOOR = 0.05588092263731523
# delay_ms, minimum edge, maximum ask, first entry second, old price cap
LANE_RULES = {
 'vol_cap_3s': (3000,.03,.85,180,True),
 'vol_main_3s': (3000,.03,.85,180,False),
 'vol_delay_1s': (1000,.03,.85,180,False),
 'vol_delay_5s': (5000,.03,.85,180,False),
 'vol_edge_05': (3000,.05,.85,180,False),
 'vol_price_75': (3000,.03,.75,180,False),
 'vol_late_210': (3000,.03,.85,210,False),
}
PAIRED8_LANES = tuple(LANE_RULES)
QUALITY_START_MS = 120_000
QUALITY_END_MS = 271_000
QUALITY_CELLS = 151
SOURCE_TIMES = ('spot_at_ms', 'spot_event_at_ms', 'spot_received_at_ms',
                'book_at_ms', 'received_at_ms')


def _integer(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return int(number) if math.isfinite(number) and number == int(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def validate_source_times(previous, q, now_ms):
    """One monotonicity gate before both signal history and pending fills.

    Invalid/missing fields make a quote unavailable; an actual regression is a
    fatal input error. End-of-window calls use an empty quote and do not reset
    the last observed timestamps.
    """
    old = (previous or {}).get('source_times', {})
    current = dict(old)
    for key in SOURCE_TIMES + ('spot_trade_id', 'spot_connection_generation'):
        value = _integer(q.get(key))
        if value is None or (key in SOURCE_TIMES and value <= 0):
            continue
        if key in old and value < old[key]:
            raise ValueError('Source time/sequence reversed: ' + key)
        if key in SOURCE_TIMES and value > now_ms:
            raise ValueError('Future source timestamp: ' + key)
        current[key] = value
    return current


def acquisition_valid(q, now_ms):
    """Transport/structure health, deliberately independent of trade filters."""
    try:
        if (q.get('source') != 'binance_prediction_ws' or q.get('orientation_verified') is not True
                or q.get('spot_connected') is not True):
            return False
        times = {key: _integer(q.get(key)) for key in SOURCE_TIMES}
        if any(t is None or not 0 < t <= now_ms for t in times.values()):
            return False
        if not times['spot_at_ms'] <= times['spot_event_at_ms'] <= times['spot_received_at_ms']:
            return False
        if any(now_ms - times[key] > 1500 for key in ('book_at_ms', 'received_at_ms')):
            return False
        for key in ('spot_trade_id', 'spot_connection_generation'):
            number = _integer(q.get(key))
            if number is None or number < 0:
                return False
        spot = float(q['spot'])
        if not math.isfinite(spot) or spot <= 0:
            return False
        for side in ('UP', 'DOWN'):
            bid, ask, depth = [float(q[side][k]) for k in ('bid', 'ask', 'ask_shares')]
            if not all(math.isfinite(x) for x in (bid, ask, depth)):
                return False
            if not 0 < bid <= ask < 1 or depth < 0:
                return False
        return True
    except (ValueError, TypeError, KeyError, OverflowError):
        return False


def observe_metadata(previous, q, *, now_ms, start_ms):
    metadata = deepcopy(previous) if previous else {'frozen': False, 'ready_at_ms': None}
    if not q:
        return metadata
    try:
        reference = Decimal(str(q.get('reference')))
        positive = reference.is_finite() and reference > 0
    except Exception:
        positive = False
    if metadata['frozen']:
        if positive and reference != Decimal(metadata['reference']):
            raise ValueError('Frozen market reference changed')
        if q.get('orientation_verified') is not True:
            raise ValueError('Frozen market orientation changed')
        return metadata
    ready = _integer(q.get('metadata_ready_at_ms'))
    if positive and q.get('orientation_verified') is True and ready is not None and 0 < ready <= now_ms:
        # A later payload cannot backdate readiness into an earlier unobserved
        # interval. Restarted stores reuse the already-frozen state above.
        declared_ready = ready
        ready = max(ready, now_ms)
        metadata.update(frozen=True, reference=str(reference), orientation_verified=True,
                        ready_at_ms=ready, declared_ready_at_ms=declared_ready,
                        ready_before_window=ready <= start_ms + 180000)
    return metadata


def observe_quality(previous, q, *, now_ms, start_ms, monotone=True, acquisition=False):
    quality = deepcopy(previous) if previous else {'cells_hex': '0', 'max_gap_ms': 0, 'closed': False}
    begin, end = start_ms + QUALITY_START_MS, start_ms + QUALITY_END_MS
    if quality['closed']:
        return quality
    valid = acquisition_valid(q, now_ms) if acquisition else next5_valid(q, now_ms)
    if begin <= now_ms < end and monotone and valid:
        cell = (now_ms - begin) // 1000
        quality['cells_hex'] = hex(int(quality['cells_hex'], 16) | (1 << cell))
        quality['max_gap_ms'] = max(quality['max_gap_ms'], now_ms - quality.get('last_valid_ms', begin))
        quality['last_valid_ms'] = now_ms
    if now_ms >= end:
        quality['max_gap_ms'] = max(quality['max_gap_ms'], end - quality.get('last_valid_ms', begin))
        quality['closed'] = True
    count = int(quality['cells_hex'], 16).bit_count()
    quality.update(valid_cells=count, expected_cells=QUALITY_CELLS, coverage=count / QUALITY_CELLS,
                   passed=quality['closed'] and count / QUALITY_CELLS >= .95 and quality['max_gap_ms'] <= 2500)
    return quality


def probability(history, q, elapsed):
    if len(history)<30 or history[-1][0]-history[0][0]<45000:
        return None
    pairs=list(zip(history,history[1:]))
    if any(b[0]-a[0]>5000 for a,b in pairs):return None
    sigma=math.sqrt(sum((math.log(b[1]/a[1])*10000)**2 for a,b in pairs)/((history[-1][0]-history[0][0])/1000))
    distance=math.log(float(q['spot'])/float(q['reference']))*10000
    z=distance/(max(SIGMA_FLOOR,sigma)*math.sqrt(max(10,300-elapsed)))
    return max(.001,min(.999,.5*(1+math.erf(z/math.sqrt(2)))))

def signal(p,q,elapsed,rule):
    _,edge,cap,begin,_=rule
    if p is None or not begin<=elapsed<=270:return None
    edges=[p-float(q['UP']['ask'])*1.05,1-p-float(q['DOWN']['ask'])*1.05]
    side=0 if edges[0]>=edges[1] else 1
    name=('UP','DOWN')[side]
    return name if edges[side]>=edge and .20<=float(q[name]['ask'])<=cap else None

def paired8_step(previous,q,*,now_ms,start_ms,end_ms,model):
    if model.get('shadow_gate_passed') is not False or end_ms-start_ms!=300000:
        raise ValueError('Fixed Shadow-only market required')
    identity={'version':PAIRED8_VERSION,'start_ms':start_ms,'end_ms':end_ms,
              'model_sha256':hashlib.sha256(json.dumps(model,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
    s=deepcopy(previous) if previous else {**identity,'history':[],
       'lanes':{lane:{'status':'waiting','fills':0,'candidates':0} for lane in PAIRED8_LANES}}
    if any(s.get(k)!=v for k,v in identity.items()):raise ValueError('Frozen identity changed')
    if now_ms<s.get('last_call_ms',now_ms):raise ValueError('Time reversed')
    if now_ms==s.get('last_call_ms'):return s,[]
    s['source_times']=validate_source_times(s,q,now_ms)
    s['metadata']=observe_metadata(s.get('metadata'),q,now_ms=now_ms,start_ms=start_ms)
    collected=_integer(q.get('collection_started_at_ms'))
    if 'collection_started_at_ms' not in s and collected is not None and 0<collected<=now_ms:
        s['collection_started_at_ms']=collected
    s['last_call_ms']=now_ms
    s['acquisition_quality']=observe_quality(s.get('acquisition_quality'),q,now_ms=now_ms,start_ms=start_ms,acquisition=True)
    tq=deepcopy(q)
    if s['metadata']['frozen']:tq['reference']=s['metadata']['reference']
    valid=bool(s['metadata']['frozen'] and acquisition_valid(tq,now_ms) and next5_valid(tq,now_ms))
    if not valid:tq['feed_ok']=False
    s['quality']=observe_quality(s.get('quality'),tq,now_ms=now_ms,start_ms=start_ms)
    s['history']=[r for r in s['history'] if now_ms-60000<=r[0]<=now_ms]
    if valid:s['history'].append([now_ms,float(tq['spot'])])
    elapsed=(now_ms-start_ms)/1000
    p=probability(s['history'],tq,elapsed) if valid else None
    plans=[]
    for lane,rule in LANE_RULES.items():
        ls=s['lanes'][lane]
        if ls['status'] in ('filled','rejected','expired'):continue
        if elapsed>270:
            ls.update(status='expired',reason='window_ended');continue
        if ls['status']=='pending':
            pending=ls['pending']
            if now_ms>pending['expires_ms']:
                ls.update(status='expired',reason='execution_timeout');continue
            if not valid or now_ms<pending['ready_ms'] or tq['book_at_ms']<=pending['book_at_ms']:continue
            side=pending['side'];ask=Decimal(str(tq[side]['ask']))
            reason=None
            if not Decimal('.20')<=ask<=Decimal(str(rule[2])) or Decimal(str(tq[side]['ask_shares']))<Decimal(2)/ask:reason='band_or_depth'
            elif rule[4] and float(ask)>float(pending['ask'])+.005+1e-12:reason='old_price_cap'
            elif signal(p,tq,elapsed,rule)!=side:reason='reconfirmation'
            if reason:
                ls.update(status='rejected',reason=reason);continue
            fill=dict(lane=lane,action='BUY_INITIAL',side=side,gross='2',ask=str(ask),reason='vol_shadow_only',
                decision_ms=pending['at_ms'],fill_ms=now_ms,decision_ask=pending['ask'],
                decision_book_ms=pending['book_at_ms'],fill_book_ms=tq['book_at_ms'],probability_up=p)
            ls.update(status='filled',fills=1,fill=deepcopy(fill));plans.append(fill)
        elif valid:
            side=signal(p,tq,elapsed,rule)
            if side is not None:
                ls.update(status='pending',candidates=1,pending=dict(side=side,ask=str(tq[side]['ask']),at_ms=now_ms,
                    ready_ms=now_ms+rule[0],expires_ms=min(now_ms+rule[0]+4000,start_ms+270000),book_at_ms=tq['book_at_ms']))
    if now_ms>=end_ms:s['history']=[]
    return s,plans
