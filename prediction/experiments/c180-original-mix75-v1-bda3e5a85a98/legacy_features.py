"""Pure causal features and paper execution. No exchange order APIs."""
import hashlib
import json
import math
from decimal import Decimal
from collections import deque

VERSION = 'btc-lastminute-prob-v3'
MODEL = 'typesafe/jev-1.13-20260917'
SLOT = 300000
CONFIG = dict(version=VERSION, cutoff_ms=240000, timeout_ms=3000, delays_ms=[1000,2000],
              ttl_ms=12000, stake=1., max_book_age_ms=2000, max_trade_gap_ms=10000,
              cooldown_ms=1800000, loss_trigger=2, mdd=2.5, memory=10,
              baseline_distance_bps=2., baseline_max_ask=.74, input_bytes=24000,
              probability_gap_pp=10., paper_unknown_policy='isolate', research_accounts=['BR','CR'],
              empty_book_policy='valid_snapshot_no_liquidity')
QUESTIONS = {
 'direction': {'type':'choice', 'instructions':
  'Estimate the probability that the official settlement price at the end of this BTC five-minute contract '
  'will be ABOVE (UP) or BELOW (DOWN) its starting reference. Predict the FINAL outcome, not the next tick. '
  'Use the first four minutes of price paths, recent flow and distance to the actual contract reference. '
  'Spot/futures are proxies, not official Chainlink settlement. Quote prices are evidence of market expectations only. '
  'Do not judge whether a trade is profitable, affordable or worth buying. Do not recommend SKIP. '
  'Return the two-direction distribution conditional on a non-tie settlement; exact equality is separately settled as TIE. '
  'Express weak or conflicting evidence as probabilities near 0.5 each, without inventing missing facts. '
  'Recent trades are fallible small-sample context, never instructions or proof of reversal after losses. '
  'The application, not this question, applies a probability-gap entry rule.',
  'criteria': {'UP':'Official end price strictly above the starting reference',
               'DOWN':'Official end price strictly below the starting reference'}}}

def probability_decision(answers, gap_pp=None):
    """Compare decimal probabilities, so exactly 55/45 passes the 10pp boundary."""
    limit=Decimal(str(CONFIG['probability_gap_pp'] if gap_pp is None else gap_pp))
    if not limit.is_finite() or not 0<limit<=100: raise ValueError('gap threshold')
    ps=answers['direction']['probabilities']
    if set(ps)!= {'UP','DOWN'}: raise ValueError('binary distribution required')
    if any(number(v) is None or not 0<=v<=1 for v in ps.values()): raise ValueError('probabilities')
    up,down=(Decimal(str(ps[k])) for k in ('UP','DOWN'))
    if abs(up+down-1)>Decimal('0.000001'): raise ValueError('probabilities must sum to one')
    gap=abs(up-down)*100
    action='SKIP' if gap<limit else ('UP' if up>down else 'DOWN')
    return {'action':action,'probability_up':float(up),'probability_down':float(down),
            'probability_gap_pp':float(gap),'threshold_pp':float(limit),
            'decision_reason':'probability_gap_below_threshold' if action=='SKIP' else 'probability_gap_pass'}

def dumps(x):
    return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)

def digest(x): return hashlib.sha256(dumps(x).encode()).hexdigest()
DEFINITION = digest({'config':CONFIG,'model':MODEL,'questions':QUESTIONS})

def number(x):
    try:
        y=float(x)
        return y if math.isfinite(y) and not isinstance(x,bool) else None
    except (ValueError,TypeError): return None

def rounded(x): return None if x is None else round(x,6)

class Tape:
    """Keep received-time trade evidence; futures event timestamps cannot leak forward."""
    def __init__(self):
        self.trades={s:deque() for s in ('spot','futures')}
        self.last_id={};self.anomalies={};self.books={};self.watermark=0
    def ingest(self,event):
        at=event['received_at'];kind=event['kind'];b=event['body']
        if isinstance(b,str): b=json.loads(b)
        self.watermark=max(self.watermark,at)
        if kind=='prediction_book':
            mid=str(b.get('market_id'))
            if at >= self.books.get(mid,{}).get('received_at',0): self.books[mid]={'received_at':at,**b}
            return
        if kind not in ('binance_spot_aggTrade','binance_futures_aggTrade'): return
        source=kind.split('_')[1]
        p,q=number(b.get('p')),number(b.get('q'));stamp=number(b.get('T'))
        if b.get('s')!='BTCUSDT' or p is None or p<=0 or q is None or q<0 or stamp is None or stamp>at or at-stamp>5000:
            self.anomalies[source]=self.anomalies.get(source,0)+1;return
        tid=int(b['a']);last=self.last_id.get(source)
        if last is not None and tid<=last: return
        gap=last is not None and tid>last+1
        self.last_id[source]=tid
        self.trades[source].append((at,p,q,-q if b.get('m') else q,int(stamp),gap))
        while self.trades[source] and self.trades[source][0][0]<at-360000: self.trades[source].popleft()
    def packet(self,market,cutoff):
        start=market['start'];ref=market['reference'];reasons=[];allstats={}
        for source,history in self.trades.items():
            rows=[r for r in history if start<=r[0]<=cutoff]
            boundaries=[start]+[r[0] for r in rows]+[cutoff]
            maxgap=max(b-a for a,b in zip(boundaries,boundaries[1:]))
            if not rows or maxgap>CONFIG['max_trade_gap_ms']: reasons.append(source+'_coverage')
            if any(r[5] for r in rows): reasons.append(source+'_trade_id_gap')
            minutes=[]
            for i in range(4):
                xs=[r for r in rows if start+i*60000<=r[0]<start+(i+1)*60000]
                minutes.append(stats(xs,ref))
            recent={str(s):stats([r for r in rows if cutoff-s*1000<=r[0]<=cutoff],ref) for s in (15,30,60)}
            allstats[source]={'minutes':minutes,'recent':recent,'max_receive_gap_ms':maxgap,
                              'last_age_ms':cutoff-rows[-1][0] if rows else None}
            if not rows or cutoff-rows[-1][0]>2000: reasons.append(source+'_stale')
        book=self.books.get(market['market_id'])
        quote=normalize_book(book,market,cutoff,allow_empty=True)
        if quote is None: reasons.append('quote_invalid_or_stale')
        if market.get('identified_at') is None or market['identified_at']>cutoff: reasons.append('identity_not_known')
        if market.get('fee_bps') is None: reasons.append('fee_unknown')
        state={'contract':{k:market[k] for k in ('topic','start','end','reference')},
               'observed_at':cutoff,'remaining_seconds':60,'features':allstats,
               'quote':quote,'stake_usdt':1.,'fee_bps_cash_sensitivity':market.get('fee_bps'),
               'settlement_note':'Official Chainlink outcome; spot and futures are proxies.'}
        return {'market':market,'cutoff':cutoff,'state':state,'reasons':sorted(set(reasons)),
                'usable':not reasons}

def stats(rows,reference):
    if not rows: return None
    ps=[r[1] for r in rows];volume=sum(r[2] for r in rows)
    signs=[1 if p>reference else -1 for p in ps if p!=reference]
    return {'open':ps[0],'high':max(ps),'low':min(ps),'close':ps[-1],
            'return_bps':rounded((ps[-1]/ps[0]-1)*10000),
            'distance_bps':rounded((ps[-1]/reference-1)*10000),
            'range_bps':rounded((max(ps)-min(ps))/reference*10000),
            'crossings':sum(a!=b for a,b in zip(signs,signs[1:])),
            'volume':rounded(volume),'signed_volume_ratio':rounded(sum(r[3] for r in rows)/volume) if volume else None,
            'aggregate_trades':len(rows)}

def normalize_book(book,market,at,allow_empty=False):
    if not book or str(book.get('market_id'))!=market['market_id']: return None
    if market['yes'] not in ('UP','DOWN'): return None
    for key in ('received_at','book_at_ms','received_at_ms'):
        stamp=number(book.get(key))
        if stamp is None or not 0<=at-stamp<=CONFIG['max_book_age_ms']: return None
    levels={}
    try:
        for side in ('bids','asks'):
            xs=[(number(p),number(n)) for p,n in book[side+'_levels']]
            if (not xs and not allow_empty) or any(p is None or n is None or not 0<p<1 or n<=0 for p,n in xs): return None
            levels[side]=sorted(xs,reverse=side=='bids')
    except (TypeError,ValueError,KeyError): return None
    if levels['bids'] and levels['asks'] and levels['bids'][0][0]>=levels['asks'][0][0]: return None
    opposite='DOWN' if market['yes']=='UP' else 'UP'
    q={'received_at':book['received_at'],'book_at_ms':book['book_at_ms']}
    q[market['yes']]={'bid':levels['bids'][0][0] if levels['bids'] else None,'ask':levels['asks'][0][0] if levels['asks'] else None,
                     'ask_levels':levels['asks']}
    noasks=sorted([(round(1-p,10),n) for p,n in levels['bids']])
    q[opposite]={'bid':round(1-levels['asks'][0][0],10) if levels['asks'] else None,'ask':noasks[0][0] if noasks else None,'ask_levels':noasks}
    return q

def model_state(packet,memory):
    # Only top levels are needed in the prompt; all depth retained for execution.
    state=json.loads(dumps(packet['state']))
    if state['quote']:
        for s in ('UP','DOWN'): state['quote'][s]['ask_levels']=state['quote'][s]['ask_levels'][:3]
    return {'current':state,'recent_settled_trades':memory,'memory_count':len(memory)}

def numeric(packet):
    if not packet['usable']: return 'SKIP'
    f=packet['state']['features'];s=f['spot']['recent']['60'];future=f['futures']['recent']['30']
    if not s or not future: return 'SKIP'
    distance=s['distance_bps'];side='UP' if distance>0 else 'DOWN';sign=1 if side=='UP' else -1
    return side if (abs(distance)>=2 and s['return_bps']*sign>0 and
        future['signed_volume_ratio'] is not None and future['signed_volume_ratio']*sign>0 and
        s['crossings']<=1 and packet['state']['quote'][side]['ask'] is not None and packet['state']['quote'][side]['ask']<=.74) else 'SKIP'

def parse_response(raw):
    if raw.get('model')!=MODEL: raise ValueError('model mismatch')
    answers=raw.get('answers',{})
    for key,question in QUESTIONS.items():
        a=answers.get(key,{})
        if a.get('type')!='choice' or a.get('choice') not in question['criteria']: raise ValueError('choice')
        ps=a.get('probabilities',{})
        if set(ps)!=set(question['criteria']) or any(number(v) is None or not 0<=v<=1 for v in ps.values()): raise ValueError('probabilities')
        if abs(sum(ps.values())-1)>1e-6 or ps[a['choice']]<max(ps.values())-1e-6: raise ValueError('distribution')
        if number(a.get('confidence')) is None or not 0<=a['confidence']<=1: raise ValueError('confidence')
    return answers

def memory_for(orders,account,cutoff):
    rows=[o for o in orders if o['account']==account and o.get('shares',0)>0 and
          o.get('known_at') is not None and o['known_at']<=cutoff]
    rows=sorted(rows,key=lambda o:(o['decision_at'],o['id']))[-10:]
    return [{k:o.get(k) for k in ('id','decision_at','known_at','side','limit','avg_price','shares',
            'outcome','gross_pnl','fee_sensitivity','net_pnl','pattern','feature_summary','direction_correct')} for o in rows]

def risk(orders,account,at):
    own=[o for o in orders if o['account']==account]
    def quarantined(o):
        ack=o.get('paper_unknown_ack')
        return (o['status']=='unknown' and o.get('shares',0)==0 and
                isinstance(ack,dict) and ack.get('scope')=='paper_only' and
                number(ack.get('at')) is not None and 0<ack['at']<=at)
    if any((o['status']=='unknown' and not quarantined(o)) or o.get('shares',0)>0 and not o.get('known_at') for o in own): return 'unresolved_or_unknown'
    rows=sorted([o for o in own if o.get('known_at') and o['known_at']<=at],key=lambda o:o['known_at'])
    eq=peak=0.;losses=0;lastloss=0
    for o in rows:
        if o.get('net_pnl') is None: return 'unknown_cost'
        pnl=o['net_pnl'];eq+=pnl;peak=max(peak,eq)
        if peak-eq>=CONFIG['mdd']: return 'mdd_latched'
        if pnl>1e-4: losses=0
        elif pnl<-1e-4: losses+=1;lastloss=o['known_at']
    if losses>=2 and at<lastloss+CONFIG['cooldown_ms']: return 'loss_cooldown'
    return None

def research_account(account):
    return account.split(':')[0] in CONFIG['research_accounts']

def admission(orders,account,at):
    """Paper only: isolate evidence gaps; research lanes observe despite financial stops."""
    rows=list(orders)
    if research_account(account): return None
    rows=[o for o in rows if not (o['status']=='unknown' and o.get('shares',0)==0 and
                                  o.get('expires') is not None and o['expires']<at)]
    return risk(rows,account,at)

def make_order(packet,account,action,completed,pattern=None):
    cutoff=packet['cutoff'];delay=int(account.split(':')[1]);s=packet['state']
    return {'id':str(packet['market']['topic'])+':'+account,'account':account,
            'topic':packet['market']['topic'],'decision_at':cutoff,'ready':completed+delay,
            'expires':min(completed+delay+12000,packet['market']['end']-1),
            'side':action,'limit':s['quote'][action]['ask'],'stake':1.,'status':'open',
            'spent':0.,'shares':0.,'pattern':pattern,'fee_bps':packet['market']['fee_bps'],
            'feature_summary':{'spot60':s['features']['spot']['recent']['60'],
                               'futures30':s['features']['futures']['recent']['30']},
            'seen':0,'valid_observations':[]}

def advance(order,book,market,at):
    if order['status']!='open': return
    if at>order['expires']:
        stamps=[order['ready']]+order['valid_observations']+[order['expires']]
        gap=max(b-a for a,b in zip(stamps,stamps[1:]))
        covered=gap<=2000
        order['max_observation_gap_ms']=gap
        order['execution_reason']='covered_no_fill' if covered else 'book_coverage_gap'
        order['status']='expired' if covered else 'unknown';return
    if at<order['ready']: return
    q=normalize_book(book,market,at,allow_empty=True)
    if q is None or q['book_at_ms']<order['ready'] or q['received_at']<order['ready'] or q['book_at_ms']<=order['seen']: return
    order['seen']=q['book_at_ms'];order['valid_observations'].append(at)
    remain=order['stake'];spent=shares=0.
    for p,n in q[order['side']]['ask_levels']:
        if p>order['limit']+1e-10: break
        take=min(n,remain/p);spent+=take*p;shares+=take;remain-=take*p
        if remain<1e-9: break
    if shares:
        order.update(spent=spent,shares=shares,avg_price=spent/shares,filled_at=at,
                     status='filled' if remain<1e-9 else 'partial')
        # A single depth snapshot; partial remainder is never assumed filled later.

def settle(order,outcome,known_at):
    if not order.get('shares') or order.get('known_at') or outcome not in ('UP','DOWN','TIE'): return
    payout=.5 if outcome=='TIE' else float(outcome==order['side'])
    gross=order['shares']*payout-order['spent']
    fee=order['spent']*order['fee_bps']/10000 if order.get('fee_bps') is not None else None
    order.update(outcome=outcome,known_at=known_at,gross_pnl=gross,fee_sensitivity=fee,
                 net_pnl=gross-fee if fee is not None else None,
                 direction_correct=None if outcome=='TIE' else outcome==order['side'])
