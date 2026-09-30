"""T6.3b adapter: T6/A/C live with frozen B and fallback observations."""
import json
import sqlite3
from contextlib import closing
from .regime_t63b_lane import FINGERPRINT, candidates
from .regime_t63_lane import eligible_execution

def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    from . import regime_worker_bridge as b
    start=int(market.start_time_ms)
    if unit_usdt not in (1,2,3) or not start+124000<=at_ms<start+136000:
        return b.C180Ready(False,'regime_unit_or_execution_window')
    try:
        with closing(b.connect(bridge.feature_db)) as db:
            row=db.execute('SELECT payload FROM decisions WHERE start=?',(start,)).fetchone()
            if row:
                d=json.loads(row[0])
            else:
                if at_ms>start+126000: return b.C180Ready(False,'regime_initial_decision_window_missed')
                row=db.execute('SELECT payload FROM features WHERE start=?',(start,)).fetchone()
                if not row: return b.C180Ready(False,'regime_features_missing_skip')
                f=json.loads(row[0])
                if (f.get('fingerprint')!=b.FINGERPRINT or f.get('market_start_ms')!=start
                    or f.get('cutoff_ms')!=start+120000 or not start+120000<=int(f['received_at_ms'])<=start+123000):
                    return b.C180Ready(False,'regime_feature_provenance_invalid')
                original=b.read_c180_signal(bridge.signal_db,start)
                if original and (original.market_topic!=market.market_topic_id or original.market_id!=market.up_market_id):
                    return b.C180Ready(False,'original_market_identity_mismatch')
                initial=bridge._first_book(market,at_ms)
                if initial is None: return b.C180Ready(False,'regime_initial_book_missing_skip')
                choices,shadow,shadow_b=candidates(f,json.loads(b._signal_json(original)) if original else None,initial,unit_usdt)
                if shadow is not None:
                    shadow['quoted_at_ms']=at_ms
                if shadow_b is not None:
                    shadow_b['quoted_at_ms']=at_ms
                d=dict(fingerprint=FINGERPRINT,market_topic=market.market_topic_id,market_id=market.up_market_id,
                       unit_usdt=str(unit_usdt),decided_at_ms=at_ms,candidates=choices,allowed=bool(choices),
                       shadow_fallback=shadow,shadow_b=shadow_b,fee_bps=str(initial['fee_bps']),selected=False,
                       reason=('t63b_pending_depth' if choices else
                               't63b_shadow_only' if (shadow or shadow_b) else 't63b_no_candidate'))
                with db: db.execute('INSERT OR IGNORE INTO decisions VALUES(?,?)',(start,json.dumps(d,sort_keys=True)))
                d=json.loads(db.execute('SELECT payload FROM decisions WHERE start=?',(start,)).fetchone()[0])
            if (d.get('fingerprint')!=FINGERPRINT or d.get('market_topic')!=market.market_topic_id
                or d.get('market_id')!=market.up_market_id or d.get('unit_usdt')!=str(unit_usdt)):
                return b.C180Ready(False,'regime_decision_identity_mismatch')
            if not d['allowed']: return b.C180Ready(False,'regime_frozen_skip:'+d['reason'])
            snap=b.read_c180_book(bridge.signal_db,start)
            book_at=bridge._book(snap,market,at_ms)
            if at_ms-book_at>1000: return b.C180Ready(False,'t63b_book_older_than_1s')
            if book_at<=last_seen_book_at_ms: return b.C180Ready(False,'quote_not_new_after_ready')
            if b.dec(snap['fee_bps'])!=b.dec(d['fee_bps']): return b.C180Ready(False,'regime_fee_changed')
            if not d['selected']:
                if at_ms>start+134500: return b.C180Ready(False,'t63b_selection_deadline')
                chosen=None
                for candidate in d['candidates']:
                    try: execution=eligible_execution(candidate,snap,unit_usdt)
                    except (ValueError,KeyError,TypeError,ArithmeticError): continue
                    chosen=candidate;break
                if chosen is None: return b.C180Ready(False,'t63b_candidate_depth_or_price')
                d.update(chosen);d.update(selected=True,selected_at_ms=at_ms,limit=chosen['cap'])
                entry=b.C180EntryDecision(d['side'],'regime_entry',d['side'],unit_usdt,execution['net_shares'],None)
                p=b.dec(d['probability']) if d['action']=='original' else None
                if p is not None and d['side']=='DOWN': p=1-p
                signal=b.C180Signal(start,market.market_topic_id,market.up_market_id,start+120000,at_ms,
                    'regime_frozen_entry',entry,p,FINGERPRINT,None,b.dec(snap['fee_bps']))
                d['signal']=b._signal_json(signal)
                with db: db.execute('UPDATE decisions SET payload=? WHERE start=?',(json.dumps(d,sort_keys=True),start))
            signal=b._from_signal_json(d['signal'])
            execution=eligible_execution(d,snap,unit_usdt)
            recheck=b.C180ExecutionRecheck(True,'regime_ready',at_ms,start+136000,b.dec(d['limit']),
                                          execution['cash'],execution['net_shares'],None)
            return b.C180Ready(True,'t63b_ready:'+d['branch'],signal,recheck,book_at)
    except (OSError,sqlite3.Error,ValueError,KeyError,TypeError,ArithmeticError) as exc:
        return b.C180Ready(False,'t63b_inputs_unavailable:'+type(exc).__name__)
