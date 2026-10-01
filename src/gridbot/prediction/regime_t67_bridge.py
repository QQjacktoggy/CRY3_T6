"""T6.7 signal adapter. Admission and submission stay in the existing ledger."""
import json
import sqlite3
from contextlib import closing

from .regime_t67_policy import FINGERPRINT, POLICY
from .regime_t67_evidence import read_inputs
from .regime_t67_lane import candidates, execution


def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    if unit_usdt not in (1, 2, 3) or not start+60000 <= at_ms < start+270000:
        return b.C180Ready(False, 't67_unit_or_execution_window')
    try:
        books, spots = read_inputs(bridge.signal_db, start, at_ms)
        if not books:
            return b.C180Ready(False, 't67_evidence_missing')
        snapshot = books[-1]
        stamp = bridge._book(snapshot, market, at_ms)
        if at_ms-stamp > 1000 or stamp <= last_seen_book_at_ms:
            return b.C180Ready(False, 't67_fresh_book_required')
        if (b.dec(snapshot['reference']) != b.dec(market.reference_price)
                or not 0 < int(snapshot['reference_received_ms']) <= at_ms):
            return b.C180Ready(False, 't67_reference_identity_invalid')
        # No observation may carry a future metadata/spot source timestamp.
        with closing(b.connect(bridge.feature_db)) as db:
            db.execute('CREATE TABLE IF NOT EXISTS t67_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
            db.commit()
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT payload FROM t67_decisions WHERE start=?', (start,)).fetchone()
            state = json.loads(row[0]) if row else dict(
                fingerprint=FINGERPRINT, market_topic=str(market.market_topic_id), market_id=str(market.up_market_id),
                unit_usdt=str(unit_usdt), fee_bps=str(snapshot['fee_bps']), selected=False)
            if (state['fingerprint'] != FINGERPRINT or state['market_topic'] != str(market.market_topic_id)
                    or state['market_id'] != str(market.up_market_id) or state['unit_usdt'] != str(unit_usdt)
                    or b.dec(state['fee_bps']) != b.dec(snapshot['fee_bps'])):
                db.rollback()
                return b.C180Ready(False, 't67_frozen_identity_unit_fee_mismatch')
            if not state['selected']:
                row = db.execute('SELECT payload FROM features WHERE start=?', (start,)).fetchone()
                features = json.loads(row[0]) if row else None
                if features and (features.get('fingerprint') != b.FINGERPRINT
                        or features.get('market_start_ms') != start or features.get('cutoff_ms') != start+120000
                        or not start+120000 <= int(features.get('received_at_ms', 0)) <= min(at_ms, start+123000)):
                    features = None
                choices = candidates(snapshot, spots, features, state, at_ms, unit_usdt, books[:-1])
                if choices:
                    chosen = choices[0]
                    state.update(chosen, selected=True, selected_at_ms=at_ms,
                                 expires_at_ms=min(start+270000, at_ms+POLICY['quote_ttl_ms']))
                state['last_evaluated_ms'] = at_ms
                db.execute('INSERT INTO t67_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                           (start, json.dumps(state, sort_keys=True, allow_nan=False)))
            db.commit()
        if not state['selected']:
            return b.C180Ready(False, 't67_no_eligible_live_candidate')
        if at_ms >= state['expires_at_ms']:
            return b.C180Ready(False, 't67_selected_quote_expired')
        ex = execution(snapshot, state['side'], unit_usdt, state['probability'], cap=state['cap'])
        entry = b.C180EntryDecision(state['side'], 'regime_entry', state['side'], unit_usdt, ex['net_shares'], None)
        signal = b.C180Signal(start, market.market_topic_id, market.up_market_id, state['selected_at_ms'],
                             state['selected_at_ms'], 'regime_frozen_entry', entry,
                             b.dec(state['probability']) if state['probability'] is not None else None,
                             FINGERPRINT, None, b.dec(state['fee_bps']))
        recheck = b.C180ExecutionRecheck(True, 't67_ready', at_ms, state['expires_at_ms'],
                                      b.dec(state['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't67_ready:'+state['branch'], signal, recheck, stamp)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        return b.C180Ready(False, 't67_inputs_unavailable:'+type(exc).__name__)
