"""Durable core-first adapter; no order or risk writes outside shared worker."""
import json
import sqlite3
from contextlib import closing

from .regime_lane import dec
from .regime_t63_lane import eligible_execution
from .regime_t67_lane import execution as new_execution
from .regime_t67c_policy import FINGERPRINT, POLICY

from .regime_t67a_bridge import freeze_core, additions


class _FeaturesMissing(ValueError):
    pass


# Only missing/stale local data can be polled again. Identity, future clocks,
# price, depth and EV failures must remain explicit, fail-closed refusals.
TRANSIENT_BOOK_REASONS = frozenset({
    't67c_book_missing', 't67c_book_receipt_stale',
    't67c_book_source_stale', 't67c_fresh_book_required',
})


def _refusal(exc, snapshot, at_ms):
    known = {
        'book identity/depth missing': 't67c_book_identity_or_depth_invalid',
        'frozen_identity_unit_fee_mismatch': 't67c_frozen_identity_unit_fee_mismatch',
        'initial_fee_changed': 't67c_initial_fee_changed',
        'candidate_price_band': 't67c_execution_price_band',
        'best ask below price band': 't67c_execution_price_band',
        'price band': 't67c_execution_price_band',
        'insufficient requested depth': 't67c_execution_insufficient_depth',
        'candidate_ev': 't67c_execution_ev',
        'fee net EV': 't67c_execution_ev',
        'invalid ask depth': 't67c_execution_depth_invalid',
        'unsorted ask depth': 't67c_execution_depth_invalid',
        'invalid fee': 't67c_execution_fee_invalid',
        'nonfinite input': 't67c_nonfinite_input',
    }
    # Exception text is matched locally against constants; never emitted.
    message = str(exc) if isinstance(exc, ValueError) else ''
    if message in ('book receipt stale or future', 'book stale or future'):
        try:
            book_at = int(snapshot['book_at_ms'])
            if message == 'book stale or future':
                return 't67c_book_source_future' if book_at > at_ms else 't67c_book_source_stale'
            clocks = [int(snapshot[k]) for k in ('received_at', 'received_at_ms', 'captured_at_ms')]
            future = any(t > at_ms or t < book_at for t in clocks)
            return 't67c_book_receipt_future' if future else 't67c_book_receipt_stale'
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return 't67c_book_clock_invalid'
    return known.get(message, 't67c_inputs_unavailable:'+type(exc).__name__)


def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    if unit_usdt not in (1, 2, 3) or not start+124000 <= at_ms < start+136000:
        return b.C180Ready(False, 't67c_unit_or_execution_window')
    snapshot = None
    try:
        snapshot = b.read_c180_book(bridge.signal_db, start)
        if snapshot is None:
            return b.C180Ready(False, 't67c_book_missing')
        stamp = bridge._book(snapshot, market, at_ms)
        if at_ms-stamp > 1000:
            return b.C180Ready(False, 't67c_fresh_book_required')
        if stamp <= last_seen_book_at_ms:
            return b.C180Ready(False, 'quote_not_new_after_ready')
        # Frozen selected decisions need no writer lock or schema/commit work.
        # Unselected callers still re-read under BEGIN IMMEDIATE before selecting.
        with closing(sqlite3.connect(bridge.feature_db.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as reader:
            reader.execute('PRAGMA query_only=ON')
            exists = reader.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t67c_decisions'").fetchone()
            row = reader.execute('SELECT payload FROM t67c_decisions WHERE start=?', (start,)).fetchone() if exists else None
            frozen = json.loads(row[0]) if row else None
        if frozen and frozen.get('selected') is True:
            d = frozen
            _identity(d, bridge, market, unit_usdt, snapshot)
            guard = d['core_guard']
        else:
            d = _persist_selection(bridge, market, unit_usdt, at_ms, snapshot)
            guard = d['core_guard']
        if not d['selected']:
            if d.get('execution_denials'):
                return b.C180Ready(False, d['execution_denials'][0])
            return b.C180Ready(False, 't67c_no_live_candidate:'+guard['reason'])
        if at_ms < d['selected_at_ms']:
            return b.C180Ready(False, 't67c_frozen_selection_future')
        if at_ms >= d['expires_at_ms']:
            return b.C180Ready(False, 't67c_selected_quote_expired')
        if not d.get('signal'):
            return b.C180Ready(False, 't67c_frozen_signal_missing')
        signal = b._from_signal_json(d['signal'])
        if (signal.market_start_ms != start or signal.market_topic != market.market_topic_id
                or signal.market_id != market.up_market_id or signal.cutoff_ms != start+120000
                or signal.completed_at_ms != d['selected_at_ms'] or not signal.is_entry
                or signal.entry.side != d['side'] or signal.entry.action != d['side']
                or signal.entry.stake_usdt != unit_usdt or signal.frozen_fee_bps != dec(d['fee_bps'])
                or signal.original_input_sha256 != FINGERPRINT):
            return b.C180Ready(False, 't67c_frozen_signal_identity_unit_fee_mismatch')
        is_core = d['branch'].startswith('core_')
        ex = (eligible_execution(d, snapshot, unit_usdt) if is_core else
              new_execution(snapshot, d['side'], unit_usdt, lower=d['lower'], cap=d['cap']))
        recheck = b.C180ExecutionRecheck(True, 't67c_ready', at_ms, d['expires_at_ms'],
                                      dec(d['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't67c_ready:'+d['branch'], signal, recheck, stamp)
    except _FeaturesMissing:
        return b.C180Ready(False, 't67c_features_missing')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        return b.C180Ready(False, _refusal(exc, snapshot, at_ms))


def _identity(d, bridge, market, unit, snapshot):
    start = int(market.start_time_ms)
    if (d['fingerprint'] != FINGERPRINT or d['market_topic'] != str(market.market_topic_id)
            or d['market_id'] != str(market.up_market_id) or d['unit_usdt'] != str(unit)
            or d['market_start_ms'] != start or d['market_end_ms'] != start+300000
            or dec(d['fee_bps']) != dec(snapshot['fee_bps'])
            or d.get('loop_id') != getattr(bridge, '_registered_loop_id', None)):
        raise ValueError('frozen_identity_unit_fee_mismatch')


def _persist_selection(bridge, market, unit_usdt, at_ms, snapshot):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    with closing(b.connect(bridge.feature_db)) as db:
        db.execute('CREATE TABLE IF NOT EXISTS t67c_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        db.commit()
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT payload FROM t67c_decisions WHERE start=?', (start,)).fetchone()
        d = json.loads(row[0]) if row else dict(
            fingerprint=FINGERPRINT, market_topic=str(market.market_topic_id), market_id=str(market.up_market_id),
            market_start_ms=start, market_end_ms=start+300000, unit_usdt=str(unit_usdt),
            selected=False, fee_bps=str(snapshot['fee_bps']),
            loop_id=getattr(bridge, '_registered_loop_id', None))
        _identity(d, bridge, market, unit_usdt, snapshot)
        if 'core_guard' not in d:
            if at_ms > start+126000:
                d['core_guard'] = dict(verified=False, empty=False, reason='initial_window_missed')
            else:
                row = db.execute('SELECT payload FROM features WHERE start=?', (start,)).fetchone()
                if row is None:
                    db.rollback()
                    raise _FeaturesMissing()
                d['core_guard'] = freeze_core(bridge, market, json.loads(row[0]), at_ms, unit_usdt)
                if dec(d['core_guard']['fee_bps']) != dec(d['fee_bps']):
                    raise ValueError('initial_fee_changed')
        guard = d['core_guard']
        if not d['selected'] and guard.get('verified') and at_ms <= start+134500:
            choices = additions(snapshot, guard['features'], unit_usdt) if guard['empty'] else guard['candidates']
            d['eligible_branches'] = [c['branch'] for c in choices]
            d['execution_denials'] = []
            for candidate in choices:
                try:
                    ex = (new_execution(snapshot, candidate['side'], unit_usdt, lower=candidate['lower'], cap=candidate['cap'])
                          if guard['empty'] else eligible_execution(candidate, snapshot, unit_usdt))
                except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    d['execution_denials'].append(_refusal(exc, snapshot, at_ms))
                    continue
                d.update(candidate, selected=True, selected_at_ms=at_ms,
                         expires_at_ms=start+136000 if not guard['empty'] else min(start+136000, at_ms+POLICY['quote_ttl_ms']))
                p = dec(d['probability']) if d.get('action') == 'original' else None
                if p is not None and d['side'] == 'DOWN':
                    p = 1-p
                entry = b.C180EntryDecision(d['side'], 'regime_entry', d['side'], unit_usdt,
                                            ex['net_shares'], None)
                signal = b.C180Signal(start, market.market_topic_id, market.up_market_id, start+120000,
                                     at_ms, 'regime_frozen_entry', entry, p, FINGERPRINT, None, dec(d['fee_bps']))
                d['signal'] = b._signal_json(signal)
                break
        d['last_evaluated_ms'] = at_ms
        db.execute('INSERT INTO t67c_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(d, sort_keys=True, allow_nan=False)))
        db.commit()
        return d
