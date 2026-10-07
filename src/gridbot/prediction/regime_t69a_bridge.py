"""T6.9a durable core-first adapter: T6.7c seven lanes plus the First UP 5bp floor.

No 180s Reference backfill. No order or risk writes outside the shared worker.
"""
import json
import sqlite3
from contextlib import closing

from .regime_lane import dec
from .regime_t63_lane import eligible_execution
from .regime_t67_lane import execution as new_execution
from .regime_t69a_policy import FINGERPRINT, POLICY

from .regime_t67a_bridge import freeze_core, additions


# Missing/stale local books inside the initial window are retried by the worker.
# Same set and same classification as T6.7c (regime_t67c_bridge).
TRANSIENT_BOOK_REASONS = frozenset({
    't69a_book_missing', 't69a_book_receipt_stale',
    't69a_book_source_stale', 't69a_fresh_book_required',
})


def _book_refusal(exc, snapshot, at_ms):
    """Name stale/future book clocks like T6.7c; None for any other error."""
    # Exception text is matched locally against constants; never emitted.
    message = str(exc) if isinstance(exc, ValueError) else ''
    if message not in ('book receipt stale or future', 'book stale or future'):
        return None
    try:
        book_at = int(snapshot['book_at_ms'])
        if message == 'book stale or future':
            return 't69a_book_source_future' if book_at > at_ms else 't69a_book_source_stale'
        clocks = [int(snapshot[k]) for k in ('received_at', 'received_at_ms', 'captured_at_ms')]
        future = any(t > at_ms or t < book_at for t in clocks)
        return 't69a_book_receipt_future' if future else 't69a_book_receipt_stale'
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return 't69a_book_clock_invalid'


def _additions(snapshot, features, unit):
    """T6.7c additions, with C-UP mirror held to the T6.9a price band (cap .70).

    A market where C-UP mirror matched the T6.7c band but fails the .70 cap is
    skipped outright, so no other addition takes the same entry instead.
    """
    lower, cap = POLICY['c_mirror_up_prior']['price_band']
    choices = []
    for candidate in additions(snapshot, features, unit):
        if candidate['branch'] == 'c_mirror_up_prior':
            try:
                new_execution(snapshot, 'UP', unit, lower=lower, cap=cap)
            except (ValueError, KeyError, TypeError, ArithmeticError):
                return []
            candidate = dict(candidate, lower=lower, cap=cap, upper=cap)
        choices.append(candidate)
    return choices


def shallow_prior_against(side, prior_bp):
    """True when the prior 15m move ran against ``side`` by at least the T6.9a floor."""
    floor = dec(POLICY['shallow_retracement']['prior_against_min_bp'])
    prior = dec(prior_bp)
    if side == 'UP':
        return prior <= -floor
    if side == 'DOWN':
        return prior >= floor
    raise ValueError('shallow side invalid')


class _FeaturesMissing(ValueError):
    pass


def _decision_payload(raw):
    d = json.loads(raw)
    if not isinstance(d, dict):
        raise ValueError('decision_payload_invalid')
    return d


def _selected_signal(d):
    from . import regime_worker_bridge as b
    raw = d['signal']
    value = json.loads(raw)
    if (not isinstance(value, dict)
            or (value.get('entry') is not None and not isinstance(value['entry'], dict))):
        raise ValueError('decision_payload_invalid')
    return b._from_signal_json(raw)


def _check_early(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    if unit_usdt not in (1, 2, 3) or not start+124000 <= at_ms < start+136000:
        return b.C180Ready(False, 't69a_unit_or_execution_window')
    phase = 'book'
    snapshot = None
    try:
        snapshot = b.read_c180_book(bridge.signal_db, start)
        if snapshot is None:
            return b.C180Ready(False, 't69a_book_missing')
        stamp = bridge._book(snapshot, market, at_ms)
        if at_ms-stamp > 1000:
            return b.C180Ready(False, 't69a_fresh_book_required')
        if stamp <= last_seen_book_at_ms:
            return b.C180Ready(False, 'quote_not_new_after_ready')
        phase = 'frozen_decision'
        # Frozen selected decisions need no writer lock or schema/commit work.
        # Unselected callers still re-read under BEGIN IMMEDIATE before selecting.
        with closing(sqlite3.connect(bridge.feature_db.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as reader:
            reader.execute('PRAGMA query_only=ON')
            exists = reader.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t69a_decisions'").fetchone()
            row = reader.execute('SELECT payload FROM t69a_decisions WHERE start=?', (start,)).fetchone() if exists else None
            frozen = _decision_payload(row[0]) if row else None
        if frozen and frozen.get('selected') is True:
            d = frozen
            _identity(d, bridge, market, unit_usdt, snapshot)
            guard = d['core_guard']
        else:
            d = _persist_selection(bridge, market, unit_usdt, at_ms, snapshot)
            guard = d['core_guard']
        if not isinstance(guard, dict):
            raise ValueError('decision_payload_invalid')
        if not d['selected']:
            if d.get('rejected_branches'):
                return b.C180Ready(False, 't69a_'+d['rejected_branches'][0]['reason'])
            return b.C180Ready(False, 't69a_no_live_candidate:'+guard['reason'])
        if at_ms < d['selected_at_ms']:
            return b.C180Ready(False, 't69a_frozen_selection_future')
        if at_ms >= d['expires_at_ms']:
            return b.C180Ready(False, 't69a_selected_quote_expired')
        if not d.get('signal'):
            return b.C180Ready(False, 't69a_frozen_signal_missing')
        signal = _selected_signal(d)
        if (signal.market_start_ms != start or signal.market_topic != market.market_topic_id
                or signal.market_id != market.up_market_id or signal.cutoff_ms != start+120000
                or signal.completed_at_ms != d['selected_at_ms'] or not signal.is_entry
                or signal.entry.side != d['side'] or signal.entry.action != d['side']
                or signal.entry.stake_usdt != unit_usdt or signal.frozen_fee_bps != dec(d['fee_bps'])
                or signal.original_input_sha256 != FINGERPRINT):
            return b.C180Ready(False, 't69a_frozen_signal_identity_unit_fee_mismatch')
        phase = 'execution'
        if not isinstance(d['branch'], str):
            raise ValueError('decision_payload_invalid')
        is_core = d['branch'].startswith('core_')
        ex = (eligible_execution(d, snapshot, unit_usdt) if is_core else
              new_execution(snapshot, d['side'], unit_usdt, lower=d['lower'], cap=d['cap']))
        recheck = b.C180ExecutionRecheck(True, 't69a_ready', at_ms, d['expires_at_ms'],
                                      dec(d['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't69a_ready:'+d['branch'], signal, recheck, stamp)
    except _FeaturesMissing:
        return b.C180Ready(False, 't69a_features_missing')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        reason = _book_refusal(exc, snapshot, at_ms) if phase == 'book' else None
        return b.C180Ready(False, reason or 't69a_inputs_unavailable:'+phase+':'+_reason_code(exc))


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
        db.execute('CREATE TABLE IF NOT EXISTS t69a_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        db.commit()
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT payload FROM t69a_decisions WHERE start=?', (start,)).fetchone()
        d = _decision_payload(row[0]) if row else dict(
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
                features = json.loads(row[0])
                if not isinstance(features, dict):
                    raise ValueError('feature_provenance')
                d['core_guard'] = freeze_core(bridge, market, features, at_ms, unit_usdt)
                if dec(d['core_guard']['fee_bps']) != dec(d['fee_bps']):
                    raise ValueError('initial_fee_changed')
        guard = d['core_guard']
        if not isinstance(guard, dict):
            raise ValueError('decision_payload_invalid')
        if not d['selected'] and guard.get('verified') is True and at_ms <= start+134500:
            choices = _additions(snapshot, guard['features'], unit_usdt) if guard['empty'] else guard['candidates']
            prior = guard['features']['prior_bp']
            blocked = [c for c in choices if c['branch'] == 'core_first_up'
                       and dec(prior) < dec(POLICY['first_up_prior_min_bp'])]
            d['rejected_branches'] = [dict(branch=c['branch'], reason='first_up_prior_below_5bp',
                                           prior_bp=str(prior)) for c in blocked]
            # Shallow retracement only when the prior 15m ran against the bet by >=5bp.
            weak_shallow = [c for c in choices if c['branch'] == 'shallow_retracement'
                            and not shallow_prior_against(c['side'], prior)]
            d['rejected_branches'] += [dict(branch=c['branch'], reason='shallow_prior_not_against_5bp',
                                            prior_bp=str(prior), side=c['side']) for c in weak_shallow]
            blocked += weak_shallow
            choices = [c for c in choices if c not in blocked]
            d['eligible_branches'] = [c['branch'] for c in choices]
            for candidate in choices:
                try:
                    ex = (new_execution(snapshot, candidate['side'], unit_usdt, lower=candidate['lower'], cap=candidate['cap'])
                          if guard['empty'] else eligible_execution(candidate, snapshot, unit_usdt))
                except (ValueError, KeyError, TypeError, ArithmeticError):
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
        db.execute('INSERT INTO t69a_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(d, sort_keys=True, allow_nan=False)))
        db.commit()
        return d


def _reason_code(exc):
    """Use a fixed diagnostic vocabulary; exception payloads stay private."""
    if isinstance(exc, sqlite3.Error):
        return 'decision_storage_unavailable'
    if isinstance(exc, OSError):
        return 'input_storage_unavailable'
    if isinstance(exc, KeyError):
        return 'input_field_missing'
    if isinstance(exc, TypeError):
        return 'input_type_invalid'
    known = {
        'candidate_price_band': 'price_band',
        'candidate_ev': 'original_ev',
        'best ask below price band': 'price_below_lower',
        'price band': 'price_band',
        'fee net EV': 'model_ev',
        'insufficient requested depth': 'insufficient_depth',
        'invalid ask depth': 'invalid_depth',
        'unsorted ask depth': 'unsorted_depth',
        'invalid fee': 'invalid_fee',
        'nonfinite input': 'nonfinite_input',
        'book identity/depth missing': 'book_identity_or_depth',
        'book receipt stale or future': 'book_receipt_clock',
        'book stale or future': 'book_clock',
        'frozen_identity_unit_fee_mismatch': 'frozen_identity_unit_fee',
        'initial_fee_changed': 'initial_fee_changed',
        'initial_book_missing': 'initial_book_missing',
        'initial_book_clock': 'initial_book_clock',
        'feature_provenance': 'feature_provenance',
        'original_identity': 'original_identity',
        'original_missing_or_invalid_for_empty_core': 'original_missing_or_invalid',
        'decision_payload_invalid': 'decision_payload_invalid',
    }
    return known.get(str(exc), 'input_value_invalid')


def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    """Only the seven T6.7c lanes, selected inside the initial entry window."""
    return _check_early(bridge, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                        last_seen_book_at_ms=last_seen_book_at_ms)
