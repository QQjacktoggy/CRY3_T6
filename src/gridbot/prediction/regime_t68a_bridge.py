"""Durable core-first adapter; no order or risk writes outside shared worker."""
import json
import sqlite3
from contextlib import closing

from .regime_lane import dec
from .regime_t63_lane import eligible_execution
from .regime_t67_lane import execution as new_execution
from .regime_t67_evidence import read_inputs
from .regime_t68a_reference import evaluate_checkpoint, core_empty_reason, _book_clock, _backfill_gate
from .evidence_retention import bounded_payload
from .regime_t68a_policy import FINGERPRINT, POLICY

from .regime_t67a_bridge import freeze_core, additions


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
        return b.C180Ready(False, 't68a_unit_or_execution_window')
    phase = 'book'
    try:
        snapshot = b.read_c180_book(bridge.signal_db, start)
        stamp = bridge._book(snapshot, market, at_ms)
        if at_ms-stamp > 1000:
            return b.C180Ready(False, 't68a_fresh_book_required')
        if stamp <= last_seen_book_at_ms:
            return b.C180Ready(False, 'quote_not_new_after_ready')
        phase = 'frozen_decision'
        # Frozen selected decisions need no writer lock or schema/commit work.
        # Unselected callers still re-read under BEGIN IMMEDIATE before selecting.
        with closing(sqlite3.connect(bridge.feature_db.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as reader:
            reader.execute('PRAGMA query_only=ON')
            exists = reader.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t68a_decisions'").fetchone()
            row = reader.execute('SELECT payload FROM t68a_decisions WHERE start=?', (start,)).fetchone() if exists else None
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
            if guard.get('verified') is True and guard.get('empty') is True:
                return b.C180Ready(False, 't68a_wait_reference_checkpoint')
            if d.get('rejected_branches'):
                return b.C180Ready(False, 't68a_first_up_prior_below_5bp')
            return b.C180Ready(False, 't68a_no_live_candidate:'+guard['reason'])
        if at_ms < d['selected_at_ms']:
            return b.C180Ready(False, 't68a_frozen_selection_future')
        if at_ms >= d['expires_at_ms']:
            return b.C180Ready(False, 't68a_selected_quote_expired')
        if not d.get('signal'):
            return b.C180Ready(False, 't68a_frozen_signal_missing')
        signal = _selected_signal(d)
        if (signal.market_start_ms != start or signal.market_topic != market.market_topic_id
                or signal.market_id != market.up_market_id or signal.cutoff_ms != start+120000
                or signal.completed_at_ms != d['selected_at_ms'] or not signal.is_entry
                or signal.entry.side != d['side'] or signal.entry.action != d['side']
                or signal.entry.stake_usdt != unit_usdt or signal.frozen_fee_bps != dec(d['fee_bps'])
                or signal.original_input_sha256 != FINGERPRINT):
            return b.C180Ready(False, 't68a_frozen_signal_identity_unit_fee_mismatch')
        phase = 'execution'
        if not isinstance(d['branch'], str):
            raise ValueError('decision_payload_invalid')
        is_core = d['branch'].startswith('core_')
        ex = (eligible_execution(d, snapshot, unit_usdt) if is_core else
              new_execution(snapshot, d['side'], unit_usdt, lower=d['lower'], cap=d['cap']))
        recheck = b.C180ExecutionRecheck(True, 't68a_ready', at_ms, d['expires_at_ms'],
                                      dec(d['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't68a_ready:'+d['branch'], signal, recheck, stamp)
    except _FeaturesMissing:
        return b.C180Ready(False, 't68a_features_missing')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        return b.C180Ready(False, 't68a_inputs_unavailable:'+phase+':'+_reason_code(exc))


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
        db.execute('CREATE TABLE IF NOT EXISTS t68a_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        db.commit()
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT payload FROM t68a_decisions WHERE start=?', (start,)).fetchone()
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
            choices = additions(snapshot, guard['features'], unit_usdt) if guard['empty'] else guard['candidates']
            blocked = [c for c in choices if c['branch'] == 'core_first_up'
                       and dec(guard['features']['prior_bp']) < dec(POLICY['first_up_prior_min_bp'])]
            d['rejected_branches'] = [dict(branch=c['branch'], reason='first_up_prior_below_5bp',
                                           prior_bp=str(guard['features']['prior_bp'])) for c in blocked]
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
        db.execute('INSERT INTO t68a_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
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
        'reference_book_receipt_invalid': 'book_receipt_clock',
        'reference_book_stale': 'book_clock',
        'reference_anchor_invalid': 'reference_anchor',
        'reference_refresh_cost_band': 'reference_cost_band',
        'reference_depth_invalid': 'invalid_depth',
        'decision_payload_invalid': 'decision_payload_invalid',
    }
    return known.get(str(exc), 'input_value_invalid')


def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    """Keep the seven original lanes, then consider one sticky late candidate."""
    start = int(market.start_time_ms)
    if start+124000 <= at_ms < start+136000:
        return _check_early(bridge, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                            last_seen_book_at_ms=last_seen_book_at_ms)
    if unit_usdt not in (1, 2, 3) or not start+180000 <= at_ms < start+183500:
        from .regime_worker_bridge import C180Ready
        return C180Ready(False, 't68a_unit_or_execution_window')
    return _check_reference(bridge, market, unit_usdt, at_ms, last_seen_book_at_ms)


def _frozen(bridge, start):
    with closing(sqlite3.connect(bridge.feature_db.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='t68a_decisions' AND type='table'").fetchone():
            return None
        row = db.execute('SELECT payload FROM t68a_decisions WHERE start=?', (start,)).fetchone()
        return _decision_payload(row[0]) if row else None


def _late_identity(bridge, market, unit, fee):
    start = int(market.start_time_ms)
    return dict(fingerprint=FINGERPRINT, market_topic=str(market.market_topic_id),
                market_id=str(market.up_market_id), market_start_ms=start, market_end_ms=start+300000,
                unit_usdt=str(unit), fee_bps=fee, loop_id=getattr(bridge, '_registered_loop_id', None))


def _exposure(bridge, identity, at_ms):
    """Consistent read-only guard; the common atomic claim rechecks the race."""
    from pathlib import Path
    path = getattr(bridge.repository, 'db_path', None)
    if path is None:
        return None
    try:
        with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            start = identity['market_start_ms']
            return dict(identity, observed_at_ms=at_ms,
                has_claim=bool(db.execute('SELECT 1 FROM prediction_regime_entry_claims WHERE market_start_ms=? LIMIT 1', (start,)).fetchone()),
                has_intent=bool(db.execute("SELECT 1 FROM prediction_order_intents i JOIN prediction_campaigns c ON c.campaign_id=i.campaign_id WHERE c.start_time_ms=? AND i.order_side='BUY' LIMIT 1", (start,)).fetchone()),
                has_position=bool(db.execute('SELECT 1 FROM prediction_campaigns WHERE start_time_ms=? AND buy_count>0 LIMIT 1', (start,)).fetchone()),
                has_unknown=bool(db.execute('SELECT 1 FROM prediction_campaigns WHERE pending_unknown=1 LIMIT 1').fetchone()
                                 or db.execute('SELECT 1 FROM prediction_order_intents WHERE unknown=1 LIMIT 1').fetchone()))
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return None


def _persist_reference_selection(bridge, market, unit, at_ms):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    books = spots = ()
    try:
        books, spots = read_inputs(bridge.signal_db, start, at_ms)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        pass
    snapshot = books[-1] if books else None
    with closing(b.connect(bridge.feature_db)) as db:
        db.execute('CREATE TABLE IF NOT EXISTS t68a_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        db.commit()
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT payload FROM t68a_decisions WHERE start=?', (start,)).fetchone()
        d = _decision_payload(row[0]) if row else dict(_late_identity(bridge, market, unit, None),
            selected=False, core_guard=dict(verified=False, empty=False, reason='initial_window_missed'))
        registered = _late_identity(bridge, market, unit, d.get('fee_bps'))
        if any(d.get(key) != registered[key] for key in registered):
            raise ValueError('frozen_identity_unit_fee_mismatch')
        if d.get('selected') is True or 'reference_attempt' in d:
            db.rollback()
            return d
        if row and not 0 <= dec(d.get('fee_bps')) <= 10000:
            raise ValueError('frozen_identity_unit_fee_mismatch')
        identity = _late_identity(bridge, market, unit, d.get('fee_bps'))
        reason = core_empty_reason(identity, d, at_ms)
        if reason is None:
            reason = _backfill_gate(identity, d, _exposure(bridge, identity, at_ms), at_ms)
        if at_ms > start+POLICY['reference_last_selection_ms']:
            reason = 'reference_selection_window_missed'
        evaluation = None
        if reason is None:
            if snapshot is None:
                reason = 'reference_checkpoint_book_missing'
            else:
                try:
                    _identity(d, bridge, market, unit, snapshot)
                    bridge._book(snapshot, market, at_ms)
                    evaluation = evaluate_checkpoint(snapshot, spots, at_ms, unit)
                    candidate = evaluation.get('candidate')
                    if candidate is None:
                        reason = evaluation['reasons'][0] if evaluation['reasons'] else 'reference_no_model_candidate'
                except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    reason = 'reference_'+_reason_code(exc)
        d['reference_attempt'] = dict(at_ms=at_ms, status='DENIED' if reason else 'SELECTED',
                                      reason=reason, evaluation=evaluation)
        if reason is None:
            d.update(candidate, selected=True, selected_at_ms=at_ms,
                     expires_at_ms=at_ms+POLICY['quote_ttl_ms'])
            entry = b.C180EntryDecision(d['side'], 'regime_entry', d['side'], unit, dec(d['net_shares']), None)
            signal = b.C180Signal(start, market.market_topic_id, market.up_market_id, start+180000,
                                 at_ms, 'regime_frozen_entry', entry, dec(d['probability']),
                                 FINGERPRINT, None, dec(d['fee_bps']))
            d['signal'] = b._signal_json(signal)
        d['last_evaluated_ms'] = at_ms
        raw = bounded_payload(json.dumps(d, sort_keys=True, allow_nan=False))
        db.execute('INSERT INTO t68a_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload', (start, raw))
        db.commit()
        return d


def _latch_reference_denial(bridge, market, unit, at_ms, reason):
    """Keep the original signal, while refusing another execution after denial."""
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    with closing(b.connect(bridge.feature_db)) as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT payload FROM t68a_decisions WHERE start=?', (start,)).fetchone()
        if not row:
            db.rollback()
            return
        d = _decision_payload(row[0])
        identity = _late_identity(bridge, market, unit, d.get('fee_bps'))
        if (d.get('branch') != 'reference_180_mid' or not d.get('selected')
                or any(d.get(key) != identity[key] for key in identity)
                or 'reference_execution_denied' in d):
            db.rollback()
            return
        d['reference_execution_denied'] = dict(at_ms=at_ms, reason=reason)
        db.execute('UPDATE t68a_decisions SET payload=? WHERE start=?',
                   (bounded_payload(json.dumps(d, sort_keys=True, allow_nan=False)), start))
        db.commit()


def _check_reference(bridge, market, unit, at_ms, seen):
    from . import regime_worker_bridge as b
    from .regime_lane import walk
    start = int(market.start_time_ms)
    d = None
    phase = 'reference_selection'
    try:
        d = _frozen(bridge, start)
        if not d or d.get('selected') is not True:
            d = _persist_reference_selection(bridge, market, unit, at_ms)
        if not d.get('selected'):
            return b.C180Ready(False, 't68a_reference_denied:'+d['reference_attempt']['reason'])
        if d['branch'] != 'reference_180_mid':
            return b.C180Ready(False, 't68a_selected_quote_expired')
        if d.get('reference_execution_denied'):
            return b.C180Ready(False, 't68a_reference_execution_denied:'+d['reference_execution_denied']['reason'])
        if at_ms < d['selected_at_ms']:
            return b.C180Ready(False, 't68a_frozen_selection_future')
        if at_ms >= d['expires_at_ms']:
            return b.C180Ready(False, 't68a_selected_quote_expired')
        phase = 'reference_book'
        books, _ = read_inputs(bridge.signal_db, start, at_ms)
        if not books:
            raise ValueError('reference_depth_invalid')
        snapshot = books[-1]
        _identity(d, bridge, market, unit, snapshot)
        bridge._book(snapshot, market, at_ms)
        stamp = _book_clock(snapshot, at_ms)
        if stamp <= seen:
            return b.C180Ready(False, 'quote_not_new_after_ready')
        signal = _selected_signal(d)
        if (signal.market_start_ms != start or signal.market_topic != market.market_topic_id
                or signal.market_id != market.up_market_id or signal.cutoff_ms != start+180000
                or signal.completed_at_ms != d['selected_at_ms'] or not signal.is_entry
                or signal.entry.side != d['side'] or signal.entry.action != d['side']
                or signal.entry.stake_usdt != unit or signal.frozen_fee_bps != dec(d['fee_bps'])
                or signal.original_input_sha256 != FINGERPRINT
                or signal.original_p_up != dec(d['probability'])):
            raise ValueError('frozen_identity_unit_fee_mismatch')
        phase = 'reference_execution'
        for side in ('UP', 'DOWN'):
            levels = snapshot['quote'][side]['ask_levels']
            if not isinstance(levels, (list, tuple)) or any(not isinstance(x, (list, tuple)) or len(x) != 2 for x in levels):
                raise ValueError('reference_depth_invalid')
            walk(levels, snapshot['fee_bps'], cap=dec('.99'), amount=unit)
        ex = new_execution(snapshot, d['side'], unit, dec(d['probability']), minimum_ev='.005', lower=d['lower'], cap=d['cap'])
        low, high = map(dec, POLICY['reference_backfill_cost_band'])
        if not low <= ex['cash']/ex['net_shares'] < high:
            raise ValueError('reference_refresh_cost_band')
        recheck = b.C180ExecutionRecheck(True, 't68a_ready', at_ms, d['expires_at_ms'],
                                       dec(d['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't68a_ready:reference_180_mid', signal, recheck, stamp)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        reason = _reason_code(exc)
        if d and d.get('selected') is True and d.get('branch') == 'reference_180_mid':
            try:
                _latch_reference_denial(bridge, market, unit, at_ms, reason)
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
                return b.C180Ready(False, 't68a_inputs_unavailable:reference_denial_storage:'+_reason_code(exc))
        return b.C180Ready(False, 't68a_inputs_unavailable:'+phase+':'+reason)
