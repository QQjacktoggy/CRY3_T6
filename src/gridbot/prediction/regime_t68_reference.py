"""Immutable, causal Reference checkpoints and a pure Live candidate scorer.

This module cannot select a Live decision, create an order, update risk, or
infer that absent core data is an empty core. Resource limits apply only to
this module's checkpoint rows. The Live bridge owns sticky selection.
"""
import errno
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from .evidence_retention import bounded_payload, require_free_space
from .regime_lane import FINGERPRINT as CORE_FINGERPRINT, dec, walk
from .regime_t67_lane import execution, model_candidate, probability

IDENTITY_KEYS = ('fingerprint', 'loop_id', 'market_topic', 'market_id',
                 'market_start_ms', 'market_end_ms', 'unit_usdt', 'fee_bps')
EXPOSURE_KEYS = ('has_claim', 'has_intent', 'has_position', 'has_unknown')
_REASON_CODES = {
    'book identity/depth missing': 'reference_book_identity_invalid',
    'book receipt stale or future': 'reference_book_receipt_invalid',
    'book stale or future': 'reference_book_clock_invalid',
    'unsupported regime order amount': 'reference_unit_invalid',
    'invalid fee': 'reference_fee_invalid',
    'invalid ask depth': 'reference_depth_invalid',
    'unsorted ask depth': 'reference_depth_unsorted',
    'insufficient requested depth': 'reference_depth_insufficient',
    'nonfinite input': 'reference_nonfinite_input',
    'spot missing/stale': 'reference_spot_missing_or_stale',
    'opening proxy anchor missing or generation changed': 'reference_opening_anchor_or_generation_unavailable',
    'volatility history incomplete': 'reference_volatility_history_unavailable',
    'spot price invalid': 'reference_spot_price_invalid',
    'reference or remaining time invalid': 'reference_anchor_or_remaining_time_invalid',
    'best ask below price band': 'reference_best_ask_below_band',
    'price band': 'reference_execution_price_outside_band',
    'fee net EV': 'reference_fee_net_ev_below_minimum',
}
for _code in ('reference_book_identity_invalid', 'reference_book_stale',
              'reference_anchor_invalid', 'reference_unit_invalid',
              'reference_net_shares_invalid', 'reference_effective_cost_outside_band',
              'reference_depth_invalid', 'reference_book_receipt_invalid'):
    _REASON_CODES[_code] = _code


def _reason(exc, fallback):
    """Unexpected exception text may contain transport URLs or credentials."""
    return _REASON_CODES.get(str(exc), fallback)


def _decimal_json(item):
    if isinstance(item, Decimal):
        return str(item)
    raise TypeError('non-JSON evidence')


def _policy():
    from .regime_t68_policy import FINGERPRINT, POLICY
    return FINGERPRINT, POLICY


def _json(value):
    """Copy public evidence into JSON values without retaining mutable inputs."""
    return json.loads(json.dumps(value, sort_keys=True, allow_nan=False, default=_decimal_json))


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t68_reference_checkpoints('
               'start INTEGER NOT NULL,checkpoint_ms INTEGER NOT NULL,payload TEXT NOT NULL,'
               'PRIMARY KEY(start,checkpoint_ms))')


def _database_path(db):
    for _, name, path in db.execute('PRAGMA database_list'):
        if name == 'main' and path:
            return Path(path)
    # Tests may use an in-memory connection; its reserve is the workspace.
    return Path.cwd() / 't68-reference-memory.sqlite3'


def _book_clock(book, at_ms):
    from .regime_worker_bridge import RegimeWorkerBridge
    start = book['market_start_ms']
    if (type(start) is not int or start <= 0 or start % 300000
            or not str(book['market_topic']) or not str(book['market_id'])):
        raise ValueError('reference_book_identity_invalid')
    market = SimpleNamespace(start_time_ms=start, market_topic_id=book['market_topic'],
                             up_market_id=book['market_id'])
    stamp = RegimeWorkerBridge._book(book, market, at_ms)
    captured = int(book['captured_at_ms'])
    if any(int(book[key]) > captured for key in ('received_at', 'received_at_ms')):
        raise ValueError('reference_book_receipt_invalid')
    if at_ms-stamp > 1000:
        raise ValueError('reference_book_stale')
    if (dec(book['reference']) <= 0
            or not 0 <= int(book['reference_received_ms']) <= captured):
        raise ValueError('reference_anchor_invalid')
    return stamp


def _execution_json(result):
    return {key: str(value) for key, value in result.items()}


def _causal_spots(spots, at_ms):
    """Do not let future, reversed receipt clocks, or malformed sources anchor a model."""
    result = []
    for spot in spots:
        try:
            event, received = int(spot['event_ms']), int(spot['received_ms'])
            generation = int(spot['generation'])
            if (spot['source'] == 'binance_spot' and generation >= 0
                    and 0 <= event <= received <= at_ms and received-event <= 1500):
                result.append(dict(source=spot['source'], generation=generation,
                                   event_ms=event, received_ms=received,
                                   price=str(dec(spot['price']))))
        except (ValueError, KeyError, TypeError, ArithmeticError):
            continue
    return result


def evaluate_checkpoint(book, spots, at_ms, amount):
    """Score both sides using evidence available at ``at_ms``; JSON-safe only.

    The model EV and +.02 stress gate are inherited from T6.7. The new price
    band is the fee-adjusted cash/net-share cost, not the original C ask band.
    """
    fingerprint, policy = _policy()
    result = dict(status='INPUTS_UNAVAILABLE', reasons=[], evaluated_at_ms=at_ms,
                  snapshot=None, p_up=None, latest=None, model=None,
                  per_side={}, candidate=None)
    try:
        amount = dec(amount)
        if amount not in (1, 2, 3):
            raise ValueError('reference_unit_invalid')
        stamp = _book_clock(book, at_ms)
        # Both sides need genuine, complete requested cash depth. A malformed
        # opposite book is unavailable evidence, not a one-sided opportunity.
        for side in ('UP', 'DOWN'):
            levels = book['quote'][side]['ask_levels']
            if (not isinstance(levels, (list, tuple)) or not levels
                    or any(not isinstance(level, (list, tuple)) or len(level) != 2 for level in levels)):
                raise ValueError('reference_depth_invalid')
            walk(levels, book['fee_bps'], cap=dec('.99'), amount=amount)
        result['snapshot'] = _json(book)
        result['book_at_ms'] = stamp
        causal = _causal_spots(spots, at_ms)
        p_up, latest, model = probability(book, causal, at_ms)
        result.update(p_up=str(p_up), latest=_json(latest), model=_json(model),
                      causal_spot_count=len(causal), status='EVALUATED')
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        result['reasons'] = [_reason(exc, 'reference_inputs_invalid')]
        return result

    low, high = map(dec, policy['reference_backfill_cost_band'])
    choices = []
    for side in ('UP', 'DOWN'):
        score = dict(status='REJECTED', reasons=[], effective_cost=None, ev_usdt=None,
                     execution=None, stress_execution=None)
        try:
            current = execution(book, side, amount)
            if current['net_shares'] <= 0:
                raise ValueError('reference_net_shares_invalid')
            cost = current['cash']/current['net_shares']
            side_probability = p_up if side == 'UP' else 1-p_up
            ev = side_probability*current['net_shares']-current['cash']
            score.update(effective_cost=str(cost), ev_usdt=str(ev),
                         execution=_execution_json(current))
            if not low <= cost < high:
                raise ValueError('reference_effective_cost_outside_band')
            if ev < dec('.03')*amount:
                raise ValueError('fee net EV')
            candidate = model_candidate('reference_180_mid', book, side, p_up, amount, model)
            shifted = {**book, 'quote': {**book['quote'], side: {'ask_levels':
                [[str(dec(price)+dec('.02')), quantity]
                 for price, quantity in book['quote'][side]['ask_levels']
                 if dec(price)+dec('.02') < 1]}}}
            stress = execution(shifted, side, amount, p_up, minimum_ev='.005')
            score.update(status='ELIGIBLE', stress_execution=_execution_json(stress))
            candidate.update(fingerprint=fingerprint, effective_cost=str(cost),
                             cash=str(current['cash']), net_shares=str(current['net_shares']),
                             limit=str(current['limit']), ev_usdt=str(ev))
            choices.append((ev, candidate))
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            score['reasons'] = [_reason(exc, 'reference_execution_invalid')]
        result['per_side'][side] = score
    result['candidate'] = _json(max(choices, key=lambda value: value[0])[1]) if choices else None
    result['reasons'] = [] if choices else ['reference_no_model_candidate']
    return result


def _matching_identity(value, identity):
    if not isinstance(value, dict):
        return False
    try:
        for key in IDENTITY_KEYS:
            if key in ('unit_usdt', 'fee_bps'):
                if dec(value[key]) != dec(identity[key]):
                    return False
            elif value[key] != identity[key]:
                return False
        return True
    except (KeyError, ValueError, TypeError, ArithmeticError):
        return False


def core_empty_reason(identity, decision, at_ms):
    """Missing or contradictory proof never opens the 180-second lane."""
    start = identity['market_start_ms']
    if not _matching_identity(decision, identity) or decision.get('selected') is not False:
        return 'reference_core_identity_or_selection_unverified'
    guard = decision.get('core_guard')
    if (not isinstance(guard, dict) or guard.get('verified') is not True
            or guard.get('empty') is not True or guard.get('candidates') != []):
        return 'reference_core_not_verified_empty'
    try:
        frozen = int(guard['frozen_at_ms'])
        captured = int(guard['initial_captured_at_ms'])
        stamp = int(guard['initial_book_at_ms'])
        features = guard['features']
        for key in ('first_bp', 'last_bp', 'prior_bp'):
            dec(features[key])
        if (not start+124000 <= frozen <= min(at_ms, start+126000)
                or not start+124000 <= captured <= frozen
                or not 0 <= captured-stamp <= 2000
                or dec(guard['fee_bps']) != dec(identity['fee_bps'])
                or features.get('fingerprint') != CORE_FINGERPRINT
                or features.get('market_start_ms') != start
                or features.get('cutoff_ms') != start+120000
                or not start+120000 <= int(features['received_at_ms']) <= min(frozen, start+123000)):
            return 'reference_core_proof_invalid'
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return 'reference_core_proof_invalid'
    return None


def _backfill_gate(identity, decision, exposure, at_ms):
    reason = core_empty_reason(identity, decision, at_ms)
    if reason:
        return reason
    if not _matching_identity(exposure, identity):
        return 'reference_exposure_snapshot_unverified'
    try:
        if not 0 <= at_ms-int(exposure['observed_at_ms']) <= 1000:
            return 'reference_exposure_snapshot_stale'
    except (KeyError, ValueError, TypeError):
        return 'reference_exposure_snapshot_unverified'
    if any(exposure.get(key) is not False for key in EXPOSURE_KEYS):
        return 'reference_existing_or_unknown_exposure'
    return None


def _validate_identity(identity, amount):
    fingerprint, _ = _policy()
    start = identity['market_start_ms']
    if (type(start) is not int or start <= 0 or start % 300000
            or identity['market_end_ms'] != start+300000
            or identity['fingerprint'] != fingerprint
            or not isinstance(identity['loop_id'], str) or not identity['loop_id']
            or not isinstance(identity['market_topic'], str) or not identity['market_topic']
            or not isinstance(identity['market_id'], str) or not identity['market_id']
            or dec(identity['unit_usdt']) != dec(amount) or dec(amount) not in (1, 2, 3)
            or (identity['fee_bps'] is not None
                and not 0 <= dec(identity['fee_bps']) <= 10000)):
        raise ValueError('reference_registered_identity_invalid')


def _identity_book(book, identity):
    try:
        return (book['market_start_ms'] == identity['market_start_ms']
                and str(book['market_topic']) == identity['market_topic']
                and str(book['market_id']) == identity['market_id']
                and dec(book['fee_bps']) == dec(identity['fee_bps']))
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return False


def capture_checkpoints(db, identity, books, spots, at_ms, amount,
                        core_decision=None, exposure=None):
    """Freeze each checkpoint's first observation, including missing evidence.

    Returns newly inserted diagnostic records. No observer paper quote is
    emitted for the Live 180-second branch. No checkpoint is reconstructed
    after grace, and the Live bridge does not depend on these observations.
    The caller must pass a feature/evidence DB, never the trading database.
    """
    _, policy = _policy()
    _validate_identity(identity, amount)
    start = identity['market_start_ms']
    if db.in_transaction:
        raise ValueError('reference_checkpoint_requires_own_transaction')
    require_free_space(_database_path(db))
    schema(db)
    records = []
    try:
        db.execute('BEGIN IMMEDIATE')
        for offset in policy['reference_checkpoints_ms']:
            due = start+offset
            if at_ms < due:
                continue
            old = db.execute('SELECT payload FROM t68_reference_checkpoints WHERE start=? AND checkpoint_ms=?',
                             (start, offset)).fetchone()
            if old:
                previous = json.loads(old[0])
                # Fee is the observed checkpoint's immutable as-of input.
                # A missing first quote must not deny later checkpoints.
                if any(previous[key] != identity[key] for key in IDENTITY_KEYS if key != 'fee_bps'):
                    raise ValueError('reference_checkpoint_frozen_identity_changed')
                continue
            record = dict(identity, checkpoint_ms=offset, checkpoint_at_ms=due,
                          observed_at_ms=at_ms, evaluated_at_ms=None,
                          status='MISSED_CHECKPOINT', reasons=['reference_checkpoint_missed'],
                          evaluation=None, backfill_eligible=False,
                          backfill_reason='reference_checkpoint_missed', paper_quote=None,
                          backfill_mode=policy['reference_backfill_mode'])
            if at_ms <= due+policy['reference_checkpoint_grace_ms']:
                # Select the latest observed sample, not the latest successful
                # one. Invalid first evidence remains visible and immutable.
                samples = [book for book in books if isinstance(book, dict)
                           and type(book.get('captured_at_ms')) is int
                           and due <= book['captured_at_ms'] <= at_ms]
                book = max(samples, key=lambda item: (item['captured_at_ms'], item.get('book_at_ms', 0))) if samples else None
                if book is None:
                    evaluation = dict(status='INPUTS_UNAVAILABLE', reasons=['reference_checkpoint_book_missing'],
                                      snapshot=None, per_side={}, candidate=None)
                elif not _identity_book(book, identity):
                    evaluation = dict(status='INPUTS_UNAVAILABLE', reasons=['reference_checkpoint_book_identity_mismatch'],
                                      snapshot=None, per_side={}, candidate=None)
                else:
                    evaluation = evaluate_checkpoint(book, spots, at_ms, amount)
                record.update(status=evaluation['status'], reasons=evaluation['reasons'],
                              evaluated_at_ms=at_ms, evaluation=evaluation,
                              backfill_reason='reference_not_backfill_checkpoint')
                candidate = evaluation.get('candidate')
                if offset == 180000:
                    gate = _backfill_gate(identity, core_decision, exposure, at_ms)
                    record['backfill_reason'] = gate or ('reference_no_model_candidate' if candidate is None else 'reference_backfill_eligible')
                    if gate is None and candidate is not None:
                        record['backfill_eligible'] = True
            records.append(record)
        raws = [bounded_payload(json.dumps(record, sort_keys=True, allow_nan=False)) for record in records]
        if records:
            rows, size = db.execute('SELECT COUNT(*),COALESCE(SUM(length(CAST(payload AS BLOB))),0) '
                                    'FROM t68_reference_checkpoints').fetchone()
            if (rows+len(records) > policy['checkpoint_max_rows']
                    or size+sum(len(raw.encode('utf-8')) for raw in raws) > policy['checkpoint_max_payload_bytes']):
                raise OSError(errno.ENOSPC, 'reference checkpoint storage budget reached')
            for record, raw in zip(records, raws):
                db.execute('INSERT INTO t68_reference_checkpoints VALUES(?,?,?)',
                           (start, record['checkpoint_ms'], raw))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    return records
