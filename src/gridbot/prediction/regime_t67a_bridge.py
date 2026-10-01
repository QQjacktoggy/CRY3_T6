"""Durable core-first adapter; no order or risk writes outside shared worker."""
import json
import sqlite3
from contextlib import closing

from .regime_lane import dec, state_of, walk
from .regime_t63_lane import eligible_execution
from .regime_t65_lane import candidates as core_candidates
from .regime_t67_lane import execution as new_execution
from .regime_t67a_policy import FINGERPRINT, POLICY


def core_name(candidate):
    if candidate['branch'] == 'C_reversal_netdown':
        return 'core_c_down'
    if candidate['state'] == 'reversal' and candidate['action'] == 'first':
        return 'core_first_' + candidate['side'].lower()
    if candidate['state'] == 'stall' and candidate['side'] == 'DOWN':
        return 'core_stall_down'
    if candidate['state'] == 'continuation' and candidate['action'] == 'original':
        return 'core_continuation_original'
    raise ValueError('retired_core_candidate')


def freeze_core(bridge, market, features, at_ms, unit):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    if (features.get('fingerprint') != b.FINGERPRINT
            or features.get('market_start_ms') != start or features.get('cutoff_ms') != start+120000
            or not start+120000 <= int(features['received_at_ms']) <= min(at_ms, start+123000)):
        raise ValueError('feature_provenance')
    initial = bridge._first_book(market, at_ms)
    if initial is None:
        raise ValueError('initial_book_missing')
    cutoff = int(initial['captured_at_ms'])
    stamp = bridge._book(initial, market, cutoff)
    if not start+124000 <= cutoff <= min(at_ms, start+126000) or cutoff-stamp > 2000:
        raise ValueError('initial_book_clock')
    original = b.read_c180_signal(bridge.signal_db, start)
    payload = json.loads(b._signal_json(original)) if original else None
    if payload and (original.market_topic != market.market_topic_id or original.market_id != market.up_market_id):
        raise ValueError('original_identity')
    choices, _ = core_candidates(features, payload, initial, unit)
    if not choices:
        # Preserve valid old core on its chosen-side depth; require complete
        # two-sided eligibility only before admitting additive execution.
        for side in ('UP', 'DOWN'):
            walk(initial['quote'][side]['ask_levels'], initial['fee_bps'], cap=dec('.99'), amount=unit)
        if state_of(features['first_bp'], features['last_bp']) in ('continuation', 'flat', 'stall'):
            if (not payload or payload['market_start_ms'] != start or payload['cutoff_ms'] != start+120000
                    or not start+120000 <= int(payload['completed_at_ms']) <= min(cutoff, start+123000)):
                raise ValueError('original_missing_or_invalid_for_empty_core')
    for candidate in choices:
        candidate['source_branch'] = candidate['branch']
        candidate['branch'] = core_name(candidate)
    return dict(verified=True, empty=not choices, candidates=choices, frozen_at_ms=at_ms,
                initial_book_at_ms=stamp, initial_captured_at_ms=cutoff, fee_bps=str(initial['fee_bps']),
                features=features, reason='core_reserved' if choices else 'core_verified_empty')


def additions(snapshot, features, unit):
    first, last, prior = (dec(features[k]) for k in ('first_bp', 'last_bp', 'prior_bp'))
    net = ((1+first/10000)*(1+last/10000)-1)*10000
    choices = []
    if state_of(first, last) == 'reversal' and net >= 1 and prior >= 1:
        try:
            new_execution(snapshot, 'UP', unit, lower='.65', cap='.75')
            choices.append(dict(branch='c_mirror_up_prior', side='UP', action='c_mirror_up_prior',
                                probability=None, lower='.65', cap='.75', upper='.75'))
        except (ValueError, KeyError, TypeError, ArithmeticError):
            pass
    if first*last < 0 and abs(first) >= 1 and abs(first) >= 2*abs(last):
        side = 'UP' if net > 0 else 'DOWN'
        try:
            new_execution(snapshot, side, unit, lower='.10', cap='.75')
            choices.append(dict(branch='shallow_retracement', side=side, action='shallow_retracement',
                                probability=None, lower='.10', cap='.75', upper='.75'))
        except (ValueError, KeyError, TypeError, ArithmeticError):
            pass
    return choices


def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    if unit_usdt not in (1, 2, 3) or not start+124000 <= at_ms < start+136000:
        return b.C180Ready(False, 't67a_unit_or_execution_window')
    try:
        snapshot = b.read_c180_book(bridge.signal_db, start)
        stamp = bridge._book(snapshot, market, at_ms)
        if at_ms-stamp > 1000:
            return b.C180Ready(False, 't67a_fresh_book_required')
        if stamp <= last_seen_book_at_ms:
            return b.C180Ready(False, 'quote_not_new_after_ready')
        with closing(b.connect(bridge.feature_db)) as db:
            db.execute('CREATE TABLE IF NOT EXISTS t67a_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
            db.commit()
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT payload FROM t67a_decisions WHERE start=?', (start,)).fetchone()
            d = json.loads(row[0]) if row else dict(
                fingerprint=FINGERPRINT, market_topic=str(market.market_topic_id), market_id=str(market.up_market_id),
                market_start_ms=start, market_end_ms=start+300000, unit_usdt=str(unit_usdt),
                selected=False, fee_bps=str(snapshot['fee_bps']),
                loop_id=getattr(bridge, '_registered_loop_id', None))
            if (d['fingerprint'] != FINGERPRINT or d['market_topic'] != str(market.market_topic_id)
                    or d['market_id'] != str(market.up_market_id) or d['unit_usdt'] != str(unit_usdt)
                    or dec(d['fee_bps']) != dec(snapshot['fee_bps'])
                    or d.get('loop_id') != getattr(bridge, '_registered_loop_id', None)):
                db.rollback()
                return b.C180Ready(False, 't67a_frozen_identity_unit_fee_mismatch')
            if 'core_guard' not in d:
                if at_ms > start+126000:
                    d['core_guard'] = dict(verified=False, empty=False, reason='initial_window_missed')
                else:
                    row = db.execute('SELECT payload FROM features WHERE start=?', (start,)).fetchone()
                    if row is None:
                        db.rollback()
                        return b.C180Ready(False, 't67a_features_missing')
                    d['core_guard'] = freeze_core(bridge, market, json.loads(row[0]), at_ms, unit_usdt)
                    if dec(d['core_guard']['fee_bps']) != dec(d['fee_bps']):
                        raise ValueError('initial_fee_changed')
            guard = d['core_guard']
            if not d['selected'] and guard.get('verified') and at_ms <= start+134500:
                choices = additions(snapshot, guard['features'], unit_usdt) if guard['empty'] else guard['candidates']
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
            db.execute('INSERT INTO t67a_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                       (start, json.dumps(d, sort_keys=True, allow_nan=False)))
            db.commit()
        if not d['selected']:
            return b.C180Ready(False, 't67a_no_live_candidate:'+guard['reason'])
        if at_ms >= d['expires_at_ms']:
            return b.C180Ready(False, 't67a_selected_quote_expired')
        if not d.get('signal'):
            return b.C180Ready(False, 't67a_frozen_signal_missing')
        signal = b._from_signal_json(d['signal'])
        if (signal.market_start_ms != start or signal.market_topic != market.market_topic_id
                or signal.market_id != market.up_market_id or signal.cutoff_ms != start+120000
                or signal.completed_at_ms != d['selected_at_ms'] or not signal.is_entry
                or signal.entry.side != d['side'] or signal.entry.action != d['side']
                or signal.entry.stake_usdt != unit_usdt or signal.frozen_fee_bps != dec(d['fee_bps'])
                or signal.original_input_sha256 != FINGERPRINT):
            return b.C180Ready(False, 't67a_frozen_signal_identity_unit_fee_mismatch')
        is_core = d['branch'].startswith('core_')
        ex = (eligible_execution(d, snapshot, unit_usdt) if is_core else
              new_execution(snapshot, d['side'], unit_usdt, lower=d['lower'], cap=d['cap']))
        recheck = b.C180ExecutionRecheck(True, 't67a_ready', at_ms, d['expires_at_ms'],
                                      dec(d['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't67a_ready:'+d['branch'], signal, recheck, stamp)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        return b.C180Ready(False, 't67a_inputs_unavailable:'+type(exc).__name__)
