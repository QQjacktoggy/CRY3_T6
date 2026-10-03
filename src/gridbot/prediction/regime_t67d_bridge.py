"""Durable core-first adapter; no order or risk writes outside shared worker."""
import json
import sqlite3
from contextlib import closing

from .regime_lane import dec
from .regime_t63_lane import eligible_execution
from .regime_t67_lane import execution as new_execution
from .regime_t67d_policy import FINGERPRINT, POLICY

from .regime_t67a_bridge import freeze_core, additions


class _FeaturesMissing(ValueError):
    pass


def check_signal(bridge, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
    from . import regime_worker_bridge as b
    start = int(market.start_time_ms)
    if unit_usdt not in (1, 2, 3) or not start+124000 <= at_ms < start+136000:
        return b.C180Ready(False, 't67d_unit_or_execution_window')
    try:
        snapshot = b.read_c180_book(bridge.signal_db, start)
        stamp = bridge._book(snapshot, market, at_ms)
        if at_ms-stamp > 1000:
            return b.C180Ready(False, 't67d_fresh_book_required')
        if stamp <= last_seen_book_at_ms:
            return b.C180Ready(False, 'quote_not_new_after_ready')
        # Frozen selected decisions need no writer lock or schema/commit work.
        # Unselected callers still re-read under BEGIN IMMEDIATE before selecting.
        with closing(sqlite3.connect(bridge.feature_db.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as reader:
            reader.execute('PRAGMA query_only=ON')
            exists = reader.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t67d_decisions'").fetchone()
            row = reader.execute('SELECT payload FROM t67d_decisions WHERE start=?', (start,)).fetchone() if exists else None
            frozen = json.loads(row[0]) if row else None
        if frozen and frozen.get('selected') is True:
            d = frozen
            _identity(d, bridge, market, unit_usdt, snapshot)
            guard = d['core_guard']
        else:
            d = _persist_selection(bridge, market, unit_usdt, at_ms, snapshot)
            guard = d['core_guard']
        if not d['selected']:
            return b.C180Ready(False, 't67d_no_live_candidate:'+guard['reason'])
        if at_ms < d['selected_at_ms']:
            return b.C180Ready(False, 't67d_frozen_selection_future')
        if at_ms >= d['expires_at_ms']:
            return b.C180Ready(False, 't67d_selected_quote_expired')
        if not d.get('signal'):
            return b.C180Ready(False, 't67d_frozen_signal_missing')
        signal = b._from_signal_json(d['signal'])
        if (signal.market_start_ms != start or signal.market_topic != market.market_topic_id
                or signal.market_id != market.up_market_id or signal.cutoff_ms != start+120000
                or signal.completed_at_ms != d['selected_at_ms'] or not signal.is_entry
                or signal.entry.side != d['side'] or signal.entry.action != d['side']
                or signal.entry.stake_usdt != unit_usdt or signal.frozen_fee_bps != dec(d['fee_bps'])
                or signal.original_input_sha256 != FINGERPRINT):
            return b.C180Ready(False, 't67d_frozen_signal_identity_unit_fee_mismatch')
        is_core = d['branch'].startswith('core_')
        ex = (eligible_execution(d, snapshot, unit_usdt) if is_core else
              new_execution(snapshot, d['side'], unit_usdt, lower=d['lower'], cap=d['cap']))
        recheck = b.C180ExecutionRecheck(True, 't67d_ready', at_ms, d['expires_at_ms'],
                                      dec(d['cap']), ex['cash'], ex['net_shares'], None)
        return b.C180Ready(True, 't67d_ready:'+d['branch'], signal, recheck, stamp)
    except _FeaturesMissing:
        return b.C180Ready(False, 't67d_features_missing')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
        return b.C180Ready(False, 't67d_inputs_unavailable:'+type(exc).__name__)


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
        db.execute('CREATE TABLE IF NOT EXISTS t67d_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        db.commit()
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT payload FROM t67d_decisions WHERE start=?', (start,)).fetchone()
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
        # The eighth route uses immutable checkpoints, not the latest favorable quote.
        if not d['selected'] and guard.get('verified') and guard.get('empty'):
            candidate = _flat_candidate(bridge, market, d, at_ms, unit_usdt)
            if candidate is not None:
                selected_at, candidate, ex = candidate
                d.update(candidate, selected=True, selected_at_ms=selected_at,
                         expires_at_ms=min(start+136000, selected_at+POLICY['quote_ttl_ms']))
                entry = b.C180EntryDecision(d['side'], 'regime_entry', d['side'], unit_usdt,
                                            ex['net_shares'], None)
                signal = b.C180Signal(start, market.market_topic_id, market.up_market_id, start+120000,
                                     selected_at, 'regime_frozen_entry', entry, None, FINGERPRINT, None, dec(d['fee_bps']))
                d['signal'] = b._signal_json(signal)
        d['last_evaluated_ms'] = at_ms
        db.execute('INSERT INTO t67d_decisions VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(d, sort_keys=True, allow_nan=False)))
        db.commit()
        return d


def _favorite(snapshot):
    asks = {}
    for side in ('UP', 'DOWN'):
        ask = dec(snapshot['quote'][side]['ask'])
        if ask != dec(snapshot['quote'][side]['ask_levels'][0][0]):
            raise ValueError('actual_ask_depth_mismatch')
        asks[side] = ask
    if asks['UP'] == asks['DOWN']:
        raise ValueError('favorite_tie')
    return 'UP' if asks['UP'] > asks['DOWN'] else 'DOWN'


def _checkpoint(bridge, market, at_ms, beginning, ending):
    """Read the first valid fresh sample; price/depth eligibility is tested once."""
    start = int(market.start_time_ms)
    with closing(sqlite3.connect(bridge.signal_db.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        rows = db.execute('SELECT snapshot_json FROM c180_book_events WHERE market_start_ms=? '
                          'AND captured_at_ms>=? AND captured_at_ms<=? ORDER BY captured_at_ms,book_at_ms',
                          (start, start+beginning, min(at_ms, start+ending)))
        for row in rows:
            sample = json.loads(row[0])
            try:
                cutoff = int(sample['captured_at_ms'])
                stamp = bridge._book(sample, market, cutoff)
                if not start+beginning <= cutoff <= min(at_ms, start+ending) or cutoff-stamp > 1000:
                    continue
                return sample
            except (ValueError, KeyError, TypeError):
                continue
    return None


def _flat_candidate(bridge, market, d, at_ms, unit):
    """Freeze rejection or approval at the first confirmation. No later search."""
    start = int(market.start_time_ms)
    status = d.setdefault('flat_guard', {})
    if status.get('terminal'):
        return None
    features = d['core_guard']['features']
    if not abs(dec(features['first_bp'])) < dec('.5') or not abs(dec(features['last_bp'])) < dec('.5'):
        status.update(terminal=True, reason='non_flat')
        return None
    if at_ms < start+128000:
        status['reason'] = 'awaiting_confirmation'
        return None
    initial = _checkpoint(bridge, market, at_ms, 124000, 126000)
    confirmation = _checkpoint(bridge, market, at_ms, 128000, 129500)
    if confirmation is None:
        if at_ms > start+129500:
            status.update(terminal=True, reason='confirmation_missing')
        return None
    status.update(terminal=True, initial=initial, confirmation=confirmation,
                  reason='checkpoint_rejected')
    try:
        if initial is None or int(initial['captured_at_ms']) != d['core_guard']['initial_captured_at_ms']:
            raise ValueError('initial_checkpoint_mismatch')
        if dec(initial['fee_bps']) != dec(d['fee_bps']) or dec(confirmation['fee_bps']) != dec(d['fee_bps']):
            raise ValueError('checkpoint_fee_changed')
        side = _favorite(initial)
        if side != _favorite(confirmation):
            raise ValueError('favorite_changed')
        ex = new_execution(confirmation, side, unit, lower='.65', cap='.80')
        selected_at = int(confirmation['captured_at_ms'])
        status.update(reason='flat_verified', side=side, selected_at_ms=selected_at)
        return selected_at, dict(branch='flat_favorite', side=side, action='flat_favorite',
                                 probability=None, lower='.65', cap=str(ex['limit']), upper='.80'), ex
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        status['reason'] = str(exc)
        return None
