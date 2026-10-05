"""Independent T6.9a paper observer; only feature/evidence DB writes, no orders."""
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from .regime_lane import dec
from .regime_t67_evidence import read_inputs
from .regime_t69a_policy import PROFILE, FINGERPRINT


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t69a_shadow_states(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS t69a_shadow_quotes(start INTEGER,branch TEXT,payload TEXT NOT NULL,PRIMARY KEY(start,branch))')
    db.execute('CREATE TABLE IF NOT EXISTS t69a_shadow_outcomes(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
    db.commit()


def _usable_depth(book):
    """Incomplete public depth cannot form a Shadow trigger/checkpoint."""
    try:
        for side in ('UP', 'DOWN'):
            levels = book['quote'][side]['ask_levels']
            if not isinstance(levels, (list, tuple)) or not levels:
                return False
            for level in levels:
                if not isinstance(level, (list, tuple)) or len(level) != 2:
                    return False
                price, quantity = (dec(value) for value in level)
                if (not price.is_finite() or not quantity.is_finite()
                        or not 0 < price < 1 or quantity <= 0):
                    return False
        return True
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False


def observe(db, prediction_db, signal_db, at_ms, symbol='BTCUSDT'):
    """Flat F1-F4 and R* paper quotes, recorded even when Live is filled/held."""
    from .regime_t69a_rstar_shadow import RSTAR_POLICY, in_window
    start = at_ms//300000*300000
    flat_window = start+60000 <= at_ms < start+270000
    rstar_window = in_window(at_ms-start) and symbol in RSTAR_POLICY['markets']
    if not flat_window and not rstar_window:
        return 'outside_shadow_window'
    with closing(sqlite3.connect(Path(prediction_db).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as p:
        p.execute('PRAGMA query_only=ON')
        p.execute('BEGIN')
        row = p.execute("SELECT l.loop_id,s.market_topic_id,s.market_id FROM prediction_loops l "
                        "JOIN prediction_regime_slots s ON s.loop_id=l.loop_id "
                        "WHERE l.strategy_profile=? AND l.mode='LIVE' AND l.state='RUNNING' "
                        "AND s.market_start_ms=? AND s.verified_at_ms IS NOT NULL", (PROFILE, start)).fetchone()
        if row is None:
            return 'no_active_verified_loop'
        # A collector observes only the loop bound to its own asset.
        bound = p.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prediction_loop_market_bindings'").fetchone()
        bound = p.execute('SELECT symbol FROM prediction_loop_market_bindings WHERE loop_id=?', (row[0],)).fetchone() if bound else None
        if (bound[0] if bound else 'BTCUSDT') != symbol:
            return 'other_asset_loop'
        unit_row = p.execute("SELECT config_value_json FROM prediction_runtime_config "
                             "WHERE config_key='prediction_selected_order_unit'").fetchone()
    if unit_row is None:
        return 'shadow_unit_missing'
    unit = dec(json.loads(unit_row[0])['order_unit_usdt'])
    if unit not in (1, 2, 3):
        raise ValueError('shadow_unit_invalid')
    books, spots = read_inputs(signal_db, start, at_ms)
    book = books[-1] if books else None
    identity = dict(fingerprint=FINGERPRINT, loop_id=row[0], market_topic=str(row[1]),
                    market_id=str(row[2]), market_start_ms=start, market_end_ms=start+300000,
                    end_ms=start+300000, unit_usdt=str(unit),
                    fee_bps=str(book['fee_bps']) if book and 'fee_bps' in book else None)
    try:
        stored = db.execute('SELECT payload FROM t69a_decisions WHERE start=?', (start,)).fetchone()
        core_decision = json.loads(stored[0]) if stored else None
    except sqlite3.Error:
        core_decision = None
    if identity['fee_bps'] is None:
        return 'shadow_book_missing'
    schema(db)
    if not flat_window:
        # R* late favourite chase: separate policy, paper quote only.
        from .regime_t69a_rstar_shadow import observe as observe_rstar
        try:
            from .regime_t69a_rstar_shadow import fetch_klines
            observe_rstar(db, identity, books, spots, at_ms, core_decision, fetch_klines)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            db.rollback()
            return 'rstar_shadow_rejected'
        return 'rstar_observed'
    # Only Flat F1-F4 are observed; the T6.7 research routes are retired here.
    from .regime_t69a_flat_shadow import observe as observe_flat
    try:
        observe_flat(db, identity, books, spots, at_ms, unit, core_decision)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        db.rollback()
        return 'flat_shadow_rejected'
    return 'shadow_observed'


async def resolve_once(db, at_ms, fetch):
    """One official readonly detail; immutable confirmed winner, no Live DB writes."""
    from .models import MarketInfo
    from .worker import PredictionWorker
    schema(db)
    row = db.execute('SELECT q.start,q.payload FROM t69a_shadow_quotes q '
                     'LEFT JOIN t69a_shadow_outcomes o ON o.start=q.start '
                     'WHERE q.start+300000<=? AND COALESCE(json_extract(o.payload,\'$.complete\'),0)=0 '
                     'ORDER BY COALESCE(json_extract(o.payload,\'$.last_checked_at_ms\'),0),q.start LIMIT 1',
                     (at_ms,)).fetchone()
    if row is None:
        return 'complete'
    start, quote = row[0], json.loads(row[1])
    value = {k: quote[k] for k in ('fingerprint', 'loop_id', 'market_topic', 'market_id',
                                  'market_start_ms', 'market_end_ms', 'end_ms')}
    value.update(complete=False, winner=None, final_side=None, last_checked_at_ms=at_ms)
    try:
        if value['fingerprint'] != FINGERPRINT:
            raise ValueError('shadow_policy_mismatch')
        detail = await fetch(value['market_topic'])
        m = MarketInfo.from_api(detail)
        if (str(m.market_topic_id) != str(value['market_topic']) or str(m.up_market_id) != str(value['market_id'])
                or m.start_time_ms != start or m.end_time_ms != start+300000):
            raise ValueError('shadow_official_identity_mismatch')
        value['official_status'] = m.status
        winner = PredictionWorker._official_shadow_resolution(detail) if m.status in ('CLOSED', 'RESOLVED', 'SETTLED') else None
        if winner in ('UP', 'DOWN', 'DRAW'):
            value.update(complete=True, winner=winner, final_side=winner, known_at_ms=at_ms, official_detail=detail)
    except Exception as exc:
        value['last_error'] = type(exc).__name__
    with db:
        db.execute('INSERT INTO t69a_shadow_outcomes VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(value, sort_keys=True)))
    return 'resolved' if value['complete'] else 'pending'

