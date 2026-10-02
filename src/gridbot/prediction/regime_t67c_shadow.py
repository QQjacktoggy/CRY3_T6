"""Independent T6.7c paper observer; only feature/evidence DB writes, no orders."""
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

from .regime_lane import dec
from .regime_t67_evidence import read_inputs
from .regime_t67_lane import candidates, execution
from .regime_t67c_policy import PROFILE, FINGERPRINT, SHADOW_BRANCHES


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t67c_shadow_states(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS t67c_shadow_quotes(start INTEGER,branch TEXT,payload TEXT NOT NULL,PRIMARY KEY(start,branch))')
    db.execute('CREATE TABLE IF NOT EXISTS t67c_shadow_outcomes(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
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


def observe(db, prediction_db, signal_db, at_ms):
    """First causal executable quote per branch, even when Live is filled/held."""
    from .regime_worker_bridge import RegimeWorkerBridge
    start = at_ms//300000*300000
    if not start+60000 <= at_ms < start+270000:
        return 'outside_shadow_window'
    with closing(sqlite3.connect(Path(prediction_db).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as p:
        row = p.execute("SELECT l.loop_id,s.market_topic_id,s.market_id FROM prediction_loops l "
                        "JOIN prediction_regime_slots s ON s.loop_id=l.loop_id "
                        "WHERE l.strategy_profile=? AND l.mode='LIVE' AND l.state='RUNNING' "
                        "AND s.market_start_ms=? AND s.verified_at_ms IS NOT NULL", (PROFILE, start)).fetchone()
        if row is None:
            return 'no_active_verified_loop'
        unit_row = p.execute("SELECT config_value_json FROM prediction_runtime_config "
                             "WHERE config_key='prediction_selected_order_unit'").fetchone()
    if unit_row is None:
        return 'shadow_unit_missing'
    unit = dec(json.loads(unit_row[0])['order_unit_usdt'])
    if unit not in (1, 2, 3):
        raise ValueError('shadow_unit_invalid')
    books, spots = read_inputs(signal_db, start, at_ms)
    if not books:
        return 'shadow_book_missing'
    book = books[-1]
    market = SimpleNamespace(start_time_ms=start, market_topic_id=row[1], up_market_id=row[2])
    stamp = RegimeWorkerBridge._book(book, market, at_ms)
    if at_ms-stamp > 1000 or int(book['reference_received_ms']) > at_ms or dec(book['reference']) <= 0:
        raise ValueError('shadow_reference_or_book_stale')
    if not _usable_depth(book):
        return 'shadow_book_depth_unavailable'
    # Invalid historical depth is missing evidence, never a manufactured
    # trigger or confirmation. Keep the valid book clocks unchanged.
    prior_books = [snapshot for snapshot in books[:-1] if _usable_depth(snapshot)]
    schema(db)
    prior = db.execute('SELECT payload FROM t67c_shadow_states WHERE start=?', (start,)).fetchone()
    state = json.loads(prior[0]) if prior else dict(
        fingerprint=FINGERPRINT, loop_id=row[0], market_topic=row[1], market_id=row[2],
        market_start_ms=start, market_end_ms=start+300000, end_ms=start+300000,
        unit_usdt=str(unit), fee_bps=str(book['fee_bps']))
    if (state['fingerprint'] != FINGERPRINT or state['loop_id'] != row[0]
            or state['market_topic'] != row[1] or state['market_id'] != row[2]
            or dec(state['unit_usdt']) != unit or dec(state['fee_bps']) != dec(book['fee_bps'])):
        raise ValueError('shadow_frozen_identity_unit_fee_mismatch')
    # No feature input means the public-model observer cannot manufacture any
    # old core, shallow, C-UP or Live eligibility.
    choices = candidates(book, spots, None, state, at_ms, unit, prior_books)
    with db:
        for c in choices:
            if c['branch'] not in SHADOW_BRANCHES:
                continue
            ex = execution(book, c['side'], unit, c['probability'], cap=c['cap'], lower=c['lower'])
            quote = dict(fingerprint=FINGERPRINT, loop_id=row[0], branch=c['branch'],
                         market_topic=row[1], market_id=row[2], market_start_ms=start,
                         market_end_ms=start+300000, end_ms=start+300000, unit_usdt=str(unit),
                         side=c['side'], quoted_at_ms=at_ms, book_at_ms=stamp,
                         source_receive_ms=int(book['received_at_ms']), captured_at_ms=int(book['captured_at_ms']),
                         reference=str(book['reference']), fee_bps=str(book['fee_bps']),
                         cash=str(ex['cash']), net_shares=str(ex['net_shares']), cap=c['cap'],
                         probability=c['probability'], model=c.get('model'), fill_status='PAPER_QUOTE_ONLY')
            db.execute('INSERT OR IGNORE INTO t67c_shadow_quotes VALUES(?,?,?)',
                       (start, c['branch'], json.dumps(quote, sort_keys=True, allow_nan=False)))
        state['last_evaluated_ms'] = at_ms
        db.execute('INSERT INTO t67c_shadow_states VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(state, sort_keys=True, allow_nan=False)))
    return 'shadow_observed'


async def resolve_once(db, at_ms, fetch):
    """One official readonly detail; immutable confirmed winner, no Live DB writes."""
    from .models import MarketInfo
    from .worker import PredictionWorker
    schema(db)
    row = db.execute('SELECT q.start,q.payload FROM t67c_shadow_quotes q '
                     'LEFT JOIN t67c_shadow_outcomes o ON o.start=q.start '
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
        db.execute('INSERT INTO t67c_shadow_outcomes VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(value, sort_keys=True)))
    return 'resolved' if value['complete'] else 'pending'
