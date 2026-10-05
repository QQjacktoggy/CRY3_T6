"""T6.9a R* late favourite chase; paper quotes only, never an order or claim.

Inside 270-295s, once Binance spot is at least ``z_min`` trailing 1s standard
deviations away from the market-open spot, the favourite (the side spot is on)
is paper-bought at its executable ask if that ask sits inside the price band,
and held to settlement. One quote per market: the first qualifying tick.

R* has its own policy dict and fingerprint so the T6.9a Live fingerprint, risk
state, MDD and HS stay untouched. Quotes share ``t69a_shadow_quotes`` (stamped
with the T6.9a fingerprint) so official resolution and the T6.9a report treat
them like the Flat F1-F4 paper routes.
"""
import hashlib
import json
import math
import sqlite3
from contextlib import closing

from .regime_lane import dec
from .regime_t67_lane import execution
from .regime_t69a_policy import FINGERPRINT

BRANCH = 'late_favourite_chase'
RSTAR_POLICY = dict(
    branch=BRANCH, version=1, base_fingerprint=FINGERPRINT, mode='shadow_paper_quote_only',
    markets=('BTCUSDT',),
    window_ms=[270000, 295000],
    # Observation keeps running for one tick after the window to finalize.
    finalize_ms=296000,
    open_price='binance_spot_last_event_at_or_before_market_start',
    open_max_gap_ms=1500,
    spot_max_age_ms=2000,
    sigma='sample_std_of_1s_log_returns_last_tick_per_second_forward_filled',
    # Pre-open window, so the market's own move cannot inflate sigma. The
    # evidence store keeps 20 minutes of spot; 15 minutes before open stays
    # inside it at 295s.
    sigma_window_ms=900000,
    sigma_window_end='market_open',
    sigma_min_coverage='0.5',
    sigma_freeze='first_tick_in_window',
    z='ln(spot/open)/(sigma_1s*sqrt(elapsed_s_since_open))',
    z_min='3',
    favourite='side_of_spot_vs_open',
    price_band=['0.90', '0.98'],
    stake_usdt='1',
    book_max_age_ms=1500,
    one_quote_per_market='first_qualifying_tick',
)
RSTAR_FINGERPRINT = hashlib.sha256(json.dumps(RSTAR_POLICY, sort_keys=True).encode()).hexdigest()
WINDOW = tuple(RSTAR_POLICY['window_ms'])
FINALIZE_MS = RSTAR_POLICY['finalize_ms']


def in_window(offset_ms):
    return WINDOW[0] <= offset_ms < FINALIZE_MS


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t69a_rstar_states(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')


def _spots(spots, at_ms):
    from .regime_t69_reference import _causal_spots
    rows = _causal_spots(spots, at_ms)
    rows.sort(key=lambda s: (s['event_ms'], s['received_ms']))
    return rows


def open_spot(rows, start, gap=RSTAR_POLICY['open_max_gap_ms']):
    """Latest Binance spot at or before the market open, within ``gap``."""
    hits = [s for s in rows if start-gap <= s['event_ms'] <= start]
    return (dec(hits[-1]['price']), hits[-1]['event_ms']) if hits else (None, None)


def sigma_1s(rows, end_ms, window_ms=RSTAR_POLICY['sigma_window_ms'],
             min_coverage=RSTAR_POLICY['sigma_min_coverage']):
    """Sample std of 1s log returns over ``[end-window, end]``.

    Each second closes at its last tick; seconds without a tick carry the
    previous close forward. Returns ``(sigma, returns, coverage)``.
    """
    first, last = (end_ms-window_ms)//1000, end_ms//1000
    closes = {}
    for s in rows:
        second = s['event_ms']//1000
        if first <= second <= last:
            closes[second] = float(s['price'])
    span = last-first+1
    coverage = len(closes)/span if span > 0 else 0
    if not closes or coverage < float(min_coverage):
        return None, 0, coverage
    returns, previous = [], None
    for second in range(min(closes), last+1):
        price = closes.get(second, previous)
        if previous is not None:
            returns.append(math.log(price/previous))
        previous = price
    if len(returns) < 2:
        return None, len(returns), coverage
    mean = sum(returns)/len(returns)
    var = sum((r-mean)**2 for r in returns)/(len(returns)-1)
    sigma = math.sqrt(var)
    return (sigma if sigma > 0 else None), len(returns), coverage


def z_score(spot, opening, sigma, elapsed_s):
    if opening <= 0 or spot <= 0 or sigma is None or sigma <= 0 or elapsed_s <= 0:
        return None
    return math.log(float(spot)/float(opening))/(sigma*math.sqrt(elapsed_s))


def _latest_book(books, identity, at_ms):
    from .regime_t69a_flat_shadow import _valid_book
    start = identity['market_start_ms']
    for book in reversed(books):
        if not isinstance(book, dict):
            continue
        stamp = _valid_book(book, identity)
        if stamp is None:
            continue
        captured = int(book['captured_at_ms'])
        if (start+WINDOW[0] <= captured <= start+WINDOW[1] and captured <= at_ms
                and at_ms-captured <= RSTAR_POLICY['book_max_age_ms']):
            return book, stamp
        return None
    return None


def _depth(levels, high):
    return [[str(dec(p)), str(dec(q))] for p, q in levels if dec(p) <= dec(high)][:5]


def read_history(signal_db, start, window_ms=RSTAR_POLICY['sigma_window_ms']):
    """Read-only pre-open Binance spot tape for the frozen sigma."""
    from .regime_t67_evidence import evidence_path
    path = evidence_path(signal_db)
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as db:
        db.execute('PRAGMA query_only=ON')
        return [dict(source=r[0], generation=r[1], event_ms=r[2], received_ms=r[3], price=r[4])
                for r in db.execute('SELECT source,generation,event_ms,received_ms,price FROM spot '
                                    'WHERE received_ms BETWEEN ? AND ? ORDER BY received_ms,event_ms',
                                    (start-window_ms-1000, start+1500))]


def observe(db, identity, books, spots, at_ms, core_decision, history):
    """Freeze open/sigma once, then paper-quote the first qualifying tick.

    ``history(start)`` returns the pre-open spot tape; it is read only until
    sigma is frozen.
    """
    start = identity['market_start_ms']
    schema(db)
    row = db.execute('SELECT payload FROM t69a_rstar_states WHERE start=?', (start,)).fetchone()
    state = json.loads(row[0]) if row else dict(
        fingerprint=FINGERPRINT, rstar_fingerprint=RSTAR_FINGERPRINT, loop_id=identity['loop_id'],
        market_start_ms=start, market_id=identity['market_id'], ticks=0, near_misses={})
    if (state['fingerprint'] != FINGERPRINT or state['rstar_fingerprint'] != RSTAR_FINGERPRINT
            or state['loop_id'] != identity['loop_id'] or state['market_id'] != identity['market_id']):
        raise ValueError('rstar_frozen_identity_mismatch')
    if state.get('terminal'):
        return state
    offset = at_ms-start
    reason = None
    if offset > WINDOW[1]:
        reason = 'no_signal'
    elif offset >= WINDOW[0]:
        reason = _tick(db, state, identity, books, spots, at_ms, core_decision, history)
    if reason is not None:
        state.update(terminal=True, reason=reason)
    state['last_evaluated_ms'] = at_ms
    db.execute('INSERT INTO t69a_rstar_states VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
               (start, json.dumps(state, sort_keys=True, allow_nan=False)))
    db.commit()
    return state


def _miss(state, name):
    state['near_misses'][name] = state['near_misses'].get(name, 0)+1


def _tick(db, state, identity, books, spots, at_ms, core_decision, history):
    """Return a terminal reason, or None to keep watching this market."""
    start = identity['market_start_ms']
    if 'sigma_1s' not in state:
        # Open anchor and sigma both come from the pre-open tape, read once.
        tape = _spots(history(start), at_ms)
        opening, event = open_spot(tape, start)
        if opening is None or opening <= 0:
            return 'open_spot_missing'
        state.update(open_price=str(opening), open_event_ms=event)
        sigma, count, coverage = sigma_1s(tape, start)
        if sigma is None:
            # The pre-open tape cannot improve later; judged once.
            state.update(sigma_returns=count, sigma_coverage=f'{coverage:.4f}')
            return 'sigma_unavailable'
        state.update(sigma_1s=repr(sigma), sigma_returns=count, sigma_coverage=f'{coverage:.4f}',
                     sigma_end_ms=start)
    state['ticks'] += 1
    rows = _spots(spots, at_ms)
    if not rows or at_ms-rows[-1]['received_ms'] > RSTAR_POLICY['spot_max_age_ms']:
        _miss(state, 'spot_stale')
        return None
    spot = rows[-1]
    elapsed = (spot['event_ms']-start)/1000
    z = z_score(dec(spot['price']), dec(state['open_price']), float(state['sigma_1s']), elapsed)
    if z is None:
        _miss(state, 'z_invalid')
        return None
    if abs(z) > abs(float(state.get('max_abs_z', 0))):
        state['max_abs_z'] = f'{z:.4f}'
    if abs(z) < float(RSTAR_POLICY['z_min']):
        _miss(state, 'z_below_min')
        return None
    side = 'UP' if z > 0 else 'DOWN'
    found = _latest_book(books, identity, at_ms)
    if found is None:
        _miss(state, 'book_missing_or_stale')
        return None
    book, stamp = found
    low, high = RSTAR_POLICY['price_band']
    unit = dec(RSTAR_POLICY['stake_usdt'])
    try:
        ex = execution(book, side, unit, None, cap=high, lower=low)
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        from .regime_t69a_flat_shadow import _rejection
        _miss(state, _rejection(exc))
        return None
    other = 'DOWN' if side == 'UP' else 'UP'
    levels = book['quote'][side]['ask_levels']
    quote = dict(
        fingerprint=FINGERPRINT, rstar_fingerprint=RSTAR_FINGERPRINT, loop_id=identity['loop_id'],
        branch=BRANCH, market_topic=identity['market_topic'], market_id=identity['market_id'],
        market_start_ms=start, market_end_ms=identity['market_end_ms'], end_ms=identity['market_end_ms'],
        unit_usdt=str(unit), side=side, quoted_at_ms=int(book['captured_at_ms']), book_at_ms=stamp,
        source_receive_ms=int(book['received_at_ms']), captured_at_ms=int(book['captured_at_ms']),
        reference=str(book['reference']), fee_bps=str(book['fee_bps']), cash=str(ex['cash']),
        net_shares=str(ex['net_shares']), cap=str(ex['limit']), lower=low, upper=high,
        best_ask=str(dec(levels[0][0])), other_best_ask=str(dec(book['quote'][other]['ask_levels'][0][0])),
        ask_depth=_depth(levels, high), spot_price=str(dec(spot['price'])), spot_event_ms=spot['event_ms'],
        spot_received_ms=spot['received_ms'], open_price=state['open_price'],
        open_event_ms=state['open_event_ms'], sigma_1s=state['sigma_1s'], elapsed_s=f'{elapsed:.3f}',
        z=f'{z:.4f}', signal_at_ms=at_ms, probability=None, model=None, fill_status='PAPER_QUOTE_ONLY',
        live_branch=(core_decision.get('branch') if isinstance(core_decision, dict)
                     and core_decision.get('selected') else None))
    db.execute('INSERT OR IGNORE INTO t69a_shadow_quotes VALUES(?,?,?)',
               (start, BRANCH, json.dumps(quote, sort_keys=True, allow_nan=False)))
    state.update(side=side, z=quote['z'])
    return 'quoted'
