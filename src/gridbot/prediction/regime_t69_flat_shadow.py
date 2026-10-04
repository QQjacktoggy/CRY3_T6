"""T6.9 Flat F2-F4 paper routes; feature DB writes only, never an order or claim.

Each route freezes the first observed valid checkpoint and judges it once.
Quotes share ``t69_shadow_quotes`` so official resolution and the report
treat them like the other T6.9 Shadow branches.
"""
import json
from decimal import Decimal

from .regime_lane import dec
from .regime_t67_lane import execution
from .regime_t69_policy import FINGERPRINT, POLICY

RULES = POLICY['flat_shadow']
CHECKPOINTS = {
    'initial': (124000, 126000),
    'confirmation': (128000, 129500),
    'hold_initial': (120000, 121500),
    'hold_confirmation': (180000, 181500),
}
# A route is final once its last needed window has passed.
DEADLINES = {'flat_quiet_favorite': 129500, 'flat_cheap_prior': 134500, 'flat_hold_180': 181500}


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t69_flat_shadow_states(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')


def _valid_book(book, identity):
    """Same freshness, identity and depth gates as the Reference checkpoints."""
    from .regime_t69_reference import _book_clock, _identity_book
    from .regime_t69_shadow import _usable_depth
    try:
        if not _identity_book(book, identity) or not _usable_depth(book):
            return None
        captured = int(book['captured_at_ms'])
        stamp = _book_clock(book, captured)
        for side in ('UP', 'DOWN'):
            quote = book['quote'][side]
            if 'ask' in quote and dec(quote['ask']) != dec(quote['ask_levels'][0][0]):
                return None
        return stamp
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return None


def _summary(book, stamp):
    asks = {side: str(dec(book['quote'][side]['ask_levels'][0][0])) for side in ('UP', 'DOWN')}
    return dict(captured_at_ms=int(book['captured_at_ms']), book_at_ms=stamp,
                source_receive_ms=int(book['received_at_ms']), fee_bps=str(book['fee_bps']),
                reference=str(book['reference']), asks=asks)


def _favorite(point):
    up, down = dec(point['asks']['UP']), dec(point['asks']['DOWN'])
    if up == down:
        return None
    return 'UP' if up > down else 'DOWN'


def _cheaper(point):
    return 'UP' if dec(point['asks']['UP']) < dec(point['asks']['DOWN']) else 'DOWN'


def _spot_at(spots, boundary, gap, at_ms):
    """Latest causal Binance spot event at or before a minute boundary."""
    from .regime_t69_reference import _causal_spots
    rows = [s for s in _causal_spots(spots, at_ms) if boundary-gap <= s['event_ms'] <= boundary]
    rows.sort(key=lambda s: (s['event_ms'], s['received_ms']))
    return dec(rows[-1]['price']) if rows else None


def _core(decision, identity):
    """Only a verified empty T6.9 core proves that a Flat route may be studied."""
    if not isinstance(decision, dict):
        return None, 'core_decision_missing'
    if (decision.get('fingerprint') != FINGERPRINT or decision.get('loop_id') != identity['loop_id']
            or decision.get('market_start_ms') != identity['market_start_ms']
            or decision.get('market_id') != identity['market_id']):
        return None, 'core_identity_mismatch'
    guard = decision.get('core_guard')
    if not isinstance(guard, dict) or guard.get('verified') is not True:
        return None, 'core_unverified'
    if guard.get('empty') is not True or guard.get('candidates') != []:
        return None, 'core_nonempty'
    try:
        features = guard['features']
        moves = {k: dec(features[k]) for k in ('first_bp', 'last_bp', 'prior_bp')}
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None, 'core_features_invalid'
    first, last = moves['first_bp'], moves['last_bp']
    moves['net_bp'] = ((1+first/10000)*(1+last/10000)-1)*10000
    return moves, None


def _quote(identity, branch, book, point, side, unit, low, high, extra):
    ex = execution(book, side, unit, None, cap=high, lower=low)
    return dict(fingerprint=FINGERPRINT, loop_id=identity['loop_id'], branch=branch,
                market_topic=identity['market_topic'], market_id=identity['market_id'],
                market_start_ms=identity['market_start_ms'], market_end_ms=identity['market_end_ms'],
                end_ms=identity['market_end_ms'], unit_usdt=str(unit), side=side,
                quoted_at_ms=point['captured_at_ms'], book_at_ms=point['book_at_ms'],
                source_receive_ms=point['source_receive_ms'], captured_at_ms=point['captured_at_ms'],
                reference=point['reference'], fee_bps=point['fee_bps'], cash=str(ex['cash']),
                net_shares=str(ex['net_shares']), cap=str(ex['limit']), lower=low, upper=high,
                probability=None, model=None, fill_status='PAPER_QUOTE_ONLY', **extra)


def _pending(points, name):
    return name not in points


def _same_fee(*points):
    return len({dec(p['fee_bps']) for p in points}) == 1


def _quiet(route, moves, points, books, identity, unit):
    rule = RULES['flat_quiet_favorite']
    first, last, net, prior = (moves[k] for k in ('first_bp', 'last_bp', 'net_bp', 'prior_bp'))
    if abs(first) < dec('0.5') and abs(last) < dec('0.5'):
        return 'flat_favorite_state'
    limit = dec(rule['minute_abs_max_exclusive_bp'])
    if (not abs(first) < limit or not abs(last) < limit
            or not abs(net) < dec(rule['net_abs_max_exclusive_bp'])
            or not abs(prior) < dec(rule['prior_abs_max_exclusive_bp'])):
        return 'not_quiet'
    name = 'initial'
    initial, confirmation = points.get('initial'), points.get('confirmation')
    if initial is None:
        # Not yet observed stays pending; a passed window is final.
        return None if _pending(points, name) else 'initial_missing'
    if confirmation is None:
        return None
    if not _same_fee(initial, confirmation):
        return 'checkpoint_fee_changed'
    side = _favorite(initial)
    if side is None or side != _favorite(confirmation):
        return 'favorite_tie_or_changed'
    route['quote'] = _quote(identity, 'flat_quiet_favorite', books['confirmation'], confirmation,
                            side, unit, *rule['price_band'], {})
    return 'quoted'


def _cheap(route, moves, points, books, identity, unit, latest):
    rule = RULES['flat_cheap_prior']
    net, prior = moves['net_bp'], moves['prior_bp']
    if not abs(net) < dec(rule['net_abs_max_exclusive_bp']):
        return 'not_quiet'
    name = 'initial'
    initial = points.get('initial')
    if initial is None:
        # Not yet observed stays pending; a passed window is final.
        return None if _pending(points, name) else 'initial_missing'
    side = _cheaper(initial)
    floor = dec(rule['prior_abs_min_bp'])
    if not ((side == 'UP' and prior >= floor) or (side == 'DOWN' and prior <= -floor)):
        return 'cheaper_side_not_aligned_with_prior'
    if latest is None:
        return None
    book, point = latest
    low, high = rule['quote_ms']
    start = identity['market_start_ms']
    if not start+low <= point['captured_at_ms'] <= start+high or not _same_fee(initial, point):
        return None
    try:
        route['quote'] = _quote(identity, 'flat_cheap_prior', book, point, side, unit,
                                *rule['price_band'], dict(initial_captured_at_ms=initial['captured_at_ms']))
    except (ValueError, KeyError, TypeError, ArithmeticError):
        route['checked_books'] = route.get('checked_books', 0)+1
        return None
    return 'quoted'


def _hold(route, moves, points, books, identity, unit, spots, at_ms):
    rule = RULES['flat_hold_180']
    limit = dec(rule['minute_abs_max_exclusive_bp'])
    if not abs(moves['first_bp']) < limit or not abs(moves['last_bp']) < limit:
        return 'not_flat'
    name = 'hold_initial'
    initial, confirmation = points.get('hold_initial'), points.get('hold_confirmation')
    if initial is None:
        # Not yet observed stays pending; a passed window is final.
        return None if _pending(points, name) else 'initial_missing'
    if confirmation is None:
        return None
    start = identity['market_start_ms']
    gap = rule['spot_max_gap_ms']
    opening, closing = _spot_at(spots, start+120000, gap, at_ms), _spot_at(spots, start+180000, gap, at_ms)
    if opening is None or closing is None or opening <= 0:
        return 'third_minute_spot_missing'
    third = (closing/opening-1)*10000
    route['third_bp'] = str(third)
    if not abs(third) < limit:
        return 'third_minute_moved'
    if not _same_fee(initial, confirmation):
        return 'checkpoint_fee_changed'
    side = _favorite(initial)
    if side is None or side != _favorite(confirmation):
        return 'favorite_tie_or_changed'
    route['quote'] = _quote(identity, 'flat_hold_180', books['hold_confirmation'], confirmation,
                            side, unit, *rule['price_band'], dict(third_bp=str(third)))
    return 'quoted'


def observe(db, identity, books, spots, at_ms, unit, core_decision):
    """Freeze checkpoints, judge each route once, and write first paper quotes."""
    start = identity['market_start_ms']
    schema(db)
    row = db.execute('SELECT payload FROM t69_flat_shadow_states WHERE start=?', (start,)).fetchone()
    state = json.loads(row[0]) if row else dict(
        fingerprint=FINGERPRINT, loop_id=identity['loop_id'], market_start_ms=start,
        market_id=identity['market_id'], points={}, routes={})
    if (state['fingerprint'] != FINGERPRINT or state['loop_id'] != identity['loop_id']
            or state['market_id'] != identity['market_id']):
        raise ValueError('flat_shadow_frozen_identity_mismatch')
    valid = []
    for book in books:
        if isinstance(book, dict):
            stamp = _valid_book(book, identity)
            if stamp is not None:
                valid.append((book, _summary(book, stamp)))
    chosen = {}
    for name, (low, high) in CHECKPOINTS.items():
        if name in state['points']:
            continue
        hits = [(b, p) for b, p in valid if start+low <= p['captured_at_ms'] <= start+high]
        if hits:
            chosen[name] = hits[0]
            state['points'][name] = hits[0][1]
        elif at_ms > start+high:
            state['points'][name] = None
    points = state['points']
    checkpoint_books = {name: pair[0] for name, pair in chosen.items()}
    latest = valid[-1] if valid else None
    moves, gate = _core(core_decision, identity)
    for branch, deadline in DEADLINES.items():
        route = state['routes'].setdefault(branch, {})
        if route.get('terminal'):
            continue
        if moves is None:
            # The Live bridge may not have frozen its core yet.
            if at_ms > start+126500 or gate not in ('core_decision_missing', 'core_unverified'):
                route.update(terminal=True, reason=gate)
            continue
        needed = dict(flat_quiet_favorite='confirmation', flat_hold_180='hold_confirmation').get(branch)
        if needed and points.get(needed) and needed not in checkpoint_books:
            # The frozen checkpoint was seen on an earlier tick; its book is
            # gone, so it is judged now from what was frozen then.
            route.update(terminal=True, reason='checkpoint_book_unavailable')
            continue
        try:
            if branch == 'flat_quiet_favorite':
                reason = _quiet(route, moves, points, checkpoint_books, identity, unit)
            elif branch == 'flat_cheap_prior':
                reason = _cheap(route, moves, points, checkpoint_books, identity, unit, latest)
            else:
                reason = _hold(route, moves, points, checkpoint_books, identity, unit, spots, at_ms)
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            reason = 'checkpoint_rejected:'+_rejection(exc)
        if reason is None and at_ms > start+deadline:
            reason = 'no_executable_quote' if route.get('checked_books') else 'checkpoint_missing'
        if reason is not None:
            route.update(terminal=True, reason=reason)
        if reason == 'quoted':
            quote = route.pop('quote')
            quote['live_branch'] = core_decision.get('branch') if core_decision.get('selected') else None
            route['side'] = quote['side']
            db.execute('INSERT OR IGNORE INTO t69_shadow_quotes VALUES(?,?,?)',
                       (start, branch, json.dumps(quote, sort_keys=True, allow_nan=False)))
    state['last_evaluated_ms'] = at_ms
    db.execute('INSERT INTO t69_flat_shadow_states VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
               (start, json.dumps(state, sort_keys=True, allow_nan=False)))
    db.commit()
    return state


def _rejection(exc):
    return {'best ask below price band': 'price_below_lower', 'price band': 'price_band',
            'insufficient requested depth': 'insufficient_depth'}.get(str(exc), 'input_invalid')


def breakeven(cash, shares):
    """Win rate at which a paper quote's fee-net cost returns zero."""
    return cash/shares if shares > 0 else Decimal(0)
