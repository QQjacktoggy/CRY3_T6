"""T6.9a R* late near-certain favourite (BTC 5m); paper quotes only, never an order.

Mirrors the 2026-10-05 backtest (R_STAR_SHADOW_SPEC): every book observed at
270-295s is checked; the favourite is the side with the higher best ask; it
fires when that ask is .90-.98 and Binance spot sits at least 3 remaining-time
standard deviations past the price-to-beat on the favourite's side. Sigma is the
pre-market rv60 from 60 one-minute Binance klines, fetched once per market.
One paper quote per market per arm: the first qualifying book.

The paper PnL uses the backtest's top-of-book cost. The real ask levels are also
walked with the limit at that ask, recording whether 1U would have filled.

R* has its own policy dict and fingerprint so the T6.9a Live fingerprint, risk
state, MDD and HS stay untouched. Quotes share ``t69a_shadow_quotes`` (stamped
with the T6.9a fingerprint) so official resolution and the T6.9a report treat
them like the Flat F1-F4 paper routes.
"""
import hashlib
import json
import math
import urllib.parse
import urllib.request
from decimal import Decimal

from .regime_lane import dec, walk
from .regime_t69a_policy import FINGERPRINT

BRANCH = 'late_favourite_chase'
BRANCH_99 = 'late_favourite_chase_99'
RSTAR_POLICY = dict(
    version=2, base_fingerprint=FINGERPRINT, mode='shadow_paper_quote_only',
    spec='R_STAR_SHADOW_SPEC_20261005', markets=('BTCUSDT',),
    # Book observed offset o = captured_at_ms - market start; [start, end).
    window_ms=[270000, 295000],
    # Observation keeps running for one tick after the window to finalize.
    finalize_ms=296000,
    favourite='higher_best_ask_tie_up',
    tau='remaining_seconds_(300000-o)/1000',
    sigma='rv60_bp/sqrt(60)',
    rv60='sqrt(mean((ln(close/open)*1e4)^2)) over 60 Binance 1m klines opening in [start-3600s, start-60s]',
    rv60_bars=60, rv60_fetch_attempts=3,
    z='ln(spot/ref)*1e4/(sigma*sqrt(tau)); z_fav=z if UP else -z',
    spot='latest_causal_binance_spot_at_book_observed', spot_max_age_ms=1500,
    ref='market_reference_price', ref_min='10000', ref_max_abs_log_distance='0.01',
    fee_bps_on_shares=200, cost='ask/(1-fee*min(ask,1-ask)/ask)', stake_usdt='1',
    depth='walk_real_ask_levels_limit_ask_record_fillable_not_gating',
    arms={
        BRANCH: dict(enabled=True, ask_min='0.90', ask_max='0.98', z_min='3'),
        BRANCH_99: dict(enabled=True, ask_min='0.99', ask_max='0.99', z_min='5'),
    },
    one_quote_per_market_per_arm='first_qualifying_book',
)
RSTAR_FINGERPRINT = hashlib.sha256(json.dumps(RSTAR_POLICY, sort_keys=True).encode()).hexdigest()
WINDOW = tuple(RSTAR_POLICY['window_ms'])
FINALIZE_MS = RSTAR_POLICY['finalize_ms']
ARMS = tuple(name for name, arm in RSTAR_POLICY['arms'].items() if arm['enabled'])


def in_window(offset_ms):
    return WINDOW[0] <= offset_ms < FINALIZE_MS


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t69a_rstar_states(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')


def fetch_klines(start, symbol='BTCUSDT'):
    """60 pre-market 1m klines from Binance public REST, bounded body."""
    from .http_bounds import KLINES_BODY_BYTES, read_bounded
    params = urllib.parse.urlencode({'symbol': symbol, 'interval': '1m', 'startTime': start-3600000,
                                     'endTime': start-1, 'limit': RSTAR_POLICY['rv60_bars']})
    with urllib.request.urlopen('https://api.binance.com/api/v3/klines?'+params, timeout=1.5) as response:
        return json.loads(read_bounded(response, response.headers, KLINES_BODY_BYTES))


def rv60_bp(klines, start):
    """Open-to-close bp RMS of the 60 one-minute bars before the market."""
    bars = {}
    for k in klines:
        opened = int(k[0])
        if start-3600000 <= opened <= start-60000 and opened % 60000 == 0:
            o, c = float(k[1]), float(k[4])
            if not (o > 0 and c > 0 and math.isfinite(o) and math.isfinite(c)):
                raise ValueError('rv60_bar_invalid')
            bars[opened] = math.log(c/o)*1e4
    if len(bars) != RSTAR_POLICY['rv60_bars']:
        raise ValueError('rv60_bars_missing')
    value = math.sqrt(sum(r*r for r in bars.values())/len(bars))
    if not value > 0:
        raise ValueError('rv60_zero')
    return value


def z_fav(spot, ref, rv60, o_ms, fav):
    d_bp = math.log(float(spot)/float(ref))*1e4
    tau = (300000-o_ms)/1000
    z = d_bp/(rv60/math.sqrt(60)*math.sqrt(tau))
    return (z if fav == 'UP' else -z), d_bp, tau


def cost_per_share(ask, fee_bps=RSTAR_POLICY['fee_bps_on_shares']):
    fee = Decimal(fee_bps)/10000
    return ask/(1-fee*min(ask, 1-ask)/ask)


def _spot_for(rows, observed_ms):
    """Latest causal spot received by the book's observed time, at most 1.5s old."""
    hits = [s for s in rows if s['received_ms'] <= observed_ms]
    if not hits:
        return None
    spot = max(hits, key=lambda s: (s['event_ms'], s['received_ms']))
    if observed_ms-spot['event_ms'] > RSTAR_POLICY['spot_max_age_ms']:
        return None
    return spot


def _miss(state, arm, name):
    misses = state['arms'][arm].setdefault('near_misses', {})
    misses[name] = misses.get(name, 0)+1


def _observed(book):
    try:
        return int(book['captured_at_ms'])
    except (KeyError, TypeError, ValueError):
        return -1


def observe(db, identity, books, spots, at_ms, core_decision, klines):
    """Freeze rv60 once, then judge each new book and paper-quote first signals.

    ``klines(start)`` returns the raw 1m klines; it is called only until rv60
    is frozen, at most ``rv60_fetch_attempts`` times per market.
    """
    from .regime_t69_reference import _causal_spots
    start = identity['market_start_ms']
    schema(db)
    row = db.execute('SELECT payload FROM t69a_rstar_states WHERE start=?', (start,)).fetchone()
    state = json.loads(row[0]) if row else dict(
        fingerprint=FINGERPRINT, rstar_fingerprint=RSTAR_FINGERPRINT, loop_id=identity['loop_id'],
        market_start_ms=start, market_id=identity['market_id'], books_checked=0, last_book_ms=0,
        rv60_attempts=0, arms={arm: {} for arm in ARMS})
    if (state['fingerprint'] != FINGERPRINT or state['rstar_fingerprint'] != RSTAR_FINGERPRINT
            or state['loop_id'] != identity['loop_id'] or state['market_id'] != identity['market_id']):
        raise ValueError('rstar_frozen_identity_mismatch')
    open_arms = [a for a in ARMS if not state['arms'][a].get('terminal')]
    if not open_arms:
        return state
    if 'rv60_bp' not in state and at_ms-start >= WINDOW[0]:
        state['rv60_attempts'] += 1
        try:
            state['rv60_bp'] = repr(rv60_bp(klines(start), start))
        except Exception as exc:
            # The fetch is public market data; only its error class is kept.
            state['rv60_error'] = type(exc).__name__
            if state['rv60_attempts'] >= RSTAR_POLICY['rv60_fetch_attempts']:
                for arm in open_arms:
                    state['arms'][arm].update(terminal=True, reason='rv60_unavailable')
                open_arms = []
    if 'rv60_bp' in state and open_arms:
        rows = _causal_spots(spots, at_ms)
        fresh = sorted((b for b in books if isinstance(b, dict) and _observed(b) > state['last_book_ms']),
                       key=_observed)
        for book in fresh:
            o = _observed(book)-start
            state['last_book_ms'] = _observed(book)
            if not WINDOW[0] <= o < WINDOW[1]:
                continue
            state['books_checked'] += 1
            for arm in list(open_arms):
                if _judge(db, state, arm, book, o, rows, identity, at_ms, core_decision):
                    open_arms.remove(arm)
    if at_ms-start >= WINDOW[1]:
        for arm in open_arms:
            state['arms'][arm].update(terminal=True, reason='no_signal')
    state['last_evaluated_ms'] = at_ms
    db.execute('INSERT INTO t69a_rstar_states VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
               (start, json.dumps(state, sort_keys=True, allow_nan=False)))
    db.commit()
    return state


def _judge(db, state, arm, book, o, rows, identity, at_ms, core_decision):
    """True once this arm has quoted for the market."""
    from .regime_t69a_flat_shadow import _valid_book
    rule = RSTAR_POLICY['arms'][arm]
    stamp = _valid_book(book, identity)
    if stamp is None:
        _miss(state, arm, 'book_invalid')
        return False
    try:
        up, down = (dec(book['quote'][s]['ask_levels'][0][0]) for s in ('UP', 'DOWN'))
        fav = 'UP' if up >= down else 'DOWN'
        ask = up if fav == 'UP' else down
        if not dec(rule['ask_min']) <= ask <= dec(rule['ask_max']):
            _miss(state, arm, 'ask_outside_band')
            return False
        observed = _observed(book)
        spot = _spot_for(rows, observed)
        if spot is None:
            _miss(state, arm, 'spot_stale')
            return False
        ref, price = dec(book['reference']), dec(spot['price'])
        if not ref > dec(RSTAR_POLICY['ref_min']):
            _miss(state, arm, 'ref_invalid')
            return False
        if not abs(math.log(float(price)/float(ref))) < float(RSTAR_POLICY['ref_max_abs_log_distance']):
            _miss(state, arm, 'spot_ref_too_far')
            return False
        z, d_bp, tau = z_fav(price, ref, float(state['rv60_bp']), o, fav)
        best = state['arms'][arm].get('max_z_fav')
        if best is None or z > float(best):
            state['arms'][arm]['max_z_fav'] = f'{z:.4f}'
        if z < float(rule['z_min']):
            _miss(state, arm, 'z_below_min')
            return False
        unit = dec(RSTAR_POLICY['stake_usdt'])
        cost = cost_per_share(ask)
        levels = book['quote'][fav]['ask_levels']
        try:
            ex = walk(levels, book['fee_bps'], cap=ask, amount=unit)
            fill = dict(fillable=True, fill_cash=str(ex['cash']), fill_net_shares=str(ex['net_shares']),
                        fill_limit=str(ex['limit']),
                        fill_cost_per_share=str(ex['cash']/ex['net_shares']) if ex['net_shares'] > 0 else None)
        except (ValueError, ArithmeticError) as exc:
            known = ('insufficient requested depth', 'invalid ask depth', 'unsorted ask depth')
            fill = dict(fillable=False, fill_reason=str(exc) if str(exc) in known else 'walk_failed')
        quote = dict(
            fingerprint=FINGERPRINT, rstar_fingerprint=RSTAR_FINGERPRINT, loop_id=identity['loop_id'],
            branch=arm, market_topic=identity['market_topic'], market_id=identity['market_id'],
            market_start_ms=identity['market_start_ms'], market_end_ms=identity['market_end_ms'],
            end_ms=identity['market_end_ms'], unit_usdt=str(unit), side=fav, o_ms=o,
            quoted_at_ms=observed, book_at_ms=stamp, source_receive_ms=int(book['received_at_ms']),
            captured_at_ms=observed, reference=str(ref), fee_bps=str(book['fee_bps']),
            # Backtest PnL basis: 1U at the top-of-book cost per net share.
            cash=str(unit), net_shares=str(unit/cost), cost_per_share=str(cost), ask=str(ask),
            ask_levels=[[str(dec(p)), str(dec(q))] for p, q in levels[:10]],
            bid=book['quote'][fav].get('bid'), other_ask=str(down if fav == 'UP' else up),
            other_bid=book['quote']['DOWN' if fav == 'UP' else 'UP'].get('bid'),
            spot=str(price), spot_event_ms=spot['event_ms'], spot_received_ms=spot['received_ms'],
            rv60_bp=state['rv60_bp'], d_bp=f'{d_bp:.4f}', tau_s=f'{tau:.3f}', z_fav=f'{z:.4f}',
            evaluated_at_ms=at_ms, eval_latency_ms=at_ms-observed, cap=str(ask),
            lower=rule['ask_min'], upper=rule['ask_max'], probability=None, model=None,
            fill_status='PAPER_QUOTE_ONLY', **fill,
            live_branch=(core_decision.get('branch') if isinstance(core_decision, dict)
                         and core_decision.get('selected') else None))
        payload = json.dumps(quote, sort_keys=True, allow_nan=False)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        _miss(state, arm, 'input_invalid')
        return False
    db.execute('INSERT OR IGNORE INTO t69a_shadow_quotes VALUES(?,?,?)',
               (identity['market_start_ms'], arm, payload))
    state['arms'][arm].update(terminal=True, reason='quoted', side=fav, z_fav=quote['z_fav'],
                              fillable=fill['fillable'])
    return True
