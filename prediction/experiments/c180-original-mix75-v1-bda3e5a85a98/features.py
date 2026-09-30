"""Causal numeric features. All distances/returns are bps; clocks are epoch ms."""
import math
import statistics
from bisect import bisect_right

SCHEMA = 'early10-evidence-v2.1'
NUMERIC = ('market_up', 'distance_bps', 'normalized_distance', 'remaining_seconds',
           'sigma_remaining_bps', 'diffusion_up', 'spot_return_10', 'spot_return_30',
           'futures_return_10', 'futures_return_30', 'basis_bps', 'basis_change_10',
           'basis_change_30', 'basis_zscore', 'spread', 'depth_imbalance',
           'spot_market_conflict', 'basis_cross_reference', 'official_missing')
META_OPTIONS = {'edge': ('UP', 'DOWN', 'NONE'), 'conflict': ('LOW', 'HIGH'),
                'regime': ('TREND', 'REVERSAL', 'CHOP'), 'uncertainty': ('LOW', 'HIGH')}
META = tuple(f'jev_{q}_{option}' for q, options in META_OPTIONS.items() for option in options)


def sample_seconds(rows, cutoff, seconds=120):
    """Previous-tick sampling with a 2s freshness limit, never future interpolation."""
    rows = [r for r in rows if r[0] <= cutoff and r[4] <= cutoff]
    times = [r[0] for r in rows]
    sampled = []
    for at in range(cutoff-seconds*1000, cutoff+1, 1000):
        i = bisect_right(times, at)-1
        sampled.append(rows[i][1] if i >= 0 and at-rows[i][0] <= 2000 and at-rows[i][4] <= 5000 else None)
    return sampled


def market_probability(quote):
    if not quote:
        return None
    mids = []
    for side in ('UP', 'DOWN'):
        bid, ask = quote[side]['bid'], quote[side]['ask']
        if bid is None or ask is None or not 0 < bid <= ask < 1:
            return None
        mids.append((bid+ask)/2)
    return mids[0]/sum(mids)


def build(state, histories):
    cut = state['observed_at']
    spot, futures = (sample_seconds(histories[s], cut) for s in ('spot', 'futures'))
    basis = [(f/s-1)*10000 if s and f else None for s, f in zip(spot, futures)]
    reference = state['contract']['reference']
    dist = (spot[-1]/reference-1)*10000 if spot[-1] and reference else None
    returns = [math.log(b/a)*10000 for a, b in zip(spot, spot[1:]) if a and b]
    coverage = len(returns)/120
    # Explicit research floor, not fitted against recent losing trades.
    sigma_1s = max(.1, statistics.stdev(returns)) if len(returns) >= 60 and coverage >= .8 else None
    remaining = max(0, state['remaining_seconds'])
    sigma = sigma_1s*math.sqrt(remaining) if sigma_1s is not None and remaining else None
    z = dist/sigma if dist is not None and sigma else None
    market = market_probability(state['quote'])
    recent_basis = [x for x in basis[-61:] if x is not None]
    sd = statistics.stdev(recent_basis) if len(recent_basis) >= 30 else 0
    current = basis[-1]
    quote = state['quote']
    depth = None
    if quote:
        up = sum(n for _, n in quote['UP']['bid_levels'][:3])
        down = sum(n for _, n in quote['DOWN']['bid_levels'][:3])
        depth = (up-down)/(up+down) if up+down else None
    official = state.get('official_live') or {}
    fresh = (official.get('source') == 'chainlink' and official.get('verified_feed') is True
             and isinstance(official.get('price'), (int, float)) and math.isfinite(official['price']) and official['price'] > 0
             and all(isinstance(official.get(k), (int, float)) and 0 <= cut-official[k] <= 2000
                     for k in ('source_at', 'received_at')))
    status = dict(official, status='fresh') if fresh else {'status': 'missing_or_stale', 'price': None}
    def ret(source, seconds):
        value = state['features'][source]['recent'].get(str(seconds))
        return value.get('return_bps') if value else None
    result = dict(zip(NUMERIC, [None]*len(NUMERIC)))
    result.update(market_up=market, distance_bps=dist, normalized_distance=z,
                  remaining_seconds=remaining, sigma_remaining_bps=sigma,
                  diffusion_up=(1+math.erf(z/math.sqrt(2)))/2 if z is not None else None,
                  spot_return_10=ret('spot', 10), spot_return_30=ret('spot', 30),
                  futures_return_10=ret('futures', 10), futures_return_30=ret('futures', 30),
                  basis_bps=current,
                  basis_change_10=current-basis[-11] if current is not None and basis[-11] is not None else None,
                  basis_change_30=current-basis[-31] if current is not None and basis[-31] is not None else None,
                  basis_zscore=(current-statistics.mean(recent_basis))/sd if current is not None and sd else None,
                  spread=quote['UP']['ask']-quote['UP']['bid'] if market is not None else None,
                  depth_imbalance=depth,
                  spot_market_conflict=int(dist*(market-.5) < 0) if dist is not None and market is not None else None,
                  basis_cross_reference=int((spot[-1]-reference)*(futures[-1]-reference) < 0)
                    if reference and spot[-1] and futures[-1] else None,
                  official_missing=int(not fresh))
    return {'schema': SCHEMA, 'numeric': result, 'official_live': status,
            'volatility': {'method': '120s previous-tick 1s log returns; sd * sqrt(time)',
                           'coverage': coverage, 'sigma_floor_bps_per_sqrt_second': .1,
                           'calibrated': False},
            'market_baseline_note': 'normalized two-sided mid; market price, not true win probability',
            'basis_note': 'Futures discount is not independent DOWN evidence; basis-adjusted futures duplicate spot.'}


def meta_features(answers):
    return {f'jev_{q}_{o}': answers[q]['probabilities'][o] for q, options in META_OPTIONS.items() for o in options}


def overconfidence(p, numeric):
    if p is None or max(p, 1-p) < .9:
        return False
    market, z = numeric['market_up'], numeric['normalized_distance']
    side = 1 if p >= .5 else -1
    market_support = market is not None and (market if side > 0 else 1-market) >= .7
    distance_support = z is not None and side*z >= 1.28
    return not market_support and not distance_support
