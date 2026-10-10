"""Read-only paper scores from the market recorder, with rules frozen 2026-10-10.

Run from the release root (every database is opened with ``mode=ro``)::

    python3 -m scripts.market_recorder_report \\
        --recorder-db prediction/data/c180-favorite-live/market-recorder.sqlite3 \\
        --prediction-db prediction/data/prediction.sqlite3 \\
        --feature-db prediction/data/regime-target6/features.sqlite3

The rules are pre-registered; do not re-tune them on the data they score.

* FAVOURITE: at 128.0 s (124.0 s reported as a secondary check), when the
  higher-priced side's best ask is in [0.55, 0.70), buy 1 U of that side.
  Scored on the recorded asks moved up one tick (0.01) for fill slippage, with
  the market's share fee. Reported for all markets, markets with no live
  T6.9b entry, and markets where T6.9b bought the same or the opposite side.
* CHASE: a live T6.9b fill whose average price is at least 0.02 above its
  side's best ask in the last recorded sample at or before the decision.
* MASKED: the first executable quote of each masked T6.9b lane
  (``masked_first_quotes``, written from 2026-10-10) at would_cash and
  would_net_shares. Older last-tick quotes are not scored.

Winner: the official one (settlements, observer, shadow outcomes) when known,
else the reference chain: the next market's Chainlink start price against this
market's (equal is a DRAW). Markets with neither stay pending.
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from src.gridbot.prediction.market_recorder import iter_markets
from src.gridbot.prediction.regime_lane import walk


SLOT_MS = 300_000
TW = timezone(timedelta(hours=8))
TICK = Decimal('0.01')
FAVOURITE_BAND = (Decimal('0.55'), Decimal('0.70'))
CHASE = Decimal('0.02')


def _ro(path):
    return closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=5))


def _tables(db):
    return {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _tw_day(ms):
    return datetime.fromtimestamp(ms / 1000, TW).strftime('%Y-%m-%d')


def official_winners(prediction_db, feature_db, prefix):
    out = {}
    if prediction_db:
        with _ro(prediction_db) as db:
            names = _tables(db)
            if 'prediction_shadow_observer_markets' in names:
                for start, w in db.execute('SELECT start_time_ms,winner FROM prediction_shadow_observer_markets '
                                           'WHERE slug LIKE ? AND winner IS NOT NULL', (prefix + '-%',)):
                    out[int(start)] = w
            for start, w in db.execute(
                    'SELECT c.start_time_ms,s.winner FROM prediction_settlements s JOIN prediction_campaigns c '
                    'ON c.campaign_id=s.campaign_id WHERE c.slug LIKE ? AND s.winner IS NOT NULL', (prefix + '-%',)):
                out.setdefault(int(start), w)
    if feature_db:
        with _ro(feature_db) as db:
            for table in sorted(t for t in _tables(db) if t.endswith('_shadow_outcomes')):
                for start, raw in db.execute(f'SELECT start,payload FROM "{table}"'):
                    w = json.loads(raw).get('winner')
                    if w in ('UP', 'DOWN', 'DRAW'):
                        out.setdefault(int(start), w)
    return out


def campaign_references(prediction_db, prefix):
    out = {}
    if not prediction_db:
        return out
    with _ro(prediction_db) as db:
        for start, raw in db.execute('SELECT start_time_ms,payload_json FROM prediction_campaigns WHERE slug LIKE ?',
                                     (prefix + '-%',)):
            try:
                ref = json.loads(raw)['market'].get('reference_price')
            except (TypeError, ValueError, KeyError, AttributeError):
                continue
            if ref:
                out[int(start)] = Decimal(str(ref))
    return out


def winner_of(start, official, refs):
    if start in official:
        return official[start], 'official'
    a, b = refs.get(start), refs.get(start + SLOT_MS)
    if a is None or b is None:
        return None, 'pending'
    return ('UP' if b > a else 'DOWN' if b < a else 'DRAW'), 'reference_chain'


def pnl(winner, side, cash, net):
    if winner == 'DRAW':
        return net / 2 - cash
    return (net if winner == side else Decimal(0)) - cash


def sample_at(market, offset_ms):
    for s in market['samples']:
        if s['o'] == offset_ms:
            return s if s.get('q') and not s.get('stale') else None
    return None


def favourite_entry(market, offset_ms):
    """Rule FAVOURITE at one offset: None (no signal), (side, None, None) when the
    band matched but the recorded top levels cannot fill 1 U, else (side, cash, net_shares)."""
    s = sample_at(market, offset_ms)
    if s is None:
        return None
    asks = {side: s['q'][side]['ask'] for side in ('UP', 'DOWN')}
    if any(v is None for v in asks.values()):
        return None
    side = 'UP' if Decimal(asks['UP']) >= Decimal(asks['DOWN']) else 'DOWN'
    if not FAVOURITE_BAND[0] <= Decimal(asks[side]) < FAVOURITE_BAND[1]:
        return None
    merged = {}
    for p, q in s['q'][side]['asks']:
        price = min(Decimal(p) + TICK, Decimal('0.99'))
        merged[price] = merged.get(price, Decimal(0)) + Decimal(q)
    try:
        ex = walk(sorted(merged.items()), market['market'].get('fee_bps') or '200', cap=Decimal('0.99'))
    except ValueError:
        return side, None, None
    return side, ex['cash'], ex['net_shares']


def live_entries(prediction_db, feature_db, prefix, since_ms=0):
    """T6.9b selections with their fill average price and settled PnL."""
    out = {}
    if not feature_db:
        return out
    with _ro(feature_db) as db:
        if 't69a_decisions' not in _tables(db):
            return out
        for start, raw in db.execute('SELECT start,payload FROM t69a_decisions WHERE start>=?', (int(since_ms),)):
            d = json.loads(raw)
            entry = dict(masked=d.get('masked_first_quotes') or {})
            if d.get('selected') and d.get('side') in ('UP', 'DOWN'):
                entry.update(side=d['side'], branch=d.get('branch'), selected_at_ms=d.get('selected_at_ms'))
            if 'side' in entry or entry['masked']:
                out[int(start)] = entry
    if prediction_db:
        with _ro(prediction_db) as db:
            for start, entry in out.items():
                cid = f'{prefix}-updown-5m-{start // 1000}'
                shares, gross = db.execute(
                    "SELECT coalesce(sum(CAST(shares AS REAL)),0),coalesce(sum(CAST(gross_amount AS REAL)),0) "
                    "FROM prediction_fills WHERE campaign_id=? AND order_side='BUY'", (cid,)).fetchone()
                if shares:
                    entry['fill_price'] = Decimal(str(gross)) / Decimal(str(shares))
                row = db.execute('SELECT net_pnl FROM prediction_settlements WHERE campaign_id=?', (cid,)).fetchone()
                if row and row[0] is not None and shares:
                    entry['net_pnl'] = Decimal(str(row[0]))
    return out


def summarize(rows, seed=20261010, draws=2000):
    """rows: (start, pnl). Bootstrap over Taiwan days for a 95% interval."""
    n = len(rows)
    total = sum((p for _, p in rows), Decimal(0))
    out = dict(n=n, pnl=str(total.quantize(Decimal('0.0001'))),
               per_trade=str((total / n).quantize(Decimal('0.0001'))) if n else None)
    days = {}
    for start, p in rows:
        days.setdefault(_tw_day(start), []).append(float(p))
    if n and len(days) >= 2:
        rng, groups, sums = random.Random(seed), list(days.values()), []
        for _ in range(draws):
            pick = [groups[rng.randrange(len(groups))] for _ in groups]
            k = sum(len(g) for g in pick)
            sums.append(sum(sum(g) for g in pick) / k)
        sums.sort()
        out['per_trade_ci95'] = [round(sums[int(0.025 * draws)], 4), round(sums[int(0.975 * draws) - 1], 4)]
    return out


def build_report(recorder_db, prediction_db=None, feature_db=None, *, symbol='BTCUSDT', since_ms=0):
    prefix = symbol[:-4].lower()
    official = official_winners(prediction_db, feature_db, prefix)
    refs = campaign_references(prediction_db, prefix)
    live = live_entries(prediction_db, feature_db, prefix, since_ms)
    # One pass, keeping only what the rules need: months of rows do not fit in memory.
    starts, favourites, asks = [], {128_000: {}, 124_000: {}}, {}
    for m in iter_markets(recorder_db, since_ms):
        if m.get('symbol') != symbol:
            raise SystemExit(f"recorder symbol {m.get('symbol')} != --symbol {symbol}")
        start = int(m['market']['start'])
        starts.append(start)
        refs.setdefault(start, Decimal(m['market']['reference']))
        for offset, out in favourites.items():
            entry = favourite_entry(m, offset)
            if entry is not None:
                out[start] = entry
        if (live.get(start) or {}).get('side'):
            asks[start] = [(s['at'], s['q']) for s in m['samples'] if s.get('q') and not s.get('stale')]
    basis = {start: winner_of(start, official, refs) for start in starts}
    report = dict(symbol=symbol, markets=len(starts), favourite={}, chase={}, masked={},
                  winners={k: sum(1 for _, b in basis.values() if b == k)
                           for k in ('official', 'reference_chain', 'pending')})
    for offset, entries in favourites.items():
        groups = {'all': [], 'no_live_fill': [], 'live_fill_same_side': [], 'live_fill_opposite_side': []}
        unexecutable = pending = 0
        for start, (side, cash, net) in entries.items():
            if cash is None:
                unexecutable += 1
                continue
            winner = basis[start][0]
            if winner is None:
                pending += 1
                continue
            row = (start, pnl(winner, side, cash, net))
            groups['all'].append(row)
            le = live.get(start) or {}
            filled = le.get('side') if 'fill_price' in le else None
            key = ('no_live_fill' if filled is None else 'live_fill_same_side' if filled == side
                   else 'live_fill_opposite_side')
            groups[key].append(row)
        report['favourite'][f'{offset // 1000}s'] = dict(
            signals=len(entries), unexecutable=unexecutable, pending=pending,
            **{k: summarize(v) for k, v in groups.items()})
    flagged, normal, unknown = [], [], 0
    for start, entry in live.items():
        if not entry.get('side') or 'net_pnl' not in entry or entry.get('selected_at_ms') is None:
            continue
        prior = [q for at, q in asks.get(start, ()) if at <= int(entry['selected_at_ms'])]
        ask = prior[-1][entry['side']]['ask'] if prior else None
        if ask is None:
            unknown += 1
            continue
        (flagged if entry['fill_price'] >= Decimal(ask) + CHASE else normal).append((start, entry['net_pnl']))
    report['chase'] = dict(flagged=summarize(flagged), others=summarize(normal), no_recorded_ask=unknown)
    masked, masked_pending = [], 0
    for start, entry in live.items():
        winner = winner_of(start, official, refs)[0]
        for token, q in entry['masked'].items():
            if winner is None:
                masked_pending += 1
                continue
            side = token.rsplit(':', 1)[1]
            masked.append((start, pnl(winner, side, Decimal(q['would_cash']), Decimal(q['would_net_shares']))))
    report['masked'] = dict(pending=masked_pending, **summarize(masked))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description='Read-only market recorder paper scores')
    parser.add_argument('--recorder-db', required=True, type=Path)
    parser.add_argument('--prediction-db', type=Path)
    parser.add_argument('--feature-db', type=Path)
    parser.add_argument('--symbol', default='BTCUSDT', choices=('BTCUSDT', 'ETHUSDT', 'BNBUSDT'))
    parser.add_argument('--since', help='Taiwan date YYYY-MM-DD (market start)')
    args = parser.parse_args(argv)
    since_ms = 0
    if args.since:
        since_ms = int(datetime.strptime(args.since, '%Y-%m-%d').replace(tzinfo=TW).timestamp() * 1000)
    report = build_report(args.recorder_db, args.prediction_db, args.feature_db,
                          symbol=args.symbol, since_ms=since_ms)
    print(json.dumps(report, ensure_ascii=False, indent=1, sort_keys=True))


if __name__ == '__main__':
    main()
