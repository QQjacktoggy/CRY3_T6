"""T6.3b loop-local drawdown from observed official fee-net settlements."""
from decimal import Decimal

LIMIT_1U = Decimal("3.5")


def loop_drawdown(settlements, *, start_ms, now_ms):
    """Return peak, equity, max DD, trigger ID. Settlement order is knowledge order."""
    equity = peak = maximum = Decimal(0)
    trigger = None
    seen = set()
    for row in sorted(settlements, key=lambda r: (r.known_at_ms, r.settlement_id)):
        if (row.settlement_id in seen or row.known_at_ms > now_ms
                or row.market_start_ms >= start_ms):
            raise ValueError("invalid T6.3b settlement provenance")
        seen.add(row.settlement_id)
        unit = Decimal(str(row.unit_usdt))
        pnl = Decimal(str(row.net_pnl_usdt))
        if unit not in (1, 2, 3) or not pnl.is_finite():
            raise ValueError("invalid T6.3b settlement amount")
        equity += pnl / unit
        peak = max(peak, equity)
        maximum = max(maximum, peak-equity)
        if trigger is None and peak-equity >= LIMIT_1U:
            trigger = row.settlement_id
    return peak, equity, maximum, trigger
