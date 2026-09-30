"""R3: preserve early entry exclusion and hold to settlement, matching replay.

No loss stop is enabled. Fee haircut is a conservative model assumption, not
an exchange fee claim. Existing acquisition shares/cash are not charged twice.
"""
from decimal import Decimal, InvalidOperation
from collections.abc import Mapping

VERSION = "r3-a-20260914"  # Early-entry identity remains unchanged.
EXIT_POLICY_VERSION = "r3-hold-settlement-20260915"
LATE_TAKE_PROFIT_ENABLED = False
EARLY_MS = 45_000
LATE_REMAINING_MS = 30_000
MIN_NET = Decimal("0.30")
SALE_HAIRCUT = Decimal("0.05")
MAX_AGE_MS = 1500
EXIT_REASON = "s3s5 R3 late_profit"


def early_key(campaign_id):
    return f"r3_a_early:{campaign_id}"


def exit_key(campaign_id):
    return f"r3_a_exit:{campaign_id}"


def late_window(campaign, now_ms):
    # Both decision generation and execution re-check this policy.
    return LATE_TAKE_PROFIT_ENABLED and 0 < campaign.market.end_time_ms - now_ms <= LATE_REMAINING_MS


def projected_net(position, sale_gross):
    # Include earlier partial-sale cash; buy costs remain cumulative in Position.
    return (position.realized_cash + sale_gross) * (1 - SALE_HAIRCUT) - position.total_buy_cost


def sweep_bids(book, shares):
    """Return full-size cash and worst consumed bid, or None. Never assume depth."""
    data = book.get("data", book) if isinstance(book, Mapping) else {}
    levels = data.get("bids") or data.get("buy") or data.get("BUY") or []
    try:
        if not shares.is_finite() or shares <= 0:
            return None
        parsed = []
        for row in levels:
            if isinstance(row, Mapping):
                price = row.get("price", row.get("p"))
                size = row.get("quantity", row.get("size", row.get("qty", row.get("q"))))
            else:
                price, size = row[:2]
            p, n = Decimal(str(price)), Decimal(str(size))
            if not p.is_finite() or not n.is_finite() or not 0 < p < 1 or n < 0:
                return None
            parsed.append((p, n))
        remaining, gross, worst = shares, Decimal(0), None
        for p, n in sorted(parsed, reverse=True):
            take = min(remaining, n)
            if take <= 0:
                continue
            gross += take * p
            remaining -= take
            worst = p
            if remaining == 0:
                return gross, worst
    except (InvalidOperation, TypeError, ValueError, KeyError, IndexError):
        return None
    return None
