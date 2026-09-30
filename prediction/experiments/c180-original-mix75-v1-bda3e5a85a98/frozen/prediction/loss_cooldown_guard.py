"""Restart-safe consecutive-loss cooldown and loop peak drawdown."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

COOLDOWN_SECONDS = 1800
TRIGGER_LOSS_COUNT = 2
EPS = Decimal("0.0001")


def _pnl(row: Mapping[str, Any]) -> Decimal:
    try:
        return Decimal(str(row.get("net_pnl", "0")))
    except (ArithmeticError, ValueError, TypeError):
        return Decimal("0")


def filled_losses_since_win(settled_trades: list[Mapping[str, Any]]) -> tuple[int, int | None]:
    """Count filled losses after the latest win. Pass rows must already be absent."""

    losses = 0
    last_loss_settled_ms: int | None = None
    for row in reversed(list(settled_trades or ())):
        pnl = _pnl(row)
        if pnl > EPS:
            break
        if pnl < -EPS:
            losses += 1
            if last_loss_settled_ms is None:
                try:
                    last_loss_settled_ms = int(row["settled_at_ms"])
                except (KeyError, TypeError, ValueError):
                    last_loss_settled_ms = None
    return losses, last_loss_settled_ms


def evaluate_loss_cooldown(
    settled_trades: list[Mapping[str, Any]],
    now_ms: int,
) -> tuple[bool, str, int]:
    """Return (allow_entry, reason, remaining_seconds)."""

    losses, last_loss_settled_ms = filled_losses_since_win(settled_trades)
    if losses < TRIGGER_LOSS_COUNT or last_loss_settled_ms is None:
        return True, "loss_cooldown_clear", 0
    cooldown_end_ms = int(last_loss_settled_ms) + COOLDOWN_SECONDS * 1000
    now = int(now_ms)
    if now < cooldown_end_ms:
        remaining_s = (cooldown_end_ms - now) // 1000
        return False, f"loss_cooldown_active:{remaining_s}s", int(remaining_s)
    return True, "loss_cooldown_trial_allowed", 0


def loop_peak_drawdown(settled_trades: list[Mapping[str, Any]]) -> tuple[Decimal, Decimal, Decimal]:
    """Return (peak, current, drawdown) from live filled settlements. Peak starts at 0."""

    current = Decimal("0")
    peak = Decimal("0")
    for row in settled_trades or ():
        current += _pnl(row)
        if current > peak:
            peak = current
    return peak, current, current - peak
