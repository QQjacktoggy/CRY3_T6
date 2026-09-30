"""Fail-closed campaign, loop, and daily risk gate."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Iterable

from .models import ActionType, Campaign


class RiskMode(str, Enum):
    READY = "READY"
    SOFT_COOLDOWN = "SOFT_COOLDOWN"
    HARD_STOP = "HARD_STOP"
    RECONCILE = "RECONCILE"


@dataclass(frozen=True)
class RiskConfig:
    loop_loss_limit: Decimal = Decimal("-2")
    daily_loss_limit: Decimal = Decimal("-2")
    consecutive_loss_limit: int = 3
    warning_attempts: int = 6
    max_order_attempts_per_market: int = 8
    max_buy_count_per_market: int = 2
    soft_cooldown_seconds: int = 1_800
    hedge_floor_improvement: Decimal = Decimal("0.10")


@dataclass
class RiskSnapshot:
    daily_net_pnl: Decimal = Decimal("0")
    loop_net_pnl: Decimal = Decimal("0")
    consecutive_losses: int = 0
    order_attempts: int = 0
    buy_count: int = 0
    pending_unknown: bool = False
    stale_feed: bool = False
    position_known: bool = True
    api_healthy: bool = True
    campaign_id: str | None = None
    hard_stop_latched: bool = False
    soft_cooldown_until_ms: int | None = None
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RiskDecision:
    mode: RiskMode
    allow_trading: bool
    action: ActionType
    reasons: tuple[str, ...] = ()

    @property
    def should_reconcile(self) -> bool:
        return self.action is ActionType.RECONCILE


# Risk limits are evaluated against the operation the runtime is about to
# perform.  In particular, reaching the BUY cap must not prevent a controlled
# reduction of an already-open position.  Keeping these sets here (rather
# than sprinkling comparisons through ``evaluate``) also makes the policy
# explicit for callers and tests.
_BUY_ACTIONS = frozenset({ActionType.BUY_INITIAL, ActionType.BUY_ADD, ActionType.BUY_HEDGE})
_SELL_ACTIONS = frozenset({ActionType.SELL_PROFIT_LOCK, ActionType.SELL_LOSER, ActionType.SELL_PROTECTIVE})
_SAFE_ACTIONS = frozenset({ActionType.RECONCILE, ActionType.SETTLE, ActionType.CANCEL, ActionType.HOLD})


def _normalise_action(value: ActionType | str | None) -> ActionType | None:
    """Parse a requested operation without silently opening a trade.

    ``None`` is retained as ``None`` so the public default can preserve the
    original ``evaluate()`` behaviour (a ready decision reports ``HOLD``),
    while the risk checks conservatively treat an unspecified action as a new
    initial BUY.  A generic BUY/SELL spelling is accepted for small runtime
    adapters; SELL maps to the least permissive reduction action.
    """

    if value is None:
        return None
    if isinstance(value, ActionType):
        return value
    text = str(value).strip().upper()
    aliases = {
        "BUY": ActionType.BUY_INITIAL,
        "SELL": ActionType.SELL_LOSER,
    }
    if text in aliases:
        return aliases[text]
    try:
        return ActionType(text)
    except (TypeError, ValueError):
        return None


class RiskEngine:
    """Stateful risk gate; no network or persistence side effects."""

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()
        self.snapshot = RiskSnapshot()
        self.mode = RiskMode.READY

    def evaluate(
        self,
        snapshot: RiskSnapshot | None = None,
        *,
        now_ms: int | None = None,
        campaign: Campaign | None = None,
        action: ActionType | str | None = None,
        requested_action: ActionType | str | None = None,
    ) -> RiskDecision:
        """Evaluate one requested runtime operation.

        The gate is deliberately action-aware:

        * BUY limits reject ``BUY_INITIAL``/``BUY_ADD``/``BUY_HEDGE``;
        * a daily/loop/consecutive stop still permits controlled SELL
          reduction and non-order reconciliation/settlement operations;
        * the per-market order-attempt hard limit rejects every new order
          (BUY and SELL), but permits ``RECONCILE``/``SETTLE``/``CANCEL``.

        With no action supplied, limits are evaluated conservatively as if a
        new initial BUY were requested, preserving the original fail-closed
        ``can_trade()`` contract.  A ready no-action decision still reports
        ``HOLD`` for backwards compatibility.
        """

        if action is not None and requested_action is not None:
            parsed_action = _normalise_action(action)
            parsed_requested = _normalise_action(requested_action)
            if parsed_action is None or parsed_requested is None or parsed_action is not parsed_requested:
                self.mode = RiskMode.RECONCILE
                return RiskDecision(
                    self.mode,
                    False,
                    ActionType.RECONCILE,
                    ("conflicting or invalid risk action",),
                )
            requested = parsed_action
        else:
            requested_value = requested_action if requested_action is not None else action
            requested = _normalise_action(requested_value)
            if requested_value is not None and requested is None:
                self.mode = RiskMode.RECONCILE
                return RiskDecision(
                    self.mode,
                    False,
                    ActionType.RECONCILE,
                    ("invalid risk action; fail closed",),
                )

        # An omitted action is treated as a possible new BUY for limits, but
        # remains a HOLD in an otherwise-ready response.  This distinction is
        # useful during bootstrapping and keeps existing adapters compatible.
        gate_action = requested or ActionType.BUY_INITIAL
        response_action = requested or ActionType.HOLD
        snap = snapshot or self.snapshot
        # A hard stop is a process-level latch.  Passing a newly assembled
        # snapshot (for example after a reconcile poll) must not clear it;
        # only ``reset_daily`` is allowed to do that.
        if self.snapshot.hard_stop_latched:
            snap.hard_stop_latched = True
        self.snapshot = snap
        reasons: list[str] = []
        # Unknown exchange state is always handled before loss limits.  A
        # trader must reconcile first, even if the process was restarted.
        if snap.pending_unknown or not snap.position_known or not snap.api_healthy:
            if snap.pending_unknown:
                reasons.append("unknown order execution; reconcile before retry")
            if not snap.position_known:
                reasons.append("position state is not confirmed")
            if not snap.api_healthy:
                reasons.append("prediction API is unhealthy")
            self.mode = RiskMode.RECONCILE
            # Unknown execution state must be reconciled before any other
            # operation.  Reconcile itself is a permitted non-trading action;
            # the default/BUY path remains blocked.
            if gate_action is ActionType.RECONCILE:
                return RiskDecision(self.mode, True, ActionType.RECONCILE, tuple(reasons))
            return RiskDecision(self.mode, False, ActionType.RECONCILE, tuple(reasons))
        if snap.stale_feed:
            self.mode = RiskMode.RECONCILE
            reason = ("market feed is stale",)
            if gate_action is ActionType.RECONCILE:
                return RiskDecision(self.mode, True, ActionType.RECONCILE, reason)
            return RiskDecision(self.mode, False, ActionType.RECONCILE, reason)
        if snap.hard_stop_latched or snap.daily_net_pnl <= self.config.daily_loss_limit:
            snap.hard_stop_latched = True
            self.mode = RiskMode.HARD_STOP
            reasons.append("daily loss limit reached")
            if gate_action in _SELL_ACTIONS or gate_action in _SAFE_ACTIONS:
                return RiskDecision(self.mode, True, response_action, tuple(reasons))
            return RiskDecision(self.mode, False, ActionType.PAUSE, tuple(reasons))
        if snap.order_attempts >= self.config.max_order_attempts_per_market:
            self.mode = RiskMode.HARD_STOP
            reasons.append("per-market order-attempt hard limit reached")
            # The attempt limit is stricter than loss cooldown: no new order,
            # including a SELL, may be submitted.  Reconcile/settle/cancel
            # remain available so the runtime can recover and close state.
            if gate_action in _SAFE_ACTIONS:
                return RiskDecision(self.mode, True, response_action, tuple(reasons))
            return RiskDecision(self.mode, False, ActionType.PAUSE, tuple(reasons))
        if snap.buy_count >= self.config.max_buy_count_per_market:
            reasons.append("per-market BUY limit reached")
            self.mode = RiskMode.SOFT_COOLDOWN
            # The BUY cap is scoped to BUY actions only.  It must not block a
            # one-time profit lock, loser reduction, or state reconciliation.
            if gate_action in _BUY_ACTIONS:
                return RiskDecision(self.mode, False, ActionType.PAUSE, tuple(reasons))
            return RiskDecision(self.mode, True, response_action, tuple(reasons))
        if (
            snap.soft_cooldown_until_ms is not None
            and now_ms is not None
            and int(now_ms) < int(snap.soft_cooldown_until_ms)
        ):
            self.mode = RiskMode.SOFT_COOLDOWN
            reasons.append("soft cooldown is active")
            if gate_action in _SELL_ACTIONS or gate_action in _SAFE_ACTIONS:
                return RiskDecision(self.mode, True, response_action, tuple(reasons))
            return RiskDecision(self.mode, False, ActionType.PAUSE, tuple(reasons))
        if snap.loop_net_pnl <= self.config.loop_loss_limit:
            self.mode = RiskMode.SOFT_COOLDOWN
            reasons.append("loop loss limit reached")
            if now_ms is not None:
                snap.soft_cooldown_until_ms = int(now_ms) + self.config.soft_cooldown_seconds * 1000
            if gate_action in _SELL_ACTIONS or gate_action in _SAFE_ACTIONS:
                return RiskDecision(self.mode, True, response_action, tuple(reasons))
            return RiskDecision(self.mode, False, ActionType.PAUSE, tuple(reasons))
        if snap.consecutive_losses >= self.config.consecutive_loss_limit:
            self.mode = RiskMode.SOFT_COOLDOWN
            reasons.append("consecutive-loss limit reached")
            if now_ms is not None:
                snap.soft_cooldown_until_ms = int(now_ms) + self.config.soft_cooldown_seconds * 1000
            if gate_action in _SELL_ACTIONS or gate_action in _SAFE_ACTIONS:
                return RiskDecision(self.mode, True, response_action, tuple(reasons))
            return RiskDecision(self.mode, False, ActionType.PAUSE, tuple(reasons))
        if snap.order_attempts >= self.config.warning_attempts:
            reasons.append("order-attempt warning threshold reached")
        self.mode = RiskMode.READY
        return RiskDecision(self.mode, True, response_action, tuple(reasons))

    def can_trade(
        self,
        snapshot: RiskSnapshot | None = None,
        *,
        now_ms: int | None = None,
        action: ActionType | str | None = None,
        requested_action: ActionType | str | None = None,
    ) -> bool:
        """Return whether the requested operation may proceed.

        ``can_trade(..., action=...)`` intentionally shares the exact gate
        implementation with :meth:`evaluate`; callers must not reimplement
        BUY/SELL limit exceptions in the runtime loop.
        """

        return self.evaluate(
            snapshot,
            now_ms=now_ms,
            action=action,
            requested_action=requested_action,
        ).allow_trading

    def record_settlement(self, net_pnl: Decimal | str | float) -> None:
        value = Decimal(str(net_pnl))
        self.snapshot.daily_net_pnl += value
        self.snapshot.loop_net_pnl += value
        self.snapshot.consecutive_losses = self.snapshot.consecutive_losses + 1 if value < 0 else 0

    def start_loop(self) -> None:
        self.snapshot.loop_net_pnl = Decimal("0")
        self.snapshot.consecutive_losses = 0

    def mark_unknown(self, reason: str = "unknown exchange state") -> None:
        self.snapshot.pending_unknown = True
        self.snapshot.reasons.append(reason)
        self.mode = RiskMode.RECONCILE

    def mark_reconciled(self, *, position_known: bool = True, api_healthy: bool = True) -> None:
        self.snapshot.pending_unknown = False
        self.snapshot.position_known = position_known
        self.snapshot.api_healthy = api_healthy
        if position_known and api_healthy:
            self.mode = RiskMode.READY

    def reset_daily(self) -> None:
        self.snapshot.daily_net_pnl = Decimal("0")
        self.snapshot.hard_stop_latched = False
        self.mode = RiskMode.READY

    @staticmethod
    def snapshot_from_campaign(campaign: Campaign, *, daily_net_pnl: Decimal = Decimal("0"), loop_net_pnl: Decimal = Decimal("0")) -> RiskSnapshot:
        return RiskSnapshot(
            campaign_id=campaign.campaign_id,
            daily_net_pnl=daily_net_pnl,
            loop_net_pnl=loop_net_pnl,
            order_attempts=campaign.order_attempts,
            buy_count=campaign.buy_count,
            pending_unknown=campaign.pending_unknown,
        )
