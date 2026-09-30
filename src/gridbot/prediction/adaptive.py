"""Adaptive Regime Engine for Prediction markets (quality_hold_v_adaptive_v1).

Implemented strictly according to RFC Rev 2.3 (RFC-CRY3-2026-0903-REV2.3).
Provides:
- Tier and RescueState domain enums
- RouteState and AdaptiveConfig domain models
- Strict positive freshness validation (0 <= age <= limit, skew <= 500ms, monotonic)
- Clean Quote verification (complete history, no reference recross, no leader flips)
- evaluate_hysteresis_router and asymmetric debouncing router (instant downgrade, 3s upgrade)
- Decoupled adaptive_entry_decision pipeline eliminating deadline circularity and preventing duplicate entry
- Position ledger projection (loss improvement vs win sacrifice) & worst_acceptable_price orderbook bid depth sweep
- 9-state idempotent rescue state machine with deterministic client_order_id, versioned CAS persistence, and outbox separation
- Mathematical metric validators (Dynamic BE_WR, Paired Net PnL bootstrap with data sufficiency validation, Avoidable Full Loss)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
import hashlib
import json
import random
import sqlite3
import time
from typing import Any, Mapping, Sequence

from .models import (
    ActionType,
    Campaign,
    CampaignState,
    OrderSide,
    OutcomeSide,
    Position,
    QuoteSnapshot,
    ZERO,
    as_decimal,
)
from .strategy import StrategyDecision


class Tier(Enum):
    """Execution tiers for dynamic capital allocation."""

    TIER_1 = "TIER_1"
    TIER_2 = "TIER_2"
    TIER_3 = "TIER_3"

    @property
    def amount(self) -> Decimal:
        if self is Tier.TIER_1:
            return Decimal("3.0")
        if self is Tier.TIER_2:
            return Decimal("2.0")
        return Decimal("0.0")

    @property
    def risk_level(self) -> int:
        if self is Tier.TIER_1:
            return 2
        if self is Tier.TIER_2:
            return 1
        return 0

    @property
    def deadline_seconds(self) -> int:
        if self is Tier.TIER_1:
            return 90
        if self is Tier.TIER_2:
            return 150
        return 0

    @property
    def is_idle(self) -> bool:
        return self is Tier.TIER_3


class RescueState(str, Enum):
    """9-state idempotent recovery FSM states."""

    RESCUE_PLANNED = "RESCUE_PLANNED"
    RESCUE_SUBMITTED = "RESCUE_SUBMITTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
    PARTIAL_OR_FILLED = "PARTIAL_OR_FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCEL_UNKNOWN = "CANCEL_UNKNOWN"
    CANCEL_CONFIRMED = "CANCEL_CONFIRMED"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"
    RECONCILED = "RECONCILED"

    def __str__(self) -> str:
        return self.value


@dataclass
class RouteState:
    """State tracked across evaluation ticks for hysteresis and debounce."""

    confirmed_tier: Tier = Tier.TIER_3
    candidate_tier: Tier = Tier.TIER_3
    candidate_since_ms: int = 0
    consecutive_fresh_ticks: int = 0
    reserved_tier: Tier | None = None
    latched_tier: Tier | None = None
    last_seen_spot_ts: int = 0
    last_seen_book_up_ts: int = 0
    last_seen_book_down_ts: int = 0


@dataclass(frozen=True)
class AdaptiveConfig:
    """Immutable policy parameters for the adaptive regime engine."""

    # Hysteresis thresholds in basis points
    t1_enter_bps: Decimal = Decimal("8.25")
    t1_exit_bps: Decimal = Decimal("7.75")
    t2_green_enter_bps: Decimal = Decimal("5.25")
    t2_green_exit_bps: Decimal = Decimal("4.75")
    t2_yellow_enter_bps: Decimal = Decimal("6.25")
    t2_yellow_exit_bps: Decimal = Decimal("5.75")

    # Freshness limits in milliseconds
    max_spot_age_ms: int = 1_500
    max_book_age_ms: int = 2_000
    max_skew_ms: int = 500

    # Debounce parameters
    debounce_ticks: int = 3
    debounce_ms: int = 3_000

    # Remaining seconds deadlines
    t1_deadline_seconds: int = 90
    t2_deadline_seconds: int = 150

    # Common hard entry gates
    min_abs_margin_bps: Decimal = Decimal("3.5")
    min_entry_price: Decimal = Decimal("0.70")
    max_entry_price: Decimal = Decimal("0.89")
    initial_ttl_ms: int = 3_000

    # Rescue trigger parameters
    rescue_min_remaining_seconds: int = 30
    rescue_max_remaining_seconds: int = 180
    rescue_max_attempts: int = 1
    rescue_vwap_bid50_max: Decimal = Decimal("0.45")
    rescue_required_bad_snapshots: int = 2
    rescue_spot_cross_min_duration_ms: int = 1_500
    rescue_price_floor: Decimal = Decimal("0.20")
    rescue_min_improvement_abs: Decimal = Decimal("0.10")
    rescue_min_improvement_ratio: Decimal = Decimal("0.15")
    rescue_max_sacrifice_ratio: Decimal = Decimal("0.60")
    rescue_plan_validity_ms: int = 3_000
    rescue_ttl_ms: int = 3_000

    @property
    def config_hash(self) -> str:
        payload = {
            "debounce_ms": self.debounce_ms,
            "debounce_ticks": self.debounce_ticks,
            "initial_ttl_ms": self.initial_ttl_ms,
            "max_book_age_ms": self.max_book_age_ms,
            "max_entry_price": str(self.max_entry_price),
            "max_skew_ms": self.max_skew_ms,
            "max_spot_age_ms": self.max_spot_age_ms,
            "min_abs_margin_bps": str(self.min_abs_margin_bps),
            "min_entry_price": str(self.min_entry_price),
            "rescue_max_attempts": self.rescue_max_attempts,
            "rescue_max_remaining_seconds": self.rescue_max_remaining_seconds,
            "rescue_max_sacrifice_ratio": str(self.rescue_max_sacrifice_ratio),
            "rescue_min_improvement_abs": str(self.rescue_min_improvement_abs),
            "rescue_min_improvement_ratio": str(self.rescue_min_improvement_ratio),
            "rescue_min_remaining_seconds": self.rescue_min_remaining_seconds,
            "rescue_plan_validity_ms": self.rescue_plan_validity_ms,
            "rescue_price_floor": str(self.rescue_price_floor),
            "rescue_required_bad_snapshots": self.rescue_required_bad_snapshots,
            "rescue_spot_cross_min_duration_ms": self.rescue_spot_cross_min_duration_ms,
            "rescue_ttl_ms": self.rescue_ttl_ms,
            "rescue_vwap_bid50_max": str(self.rescue_vwap_bid50_max),
            "t1_deadline_seconds": self.t1_deadline_seconds,
            "t1_enter_bps": str(self.t1_enter_bps),
            "t1_exit_bps": str(self.t1_exit_bps),
            "t2_deadline_seconds": self.t2_deadline_seconds,
            "t2_green_enter_bps": str(self.t2_green_enter_bps),
            "t2_green_exit_bps": str(self.t2_green_exit_bps),
            "t2_yellow_enter_bps": str(self.t2_yellow_enter_bps),
            "t2_yellow_exit_bps": str(self.t2_yellow_exit_bps),
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


DEFAULT_ADAPTIVE_CONFIG = AdaptiveConfig()


@dataclass
class RescuePlan:
    """Bounded, persistent action plan for defensive campaign liquidation."""

    campaign_id: str
    attempt: int
    client_order_id: str
    side: OutcomeSide
    shares: Decimal
    limit_price: Decimal
    expected_vwap: Decimal
    created_at_ms: int
    ttl_ms: int = 3_000
    state: RescueState = RescueState.RESCUE_PLANNED
    order_id: str | None = None
    filled_shares: Decimal = ZERO
    filled_amount: Decimal = ZERO
    error: str | None = None
    version: int = 1
    book_timestamp: int = 0
    quote_expiry_ms: int = 0
    depth_summary: str = ""
    seen_trade_ids: set[str] = field(default_factory=set)


# ----------------------------------------------------------------------
# Freshness & Data Quality Validation
# ----------------------------------------------------------------------

def check_positive_freshness(
    now_ms: int,
    spot_ts: int,
    book_up_ts: int,
    book_down_ts: int,
    state: RouteState | None = None,
    config: AdaptiveConfig | None = None,
    *,
    update_state: bool = False,
) -> tuple[bool, str]:
    """Validate strict positive age, timestamp skew, and monotonicity across feeds."""

    conf = config or DEFAULT_ADAPTIVE_CONFIG
    spot_age_ms = now_ms - spot_ts
    book_up_age_ms = now_ms - book_up_ts
    book_down_age_ms = now_ms - book_down_ts

    # 1. Non-negative age check (strict future timestamp rejection)
    if spot_age_ms < 0:
        return False, f"future spot timestamp: now {now_ms} < spot_ts {spot_ts}"
    if book_up_age_ms < 0:
        return False, f"future book_up timestamp: now {now_ms} < book_up_ts {book_up_ts}"
    if book_down_age_ms < 0:
        return False, f"future book_down timestamp: now {now_ms} < book_down_ts {book_down_ts}"

    # 2. Maximum staleness limits
    if spot_age_ms > conf.max_spot_age_ms:
        return False, f"spot stale: {spot_age_ms}ms > {conf.max_spot_age_ms}ms"
    if book_up_age_ms > conf.max_book_age_ms:
        return False, f"book_up stale: {book_up_age_ms}ms > {conf.max_book_age_ms}ms"
    if book_down_age_ms > conf.max_book_age_ms:
        return False, f"book_down stale: {book_down_age_ms}ms > {conf.max_book_age_ms}ms"

    # 3. Inter-feed skew limit
    max_ts = max(spot_ts, book_up_ts, book_down_ts)
    min_ts = min(spot_ts, book_up_ts, book_down_ts)
    skew_ms = max_ts - min_ts
    if skew_ms > conf.max_skew_ms:
        return False, f"source skew too large: {skew_ms}ms > {conf.max_skew_ms}ms"

    # 4. Monotonicity checks
    if state is not None:
        if spot_ts < state.last_seen_spot_ts:
            return False, f"spot timestamp regressed: {spot_ts} < {state.last_seen_spot_ts}"
        if book_up_ts < state.last_seen_book_up_ts:
            return False, f"book_up timestamp regressed: {book_up_ts} < {state.last_seen_book_up_ts}"
        if book_down_ts < state.last_seen_book_down_ts:
            return False, f"book_down timestamp regressed: {book_down_ts} < {state.last_seen_book_down_ts}"

        if update_state:
            state.last_seen_spot_ts = max(state.last_seen_spot_ts, spot_ts)
            state.last_seen_book_up_ts = max(state.last_seen_book_up_ts, book_up_ts)
            state.last_seen_book_down_ts = max(state.last_seen_book_down_ts, book_down_ts)

    return True, "ok"


def check_clean_quote(
    quote: QuoteSnapshot,
    campaign: Campaign,
    *,
    history_complete: bool = True,
    max_gap_ms: int = 3_000,
) -> tuple[bool, str]:
    """Validate quote integrity: history completeness, no reference cross, no flip."""

    if not history_complete:
        return False, "history incomplete"

    # Check observed data gap if available
    observed_gap = getattr(quote, "max_gap_ms", 0)
    if observed_gap > max_gap_ms:
        return False, f"quote gap {observed_gap}ms > {max_gap_ms}ms"

    # Verify absence of leader flips and reference recross
    leader_flips = getattr(quote, "leader_flip_count", 0)
    if leader_flips > 0:
        return False, f"leader flipped {leader_flips} times"

    ref_crosses = getattr(quote, "reference_cross_count", 0)
    if ref_crosses > 0:
        return False, f"BTC crossed reference {ref_crosses} times"

    # Also check campaign history flags
    if getattr(campaign, "crossed_reference", False):
        return False, "campaign recorded reference cross"
    if getattr(campaign, "flip_confirmed", False):
        return False, "campaign recorded leader flip"

    return True, "ok"


# ----------------------------------------------------------------------
# Hysteresis Router & Asymmetric Debounce
# ----------------------------------------------------------------------

def evaluate_hysteresis_router(
    btc_margin_bps: Decimal,
    regime: str,
    state: RouteState,
    config: AdaptiveConfig | None = None,
) -> Tier:
    """Evaluate candidate tier according to regime and non-symmetric margin bands."""

    conf = config or DEFAULT_ADAPTIVE_CONFIG
    current_tier = state.confirmed_tier
    abs_margin = abs(btc_margin_bps)
    reg_upper = str(regime).upper()

    # In non-tradable or high-volatility regimes, force IDLE immediately
    if reg_upper in ("RED", "WAIT_DATA", "UNKNOWN"):
        return Tier.TIER_3

    if reg_upper == "GREEN":
        if current_tier is Tier.TIER_1:
            if abs_margin < conf.t1_exit_bps:
                return Tier.TIER_2 if abs_margin >= conf.t2_green_exit_bps else Tier.TIER_3
            return Tier.TIER_1
        elif current_tier is Tier.TIER_2:
            if abs_margin >= conf.t1_enter_bps:
                return Tier.TIER_1
            if abs_margin < conf.t2_green_exit_bps:
                return Tier.TIER_3
            return Tier.TIER_2
        else:  # current_tier is TIER_3
            if abs_margin >= conf.t1_enter_bps:
                return Tier.TIER_1
            if abs_margin >= conf.t2_green_enter_bps:
                return Tier.TIER_2
            return Tier.TIER_3

    elif reg_upper == "YELLOW":
        # YELLOW regime: TIER_1 is forbidden; highest allowed is TIER_2
        if current_tier is Tier.TIER_2:
            return Tier.TIER_3 if abs_margin < conf.t2_yellow_exit_bps else Tier.TIER_2
        else:
            return Tier.TIER_2 if abs_margin >= conf.t2_yellow_enter_bps else Tier.TIER_3

    return Tier.TIER_3


def apply_asymmetric_debounce(
    candidate_tier: Tier,
    state: RouteState,
    now_ms: int,
    config: AdaptiveConfig | None = None,
) -> Tier:
    """Apply asymmetric debouncing: instant downgrade, 3 ticks & 3000ms for upgrade.
    
    During candidate waiting for upgrade, returns TIER_3 (IDLE) to ensure no stale high-risk tier
    is accidentally used.
    """

    conf = config or DEFAULT_ADAPTIVE_CONFIG

    # 1. Downgrade (Risk reduction): Instant zero-delay execution
    if candidate_tier.risk_level < state.confirmed_tier.risk_level:
        state.confirmed_tier = candidate_tier
        state.candidate_tier = candidate_tier
        state.candidate_since_ms = now_ms
        state.consecutive_fresh_ticks = 1
        return candidate_tier

    # 2. Same Risk: Keep confirmed tier
    if candidate_tier.risk_level == state.confirmed_tier.risk_level:
        state.candidate_tier = candidate_tier
        return state.confirmed_tier

    # 3. Upgrade (Risk elevation): Requires strict AND condition (>= 3 ticks AND >= 3000ms)
    if candidate_tier != state.candidate_tier:
        state.candidate_tier = candidate_tier
        state.candidate_since_ms = now_ms
        state.consecutive_fresh_ticks = 1
        return Tier.TIER_3  # Candidate waiting: IDLE

    # Candidate matches previous candidate tick
    state.consecutive_fresh_ticks += 1
    duration_ms = now_ms - state.candidate_since_ms

    if state.consecutive_fresh_ticks >= conf.debounce_ticks and duration_ms >= conf.debounce_ms:
        state.confirmed_tier = candidate_tier
        return candidate_tier

    # Upgrade in progress: IDLE until confirmed
    return Tier.TIER_3


# ----------------------------------------------------------------------
# Two-Stage Latching Handlers
# ----------------------------------------------------------------------

def on_order_submitted(state: RouteState, tier: Tier) -> None:
    """Record initial intent reservation when order is submitted."""
    state.reserved_tier = tier


def on_order_cancelled(state: RouteState) -> None:
    """Reset reservation if submitted order was cancelled with 0 filled shares."""
    state.reserved_tier = None


def on_order_filled(state: RouteState, tier: Tier) -> None:
    """Permanently lock latched_tier when filled_shares > 0."""
    state.latched_tier = tier
    state.reserved_tier = None


# ----------------------------------------------------------------------
# Decoupled Entry Decision Pipeline
# ----------------------------------------------------------------------

def adaptive_entry_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    regime: str,
    route_state: RouteState,
    config: AdaptiveConfig | None = None,
    *,
    history_complete: bool = True,
    update_freshness_state: bool = True,
) -> StrategyDecision:
    """Pre-entry pipeline with decoupled deadlines, duplicate entry protection, and asymmetric debounce."""

    conf = config or DEFAULT_ADAPTIVE_CONFIG

    # Latching & duplicate entry invariants (P0 FIX)
    if campaign.position.has_any or route_state.latched_tier is not None or route_state.reserved_tier is not None:
        return StrategyDecision(ActionType.HOLD, campaign.state, "initial position already exists or tier reserved/latched")
    if campaign.initial_outcome is not None or campaign.buy_count > 0:
        return StrategyDecision(ActionType.HOLD, campaign.state, "campaign has already entered initial position")
    if campaign.pending_unknown or campaign.pending_intent_id:
        return StrategyDecision(ActionType.RECONCILE, campaign.state, "order state is pending/unknown; reconcile first")

    # Step 1: Common hard gates (independent of Tier)
    spot_ts = getattr(quote, "spot_observed_at_ms", 0) or quote.observed_at_ms
    book_up_ts = getattr(quote, "book_up_observed_at_ms", 0) or quote.observed_at_ms
    book_down_ts = getattr(quote, "book_down_observed_at_ms", 0) or quote.observed_at_ms

    fresh_ok, why = check_positive_freshness(
        now_ms, spot_ts, book_up_ts, book_down_ts, route_state, conf, update_state=update_freshness_state
    )
    if not fresh_ok:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"freshness rejected: {why}")

    clean_ok, why = check_clean_quote(quote, campaign, history_complete=history_complete)
    if not clean_ok:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"clean quote rejected: {why}")

    if quote.btc_spot is None or quote.reference_price is None or quote.reference_price <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "BTC spot or reference price unavailable")

    btc_margin_bps = (quote.btc_spot - quote.reference_price) / quote.reference_price * Decimal("10000")
    if abs(btc_margin_bps) < conf.min_abs_margin_bps:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"margin {abs(btc_margin_bps)} bps < {conf.min_abs_margin_bps} bps")

    side = quote.leader
    if side is None:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no confirmed leader")

    # BTC alignment check
    if side is OutcomeSide.UP and btc_margin_bps <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "BTC is not aligned with UP leader")
    if side is OutcomeSide.DOWN and btc_margin_bps >= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "BTC is not aligned with DOWN leader")

    ask = quote.ask(side)
    if ask is None or not (conf.min_entry_price <= ask <= conf.max_entry_price):
        return StrategyDecision(ActionType.HOLD, campaign.state, f"leader ask {ask} outside [{conf.min_entry_price}, {conf.max_entry_price}]")

    # Step 2: Regime evaluation to generate candidate Tier
    candidate_tier = evaluate_hysteresis_router(btc_margin_bps, regime, route_state, conf)
    if candidate_tier == Tier.TIER_3:
        # P0 FIX: Immediately apply downgrade state write-back so subsequent GREEN ticks do not bypass debounce!
        apply_asymmetric_debounce(Tier.TIER_3, route_state, now_ms, conf)
        return StrategyDecision(ActionType.HOLD, campaign.state, "candidate tier is TIER_3 (IDLE)")

    # Step 3: Tier-specific Deadline check
    tier_deadline_seconds = conf.t1_deadline_seconds if candidate_tier == Tier.TIER_1 else conf.t2_deadline_seconds
    remaining_seconds = campaign.remaining_seconds(now_ms)
    if remaining_seconds < tier_deadline_seconds:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"remaining {remaining_seconds}s < deadline {tier_deadline_seconds}s for {candidate_tier.name}")

    # Step 4: Asymmetric Debounce
    effective_tier = apply_asymmetric_debounce(candidate_tier, route_state, now_ms, conf)
    if effective_tier == Tier.TIER_3:
        return StrategyDecision(ActionType.HOLD, campaign.state, "debounce in progress: returning IDLE")

    # Step 5: Sizing & Intent Generation (Two-Stage Latching: Reserved)
    route_state.reserved_tier = effective_tier
    return StrategyDecision(
        action=ActionType.BUY_INITIAL,
        state=CampaignState.INITIAL_PENDING,
        reason=f"adaptive entry {effective_tier.name} at {ask}",
        outcome=side,
        order_side=OrderSide.BUY,
        amount=effective_tier.amount,
        limit_price=ask,
        ttl_ms=conf.initial_ttl_ms,
        trade_allowed=True,
    )


# ----------------------------------------------------------------------
# Orderbook Bid Depth Sweep & Ledger Net Improvement
# ----------------------------------------------------------------------

@dataclass
class SweepResult:
    """Executable orderbook bid depth sweep results."""

    worst_acceptable_price: Decimal
    executable_shares: Decimal
    expected_vwap: Decimal

    def __iter__(self):
        # P1 FIX: Clean fixed 3-tuple iterator without bytecode guessing
        return iter((self.worst_acceptable_price, self.executable_shares, self.expected_vwap))

    def __getitem__(self, index: int) -> Decimal:
        items = (self.worst_acceptable_price, self.executable_shares, self.expected_vwap)
        return items[index]

    def __len__(self) -> int:
        return 3


def sweep_orderbook_bids(
    bids: Sequence[Any],
    target_shares: Decimal,
    price_floor: Decimal = Decimal("0.20"),
) -> SweepResult:
    """Sweep bids from best bid downwards to find worst_acceptable_price, volume, and VWAP."""

    parsed_bids: list[tuple[Decimal, Decimal]] = []
    for item in bids:
        if isinstance(item, (tuple, list)) and len(item) >= 2:
            p, q = as_decimal(item[0]), as_decimal(item[1])
        elif isinstance(item, Mapping):
            p = as_decimal(item.get("price") or item.get("p"))
            q = as_decimal(item.get("shares") or item.get("quantity") or item.get("qty") or item.get("q"))
        elif hasattr(item, "price") and (hasattr(item, "shares") or hasattr(item, "quantity")):
            p = as_decimal(getattr(item, "price"))
            q = as_decimal(getattr(item, "shares", None) or getattr(item, "quantity"))
        else:
            continue
        if p >= price_floor and q > ZERO:
            parsed_bids.append((p, q))

    # Sort descending by price (best bid first)
    parsed_bids.sort(key=lambda x: x[0], reverse=True)

    accumulated_shares = ZERO
    accumulated_proceeds = ZERO
    lowest_filled_price = ZERO

    for price, volume in parsed_bids:
        needed = target_shares - accumulated_shares
        take = min(needed, volume)
        accumulated_shares += take
        accumulated_proceeds += take * price
        lowest_filled_price = price
        if accumulated_shares >= target_shares:
            break

    if accumulated_shares <= ZERO:
        return SweepResult(ZERO, ZERO, ZERO)

    expected_vwap = accumulated_proceeds / accumulated_shares
    return SweepResult(lowest_filled_price, accumulated_shares, expected_vwap)


def calculate_ledger_net_improvement(
    position: Position,
    executable_shares: Decimal,
    expected_vwap: Decimal,
    expected_fee: Decimal = ZERO,
    held_side: OutcomeSide | None = None,
    min_improvement_abs: Decimal = Decimal("0.10"),
    min_improvement_ratio: Decimal = Decimal("0.15"),
    max_sacrifice_ratio: Decimal = Decimal("0.60"),
) -> tuple[bool, Decimal, Decimal]:
    """Calculate loss-case net PnL improvement and win-case sacrifice against thresholds."""

    side = held_side
    if side is None:
        if position.up_shares > ZERO:
            side = OutcomeSide.UP
        elif position.down_shares > ZERO:
            side = OutcomeSide.DOWN
        else:
            return False, ZERO, min_improvement_abs

    initial_cost = position.cost(side)
    min_required = max(min_improvement_abs, initial_cost * min_improvement_ratio)

    if executable_shares <= ZERO or expected_vwap <= ZERO:
        return False, ZERO, min_required

    # Loss outcome: unsold shares expire at 0. Selling executable_shares recovers proceeds minus fee.
    proceeds = executable_shares * expected_vwap
    loss_improvement = proceeds - expected_fee
    if loss_improvement < min_required:
        return False, loss_improvement, min_required

    # Win outcome: shares expire at 1.0 USDT. Selling them at expected_vwap sacrifices (1.0 - VWAP) * shares + fee.
    win_case_sacrifice = (executable_shares * (Decimal("1.0") - expected_vwap)) + expected_fee
    max_allowed_sacrifice = initial_cost * max_sacrifice_ratio
    if win_case_sacrifice > max_allowed_sacrifice:
        return False, loss_improvement, min_required

    return True, loss_improvement, min_required


# ----------------------------------------------------------------------
# 9-State Rescue FSM & Decision Logic
# ----------------------------------------------------------------------

def step_rescue_fsm(
    plan: RescuePlan,
    event: str,
    payload: Mapping[str, Any] | None = None,
) -> RescueState:
    """Pure state transition function for the 9-state recovery FSM with idempotency."""

    data = payload or {}
    curr = plan.state
    ev = event.upper()

    if curr == RescueState.RESCUE_PLANNED:
        if ev == "SUBMIT_ORDER":
            plan.state = RescueState.RESCUE_SUBMITTED
            if data.get("order_id"):
                plan.order_id = str(data["order_id"])
        elif ev in ("SUBMIT_TIMEOUT", "API_TIMEOUT", "SUBMIT_ERROR"):
            plan.state = RescueState.SUBMIT_UNKNOWN
            plan.error = str(data.get("error", "API timeout on submit"))

    elif curr == RescueState.RESCUE_SUBMITTED:
        if ev in ("SUBMIT_TIMEOUT", "API_TIMEOUT", "SUBMIT_ERROR"):
            plan.state = RescueState.SUBMIT_UNKNOWN
            plan.error = str(data.get("error", "API timeout on submit"))
        elif ev in ("ORDER_FILLED", "FILL_RECEIVED"):
            # P1 FIX: Support authoritative cumulative_filled_shares or deduplicated incremental fills
            if "cumulative_filled_shares" in data:
                plan.filled_shares = as_decimal(data["cumulative_filled_shares"])
            else:
                trade_id = str(data.get("trade_id", ""))
                if not trade_id or trade_id not in plan.seen_trade_ids:
                    if trade_id:
                        plan.seen_trade_ids.add(trade_id)
                    filled = as_decimal(data.get("filled_shares", ZERO))
                    plan.filled_shares += filled

            if plan.filled_shares >= plan.shares:
                plan.state = RescueState.RECONCILED
            elif plan.filled_shares > ZERO:
                plan.state = RescueState.PARTIAL_OR_FILLED
        elif ev == "TTL_EXPIRED":
            plan.state = RescueState.CANCEL_REQUESTED

    elif curr == RescueState.SUBMIT_UNKNOWN:
        if ev in ("QUERY_ORDER", "RECONCILE"):
            plan.state = RescueState.RECONCILE_REQUIRED

    elif curr == RescueState.PARTIAL_OR_FILLED:
        if ev == "TTL_EXPIRED":
            if plan.filled_shares < plan.shares:
                plan.state = RescueState.CANCEL_REQUESTED
            else:
                plan.state = RescueState.RECONCILED
        elif ev in ("ORDER_FILLED", "FILL_RECEIVED"):
            if "cumulative_filled_shares" in data:
                plan.filled_shares = as_decimal(data["cumulative_filled_shares"])
            else:
                trade_id = str(data.get("trade_id", ""))
                if not trade_id or trade_id not in plan.seen_trade_ids:
                    if trade_id:
                        plan.seen_trade_ids.add(trade_id)
                    filled = as_decimal(data.get("filled_shares", ZERO))
                    plan.filled_shares += filled

            if plan.filled_shares >= plan.shares:
                plan.state = RescueState.RECONCILED

    elif curr == RescueState.CANCEL_REQUESTED:
        if ev in ("CANCEL_TIMEOUT", "API_TIMEOUT"):
            plan.state = RescueState.CANCEL_UNKNOWN
            plan.error = str(data.get("error", "API timeout on cancel"))
        elif ev in ("CANCEL_CONFIRMED", "CANCEL_OK"):
            plan.state = RescueState.CANCEL_CONFIRMED
        elif ev in ("ORDER_FILLED", "FILL_RECEIVED"):
            # Race condition: filled while cancel was in flight
            if "cumulative_filled_shares" in data:
                plan.filled_shares = as_decimal(data["cumulative_filled_shares"])
            else:
                trade_id = str(data.get("trade_id", ""))
                if not trade_id or trade_id not in plan.seen_trade_ids:
                    if trade_id:
                        plan.seen_trade_ids.add(trade_id)
                    plan.filled_shares += as_decimal(data.get("filled_shares", ZERO))

    elif curr == RescueState.CANCEL_UNKNOWN:
        if ev in ("QUERY_ORDER", "RECONCILE"):
            plan.state = RescueState.RECONCILE_REQUIRED

    elif curr == RescueState.CANCEL_CONFIRMED:
        # After cancel confirmed, mandatory reconciliation against actual exchange position
        if ev in ("RECONCILE", "AUDIT_POSITION"):
            plan.state = RescueState.RECONCILE_REQUIRED

    elif curr == RescueState.RECONCILE_REQUIRED:
        if ev in ("RECONCILE_COMPLETE", "POSITION_ALIGNED"):
            plan.state = RescueState.RECONCILED
            if "actual_position" in data:
                plan.filled_shares = as_decimal(data.get("actual_position", plan.filled_shares))

    return plan.state


def on_rescue_submitted(plan: RescuePlan, order_id: str | None = None) -> None:
    """Advance plan state to RESCUE_SUBMITTED once external API order is transmitted."""
    step_rescue_fsm(plan, "SUBMIT_ORDER", {"order_id": order_id})


def on_rescue_submit_failed(plan: RescuePlan, error: str = "submit timeout") -> None:
    """Advance plan state to SUBMIT_UNKNOWN if external API call timed out or had transport failure."""
    step_rescue_fsm(plan, "SUBMIT_TIMEOUT", {"error": error})


def adaptive_rescue_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    orderbook_bids: Sequence[Any],
    *,
    plan: RescuePlan | None = None,
    consecutive_bad_snapshots: int = 0,
    btc_cross_duration_ms: int = 0,
    rescue_attempts: int = 0,
    config: AdaptiveConfig | None = None,
) -> tuple[StrategyDecision, RescuePlan | None]:
    """Pragmatic rescue decision following RFC Rev 2.3 5 invariants & 9-state machine with outbox safety."""

    conf = config or DEFAULT_ADAPTIVE_CONFIG

    # 1. Existing Active Plan Progression
    if plan is not None:
        st = plan.state

        if st == RescueState.RESCUE_PLANNED:
            # P0 FIX: Do NOT change state to SUBMITTED here before actual API call! Keep as PLANNED.
            # Check quote freshness and plan validity before dispatching SELL
            if plan.quote_expiry_ms > 0 and now_ms > plan.quote_expiry_ms:
                return StrategyDecision(ActionType.HOLD, campaign.state, "rescue plan quote expired; replanning"), None

            return StrategyDecision(
                action=ActionType.SELL_PROTECTIVE,
                state=campaign.state,
                reason=f"submitting rescue sell order {plan.client_order_id}",
                outcome=plan.side,
                order_side=OrderSide.SELL,
                amount=plan.shares,
                limit_price=plan.limit_price,
                ttl_ms=plan.ttl_ms,
                trade_allowed=True,
            ), plan

        if st == RescueState.RESCUE_SUBMITTED:
            elapsed_ms = now_ms - plan.created_at_ms
            if elapsed_ms >= plan.ttl_ms:
                step_rescue_fsm(plan, "TTL_EXPIRED")
                return StrategyDecision(ActionType.CANCEL, campaign.state, f"rescue TTL expired ({elapsed_ms}ms >= {plan.ttl_ms}ms)"), plan
            return StrategyDecision(ActionType.HOLD, campaign.state, "rescue order submitted; awaiting fill"), plan

        if st == RescueState.SUBMIT_UNKNOWN:
            # Absolute invariant: DO NOT re-submit SELL order! Query and reconcile.
            step_rescue_fsm(plan, "RECONCILE")
            return StrategyDecision(ActionType.RECONCILE, campaign.state, "rescue submit timed out; reconcile required"), plan

        if st == RescueState.CANCEL_REQUESTED:
            elapsed_ms = now_ms - plan.created_at_ms
            if elapsed_ms >= (plan.ttl_ms * 2):
                step_rescue_fsm(plan, "CANCEL_TIMEOUT")
                return StrategyDecision(ActionType.RECONCILE, campaign.state, "rescue cancel timed out; reconcile required"), plan
            return StrategyDecision(ActionType.HOLD, campaign.state, "rescue cancel in flight"), plan

        if st in (RescueState.CANCEL_UNKNOWN, RescueState.CANCEL_CONFIRMED, RescueState.RECONCILE_REQUIRED):
            step_rescue_fsm(plan, "RECONCILE")
            return StrategyDecision(ActionType.RECONCILE, campaign.state, f"rescue in {st.value}; reconcile against position"), plan

        if st == RescueState.RECONCILED:
            return StrategyDecision(ActionType.HOLD, campaign.state, "rescue reconciled; holding residual"), plan

    # 2. Pre-condition checks to generate a NEW RescuePlan
    if not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no position to rescue"), None
    if campaign.state in (CampaignState.SETTLEMENT, CampaignState.DONE):
        return StrategyDecision(ActionType.HOLD, campaign.state, "campaign already settled"), None
    if rescue_attempts >= conf.rescue_max_attempts:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"max rescue attempts ({conf.rescue_max_attempts}) reached"), None

    remaining_seconds = campaign.remaining_seconds(now_ms)
    if not (conf.rescue_min_remaining_seconds <= remaining_seconds <= conf.rescue_max_remaining_seconds):
        return StrategyDecision(ActionType.HOLD, campaign.state, f"remaining {remaining_seconds}s outside [{conf.rescue_min_remaining_seconds}, {conf.rescue_max_remaining_seconds}]s"), None

    # Condition 3: Consecutive bad snapshots (VWAP < 0.45)
    if consecutive_bad_snapshots < conf.rescue_required_bad_snapshots:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"bad snapshots ({consecutive_bad_snapshots}) < required ({conf.rescue_required_bad_snapshots})"), None

    # Condition 4: BTC reference cross duration
    if btc_cross_duration_ms < conf.rescue_spot_cross_min_duration_ms:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"spot cross duration {btc_cross_duration_ms}ms < required {conf.rescue_spot_cross_min_duration_ms}ms"), None

    held_side = OutcomeSide.UP if campaign.position.up_shares > ZERO else OutcomeSide.DOWN
    position_shares = campaign.position.shares(held_side)
    if position_shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no shares on held side"), None

    # Sweep bids for depth & worst acceptable price
    sweep = sweep_orderbook_bids(orderbook_bids, position_shares, conf.rescue_price_floor)
    if sweep.executable_shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no bids above price floor 0.20"), None

    # Fee calculation per share
    fee_per_share = getattr(campaign, "fee_per_share", ZERO) or Decimal("0.001")
    expected_fee = sweep.executable_shares * fee_per_share

    # Ledger projection & Net Improvement check (P1 FIX: includes win sacrifice)
    passes, loss_improvement, min_required = calculate_ledger_net_improvement(
        campaign.position,
        sweep.executable_shares,
        sweep.expected_vwap,
        expected_fee,
        held_side=held_side,
        min_improvement_abs=conf.rescue_min_improvement_abs,
        min_improvement_ratio=conf.rescue_min_improvement_ratio,
        max_sacrifice_ratio=conf.rescue_max_sacrifice_ratio,
    )
    if not passes:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"net improvement {loss_improvement} USDT < required {min_required} USDT or sacrifice too high"), None

    # Deterministic client_order_id
    next_attempt = rescue_attempts + 1
    cid = f"resc_{campaign.campaign_id}_{next_attempt}"

    new_plan = RescuePlan(
        campaign_id=campaign.campaign_id,
        attempt=next_attempt,
        client_order_id=cid,
        side=held_side,
        shares=sweep.executable_shares,
        limit_price=sweep.worst_acceptable_price,
        expected_vwap=sweep.expected_vwap,
        created_at_ms=now_ms,
        ttl_ms=conf.rescue_ttl_ms,
        state=RescueState.RESCUE_PLANNED,
        book_timestamp=now_ms,
        quote_expiry_ms=now_ms + conf.rescue_plan_validity_ms,
        depth_summary=f"{sweep.executable_shares}@{sweep.expected_vwap:.4f}",
    )

    return StrategyDecision(
        action=ActionType.SELL_PROTECTIVE,
        state=campaign.state,
        reason=f"initiating rescue sell {cid} for {sweep.executable_shares} shares at {sweep.worst_acceptable_price}",
        outcome=held_side,
        order_side=OrderSide.SELL,
        amount=sweep.executable_shares,
        limit_price=sweep.worst_acceptable_price,
        ttl_ms=conf.rescue_ttl_ms,
        trade_allowed=True,
    ), new_plan


# ----------------------------------------------------------------------
# Persistence & DB Migration Helpers with Versioned CAS
# ----------------------------------------------------------------------

def init_rescue_plan_schema(conn: sqlite3.Connection) -> None:
    """Ensure prediction_rescue_plans schema exists with unique constraint and version CAS."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS prediction_rescue_plans (
            campaign_id TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            client_order_id TEXT NOT NULL,
            side TEXT NOT NULL,
            shares TEXT NOT NULL,
            limit_price TEXT NOT NULL,
            expected_vwap TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            ttl_ms INTEGER NOT NULL,
            state TEXT NOT NULL,
            order_id TEXT,
            filled_shares TEXT NOT NULL,
            filled_amount TEXT NOT NULL,
            error TEXT,
            updated_at_ms INTEGER NOT NULL,
            order_purpose TEXT NOT NULL DEFAULT 'rescue',
            version INTEGER NOT NULL DEFAULT 1,
            book_timestamp INTEGER NOT NULL DEFAULT 0,
            quote_expiry_ms INTEGER NOT NULL DEFAULT 0,
            depth_summary TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (campaign_id, client_order_id),
            UNIQUE (campaign_id, order_purpose)
        );
    """)
    conn.commit()


def persist_rescue_plan_tx(conn: sqlite3.Connection, plan: RescuePlan) -> None:
    """Idempotently insert or CAS-update a rescue plan within an active SQLite transaction."""

    cursor = conn.cursor()
    cursor.execute("""
        SELECT client_order_id, version FROM prediction_rescue_plans
        WHERE campaign_id = ? AND order_purpose = 'rescue'
    """, (plan.campaign_id,))
    row = cursor.fetchone()

    ts = int(time.time() * 1000)

    if row is None:
        cursor.execute("""
            INSERT INTO prediction_rescue_plans (
                campaign_id, attempt, client_order_id, side, shares,
                limit_price, expected_vwap, created_at_ms, ttl_ms,
                state, order_id, filled_shares, filled_amount, error,
                updated_at_ms, order_purpose, version, book_timestamp,
                quote_expiry_ms, depth_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'rescue', ?, ?, ?, ?)
        """, (
            plan.campaign_id,
            plan.attempt,
            plan.client_order_id,
            plan.side.value if hasattr(plan.side, "value") else str(plan.side),
            str(plan.shares),
            str(plan.limit_price),
            str(plan.expected_vwap),
            plan.created_at_ms,
            plan.ttl_ms,
            plan.state.value if hasattr(plan.state, "value") else str(plan.state),
            plan.order_id,
            str(plan.filled_shares),
            str(plan.filled_amount),
            plan.error,
            ts,
            plan.version,
            plan.book_timestamp,
            plan.quote_expiry_ms,
            plan.depth_summary,
        ))
    else:
        existing_cid, current_version = row
        if existing_cid != plan.client_order_id:
            raise sqlite3.IntegrityError(
                f"Rescue plan already exists for campaign {plan.campaign_id} with client_order_id {existing_cid}; cannot replace with {plan.client_order_id}"
            )

        # P1 FIX: Compare-And-Swap (CAS) optimistic concurrency control
        new_version = current_version + 1
        cursor.execute("""
            UPDATE prediction_rescue_plans
            SET state = ?, order_id = ?, filled_shares = ?, filled_amount = ?, error = ?,
                updated_at_ms = ?, version = ?, book_timestamp = ?, quote_expiry_ms = ?, depth_summary = ?
            WHERE campaign_id = ? AND order_purpose = 'rescue' AND client_order_id = ? AND version = ?
        """, (
            plan.state.value if hasattr(plan.state, "value") else str(plan.state),
            plan.order_id,
            str(plan.filled_shares),
            str(plan.filled_amount),
            plan.error,
            ts,
            new_version,
            plan.book_timestamp,
            plan.quote_expiry_ms,
            plan.depth_summary,
            plan.campaign_id,
            plan.client_order_id,
            plan.version,
        ))
        if cursor.rowcount == 0:
            raise sqlite3.OperationalError(
                f"Concurrency conflict: Rescue plan {plan.client_order_id} version {plan.version} has already been updated to version {current_version}"
            )
        plan.version = new_version

    conn.commit()


def load_rescue_plan(conn: sqlite3.Connection, campaign_id: str) -> RescuePlan | None:
    """Load an existing rescue plan from SQLite for recovery after restarts."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT campaign_id, attempt, client_order_id, side, shares,
               limit_price, expected_vwap, created_at_ms, ttl_ms,
               state, order_id, filled_shares, filled_amount, error, version,
               book_timestamp, quote_expiry_ms, depth_summary
        FROM prediction_rescue_plans
        WHERE campaign_id = ? AND order_purpose = 'rescue'
    """, (campaign_id,))
    row = cursor.fetchone()
    if row is None:
        return None

    side = OutcomeSide.UP if str(row[3]).upper() == "UP" else OutcomeSide.DOWN
    state = RescueState(row[9])
    return RescuePlan(
        campaign_id=row[0],
        attempt=row[1],
        client_order_id=row[2],
        side=side,
        shares=as_decimal(row[4]),
        limit_price=as_decimal(row[5]),
        expected_vwap=as_decimal(row[6]),
        created_at_ms=row[7],
        ttl_ms=row[8],
        state=state,
        order_id=row[10],
        filled_shares=as_decimal(row[11]),
        filled_amount=as_decimal(row[12]),
        error=row[13],
        version=row[14],
        book_timestamp=row[15] if len(row) > 15 else 0,
        quote_expiry_ms=row[16] if len(row) > 16 else 0,
        depth_summary=row[17] if len(row) > 17 else "",
    )


# ----------------------------------------------------------------------
# Pure Mathematical Metric Evaluators
# ----------------------------------------------------------------------

def calculate_dynamic_breakeven_win_rate(
    wins_pnl: Sequence[Decimal | str | float],
    losses_pnl: Sequence[Decimal | str | float],
) -> Decimal:
    """Compute exact dynamic breakeven win rate: BE_WR = |mean_loss| / (mean_win + |mean_loss|)."""

    wins = [as_decimal(w) for w in wins_pnl if as_decimal(w) > ZERO]
    losses = [abs(as_decimal(l)) for l in losses_pnl if as_decimal(l) < ZERO]

    if not wins or not losses:
        # Fallback to theoretical 1:8 unhedged ratio
        return Decimal("0.8890")

    mean_win = sum(wins, ZERO) / Decimal(str(len(wins)))
    mean_loss = sum(losses, ZERO) / Decimal(str(len(losses)))

    denom = mean_win + mean_loss
    if denom <= ZERO:
        return Decimal("0.8890")
    return mean_loss / denom


def calculate_paired_net_pnl(
    adaptive_pnls: Sequence[Decimal | str | float],
    control_pnls: Sequence[Decimal | str | float],
    *,
    block_size: int = 12,
    resamples: int = 10_000,
    seed: int = 42,
) -> dict[str, Any]:
    """Moving Block Bootstrap of paired Net PnL difference: Delta = PnL_adaptive - PnL_control.
    
    P1 FIX: If n < block_size, returns insufficient_data status rather than a false zero-width CI!
    """

    n = min(len(adaptive_pnls), len(control_pnls))
    deltas = [as_decimal(adaptive_pnls[i]) - as_decimal(control_pnls[i]) for i in range(n)]

    if not deltas:
        return {"mean": ZERO, "ci_lower": None, "ci_upper": None, "n": 0, "is_valid": False, "status": "no_data"}

    sample_mean = sum(deltas, ZERO) / Decimal(str(n))

    if n < block_size or resamples <= 0:
        return {
            "mean": sample_mean,
            "ci_lower": None,
            "ci_upper": None,
            "n": n,
            "is_valid": False,
            "status": "insufficient_data",
        }

    # Moving Block Bootstrap
    rng = random.Random(seed)
    num_blocks = (n + block_size - 1) // block_size
    bootstrap_means: list[Decimal] = []

    # Valid block start indices: 0 to n - block_size
    max_start = max(0, n - block_size)

    for _ in range(resamples):
        resampled: list[Decimal] = []
        for _ in range(num_blocks):
            start = rng.randint(0, max_start)
            resampled.extend(deltas[start : start + block_size])
        resampled = resampled[:n]
        b_mean = sum(resampled, Decimal("0")) / Decimal(str(len(resampled)))
        bootstrap_means.append(b_mean)

    bootstrap_means.sort()
    lower_idx = int(0.025 * len(bootstrap_means))
    upper_idx = int(0.975 * len(bootstrap_means))

    return {
        "mean": sample_mean,
        "ci_lower": bootstrap_means[lower_idx],
        "ci_upper": bootstrap_means[upper_idx],
        "n": n,
        "is_valid": True,
        "status": "ok",
    }


def is_avoidable_full_loss(
    final_pnl: Decimal | str | float,
    qualifying_rescue_snapshots: int,
    rescue_submitted: bool,
) -> bool:
    """Avoidable Full Loss: Final loss of 100% when >= 2 snapshots qualified for rescue but rescue was never submitted."""

    pnl = as_decimal(final_pnl)
    # Full loss is defined as <= -1.0 USDT (or -100%)
    if pnl <= Decimal("-1.0") and qualifying_rescue_snapshots >= 2 and not rescue_submitted:
        return True
    return False


__all__ = [
    "Tier",
    "RescueState",
    "RouteState",
    "AdaptiveConfig",
    "DEFAULT_ADAPTIVE_CONFIG",
    "RescuePlan",
    "SweepResult",
    "check_positive_freshness",
    "check_clean_quote",
    "evaluate_hysteresis_router",
    "apply_asymmetric_debounce",
    "on_order_submitted",
    "on_order_cancelled",
    "on_order_filled",
    "on_rescue_submitted",
    "on_rescue_submit_failed",
    "adaptive_entry_decision",
    "sweep_orderbook_bids",
    "calculate_ledger_net_improvement",
    "step_rescue_fsm",
    "adaptive_rescue_decision",
    "init_rescue_plan_schema",
    "persist_rescue_plan_tx",
    "load_rescue_plan",
    "calculate_dynamic_breakeven_win_rate",
    "calculate_paired_net_pnl",
    "is_avoidable_full_loss",
]
