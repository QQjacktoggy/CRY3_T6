"""Pure Prediction campaign strategy and PnL helpers.

The functions in this module do not call Binance, sleep, or mutate a
campaign.  The runtime can persist a returned intent before submitting it and
can safely replay the same inputs during shadow mode.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

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


DEFAULT_STRATEGY_PROFILE = "prediction-v1"
CONTROL_PROFILE = "control"
STABLE_ENTRY_HOLD_PROFILE = "prediction-v1.1-stable-entry-hold"
SHADOW_LANE_A_PROFILE = "prediction-shadow-lane-a"
SHADOW_LANE_B_PROFILE = "prediction-shadow-lane-b"
BALANCED_HOLD_PROFILE = "balanced_hold"
QUALITY_HOLD_PROFILE = "quality_hold"
QUALITY_HOLD_V2_PROFILE = "quality_hold_v2"
QUALITY_HOLD_V3_PROFIT1_PROFILE = "quality_hold_v3_profit1"
QUALITY_HOLD_V3_GATE_V1_PROFILE = "quality_hold_v3_gate_v1"
QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE = "quality_hold_v3_loss_guard_v2"
QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE = "quality_hold_v3_net_edge_v1"
QUALITY_HOLD_V3_MOMENTUM30_V1_PROFILE = "quality_hold_v3_momentum30_v1"
QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE = "quality_hold_v3_confirm30_v1"
QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE = "quality_hold_v3_history30_control_v1"
V3_MOMENTUM30_PROFILES = frozenset({
    QUALITY_HOLD_V3_MOMENTUM30_V1_PROFILE, QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE,
    QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE,
})
V3_MOMENTUM30_MIN_HISTORY_AGE_MS = 30_000
V3_MOMENTUM30_MAX_HISTORY_AGE_MS = 45_000
V3_NET_EDGE_MAX_COST_PER_SHARE = Decimal("0.85")
QUALITY_HOLD_V4_RESCUE_PROFILE = "quality_hold_v4_rescue"
QUALITY_HOLD_V5_PNL_PROFILE = "quality_hold_v5_pnl"
QUALITY_HOLD_V6_A_STAGED_PROFILE = "quality_hold_v6_a_staged"
QUALITY_HOLD_V6_BALANCED_SHADOW_PROFILE = "quality_hold_v6_balanced_shadow"
QUALITY_HOLD_V6_B_45S_PROFILE = "quality_hold_v6_b_45s"
QUALITY_HOLD_V6_C_45S_PROFILE = "quality_hold_v6_c_45s"
QUALITY_HOLD_V3_SNIPER_PROFILE = "quality_hold_v3_sniper"
LATE_MATURITY_V1_PROFILE = "late_maturity_v1"
# V7 is a direct Live lane.  It deliberately uses an explicit model feature
# envelope produced by the worker rather than pretending the binary book is
# itself a price forecast.
REGIME_VALUE_V7_PROFILE = "regime_value_v7"
REGIME_VALUE_V8_CALIBRATED_PROFILE = "regime_value_v8_calibrated"
# Public short-horizon ideas are represented as isolated Shadow-only lanes.
# These are deliberately named independently from the existing A/B lanes so
# their provenance and settlement evidence cannot be mixed accidentally.
LATENCY_SNIPE_PROFILE = "latency_snipe"
PAIR_COST_ARB_PROFILE = "pair_cost_arb"
COMPLETE_SET_ARB_V1_PROFILE = "complete_set_arb_v1"
S3S5_PAIR_V1_PROFILE = "s3s5_pair_v1"
C180_FAVORITE_HOLD_PROFILE = "c180_favorite_hold_v1"
DIVERSE5_LANES = ('trend_continuation_v1', 'reference_reversion_v1', 'spot_book_lag_v1', 'diffusion_value_v1', 'late_distance_v1')
REVERSAL5_LANES = ('open_hold_control_v1', 'open_first_flip_equal_v1', 'open_first_flip_balanced_v1', 'open_profit_lock_v1', 'open_profit_lock_rescue_v1')
NEXT5_LANES = ('early_value_v1', 'trend_value_v1', 'reversion_value_v1', 'late_oracle_watch_v1', 'passive_queue_watch_v1')
CONFIRM3_LANES = ('trend_control_1s_v1', 'reversion_control_1s_v1', 'reversion_control_3s_v1')
VALUE9_LANES = ('vol_cap_3s', 'vol_main_3s', 'vol_delay_1s', 'vol_delay_5s', 'vol_edge_05', 'vol_price_75', 'vol_late_210')

SHADOW_ONLY_PROFILES = frozenset({
    *VALUE9_LANES,
    *CONFIRM3_LANES,
    *NEXT5_LANES,
    *REVERSAL5_LANES,
    *DIVERSE5_LANES,
    *V3_MOMENTUM30_PROFILES,
    QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE,
    QUALITY_HOLD_V3_GATE_V1_PROFILE,
    QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE,
    QUALITY_HOLD_V6_B_45S_PROFILE,
    QUALITY_HOLD_V6_C_45S_PROFILE,
    QUALITY_HOLD_V3_SNIPER_PROFILE,
    LATE_MATURITY_V1_PROFILE,
    REGIME_VALUE_V8_CALIBRATED_PROFILE,
    COMPLETE_SET_ARB_V1_PROFILE,
})

# V3 gate thresholds are kept beside the pure policy so the worker and unit
# tests share the exact decimal boundary semantics.  The gate is deliberately
# an entry-only Shadow provenance layer; it does not alter the V3 position
# management policy after a BUY_INITIAL has been simulated.
V3_GATE_VERSION = QUALITY_HOLD_V3_GATE_V1_PROFILE
V3_GATE_MIN_HISTORY_AGE_MS = 30_000
V3_GATE_MAX_HISTORY_AGE_MS = 60_000
V3_GATE_TARGET_HISTORY_AGE_MS = 45_000
V3_GATE_ASK_BAND_MIN = Decimal("-0.10")
V3_GATE_ASK_BAND_MAX = Decimal("0.05")
V3_GATE_ASK_JUMP_MIN = Decimal("0.20")
V3_GATE_MARGIN_MIN_BPS = Decimal("6")
V3_GATE_MARGIN_MAX_BPS = Decimal("10")


@dataclass(frozen=True)
class V3GateConfig:
    """Immutable, serializable provenance for the V3 Shadow gate."""

    version: str = V3_GATE_VERSION
    history_min_age_ms: int = V3_GATE_MIN_HISTORY_AGE_MS
    history_max_age_ms: int = V3_GATE_MAX_HISTORY_AGE_MS
    history_target_age_ms: int = V3_GATE_TARGET_HISTORY_AGE_MS
    ask_band_min: Decimal = V3_GATE_ASK_BAND_MIN
    ask_band_max: Decimal = V3_GATE_ASK_BAND_MAX
    ask_jump_min: Decimal = V3_GATE_ASK_JUMP_MIN
    margin_min_bps: Decimal = V3_GATE_MARGIN_MIN_BPS
    margin_max_bps: Decimal = V3_GATE_MARGIN_MAX_BPS
    allowed_regimes: tuple[str, ...] = ("GREEN", "YELLOW")


V3_GATE_CONFIG = V3GateConfig()


@dataclass(frozen=True)
class PnlResult:
    mark_pnl: Decimal
    if_up: Decimal
    if_down: Decimal
    hedge_floor: Decimal
    realized_cash: Decimal
    total_invested: Decimal
    fees: Decimal

    @property
    def best_case(self) -> Decimal:
        return max(self.if_up, self.if_down)

    @property
    def worst_case(self) -> Decimal:
        return min(self.if_up, self.if_down)


def calculate_pnl(
    position: Position | Campaign,
    quote: QuoteSnapshot | None = None,
    *,
    up_bid: Decimal | str | float | None = None,
    down_bid: Decimal | str | float | None = None,
    payout_per_share: Decimal | str | float = Decimal("1"),
) -> PnlResult:
    """Return mark, both settlement outcomes, and the hedge floor.

    The calculation is deliberately conservative: a missing/stale bid is
    represented by zero rather than an optimistic last trade price.  Sell
    proceeds already live in ``realized_cash`` and every recorded fee is
    subtracted once.
    """

    ledger = position.position if isinstance(position, Campaign) else position
    if quote is not None:
        if up_bid is None:
            up_bid = quote.up_bid
        if down_bid is None:
            down_bid = quote.down_bid
    up = as_decimal(up_bid) if up_bid is not None else ZERO
    down = as_decimal(down_bid) if down_bid is not None else ZERO
    payout = as_decimal(payout_per_share)
    if payout < ZERO:
        raise ValueError("payout_per_share cannot be negative")
    base = ledger.realized_cash - ledger.total_buy_cost - ledger.fees
    if_up = base + ledger.up_shares * payout
    if_down = base + ledger.down_shares * payout
    return PnlResult(
        mark_pnl=base + ledger.up_shares * up + ledger.down_shares * down,
        if_up=if_up,
        if_down=if_down,
        hedge_floor=min(if_up, if_down),
        realized_cash=ledger.realized_cash,
        total_invested=ledger.total_buy_cost,
        fees=ledger.fees,
    )


@dataclass(frozen=True)
class StrategyConfig:
    """V1.1 policy constants derived from the first 51 Shadow markets.

    The stability gates are deliberately based on campaign history that is
    already persisted by the quote collector.  They keep the candidate
    reproducible across restarts and make the strategy profile part of the
    Shadow provenance hash in ``PredictionSettings``.
    """

    profile: str = DEFAULT_STRATEGY_PROFILE
    gate_config: V3GateConfig | None = None
    gate_provenance: str | None = None
    entry_start_seconds: int = 120
    entry_end_seconds: int = 270
    entry_min_remaining_seconds: int | None = None
    entry_max_remaining_seconds: int | None = None
    final_hold_seconds: int = 30
    trading_cutoff_seconds: int = 15
    min_entry_price: Decimal = Decimal("0.70")
    max_entry_price: Decimal = Decimal("0.89")
    require_stable_entry_history: bool = False
    max_entry_leader_flips: int = 0
    max_entry_reference_crosses: int = 0
    profit_lock_price: Decimal = Decimal("0.95")
    # Optional remaining-time ceiling and strong-lead exemption.  When set,
    # a profit lock is considered only after the campaign enters this window;
    # bids at or above the strong-lead threshold keep the full position.
    profit_lock_max_remaining_seconds: int | None = None
    profit_lock_strong_hold_bid: Decimal | None = None
    final_hold_min_price: Decimal = Decimal("0.80")
    reversal_max_loser_price: Decimal = Decimal("0.30")
    loser_min_sell_price: Decimal = Decimal("0.25")
    loser_confirm_price: Decimal = Decimal("0.80")
    max_buy_usdt: Decimal = Decimal("1")
    max_market_buy_usdt: Decimal = Decimal("2")
    # Prediction quote fees are configured separately from strategy price.
    # V1 defaults to zero because the quote response is the source of truth;
    # a non-zero value can be supplied for conservative shadow analysis.
    hedge_fee_usdt: Decimal = Decimal("0")
    hedge_floor_improvement: Decimal = Decimal("0.10")
    max_profit_lock_fraction: Decimal = Decimal("0.25")
    profit_lock_enabled: bool = True
    max_loser_unwind_fraction: Decimal = Decimal("0.50")
    loser_unwind_fraction: Decimal = Decimal("0.25")
    loser_unwind_min_pnl_improvement: Decimal = Decimal("0.05")
    loser_outcome_floor: Decimal = Decimal("-1")
    max_profit_locks: int = 1
    max_hedges: int = 1
    max_loser_unwinds: int = 2
    max_initial_attempts: int = 2
    max_scale_in_attempts: int = 0
    max_hedge_attempts: int = 2
    initial_ttl_ms: int = 3_000
    hedge_ttl_ms: int = 2_000
    max_quote_age_ms: int = 1_500
    min_hedged_wait_ms: int = 5_000
    min_direction_confirm_ms: int = 5_000
    require_quote_stable_final: bool = False
    min_entry_leader_duration_seconds: int = 0
    max_entry_leader_duration_seconds: int | None = None
    require_clean_entry_quote: bool = False
    require_btc_alignment: bool = True
    # Absolute BTC/reference displacement in basis points.  This is a small,
    # deterministic proxy for the public bots' window-delta/fair-value gate.
    min_btc_move_bps: Decimal = Decimal("0")
    # Optional side-specific floors.  A missing override falls back to the
    # common floor above, preserving every existing profile's behaviour.
    min_up_btc_move_bps: Decimal | None = None
    min_down_btc_move_bps: Decimal | None = None
    max_btc_move_bps: Decimal | None = None
    max_adverse_btc_velocity_diff: Decimal | None = None
    balanced_relaxation_enabled: bool = False
    balanced_relax_max_bps: Decimal = Decimal("4")
    # Pair-cost lane controls.  It is a top-of-book immediate-fill proxy, not
    # a true maker/queue simulation; keep it disabled for ordinary profiles.
    pair_cost_enabled: bool = False
    pair_cost_max_total_price: Decimal = Decimal("0.985")
    pair_cost_min_floor: Decimal = Decimal("0.005")
    pair_cost_total_cost_bps: Decimal = Decimal("0")
    allow_hedge: bool = True
    # V4 reduce-only reversal rescue.  This is intentionally separate from
    # the existing hedge/unwind path: it never buys the opposite side and it
    # is admitted only for cheap initial entries whose held bid has failed.
    direct_rescue_enabled: bool = False
    rescue_entry_price_max: Decimal = Decimal("0.80")
    rescue_bid_trigger: Decimal = Decimal("0.20")
    rescue_min_sell_price: Decimal = Decimal("0.15")
    rescue_confirm_bid: Decimal = Decimal("0.80")
    rescue_min_leader_duration_ms: int = 10_000
    rescue_min_remaining_seconds: int = 30
    rescue_min_pnl_improvement: Decimal = Decimal("0.05")
    rescue_outcome_floor: Decimal = Decimal("-2.00")
    # PnL-first staged sizing and reduce-only exits.  The first BUY remains
    # one USDT even when the operator selects two; the second unit is admitted
    # only after fresh BTC/book confirmation.
    pnl_priority_enabled: bool = False
    pnl_scale_in_enabled: bool = False
    pnl_scale_in_usdt: Decimal = Decimal("1")
    pnl_scale_min_hold_seconds: int = 30
    pnl_scale_min_remaining_seconds: int = 60
    pnl_scale_max_remaining_seconds: int | None = None
    pnl_scale_max_ask: Decimal = Decimal("0.85")
    # ``None`` preserves legacy V5 scale-in behaviour; V6 supplies a tight
    # explicit premium cap for the second leg.
    pnl_scale_max_entry_premium: Decimal | None = None
    pnl_scale_min_btc_bps: Decimal = Decimal("2")
    pnl_scale_min_leader_duration_ms: int = 10_000
    pnl_scale_max_leader_duration_ms: int | None = None
    pnl_profit_min_gain: Decimal = Decimal("0.10")
    pnl_profit_btc_drawdown_bps: Decimal = Decimal("2")
    pnl_profit_min_remaining_seconds: int = 30
    pnl_reversal_min_leader_duration_ms: int = 10_000
    pnl_exit_min_bid: Decimal = Decimal("0.01")
    # Conservative fixed-unit reversal insurance retained for legacy internal
    # profiles.  It is not exposed by any operator-selectable lane.
    flip1_enabled: bool = False
    flip1_hedge_min_remaining_seconds: int = 60
    flip1_hedge_max_remaining_seconds: int = 90
    flip1_hedge_min_price: Decimal = Decimal("0.45")
    flip1_hedge_max_price: Decimal = Decimal("0.55")
    flip1_initial_bid_min: Decimal = Decimal("0.45")
    flip1_initial_bid_max: Decimal = Decimal("0.55")
    flip1_hedge_min_leader_duration_ms: int = 30_000
    flip1_min_post_hedge_floor: Decimal = Decimal("-0.85")
    flip1_unwind_min_remaining_seconds: int = 30
    flip1_unwind_max_remaining_seconds: int = 45
    flip1_unwind_min_btc_move_bps: Decimal = Decimal("1")
    flip1_unwind_min_winner_bid: Decimal = Decimal("0.75")
    flip1_unwind_min_bid_gap: Decimal = Decimal("0.20")
    # Late-window loss containment for the flip1 live lane.  A committed
    # reversal near the BTC reference first reduces the original leg by 50%;
    # a later recovery can liquidate the remainder before the hard cutoff.
    late_risk_exit_enabled: bool = False
    late_risk_partial_min_remaining_seconds: int = 30
    late_risk_partial_max_remaining_seconds: int = 60
    late_risk_near_reference_bps: Decimal = Decimal("0.5")
    late_risk_partial_fraction: Decimal = Decimal("0.50")
    late_risk_recovery_min_remaining_seconds: int = 15
    late_risk_recovery_max_remaining_seconds: int = 30
    late_risk_recovery_min_bid: Decimal = Decimal("0.80")
    protective_exit_enabled: bool = False
    protective_exit_bid: Decimal = Decimal("0")
    protective_exit_min_hold_seconds: int = 5
    # V7 multi-regime fair-value controls.  The worker records the feature
    # values used below into the durable quote payload so every live decision
    # can be replayed.  These values are intentionally isolated from V3-V6.
    v7_enabled: bool = False
    v7_min_feature_samples: int = 3
    v7_trend_min_remaining_seconds: int = 75
    v7_trend_max_remaining_seconds: int = 180
    v7_trend_max_ask: Decimal = Decimal("0.75")
    v7_trend_min_edge: Decimal = Decimal("0.05")
    v7_trend_min_abs_z: Decimal = Decimal("0.85")
    v7_trend_min_leader_duration_seconds: int = 60
    v7_late_min_remaining_seconds: int = 20
    v7_late_max_remaining_seconds: int = 60
    v7_late_min_probability: Decimal = Decimal("0.94")
    v7_late_max_ask: Decimal = Decimal("0.88")
    v7_late_min_edge: Decimal = Decimal("0.04")
    v7_late_min_abs_z: Decimal = Decimal("1.55")
    v7_late_min_leader_duration_seconds: int = 20
    v7_max_book_spread: Decimal = Decimal("0.04")
    v7_cross_cooldown_seconds: int = 30
    v7_high_volatility_bps_sqrt_second: Decimal = Decimal("0.95")
    v7_scale_min_hold_seconds: int = 20
    v7_scale_min_remaining_seconds: int = 75
    v7_scale_max_remaining_seconds: int = 165
    v7_scale_min_edge: Decimal = Decimal("0.08")
    v7_scale_min_z_delta: Decimal = Decimal("0")
    v7_scale_max_entry_premium: Decimal = Decimal("0.01")
    # V8 is a frozen Shadow calibration of the V7 Brownian probability.  It
    # deliberately shrinks confidence toward 50%, subtracts an uncertainty
    # margin, and charges an all-in execution reserve before testing edge.
    v8_enabled: bool = False
    v8_min_remaining_seconds: int = 60
    v8_max_remaining_seconds: int = 120
    v8_probability_shrink: Decimal = Decimal("0.85")
    v8_uncertainty_margin: Decimal = Decimal("0.02")
    v8_total_cost_bps: Decimal = Decimal("150")
    v8_min_conservative_edge: Decimal = Decimal("0.05")
    v8_max_ask: Decimal = Decimal("0.90")
    v8_min_abs_z: Decimal = Decimal("0.85")
    v8_min_leader_duration_seconds: int = 60

    @classmethod
    def for_profile(cls, profile: str | None) -> "StrategyConfig":
        selected = str(profile or DEFAULT_STRATEGY_PROFILE).strip().lower()
        if selected in (*CONFIRM3_LANES, *VALUE9_LANES):
            return replace(cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=selected,max_buy_usdt=Decimal('2'),max_market_buy_usdt=Decimal('2.1'),
                max_initial_attempts=1,max_scale_in_attempts=0,max_hedge_attempts=0,
                gate_provenance=('value9-v1:research-only:first-eligible:cost300bps:stress500bps'
                    if selected in VALUE9_LANES else
                    'confirm3-v1:research-controls:cost300bps:not-executable'))
        if selected in NEXT5_LANES:
            return replace(cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=selected,max_buy_usdt=Decimal('2'),max_market_buy_usdt=Decimal('4.5'),
                max_initial_attempts=1,max_scale_in_attempts=0,max_hedge_attempts=0,
                gate_provenance='next5-v1:collection-only:model-d42d7459e245b0ccd22fed3f269fb93818e3f62842fc12b328bf2c357d67c51c')
        if selected in REVERSAL5_LANES:
            return replace(cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=selected,max_buy_usdt=Decimal('2.5'),max_market_buy_usdt=Decimal('4.5'),
                max_initial_attempts=1,max_scale_in_attempts=0,max_hedge_attempts=1,
                gate_provenance='reversal5-v1:early30:observed5s:delay1s:cost300:stress500:uncalibrated')
        if selected in DIVERSE5_LANES:
            return replace(cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=selected, max_buy_usdt=Decimal("2"), max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=1, max_scale_in_attempts=0, max_hedge_attempts=0,
                gate_provenance="diverse5-frozen-v1:uncalibrated:cost300bps")
        if selected == DEFAULT_STRATEGY_PROFILE:
            return cls(profile=DEFAULT_STRATEGY_PROFILE)
        if selected == CONTROL_PROFILE:
            # ``control`` is the operator-facing name for the baseline policy.
            return cls(profile=CONTROL_PROFILE)
        if selected == STABLE_ENTRY_HOLD_PROFILE:
            return cls(
                profile=STABLE_ENTRY_HOLD_PROFILE,
                entry_end_seconds=210,
                require_stable_entry_history=True,
                max_entry_leader_flips=0,
                max_entry_reference_crosses=0,
                profit_lock_enabled=False,
            )
        if selected == SHADOW_LANE_A_PROFILE:
            return cls(
                profile=SHADOW_LANE_A_PROFILE,
                entry_end_seconds=210,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                profit_lock_enabled=False,
                allow_hedge=False,
                protective_exit_enabled=True,
                protective_exit_bid=Decimal("0.25"),
                protective_exit_min_hold_seconds=5,
            )
        if selected == SHADOW_LANE_B_PROFILE:
            return cls(
                profile=SHADOW_LANE_B_PROFILE,
                entry_end_seconds=150,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                profit_lock_enabled=False,
                allow_hedge=False,
                protective_exit_enabled=True,
                protective_exit_bid=Decimal("0.55"),
                protective_exit_min_hold_seconds=5,
            )
        if selected == BALANCED_HOLD_PROFILE:
            return cls(
                profile=BALANCED_HOLD_PROFILE,
                entry_start_seconds=90,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.89"),
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=60,
                require_clean_entry_quote=True,
                require_btc_alignment=False,
                profit_lock_enabled=False,
                allow_hedge=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_PROFILE:
            # Permanent Live profile promoted by the operator after the
            # 2026-08-28 Live replay.  It opens the observation window earlier
            # than balanced_hold while rejecting late/overextended leaders
            # and excessive BTC moves.
            return cls(
                profile=QUALITY_HOLD_PROFILE,
                entry_start_seconds=60,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                initial_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=30,
                max_entry_leader_duration_seconds=90,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("0"),
                max_btc_move_bps=Decimal("11"),
                profit_lock_enabled=False,
                allow_hedge=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_V2_PROFILE:
            # Isolated comparison lane derived from the 2026-08-29 replay.
            # It filters weak BTC displacement while admitting leaders up to
            # 100 seconds.  It intentionally has no late reverse add/hedge.
            return cls(
                profile=QUALITY_HOLD_V2_PROFILE,
                entry_start_seconds=60,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                initial_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=30,
                max_entry_leader_duration_seconds=100,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("3.5"),
                max_btc_move_bps=Decimal("11"),
                profit_lock_enabled=False,
                allow_hedge=False,
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_V3_PROFIT1_PROFILE:
            # V3 keeps every V2 quality/risk gate but uses the directional
            # displacement floors validated by the 2026-08-29 replay.  It is
            # an independent Live lane, with no reverse add or hedge.
            return cls(
                profile=QUALITY_HOLD_V3_PROFIT1_PROFILE,
                entry_start_seconds=60,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                initial_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=30,
                max_entry_leader_duration_seconds=100,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("2"),
                min_up_btc_move_bps=Decimal("3"),
                min_down_btc_move_bps=Decimal("2"),
                max_btc_move_bps=Decimal("11"),
                profit_lock_enabled=False,
                allow_hedge=False,
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_V3_GATE_V1_PROFILE:
            # The gate lane is an exact V3 policy clone.  Its only behavioural
            # difference is applied by the Shadow worker before BUY_INITIAL;
            # keeping this as a replace makes config-parity auditable.
            return replace(
                cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=QUALITY_HOLD_V3_GATE_V1_PROFILE,
                gate_config=V3_GATE_CONFIG,
                gate_provenance=V3_GATE_VERSION,
            )
        if selected in V3_MOMENTUM30_PROFILES:
            return replace(
                cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=selected,
                gate_provenance=f"first-base-candidate-v1:{selected}:history-control" if selected == QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE else f"first-base-candidate-v1:{selected}:sign-only:not-calibrated-EV",
                max_buy_usdt=Decimal("2"),
                max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=1,
                max_scale_in_attempts=0,
                max_hedge_attempts=0,
            )
        if selected == QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE:
            # Research hypothesis only: the worker freezes the FIRST complete
            # Base V3 candidate, then applies a modeled all-in cost cap. Keep
            # the Base ask ceiling here: lowering it would permit later entry.
            return replace(
                cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE,
                gate_provenance="first-base-candidate-v1:modeled-cost-cap=0.85:not-calibrated-EV",
                max_buy_usdt=Decimal("2"),
                max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=1,
                max_scale_in_attempts=0,
                max_hedge_attempts=0,
            )
        if selected == QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE:
            # Aggregate replay over 139 settled V3 fills selected these two
            # causal restrictions.  They preserve V3 position management and
            # only reject weak DOWN displacement and over-mature leaders.
            return replace(
                cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE,
                min_btc_move_bps=Decimal("3"),
                min_up_btc_move_bps=Decimal("3"),
                min_down_btc_move_bps=Decimal("3"),
                max_entry_leader_duration_seconds=80,
            )
        if selected == QUALITY_HOLD_V3_SNIPER_PROFILE:
            # V3-Sniper: Optimized V3 with 3.1 bps displacement floor,
            # max entry price capped at 0.88, and adverse BTC velocity filter.
            return replace(
                cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=QUALITY_HOLD_V3_SNIPER_PROFILE,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.88"),
                min_btc_move_bps=Decimal("3.1"),
                min_up_btc_move_bps=Decimal("3.1"),
                min_down_btc_move_bps=Decimal("3.1"),
                max_adverse_btc_velocity_diff=Decimal("-30.0"),
            )
        if selected == LATE_MATURITY_V1_PROFILE:
            # Pre-registered fresh-200 candidate: exact V3 policy with only
            # the entry clock narrowed to elapsed 180-239 seconds.
            return replace(
                cls.for_profile(QUALITY_HOLD_V3_PROFIT1_PROFILE),
                profile=LATE_MATURITY_V1_PROFILE,
                entry_start_seconds=180,
                entry_end_seconds=240,
                max_initial_attempts=1,
            )
        if selected == QUALITY_HOLD_V4_RESCUE_PROFILE:
            # V4 preserves V3's entry gates and adds a reduce-only response
            # for the observed low-entry reversal cluster.  It never adds a
            # second BUY: a reversal hedge at these prices is uneconomic.
            return cls(
                profile=QUALITY_HOLD_V4_RESCUE_PROFILE,
                entry_start_seconds=60,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                initial_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=30,
                max_entry_leader_duration_seconds=100,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("2"),
                min_up_btc_move_bps=Decimal("3"),
                min_down_btc_move_bps=Decimal("2"),
                max_btc_move_bps=Decimal("11"),
                profit_lock_enabled=False,
                allow_hedge=False,
                direct_rescue_enabled=True,
                rescue_entry_price_max=Decimal("0.80"),
                rescue_bid_trigger=Decimal("0.20"),
                rescue_min_sell_price=Decimal("0.15"),
                rescue_confirm_bid=Decimal("0.80"),
                rescue_min_leader_duration_ms=10_000,
                rescue_min_remaining_seconds=30,
                rescue_min_pnl_improvement=Decimal("0.05"),
                rescue_outcome_floor=Decimal("-2.00"),
                max_loser_unwinds=2,
                max_loser_unwind_fraction=Decimal("0.50"),
                loser_unwind_fraction=Decimal("0.25"),
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_V5_PNL_PROFILE:
            # V5 keeps the V3/V4 entry quality gates, starts with one USDT,
            # and conditionally adds one more unit only while BTC and the
            # selected book leader remain aligned.  Exits are reduce-only.
            return cls(
                profile=QUALITY_HOLD_V5_PNL_PROFILE,
                entry_start_seconds=60,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                initial_ttl_ms=5_000,
                hedge_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=30,
                max_entry_leader_duration_seconds=100,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("2"),
                min_up_btc_move_bps=Decimal("3"),
                min_down_btc_move_bps=Decimal("2"),
                max_btc_move_bps=Decimal("11"),
                profit_lock_enabled=False,
                allow_hedge=False,
                direct_rescue_enabled=False,
                pnl_priority_enabled=True,
                max_scale_in_attempts=1,
                pnl_scale_in_usdt=Decimal("1"),
                pnl_scale_min_hold_seconds=30,
                pnl_scale_min_remaining_seconds=60,
                pnl_scale_max_ask=Decimal("0.85"),
                pnl_scale_min_btc_bps=Decimal("2"),
                pnl_scale_min_leader_duration_ms=10_000,
                pnl_profit_min_gain=Decimal("0.10"),
                pnl_profit_btc_drawdown_bps=Decimal("2"),
                pnl_profit_min_remaining_seconds=30,
                pnl_reversal_min_leader_duration_ms=10_000,
                pnl_exit_min_bid=Decimal("0.01"),
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_V6_A_STAGED_PROFILE:
            # A-grade staged sizing: enter one unit only after the mature
            # leader window, then add one unit only after another 30 seconds
            # of uninterrupted confirmation at a value-protecting price.
            return cls(
                profile=QUALITY_HOLD_V6_A_STAGED_PROFILE,
                entry_start_seconds=120,
                entry_end_seconds=285,
                entry_min_remaining_seconds=60,
                entry_max_remaining_seconds=180,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=1,
                initial_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=60,
                max_entry_leader_duration_seconds=100,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("2"),
                min_up_btc_move_bps=Decimal("3"),
                min_down_btc_move_bps=Decimal("2"),
                max_btc_move_bps=Decimal("11"),
                profit_lock_enabled=False,
                allow_hedge=False,
                direct_rescue_enabled=False,
                pnl_priority_enabled=False,
                pnl_scale_in_enabled=True,
                max_scale_in_attempts=1,
                pnl_scale_in_usdt=Decimal("1"),
                pnl_scale_min_hold_seconds=30,
                pnl_scale_min_remaining_seconds=60,
                pnl_scale_max_remaining_seconds=150,
                pnl_scale_max_ask=Decimal("0.85"),
                pnl_scale_max_entry_premium=Decimal("0.01"),
                pnl_scale_min_btc_bps=Decimal("2"),
                pnl_scale_min_leader_duration_ms=90_000,
                pnl_scale_max_leader_duration_ms=100_000,
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == QUALITY_HOLD_V6_B_45S_PROFILE:
            return replace(
                cls.for_profile(QUALITY_HOLD_V6_A_STAGED_PROFILE),
                profile=QUALITY_HOLD_V6_B_45S_PROFILE,
                min_entry_leader_duration_seconds=45,
                max_entry_leader_duration_seconds=120,
                min_entry_price=Decimal("0.75"),
            )
        if selected == QUALITY_HOLD_V6_C_45S_PROFILE:
            return replace(
                cls.for_profile(QUALITY_HOLD_V6_A_STAGED_PROFILE),
                profile=QUALITY_HOLD_V6_C_45S_PROFILE,
                min_entry_leader_duration_seconds=45,
                max_entry_leader_duration_seconds=120,
                min_entry_price=Decimal("0.78"),
            )
        if selected == QUALITY_HOLD_V6_BALANCED_SHADOW_PROFILE:
            # Shadow-only comparison: it admits the early (<180s remaining)
            # low-displacement branch, but never enables staged sizing.
            return cls(
                profile=QUALITY_HOLD_V6_BALANCED_SHADOW_PROFILE,
                entry_start_seconds=60,
                entry_end_seconds=285,
                entry_min_remaining_seconds=60,
                entry_max_remaining_seconds=180,
                min_entry_price=Decimal("0.70"),
                max_entry_price=Decimal("0.92"),
                max_initial_attempts=1,
                initial_ttl_ms=5_000,
                require_quote_stable_final=True,
                min_entry_leader_duration_seconds=60,
                max_entry_leader_duration_seconds=100,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("2"),
                min_up_btc_move_bps=Decimal("3"),
                min_down_btc_move_bps=Decimal("2"),
                max_btc_move_bps=Decimal("11"),
                balanced_relaxation_enabled=True,
                balanced_relax_max_bps=Decimal("4"),
                profit_lock_enabled=False,
                allow_hedge=False,
                direct_rescue_enabled=False,
                pnl_priority_enabled=False,
                pnl_scale_in_enabled=False,
                max_scale_in_attempts=0,
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == REGIME_VALUE_V7_PROFILE:
            # Live V7 uses a BTC/reference probability model as a *value
            # gate*, then validates the executable Prediction ask.  It starts
            # with one USDT, permits a second unit only after a strengthened
            # signal, and otherwise holds to settlement.  In high volatility
            # it makes no directional entry: the current executor cannot
            # guarantee atomic two-leg pair fills, so taking an unhedged first
            # leg would be less safe than abstaining.
            return cls(
                profile=REGIME_VALUE_V7_PROFILE,
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=1,
                max_scale_in_attempts=1,
                initial_ttl_ms=8_000,
                hedge_ttl_ms=8_000,
                require_btc_alignment=True,
                profit_lock_enabled=False,
                allow_hedge=False,
                direct_rescue_enabled=False,
                pnl_priority_enabled=False,
                pnl_scale_in_enabled=False,
                flip1_enabled=False,
                late_risk_exit_enabled=False,
                protective_exit_enabled=False,
                v7_enabled=True,
            )
        if selected == REGIME_VALUE_V8_CALIBRATED_PROFILE:
            return replace(
                cls.for_profile(REGIME_VALUE_V7_PROFILE),
                profile=REGIME_VALUE_V8_CALIBRATED_PROFILE,
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=Decimal("1"),
                max_scale_in_attempts=0,
                v7_enabled=False,
                v8_enabled=True,
            )
        if selected == LATENCY_SNIPE_PROFILE:
            return cls(
                profile=LATENCY_SNIPE_PROFILE,
                entry_start_seconds=240,
                entry_end_seconds=285,
                min_entry_price=Decimal("0.55"),
                max_entry_price=Decimal("0.94"),
                require_quote_stable_final=True,
                require_clean_entry_quote=True,
                require_btc_alignment=True,
                min_btc_move_bps=Decimal("3"),
                profit_lock_enabled=False,
                allow_hedge=False,
                protective_exit_enabled=False,
            )
        if selected == PAIR_COST_ARB_PROFILE:
            return cls(
                profile=PAIR_COST_ARB_PROFILE,
                entry_start_seconds=45,
                entry_end_seconds=270,
                min_entry_price=Decimal("0.05"),
                max_entry_price=Decimal("0.95"),
                require_btc_alignment=False,
                pair_cost_enabled=True,
                pair_cost_max_total_price=Decimal("0.985"),
                pair_cost_min_floor=Decimal("0.005"),
                max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=1,
                max_hedge_attempts=1,
                profit_lock_enabled=False,
                protective_exit_enabled=False,
            )
        if selected == COMPLETE_SET_ARB_V1_PROFILE:
            # Shadow-only equal-share pair.  The tighter sum cap leaves room
            # for a 125 bps fee reserve plus 25 bps slippage reserve.
            return replace(
                cls.for_profile(PAIR_COST_ARB_PROFILE),
                profile=COMPLETE_SET_ARB_V1_PROFILE,
                pair_cost_max_total_price=Decimal("0.970"),
                pair_cost_min_floor=Decimal("0.05"),
                pair_cost_total_cost_bps=Decimal("150"),
            )
        if selected in {C180_FAVORITE_HOLD_PROFILE, "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            return replace(
                cls.for_profile("fav_only_v1"),
                profile=selected,
                entry_start_seconds=60 if selected == "regime_target6_7_v1" else 120,
                entry_end_seconds=270 if selected == "regime_target6_7_v1" else 184 if selected in ("regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1") else 136,
                max_initial_attempts=1,
                max_scale_in_attempts=0,
                max_hedge_attempts=0,
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=Decimal("1"),
                initial_ttl_ms=12_000,
                profit_lock_enabled=False,
                protective_exit_enabled=False,
                late_risk_exit_enabled=False,
                pnl_scale_in_enabled=False,
            )
        if selected in {S3S5_PAIR_V1_PROFILE, "fav_only_v1", "fav_only_v2", "fav_only_v3", "fav_only_v4", "fav_p3"}:
            return cls(
                profile=selected,
                entry_start_seconds=20,
                entry_end_seconds=270,
                min_entry_price=Decimal("0.20"),
                max_entry_price=Decimal("0.85"),
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=Decimal("2"),
                max_initial_attempts=2,
                max_scale_in_attempts=1,
                initial_ttl_ms=5_000,
                profit_lock_enabled=False,
                allow_hedge=False,
                require_btc_alignment=False,
                require_quote_stable_final=False,
                pnl_scale_in_enabled=False,
            )
        raise ValueError(f"unsupported prediction strategy profile: {profile!r}")

    @property
    def provenance_payload(self) -> dict[str, object]:
        """Secret-free strategy identity included in Shadow provenance."""

        payload: dict[str, object] = {
            "profile": self.profile,
            "entry_start_seconds": int(self.entry_start_seconds),
            "entry_end_seconds": int(self.entry_end_seconds),
            "entry_min_remaining_seconds": self.entry_min_remaining_seconds,
            "entry_max_remaining_seconds": self.entry_max_remaining_seconds,
            "min_entry_price": str(self.min_entry_price),
            "max_entry_price": str(self.max_entry_price),
            "require_stable_entry_history": bool(self.require_stable_entry_history),
            "max_entry_leader_flips": int(self.max_entry_leader_flips),
            "max_entry_reference_crosses": int(self.max_entry_reference_crosses),
            "profit_lock_enabled": bool(self.profit_lock_enabled),
            "profit_lock_price": str(self.profit_lock_price),
            "profit_lock_max_remaining_seconds": self.profit_lock_max_remaining_seconds,
            "profit_lock_strong_hold_bid": (
                str(self.profit_lock_strong_hold_bid)
                if self.profit_lock_strong_hold_bid is not None
                else None
            ),
            "max_profit_lock_fraction": str(self.max_profit_lock_fraction),
            "max_hedges": int(self.max_hedges),
            "max_loser_unwinds": int(self.max_loser_unwinds),
            "require_btc_alignment": bool(self.require_btc_alignment),
            "require_quote_stable_final": bool(self.require_quote_stable_final),
            "min_entry_leader_duration_seconds": int(self.min_entry_leader_duration_seconds),
            "max_entry_leader_duration_seconds": (
                int(self.max_entry_leader_duration_seconds)
                if self.max_entry_leader_duration_seconds is not None
                else None
            ),
            "require_clean_entry_quote": bool(self.require_clean_entry_quote),
            "min_btc_move_bps": str(self.min_btc_move_bps),
            "min_up_btc_move_bps": (
                str(self.min_up_btc_move_bps) if self.min_up_btc_move_bps is not None else None
            ),
            "min_down_btc_move_bps": (
                str(self.min_down_btc_move_bps) if self.min_down_btc_move_bps is not None else None
            ),
            "max_btc_move_bps": (
                str(self.max_btc_move_bps) if self.max_btc_move_bps is not None else None
            ),
            "max_adverse_btc_velocity_diff": (
                str(self.max_adverse_btc_velocity_diff)
                if self.max_adverse_btc_velocity_diff is not None
                else None
            ),
            "balanced_relaxation_enabled": bool(self.balanced_relaxation_enabled),
            "balanced_relax_max_bps": str(self.balanced_relax_max_bps),
            "pair_cost_enabled": bool(self.pair_cost_enabled),
            "pair_cost_max_total_price": str(self.pair_cost_max_total_price),
            "pair_cost_min_floor": str(self.pair_cost_min_floor),
            "pair_cost_total_cost_bps": str(self.pair_cost_total_cost_bps),
            "allow_hedge": bool(self.allow_hedge),
            "direct_rescue_enabled": bool(self.direct_rescue_enabled),
            "rescue_entry_price_max": str(self.rescue_entry_price_max),
            "rescue_bid_trigger": str(self.rescue_bid_trigger),
            "rescue_min_sell_price": str(self.rescue_min_sell_price),
            "rescue_confirm_bid": str(self.rescue_confirm_bid),
            "rescue_min_leader_duration_ms": int(self.rescue_min_leader_duration_ms),
            "rescue_min_remaining_seconds": int(self.rescue_min_remaining_seconds),
            "rescue_min_pnl_improvement": str(self.rescue_min_pnl_improvement),
            "rescue_outcome_floor": str(self.rescue_outcome_floor),
            "pnl_priority_enabled": bool(self.pnl_priority_enabled),
            "pnl_scale_in_enabled": bool(self.pnl_scale_in_enabled),
            "max_scale_in_attempts": int(self.max_scale_in_attempts),
            "pnl_scale_in_usdt": str(self.pnl_scale_in_usdt),
            "pnl_scale_min_hold_seconds": int(self.pnl_scale_min_hold_seconds),
            "pnl_scale_min_remaining_seconds": int(self.pnl_scale_min_remaining_seconds),
            "pnl_scale_max_remaining_seconds": self.pnl_scale_max_remaining_seconds,
            "pnl_scale_max_ask": str(self.pnl_scale_max_ask),
            "pnl_scale_max_entry_premium": (
                str(self.pnl_scale_max_entry_premium)
                if self.pnl_scale_max_entry_premium is not None
                else None
            ),
            "pnl_scale_min_btc_bps": str(self.pnl_scale_min_btc_bps),
            "pnl_scale_min_leader_duration_ms": int(self.pnl_scale_min_leader_duration_ms),
            "pnl_scale_max_leader_duration_ms": self.pnl_scale_max_leader_duration_ms,
            "pnl_profit_min_gain": str(self.pnl_profit_min_gain),
            "pnl_profit_btc_drawdown_bps": str(self.pnl_profit_btc_drawdown_bps),
            "pnl_profit_min_remaining_seconds": int(self.pnl_profit_min_remaining_seconds),
            "pnl_reversal_min_leader_duration_ms": int(self.pnl_reversal_min_leader_duration_ms),
            "pnl_exit_min_bid": str(self.pnl_exit_min_bid),
            "flip1_enabled": bool(self.flip1_enabled),
            "flip1_hedge_min_remaining_seconds": int(self.flip1_hedge_min_remaining_seconds),
            "flip1_hedge_max_remaining_seconds": int(self.flip1_hedge_max_remaining_seconds),
            "flip1_hedge_min_price": str(self.flip1_hedge_min_price),
            "flip1_hedge_max_price": str(self.flip1_hedge_max_price),
            "flip1_initial_bid_min": str(self.flip1_initial_bid_min),
            "flip1_initial_bid_max": str(self.flip1_initial_bid_max),
            "flip1_hedge_min_leader_duration_ms": int(self.flip1_hedge_min_leader_duration_ms),
            "flip1_min_post_hedge_floor": str(self.flip1_min_post_hedge_floor),
            "flip1_unwind_min_remaining_seconds": int(self.flip1_unwind_min_remaining_seconds),
            "flip1_unwind_max_remaining_seconds": int(self.flip1_unwind_max_remaining_seconds),
            "flip1_unwind_min_btc_move_bps": str(self.flip1_unwind_min_btc_move_bps),
            "flip1_unwind_min_winner_bid": str(self.flip1_unwind_min_winner_bid),
            "flip1_unwind_min_bid_gap": str(self.flip1_unwind_min_bid_gap),
            "late_risk_exit_enabled": bool(self.late_risk_exit_enabled),
            "late_risk_partial_min_remaining_seconds": int(self.late_risk_partial_min_remaining_seconds),
            "late_risk_partial_max_remaining_seconds": int(self.late_risk_partial_max_remaining_seconds),
            "late_risk_near_reference_bps": str(self.late_risk_near_reference_bps),
            "late_risk_partial_fraction": str(self.late_risk_partial_fraction),
            "late_risk_recovery_min_remaining_seconds": int(self.late_risk_recovery_min_remaining_seconds),
            "late_risk_recovery_max_remaining_seconds": int(self.late_risk_recovery_max_remaining_seconds),
            "late_risk_recovery_min_bid": str(self.late_risk_recovery_min_bid),
            "initial_ttl_ms": int(self.initial_ttl_ms),
            "hedge_ttl_ms": int(self.hedge_ttl_ms),
            "protective_exit_enabled": bool(self.protective_exit_enabled),
            "protective_exit_bid": str(self.protective_exit_bid),
            "protective_exit_min_hold_seconds": int(self.protective_exit_min_hold_seconds),
            "v7_enabled": bool(self.v7_enabled),
            "v7_min_feature_samples": int(self.v7_min_feature_samples),
            "v7_trend_min_remaining_seconds": int(self.v7_trend_min_remaining_seconds),
            "v7_trend_max_remaining_seconds": int(self.v7_trend_max_remaining_seconds),
            "v7_trend_max_ask": str(self.v7_trend_max_ask),
            "v7_trend_min_edge": str(self.v7_trend_min_edge),
            "v7_trend_min_abs_z": str(self.v7_trend_min_abs_z),
            "v7_trend_min_leader_duration_seconds": int(self.v7_trend_min_leader_duration_seconds),
            "v7_late_min_remaining_seconds": int(self.v7_late_min_remaining_seconds),
            "v7_late_max_remaining_seconds": int(self.v7_late_max_remaining_seconds),
            "v7_late_min_probability": str(self.v7_late_min_probability),
            "v7_late_max_ask": str(self.v7_late_max_ask),
            "v7_late_min_edge": str(self.v7_late_min_edge),
            "v7_late_min_abs_z": str(self.v7_late_min_abs_z),
            "v7_late_min_leader_duration_seconds": int(self.v7_late_min_leader_duration_seconds),
            "v7_max_book_spread": str(self.v7_max_book_spread),
            "v7_cross_cooldown_seconds": int(self.v7_cross_cooldown_seconds),
            "v7_high_volatility_bps_sqrt_second": str(self.v7_high_volatility_bps_sqrt_second),
            "v7_scale_min_hold_seconds": int(self.v7_scale_min_hold_seconds),
            "v7_scale_min_remaining_seconds": int(self.v7_scale_min_remaining_seconds),
            "v7_scale_max_remaining_seconds": int(self.v7_scale_max_remaining_seconds),
            "v7_scale_min_edge": str(self.v7_scale_min_edge),
            "v7_scale_min_z_delta": str(self.v7_scale_min_z_delta),
            "v7_scale_max_entry_premium": str(self.v7_scale_max_entry_premium),
            "v8_enabled": bool(self.v8_enabled),
            "v8_min_remaining_seconds": int(self.v8_min_remaining_seconds),
            "v8_max_remaining_seconds": int(self.v8_max_remaining_seconds),
            "v8_probability_shrink": str(self.v8_probability_shrink),
            "v8_uncertainty_margin": str(self.v8_uncertainty_margin),
            "v8_total_cost_bps": str(self.v8_total_cost_bps),
            "v8_min_conservative_edge": str(self.v8_min_conservative_edge),
            "v8_max_ask": str(self.v8_max_ask),
            "v8_min_abs_z": str(self.v8_min_abs_z),
            "v8_min_leader_duration_seconds": int(self.v8_min_leader_duration_seconds),
        }
        if self.profile in V3_MOMENTUM30_PROFILES:
            payload["momentum30_research"] = {
                "version": self.gate_provenance,
                "candidate_policy": "first_complete_base_v3_candidate_never_retry",
                "history_min_age_ms": V3_MOMENTUM30_MIN_HISTORY_AGE_MS,
                "history_max_age_ms": V3_MOMENTUM30_MAX_HISTORY_AGE_MS,
                "history_selection": "latest_source_timestamp_in_age_window",
                "min_signed_btc_change_bps": None if self.profile == QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE else "0",
                "min_same_side_ask_change": "0" if self.profile == QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE else None,
                "historical_availability": "max_book_received_btc_observed_source_observed",
                "max_source_age_at_availability_ms": 1500,
                "research_status": "unvalidated_hypothesis_not_calibrated_EV",
                "comparison_role": "matched_history_availability_control" if self.profile == QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE else "directional_treatment_vs_matched_history_control",
            }
        if self.profile == QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE:
            payload["net_edge_research"] = {
                "version": self.gate_provenance,
                "max_modeled_cost_per_share": str(V3_NET_EDGE_MAX_COST_PER_SHARE),
                "candidate_policy": "first_complete_base_v3_candidate_never_retry",
                "cost_model": "gross_cash_fee_plus_slippage_reserve",
                "research_status": "unvalidated_hypothesis_not_calibrated_EV",
            }
        if self.gate_config is not None:
            gate = self.gate_config
            payload["gate_provenance"] = {
                "version": self.gate_provenance or gate.version,
                "scope": "BUY_INITIAL",
                "history_age_ms": {
                    "min": gate.history_min_age_ms,
                    "max": gate.history_max_age_ms,
                    "target": gate.history_target_age_ms,
                },
                "ask_move_bands": [
                    {
                        "min_inclusive": str(gate.ask_band_min),
                        "max_exclusive": str(gate.ask_band_max),
                    },
                    {"min_inclusive": str(gate.ask_jump_min)},
                ],
                "signed_btc_margin_bps": {
                    "min_inclusive": str(gate.margin_min_bps),
                    "max_exclusive": str(gate.margin_max_bps),
                },
                "allowed_regimes": list(gate.allowed_regimes),
            }
        if self.profile == "regime_target6_v1":
            from .regime_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile == "regime_target6_1_v1":
            from .regime_t61_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile in ("regime_target6_2_v1", 'regime_target6_3_v1'):
            from .regime_t62_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile == "regime_target6_3_v1":
            from .regime_t63_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile == "regime_target6_3a_v1":
            from .regime_t63a_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile == "regime_target6_3b_v1":
            from .regime_t63b_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile == "regime_target6_5_v1":
            from .regime_t65_lane import FINGERPRINT
            payload["regime_policy_fingerprint"] = FINGERPRINT
        if self.profile == "regime_target6_7_v1":
            from .regime_t67_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t67_policy"] = POLICY
        if self.profile == "regime_target6_7a_v1":
            from .regime_t67a_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t67a_policy"] = POLICY
        if self.profile == "regime_target6_9_v1":
            from .regime_t69_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t69_policy"] = POLICY
        if self.profile == "regime_target6_9a_v1":
            from .regime_t69a_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t69a_policy"] = POLICY
        if self.profile == "regime_target6_8a_v1":
            from .regime_t68a_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t68a_policy"] = POLICY
        if self.profile == "regime_target6_8_v1":
            from .regime_t68_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t68_policy"] = POLICY
        if self.profile == "regime_target6_7d_v1":
            from .regime_t67d_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t67d_policy"] = POLICY
        if self.profile == "regime_target6_7c_v1":
            from .regime_t67c_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t67c_policy"] = POLICY
        if self.profile == "regime_target6_7b_v1":
            from .regime_t67b_policy import FINGERPRINT, POLICY
            payload["regime_policy_fingerprint"] = FINGERPRINT
            payload["t67b_policy"] = POLICY
        return payload


@dataclass(frozen=True)
class StrategyDecision:
    action: ActionType
    state: CampaignState
    reason: str
    outcome: OutcomeSide | None = None
    order_side: OrderSide | None = None
    amount: Decimal = ZERO
    limit_price: Decimal | None = None
    ttl_ms: int = 0
    trade_allowed: bool = False
    pre_hedge_floor: Decimal | None = None
    post_hedge_floor: Decimal | None = None
    hedge_floor_improvement: Decimal | None = None

    @property
    def is_trade(self) -> bool:
        return self.action in {
            ActionType.BUY_INITIAL,
            ActionType.BUY_ADD,
            ActionType.BUY_HEDGE,
            ActionType.SELL_PROFIT_LOCK,
            ActionType.SELL_LOSER,
            ActionType.SELL_PROTECTIVE,
        }


def quality_hold_v3_gate(
    current_quote: QuoteSnapshot,
    historical_quotes: QuoteSnapshot | Mapping[str, Any] | list[QuoteSnapshot | Mapping[str, Any]] | tuple[QuoteSnapshot | Mapping[str, Any], ...] | None,
    regime: Mapping[str, Any] | None,
    leader_side: OutcomeSide | str | None = None,
) -> dict[str, Any]:
    """Evaluate the V3 entry-only Shadow gate with structured telemetry.

    Candidate history is deliberately selected here so every caller gets the
    same inclusive 30--60 second, no-future, nearest-45-second semantics.
    Equal-distance candidates prefer the older quote.  A single quote remains
    accepted for backwards-compatible callers.
    """

    try:
        leader = (
            current_quote.leader
            if leader_side is None
            else leader_side if isinstance(leader_side, OutcomeSide)
            else OutcomeSide(str(leader_side).strip().upper())
        )
    except (TypeError, ValueError):
        leader = None
    telemetry: dict[str, Any] = {
        "gate_version": V3_GATE_VERSION,
        "allowed": False,
        "reason": "gate data unavailable",
        "regime_status": None,
        "median_path_bps": None,
        "fast_median_path_bps": None,
        "ask_move": None,
        "history_age_ms": None,
        "signed_btc_margin_bps": None,
        "leader": leader.value if leader is not None else None,
        "history_candidate_count": 0,
        "history_observed_at_ms": None,
    }
    if isinstance(regime, Mapping):
        raw_status = regime.get("status")
        telemetry["regime_status"] = (
            str(raw_status).strip().upper() if raw_status not in (None, "") else None
        )
        telemetry["median_path_bps"] = regime.get("median_path_bps")
        telemetry["fast_median_path_bps"] = regime.get("fast_median_path_bps")

    if leader is None:
        telemetry["reason"] = "missing current quote leader"
        return telemetry
    current_ask = current_quote.ask(leader)
    try:
        current_ask = as_decimal(current_ask) if current_ask is not None else None
    except (ArithmeticError, TypeError, ValueError):
        current_ask = None
    if current_ask is None or not current_ask.is_finite() or current_ask <= ZERO:
        telemetry["reason"] = "missing current leader ask"
        return telemetry

    if historical_quotes is None:
        candidates: list[QuoteSnapshot | Mapping[str, Any]] = []
    elif isinstance(historical_quotes, (QuoteSnapshot, Mapping)):
        candidates = [historical_quotes]
    else:
        candidates = list(historical_quotes)
    current_at = int(current_quote.observed_at_ms)
    target_at = current_at - V3_GATE_TARGET_HISTORY_AGE_MS
    valid_candidates: list[tuple[int, QuoteSnapshot | Mapping[str, Any]]] = []
    for candidate in candidates:
        try:
            candidate_at = int(
                getattr(candidate, "observed_at_ms", 0)
                if isinstance(candidate, QuoteSnapshot)
                else candidate.get("observed_at_ms", 0)
                if isinstance(candidate, Mapping)
                else 0
            )
        except (TypeError, ValueError):
            continue
        age = current_at - candidate_at
        if V3_GATE_MIN_HISTORY_AGE_MS <= age <= V3_GATE_MAX_HISTORY_AGE_MS:
            valid_candidates.append((candidate_at, candidate))
    telemetry["history_candidate_count"] = len(valid_candidates)
    selected_history: QuoteSnapshot | Mapping[str, Any] | None = None
    historical_at: int | None = None
    if valid_candidates:
        # The timestamp is the second key: equal distance chooses older.
        historical_at, selected_history = min(
            valid_candidates,
            key=lambda item: (abs(item[0] - target_at), item[0]),
        )
    telemetry["history_observed_at_ms"] = historical_at
    history_age = current_at - historical_at if historical_at is not None else None
    telemetry["history_age_ms"] = history_age

    def value_from_history(key: str) -> Any:
        if isinstance(selected_history, QuoteSnapshot):
            return getattr(selected_history, key, None)
        if isinstance(selected_history, Mapping):
            value = selected_history.get(key)
            payload = selected_history.get("payload")
            if value is None and isinstance(payload, Mapping):
                value = payload.get(key)
            return value
        return None

    if selected_history is None or history_age is None:
        telemetry["reason"] = "historical quote age outside 30-60 seconds"
        return telemetry

    historical_ask = (
        selected_history.ask(leader)
        if isinstance(selected_history, QuoteSnapshot)
        else value_from_history("up_ask" if leader is OutcomeSide.UP else "down_ask")
    )
    try:
        historical_ask = as_decimal(historical_ask) if historical_ask is not None else None
    except (ArithmeticError, TypeError, ValueError):
        historical_ask = None
    if historical_ask is None or not historical_ask.is_finite() or historical_ask <= ZERO:
        telemetry["reason"] = "missing historical same-side ask"
        return telemetry

    ask_move = current_ask - historical_ask
    telemetry["ask_move"] = str(ask_move)
    status = telemetry["regime_status"]
    if status not in {"GREEN", "YELLOW"}:
        telemetry["reason"] = (
            "regime status unavailable"
            if status is None
            else f"regime status {status} blocks entry"
        )
        return telemetry

    try:
        spot = as_decimal(current_quote.btc_spot) if current_quote.btc_spot is not None else None
        reference = as_decimal(current_quote.reference_price) if current_quote.reference_price is not None else None
    except (ArithmeticError, TypeError, ValueError):
        spot = None
        reference = None
    if (
        spot is None
        or reference is None
        or not spot.is_finite()
        or not reference.is_finite()
        or reference <= ZERO
    ):
        telemetry["reason"] = "missing spot/reference for signed BTC margin"
        return telemetry
    signed_margin = (spot - reference) / reference * Decimal("10000")
    if leader is OutcomeSide.DOWN:
        signed_margin = -signed_margin
    if not signed_margin.is_finite():
        telemetry["reason"] = "signed BTC margin is unavailable"
        return telemetry
    telemetry["signed_btc_margin_bps"] = str(signed_margin)
    gate = V3_GATE_CONFIG
    ask_band = (
        gate.ask_band_min <= ask_move < gate.ask_band_max
        or ask_move >= gate.ask_jump_min
    )
    margin_band = gate.margin_min_bps <= signed_margin < gate.margin_max_bps
    telemetry["allowed"] = bool(ask_band or margin_band)
    if ask_band:
        telemetry["reason"] = "allowed by ask_move band"
    elif margin_band:
        telemetry["reason"] = "allowed by signed BTC margin band"
    else:
        telemetry["reason"] = "ask_move and signed BTC margin outside gate"
    return telemetry


def _quote_usable(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig) -> tuple[bool, str]:
    if not quote.feed_ok:
        return False, "feed is unhealthy"
    if quote.age_ms(now_ms) > config.max_quote_age_ms:
        return False, "quote is stale"
    if campaign.pending_unknown or campaign.pending_intent_id:
        return False, "order state is pending/unknown; reconcile first"
    return True, "ok"


def _btc_aligned(side: OutcomeSide, quote: QuoteSnapshot, config: StrategyConfig) -> bool:
    if not config.require_btc_alignment:
        return True
    if quote.btc_spot is None or quote.reference_price is None:
        return False
    if side is OutcomeSide.UP:
        return quote.btc_spot >= quote.reference_price
    return quote.btc_spot <= quote.reference_price


def _btc_move_bps(quote: QuoteSnapshot) -> Decimal | None:
    """Return absolute spot/reference displacement in basis points."""

    if quote.btc_spot is None or quote.reference_price is None:
        return None
    if quote.reference_price <= ZERO:
        return None
    move = abs(quote.btc_spot - quote.reference_price) / quote.reference_price * Decimal("10000")
    return move if move.is_finite() else None


def _aligned_btc_bps(
    side: OutcomeSide,
    spot: Decimal | None,
    reference: Decimal | None,
) -> Decimal | None:
    """Return signed BTC displacement from the held side's perspective."""

    if spot is None or reference is None or reference <= ZERO:
        return None
    move = (spot - reference) / reference * Decimal("10000")
    if side is OutcomeSide.DOWN:
        move = -move
    return move if move.is_finite() else None


def _btc_direction(quote: QuoteSnapshot) -> OutcomeSide | None:
    """Return the current strict BTC/reference side; equality is no signal."""

    if quote.btc_spot is None or quote.reference_price is None:
        return None
    if quote.btc_spot > quote.reference_price:
        return OutcomeSide.UP
    if quote.btc_spot < quote.reference_price:
        return OutcomeSide.DOWN
    return None


def _in_entry_range(value: Decimal | None, config: StrategyConfig) -> bool:
    return value is not None and config.min_entry_price <= value <= config.max_entry_price


def _buy(action: ActionType, state: CampaignState, side: OutcomeSide, price: Decimal, config: StrategyConfig, reason: str) -> StrategyDecision:
    return StrategyDecision(action, state, reason, side, OrderSide.BUY, config.max_buy_usdt, price, config.initial_ttl_ms if action is ActionType.BUY_INITIAL else config.hedge_ttl_ms, True)


def _buy_amount(
    action: ActionType,
    state: CampaignState,
    side: OutcomeSide,
    price: Decimal,
    amount: Decimal,
    config: StrategyConfig,
    reason: str,
) -> StrategyDecision:
    """Build a BUY intent with a lane-specific notional amount."""

    return StrategyDecision(
        action,
        state,
        reason,
        side,
        OrderSide.BUY,
        amount,
        price,
        config.initial_ttl_ms if action is ActionType.BUY_INITIAL else config.hedge_ttl_ms,
        True,
    )


def entry_decision(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig | None = None) -> StrategyDecision:
    """Decide whether to create the one allowed initial BUY intent."""

    policy = config or StrategyConfig()
    elapsed = campaign.elapsed_seconds(now_ms)
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "last 15 seconds: no trading")
    if (
        policy.entry_max_remaining_seconds is not None
        and remaining > policy.entry_max_remaining_seconds
        and not policy.balanced_relaxation_enabled
    ):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "entry is earlier than the configured remaining-time window")
    if policy.entry_min_remaining_seconds is not None and remaining < policy.entry_min_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "entry is later than the configured remaining-time window")
    if elapsed < policy.entry_start_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "observe before minute three")
    if elapsed >= policy.entry_end_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial-entry window closed")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    if campaign.position.has_any or campaign.initial_outcome is not None or campaign.buy_count:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial position already exists")
    side = quote.leader
    price = quote.ask(side) if side is not None else None
    if side is None or price is None:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "no confirmed leader")
    if not _in_entry_range(price, policy):
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.OBSERVE,
            f"leader ask outside {policy.min_entry_price}-{policy.max_entry_price}",
        )
    if policy.require_quote_stable_final and not quote.stable_final:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "stable final quote not confirmed")
    min_leader_ms = max(0, int(policy.min_entry_leader_duration_seconds)) * 1000
    if min_leader_ms and quote.leader_duration_ms < min_leader_ms:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.OBSERVE,
            f"leader has not been stable for {policy.min_entry_leader_duration_seconds} seconds",
        )
    max_leader_seconds = policy.max_entry_leader_duration_seconds
    if max_leader_seconds is not None and quote.leader_duration_ms > max(0, int(max_leader_seconds)) * 1000:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.OBSERVE,
            f"leader duration exceeds {max_leader_seconds} seconds",
        )
    if policy.require_clean_entry_quote:
        if quote.flip_confirmed:
            return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "entry blocked: flip confirmation active")
        if quote.btc_crossed_reference:
            return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "entry blocked: BTC crossed reference")
        if quote.reference_recross:
            return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "entry blocked: BTC reference recross")
    if not _btc_aligned(side, quote, policy):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "BTC is not on the leader side of reference")
    min_move_bps = policy.min_btc_move_bps
    if side is OutcomeSide.UP and policy.min_up_btc_move_bps is not None:
        min_move_bps = policy.min_up_btc_move_bps
    elif side is OutcomeSide.DOWN and policy.min_down_btc_move_bps is not None:
        min_move_bps = policy.min_down_btc_move_bps
    balanced_relaxed = False
    if min_move_bps > ZERO or policy.max_btc_move_bps is not None or policy.balanced_relaxation_enabled:
        move_bps = _btc_move_bps(quote)
        if move_bps is None:
            return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "BTC/reference displacement unavailable")
        if (
            policy.balanced_relaxation_enabled
            and policy.entry_max_remaining_seconds is not None
            and remaining > policy.entry_max_remaining_seconds
        ):
            if move_bps >= policy.balanced_relax_max_bps:
                return StrategyDecision(
                    ActionType.HOLD,
                    CampaignState.OBSERVE,
                    f"balanced relaxed branch requires BTC displacement below {policy.balanced_relax_max_bps} bps",
                )
            balanced_relaxed = True
        if move_bps < min_move_bps:
            return StrategyDecision(
                ActionType.HOLD,
                CampaignState.OBSERVE,
                f"BTC/reference displacement {move_bps} bps below {min_move_bps} bps",
            )
        if policy.max_btc_move_bps is not None and move_bps > policy.max_btc_move_bps:
            return StrategyDecision(
                ActionType.HOLD,
                CampaignState.OBSERVE,
                f"BTC/reference displacement {move_bps} bps above {policy.max_btc_move_bps} bps",
            )
    if (
        policy.max_adverse_btc_velocity_diff is not None
        and campaign.prior_spot is not None
        and quote.btc_spot is not None
    ):
        vel = (
            quote.btc_spot - campaign.prior_spot
            if side is OutcomeSide.UP
            else campaign.prior_spot - quote.btc_spot
        )
        if vel < policy.max_adverse_btc_velocity_diff:
            return StrategyDecision(
                ActionType.HOLD,
                CampaignState.OBSERVE,
                f"entry blocked: adverse BTC velocity {vel:+.2f} below {policy.max_adverse_btc_velocity_diff:+.2f}",
            )
    if campaign.initial_attempts >= policy.max_initial_attempts:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "initial attempt limit reached")
    if policy.require_stable_entry_history:
        if campaign.leader_flip_count > policy.max_entry_leader_flips:
            return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "entry blocked: pre-entry leader flip")
        if campaign.reference_cross_count > policy.max_entry_reference_crosses:
            return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "entry blocked: pre-entry BTC reference cross")
    reason = "confirmed minute-3-to-5 entry"
    if balanced_relaxed:
        reason = "confirmed balanced relaxed branch: early entry with BTC displacement below 4 bps"
    return _buy(ActionType.BUY_INITIAL, CampaignState.INITIAL_PENDING, side, price, policy, reason)


def pair_cost_entry_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Open the cheaper leg when both top-of-book asks fit under one payout.

    This mirrors the public pair-cost market-maker idea at the safest level
    available to the current collector: equal-share, top-of-book fills.  It
    intentionally does not claim maker priority or queue position.
    """

    policy = config or StrategyConfig()
    elapsed = campaign.elapsed_seconds(now_ms)
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "last 15 seconds: no trading")
    if elapsed < policy.entry_start_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "pair-cost lane warming up")
    if elapsed >= policy.entry_end_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "pair-cost entry window closed")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    if campaign.position.has_any or campaign.initial_outcome is not None or campaign.buy_count:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial pair-cost leg already exists")
    up_ask = quote.ask(OutcomeSide.UP)
    down_ask = quote.ask(OutcomeSide.DOWN)
    if not _in_entry_range(up_ask, policy) or not _in_entry_range(down_ask, policy):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "pair-cost ask outside configured range")
    if up_ask is None or down_ask is None or up_ask <= ZERO or down_ask <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "both executable asks are required")
    if not up_ask.is_finite() or not down_ask.is_finite():
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "pair-cost ask is non-finite")
    pair_cost = up_ask + down_ask
    if pair_cost > policy.pair_cost_max_total_price:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.OBSERVE,
            f"pair cost {pair_cost} above {policy.pair_cost_max_total_price}",
        )
    if campaign.initial_attempts >= policy.max_initial_attempts:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "initial attempt limit reached")
    # Equal shares are sized from the more expensive leg so that completing
    # the pair cannot exceed the one-USDT-per-order envelope.
    selected = OutcomeSide.UP if up_ask <= down_ask else OutcomeSide.DOWN
    selected_ask = up_ask if selected is OutcomeSide.UP else down_ask
    max_ask = max(up_ask, down_ask)
    try:
        shares = policy.max_buy_usdt / max_ask
        amount = shares * selected_ask
        total_gross = shares * pair_cost
        projected_cost = total_gross * policy.pair_cost_total_cost_bps / Decimal("10000")
        locked_floor = shares - total_gross - projected_cost
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "pair-cost sizing unavailable")
    if shares <= ZERO or amount <= ZERO or not amount.is_finite():
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "pair-cost sizing is invalid")
    if locked_floor < policy.pair_cost_min_floor:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.OBSERVE,
            f"pair locked floor {locked_floor} below {policy.pair_cost_min_floor} after costs",
        )
    return _buy_amount(
        ActionType.BUY_INITIAL,
        CampaignState.INITIAL_PENDING,
        selected,
        selected_ask,
        amount,
        policy,
        f"pair-cost top-of-book candidate at {pair_cost}",
    )


def protective_exit_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
    *,
    entry_at_ms: int | None = None,
) -> StrategyDecision:
    """Exit the complete initial leg after the configured bid-stop hold.

    This is deliberately a separate Shadow-only policy primitive.  It models
    the replay's ``price`` protective exit: once the minimum hold has elapsed,
    the held-side bid itself is sufficient to trigger the exit.  No reversal,
    hedge, or post-selection signal is inferred here.
    """

    policy = config or StrategyConfig()
    if not policy.protective_exit_enabled:
        return StrategyDecision(ActionType.HOLD, campaign.state, "protective exit disabled")
    if campaign.pending_unknown or campaign.pending_intent_id:
        return StrategyDecision(ActionType.RECONCILE, campaign.state, "order state is pending/unknown; reconcile first")
    side = campaign.initial_outcome
    if side is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial position for protective exit")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    filled_at = entry_at_ms if entry_at_ms is not None else getattr(campaign, "initial_filled_at_ms", None)
    if filled_at is None:
        return StrategyDecision(ActionType.HOLD, campaign.state, "protective exit entry timestamp unavailable")
    if int(now_ms) - int(filled_at) < int(policy.protective_exit_min_hold_seconds) * 1000:
        return StrategyDecision(ActionType.HOLD, campaign.state, "protective exit minimum hold not reached")
    bid = quote.bid(side)
    if bid is None or bid > policy.protective_exit_bid:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"protective bid stop not triggered at {policy.protective_exit_bid}")
    shares = campaign.position.shares(side)
    if shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no held shares for protective exit")
    return StrategyDecision(
        ActionType.SELL_PROTECTIVE,
        CampaignState.INITIAL_POSITION,
        f"protective bid stop <= {policy.protective_exit_bid}",
        side,
        OrderSide.SELL,
        shares,
        bid,
        policy.hedge_ttl_ms,
        True,
    )


def late_risk_exit_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Reduce a fragile flip1 position, then close a late recovery.

    The first stage is intentionally narrow: the committed book leader must
    have flipped against the initial leg while BTC is within 0.5 bps of the
    settlement reference.  The second stage exits what remains only after a
    prior flip, a recovered held bid, and BTC alignment with the initial leg.
    """

    policy = config or StrategyConfig()
    if not policy.late_risk_exit_enabled:
        return StrategyDecision(ActionType.HOLD, campaign.state, "late-risk exit disabled")
    if campaign.pending_unknown or campaign.pending_intent_id:
        return StrategyDecision(ActionType.RECONCILE, campaign.state, "order state is pending/unknown; reconcile first")
    side = campaign.initial_outcome
    if side is None or campaign.hedge_used or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "late-risk exit requires one unhedged initial leg")
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "last 15 seconds: trading forbidden")
    if remaining > policy.late_risk_partial_max_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, campaign.state, "late-risk window not reached")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    if campaign.leader_flip_count <= 0:
        return StrategyDecision(ActionType.HOLD, campaign.state, "late-risk exit requires a prior committed flip")
    bid = quote.bid(side)
    available = campaign.position.shares(side)
    if bid is None or available <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "held-side bid or shares unavailable")
    if policy.profit_lock_strong_hold_bid is not None and bid >= policy.profit_lock_strong_hold_bid:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"strong lead bid >= {policy.profit_lock_strong_hold_bid}: hold remainder")

    if (
        policy.late_risk_recovery_min_remaining_seconds < remaining
        <= policy.late_risk_recovery_max_remaining_seconds
    ):
        if bid < policy.late_risk_recovery_min_bid:
            return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, f"late recovery bid below {policy.late_risk_recovery_min_bid}")
        if _btc_direction(quote) is not side:
            return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "late recovery BTC direction is not aligned")
        return StrategyDecision(
            ActionType.SELL_PROTECTIVE,
            CampaignState.INITIAL_POSITION,
            f"late-risk recovery exit: close remainder at bid >= {policy.late_risk_recovery_min_bid}",
            side,
            OrderSide.SELL,
            available,
            bid,
            policy.hedge_ttl_ms,
            True,
        )

    if not (
        policy.late_risk_partial_min_remaining_seconds < remaining
        <= policy.late_risk_partial_max_remaining_seconds
    ):
        return StrategyDecision(ActionType.HOLD, campaign.state, "outside late-risk reduction windows")
    original = campaign.position.initial_shares(side) or available
    if campaign.profit_lock_used or available < original:
        return StrategyDecision(ActionType.HOLD, campaign.state, "late-risk partial reduction already used")
    if quote.leader is not side.opposite:
        return StrategyDecision(ActionType.HOLD, campaign.state, "late-risk partial exit requires opposite committed leader")
    move_bps = _btc_move_bps(quote)
    if move_bps is None or move_bps > policy.late_risk_near_reference_bps:
        return StrategyDecision(ActionType.HOLD, campaign.state, f"BTC is not within {policy.late_risk_near_reference_bps} bps of reference")
    shares = min(available, original * policy.late_risk_partial_fraction)
    if shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no shares available for late-risk reduction")
    percent = policy.late_risk_partial_fraction * Decimal("100")
    return StrategyDecision(
        ActionType.SELL_PROTECTIVE,
        CampaignState.INITIAL_POSITION,
        f"late-risk partial exit: sell {percent.normalize():f}% after reverse flip near BTC reference",
        side,
        OrderSide.SELL,
        shares,
        bid,
        policy.hedge_ttl_ms,
        True,
    )


def profit_lock_decision(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig | None = None) -> StrategyDecision:
    """Sell a configured fraction of the initial leg inside the lock window."""

    policy = config or StrategyConfig()
    if not policy.profit_lock_enabled:
        return StrategyDecision(ActionType.HOLD, campaign.state, "profit lock disabled for stable-entry profile")
    if campaign.profit_lock_used:
        return StrategyDecision(ActionType.HOLD, campaign.state, "profit lock already used")
    if campaign.hedge_used or campaign.initial_outcome is None:
        return StrategyDecision(ActionType.HOLD, campaign.state, "profit lock is only for the initial leg")
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.final_hold_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "final 30 seconds: hold stable remainder")
    if (
        policy.profit_lock_max_remaining_seconds is not None
        and remaining > policy.profit_lock_max_remaining_seconds
    ):
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"profit lock waits for <= {policy.profit_lock_max_remaining_seconds} seconds remaining",
        )
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    side = campaign.initial_outcome
    bid = quote.bid(side)
    if bid is None or bid < policy.profit_lock_price:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"profit-lock bid below {policy.profit_lock_price}",
        )
    if policy.profit_lock_strong_hold_bid is not None and bid >= policy.profit_lock_strong_hold_bid:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"strong lead bid >= {policy.profit_lock_strong_hold_bid}: hold full position",
        )
    if not _btc_aligned(side, quote, policy) and policy.require_btc_alignment:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "profit lock direction is not stable")
    pnl = calculate_pnl(campaign, quote)
    if pnl.mark_pnl <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "profit lock requires positive mark PnL")
    available = campaign.position.shares(side)
    original = campaign.position.initial_shares(side) or available
    shares = min(available, original * policy.max_profit_lock_fraction)
    if shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "no shares available for profit lock")
    percent = policy.max_profit_lock_fraction * Decimal("100")
    return StrategyDecision(
        ActionType.SELL_PROFIT_LOCK,
        CampaignState.PROFIT_LOCK,
        f"one-time {percent.normalize():f}% profit lock",
        side,
        OrderSide.SELL,
        shares,
        bid,
        policy.hedge_ttl_ms,
        True,
    )


def pnl_priority_exit_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Close the full held leg on confirmed BTC reversal or protected profit."""

    policy = config or StrategyConfig()
    # Staged lanes intentionally hold one side to settlement.  Keep the
    # reduce-only PnL exit exclusive to the legacy V5 flag; the separate
    # scale-in flag must never silently enable an exit path.
    if not policy.pnl_priority_enabled:
        return StrategyDecision(ActionType.HOLD, campaign.state, "PnL-priority exit disabled")
    if campaign.initial_outcome is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no PnL-priority position")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )

    side = campaign.initial_outcome
    shares = campaign.position.shares(side)
    bid = quote.bid(side)
    if shares <= ZERO or bid is None or bid < policy.pnl_exit_min_bid:
        if campaign.remaining_seconds(now_ms) <= policy.trading_cutoff_seconds:
            return StrategyDecision(
                ActionType.RECONCILE,
                CampaignState.SETTLEMENT,
                "PnL emergency exit is waiting for an executable held-side bid",
            )
        return StrategyDecision(ActionType.HOLD, campaign.state, "PnL exit has no executable held-side bid")

    # The final trading cutoff disables new entries, but it must not disable a
    # reduction of a position that is already open.  Previously a failed/stale
    # profit-lock order could reach this branch and then be carried to
    # settlement with the full losing position still attached.
    if campaign.remaining_seconds(now_ms) <= policy.trading_cutoff_seconds:
        return StrategyDecision(
            ActionType.SELL_PROTECTIVE,
            CampaignState.SETTLEMENT,
            "PnL emergency exit: close held position before settlement cutoff",
            side,
            OrderSide.SELL,
            shares,
            bid,
            policy.hedge_ttl_ms,
            True,
        )

    current_bps = _aligned_btc_bps(side, quote.btc_spot, quote.reference_price)
    prior_bps = _aligned_btc_bps(side, campaign.prior_spot, quote.reference_price)
    if (
        current_bps is not None
        and prior_bps is not None
        and current_bps <= ZERO
        and prior_bps <= ZERO
        and quote.leader_duration_ms >= policy.pnl_reversal_min_leader_duration_ms
    ):
        return StrategyDecision(
            ActionType.SELL_PROTECTIVE,
            CampaignState.INITIAL_POSITION,
            "PnL-priority exit: BTC reversed on two consecutive fresh quotes",
            side,
            OrderSide.SELL,
            shares,
            bid,
            policy.hedge_ttl_ms,
            True,
        )

    remaining = campaign.remaining_seconds(now_ms)
    peak_bps = campaign.pnl_btc_peak_bps
    if (
        remaining > policy.pnl_profit_min_remaining_seconds
        and peak_bps is not None
        and current_bps is not None
        and peak_bps - current_bps >= policy.pnl_profit_btc_drawdown_bps
    ):
        try:
            average_entry = campaign.position.cost(side) / shares
        except (ArithmeticError, ValueError, TypeError):
            average_entry = ZERO
        if average_entry > ZERO and average_entry.is_finite() and bid >= average_entry + policy.pnl_profit_min_gain:
            return StrategyDecision(
                ActionType.SELL_PROFIT_LOCK,
                CampaignState.PROFIT_LOCK,
                "PnL-priority lock: +0.10 bid gain with BTC peak drawdown",
                side,
                OrderSide.SELL,
                shares,
                bid,
                policy.hedge_ttl_ms,
                True,
            )
    return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL-priority exit conditions not met")


def pnl_scale_in_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Add exactly one USDT only after the initial leg remains confirmed."""

    policy = config or StrategyConfig()
    if not (policy.pnl_priority_enabled or policy.pnl_scale_in_enabled):
        return StrategyDecision(ActionType.HOLD, campaign.state, "PnL scale-in disabled")
    side = campaign.initial_outcome
    if side is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial position to scale")
    if campaign.buy_count >= 2:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL scale-in already filled")
    if campaign.scale_in_attempts >= policy.max_scale_in_attempts:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL scale-in attempt limit reached")
    filled_at = campaign.initial_filled_at_ms
    if filled_at is None:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial fill timestamp unavailable")
    if now_ms - int(filled_at) < policy.pnl_scale_min_hold_seconds * 1000:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL scale-in minimum hold not reached")
    remaining = campaign.remaining_seconds(now_ms)
    if remaining < policy.pnl_scale_min_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL scale-in window closed")
    if policy.pnl_scale_max_remaining_seconds is not None and remaining > policy.pnl_scale_max_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL scale-in window has not opened")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )
    if quote.leader is not side:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "PnL scale-in requires the same leader")
    if quote.leader_duration_ms < policy.pnl_scale_min_leader_duration_ms:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "PnL scale-in leader duration is too short")
    if (
        policy.pnl_scale_max_leader_duration_ms is not None
        and quote.leader_duration_ms > policy.pnl_scale_max_leader_duration_ms
    ):
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "PnL scale-in leader duration is beyond the add window")
    if quote.flip_confirmed or quote.btc_crossed_reference or quote.reference_recross:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "PnL scale-in blocked by flip/cross")
    aligned_bps = _aligned_btc_bps(side, quote.btc_spot, quote.reference_price)
    if aligned_bps is None or aligned_bps < policy.pnl_scale_min_btc_bps:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "PnL scale-in BTC confirmation is too weak")
    ask = quote.ask(side)
    if ask is None or ask <= ZERO or ask > policy.pnl_scale_max_ask or not ask.is_finite():
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, f"PnL scale-in ask is above {policy.pnl_scale_max_ask}")
    initial_shares = campaign.position.initial_shares(side)
    initial_cost = campaign.position.cost(side)
    if initial_shares <= ZERO or initial_cost <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial position cost unavailable for scale-in guard")
    initial_average = initial_cost / initial_shares
    if (
        policy.pnl_scale_max_entry_premium is not None
        and ask > initial_average + policy.pnl_scale_max_entry_premium
    ):
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"PnL scale-in ask exceeds initial average plus {policy.pnl_scale_max_entry_premium}",
        )
    amount = policy.pnl_scale_in_usdt
    if amount <= ZERO or campaign.total_invested + amount > policy.max_market_buy_usdt:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "PnL scale-in market BUY cap reached")
    return _buy_amount(
        ActionType.BUY_ADD,
        CampaignState.INITIAL_POSITION,
        side,
        ask,
        amount,
        policy,
        "staged add: same leader, no recross, BTC confirmed, ask is within initial value guard",
    )


def hedge_decision(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig | None = None) -> StrategyDecision:
    """Create the single 1 USDT reversal hedge when both feeds confirm."""

    policy = config or StrategyConfig()
    if campaign.hedge_used:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "hedge already used")
    if campaign.initial_outcome is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial leg to hedge")
    if campaign.remaining_seconds(now_ms) <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "last 15 seconds: hedge forbidden")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    original = campaign.initial_outcome
    opposite = original.opposite
    loser_bid = quote.bid(original)
    ask = quote.ask(opposite)
    if quote.leader is not opposite:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "opposite side is not leading")
    if loser_bid is None or loser_bid > policy.reversal_max_loser_price:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial side has not failed enough")
    if not _in_entry_range(ask, policy):
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "hedge ask outside 0.70-0.89")
    if not quote.flip_confirmed or not quote.btc_crossed_reference:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "reversal lacks two-feed confirmation")
    if campaign.total_invested + policy.max_buy_usdt > policy.max_market_buy_usdt:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "market BUY cap would be exceeded")
    if campaign.hedge_attempts >= policy.max_hedge_attempts:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "hedge attempt limit reached")
    # Before submitting a hedge, estimate the second leg conservatively from
    # the actual executable LIMIT ask.  A 1 USDT order at ``ask`` can buy no
    # more than 1/ask shares; using a better last price would overstate the
    # guaranteed settlement floor.
    try:
        if ask <= ZERO or not ask.is_finite():
            raise ValueError("invalid hedge ask")
        pre_floor = calculate_pnl(campaign.position).hedge_floor
        estimated_shares = policy.max_buy_usdt / ask
        if estimated_shares <= ZERO or not estimated_shares.is_finite():
            raise ValueError("invalid estimated hedge shares")
        projected = replace(campaign.position)
        projected.add_buy(
            opposite,
            estimated_shares,
            policy.max_buy_usdt,
            policy.hedge_fee_usdt,
            initial=False,
        )
        post_floor = calculate_pnl(projected).hedge_floor
        improvement = post_floor - pre_floor
        if not pre_floor.is_finite() or not post_floor.is_finite() or not improvement.is_finite():
            raise ValueError("non-finite hedge floor")
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "cannot calculate conservative hedge floor")
    if improvement < policy.hedge_floor_improvement:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"hedge floor improvement {improvement} is below {policy.hedge_floor_improvement}",
            pre_hedge_floor=pre_floor,
            post_hedge_floor=post_floor,
            hedge_floor_improvement=improvement,
        )
    if post_floor < Decimal("-0.90"):
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"post-hedge floor {post_floor} is below -0.90",
            pre_hedge_floor=pre_floor,
            post_hedge_floor=post_floor,
            hedge_floor_improvement=improvement,
        )
    decision = _buy(ActionType.BUY_HEDGE, CampaignState.HEDGE_PENDING, opposite, ask, policy, "confirmed reversal hedge")
    return replace(
        decision,
        pre_hedge_floor=pre_floor,
        post_hedge_floor=post_floor,
        hedge_floor_improvement=improvement,
    )


def flip1_hedge_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Buy one USDT of the opposite leg only near a committed even-market flip."""

    policy = config or StrategyConfig()
    if not policy.flip1_enabled:
        return StrategyDecision(ActionType.HOLD, campaign.state, "flip1 hedge disabled")
    if campaign.hedge_used:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "flip1 hedge already used")
    if campaign.initial_outcome is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial leg to insure")
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.flip1_hedge_min_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "flip1 hedge window has closed")
    if remaining > policy.flip1_hedge_max_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "flip1 waits for 60-90 seconds remaining")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )
    original = campaign.initial_outcome
    reverse = original.opposite
    original_bid = quote.bid(original)
    reverse_ask = quote.ask(reverse)
    if quote.leader is not reverse or not quote.stable_final:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "reverse leader is not committed")
    if quote.leader_duration_ms < policy.flip1_hedge_min_leader_duration_ms:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "reverse leader has not persisted for 30 seconds")
    if _btc_direction(quote) is not reverse:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "BTC is not on the reverse side")
    if (
        original_bid is None
        or original_bid < policy.flip1_initial_bid_min
        or original_bid > policy.flip1_initial_bid_max
    ):
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial bid is outside the flip1 balance zone")
    if (
        reverse_ask is None
        or reverse_ask < policy.flip1_hedge_min_price
        or reverse_ask > policy.flip1_hedge_max_price
        or reverse_ask <= ZERO
        or not reverse_ask.is_finite()
    ):
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "reverse ask is outside 0.45-0.55")
    if campaign.total_invested + policy.max_buy_usdt > policy.max_market_buy_usdt:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "market BUY cap would be exceeded")
    if campaign.hedge_attempts >= policy.max_hedge_attempts:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "flip1 hedge attempt limit reached")
    try:
        pre_floor = calculate_pnl(campaign.position).hedge_floor
        estimated_shares = policy.max_buy_usdt / reverse_ask
        projected = replace(campaign.position)
        projected.add_buy(
            reverse,
            estimated_shares,
            policy.max_buy_usdt,
            policy.hedge_fee_usdt,
            initial=False,
        )
        post_floor = calculate_pnl(projected).hedge_floor
        improvement = post_floor - pre_floor
        if not pre_floor.is_finite() or not post_floor.is_finite() or not improvement.is_finite():
            raise ValueError("non-finite flip1 floor")
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "cannot calculate flip1 hedge floor")
    if improvement < policy.hedge_floor_improvement:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"flip1 floor improvement {improvement} is below {policy.hedge_floor_improvement}",
            pre_hedge_floor=pre_floor,
            post_hedge_floor=post_floor,
            hedge_floor_improvement=improvement,
        )
    if post_floor < policy.flip1_min_post_hedge_floor:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"flip1 post-hedge floor {post_floor} is below {policy.flip1_min_post_hedge_floor}",
            pre_hedge_floor=pre_floor,
            post_hedge_floor=post_floor,
            hedge_floor_improvement=improvement,
        )
    decision = _buy(
        ActionType.BUY_HEDGE,
        CampaignState.HEDGE_PENDING,
        reverse,
        reverse_ask,
        policy,
        "flip1 committed reversal: buy opposite 1 USDT",
    )
    return replace(
        decision,
        pre_hedge_floor=pre_floor,
        post_hedge_floor=post_floor,
        hedge_floor_improvement=improvement,
    )


def flip1_unwind_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Sell the entire non-BTC leg only on strong 30-45 second consensus."""

    policy = config or StrategyConfig()
    if not policy.flip1_enabled or not campaign.hedge_used:
        return StrategyDecision(ActionType.HOLD, campaign.state, "flip1 unwind requires its hedge")
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.flip1_unwind_min_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "last 30 seconds: keep remaining position")
    if remaining > policy.flip1_unwind_max_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "flip1 waits for 30-45 second unwind window")
    if campaign.hedged_at_ms is None or now_ms - campaign.hedged_at_ms < policy.min_hedged_wait_ms:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "wait after flip1 hedge")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )
    btc_side = _btc_direction(quote)
    btc_move = _btc_move_bps(quote)
    if btc_side is None or btc_move is None or btc_move < policy.flip1_unwind_min_btc_move_bps:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "BTC move is below flip1 unwind threshold")
    if quote.leader is not btc_side:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "BTC and market leader disagree")
    if quote.leader_duration_ms < policy.min_direction_confirm_ms:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "selected side has not led for 30 seconds")
    loser = btc_side.opposite
    winner_bid = quote.bid(btc_side)
    loser_bid = quote.bid(loser)
    if winner_bid is None or winner_bid < policy.flip1_unwind_min_winner_bid:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "kept-side bid is below 0.75")
    if loser_bid is None or loser_bid < policy.loser_min_sell_price:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "non-BTC bid is below the sell floor")
    if winner_bid - loser_bid < policy.flip1_unwind_min_bid_gap:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "book gap is below flip1 consensus threshold")
    if campaign.loser_unwind_count >= policy.max_loser_unwinds:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "flip1 unwind already used")
    shares = campaign.position.shares(loser)
    if shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "no non-BTC shares remain")
    try:
        before = calculate_pnl(campaign, quote)
        projected = replace(campaign.position)
        projected.add_sell(loser, shares, shares * loser_bid)
        after = calculate_pnl(projected, quote)
        winner_before = before.if_up if btc_side is OutcomeSide.UP else before.if_down
        winner_after = after.if_up if btc_side is OutcomeSide.UP else after.if_down
        improvement = winner_after - winner_before
        if improvement < policy.loser_unwind_min_pnl_improvement:
            return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "flip1 unwind does not improve selected outcome enough")
        if min(after.if_up, after.if_down) < policy.loser_outcome_floor:
            return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "flip1 unwind exceeds the outcome-loss floor")
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "flip1 unwind PnL guard unavailable")
    return StrategyDecision(
        ActionType.SELL_LOSER,
        CampaignState.UNWIND_LOSER,
        "flip1 BTC/book consensus: sell full non-BTC leg",
        loser,
        OrderSide.SELL,
        shares,
        loser_bid,
        policy.hedge_ttl_ms,
        True,
    )


def pair_cost_hedge_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Complete an equal-share pair when the second ask still fits the cap."""

    policy = config or StrategyConfig()
    if campaign.hedge_used:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "pair-cost hedge already used")
    if campaign.initial_outcome is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial pair-cost leg to complete")
    if campaign.remaining_seconds(now_ms) <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "last 15 seconds: pair completion forbidden")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    if campaign.hedge_attempts >= policy.max_hedge_attempts:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "pair-cost hedge attempt limit reached")
    original = campaign.initial_outcome
    opposite = original.opposite
    ask = quote.ask(opposite)
    initial_shares = campaign.position.initial_shares(original)
    if initial_shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "initial pair shares unavailable")
    if not _in_entry_range(ask, policy) or ask is None or ask <= ZERO or not ask.is_finite():
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "pair completion ask outside configured range")
    initial_cost = campaign.position.cost(original)
    try:
        average_initial = initial_cost / initial_shares
        pair_cost = average_initial + ask
        amount = initial_shares * ask
        if not pair_cost.is_finite() or not amount.is_finite():
            raise ValueError("non-finite pair-cost calculation")
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "pair-cost completion sizing unavailable")
    if pair_cost > policy.pair_cost_max_total_price:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"pair completion cost {pair_cost} above {policy.pair_cost_max_total_price}",
        )
    if amount <= ZERO or amount > policy.max_buy_usdt:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "pair completion exceeds order cap")
    if campaign.total_invested + amount > policy.max_market_buy_usdt:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "market BUY cap would be exceeded")
    try:
        pre_floor = calculate_pnl(campaign.position).hedge_floor
        projected = replace(campaign.position)
        completion_cost = (
            amount * policy.pair_cost_total_cost_bps / Decimal("10000")
        )
        projected.add_buy(
            opposite,
            initial_shares,
            amount,
            policy.hedge_fee_usdt + completion_cost,
            initial=False,
        )
        post_floor = calculate_pnl(projected).hedge_floor
        improvement = post_floor - pre_floor
        if not pre_floor.is_finite() or not post_floor.is_finite() or not improvement.is_finite():
            raise ValueError("non-finite pair-cost floor")
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "cannot calculate pair-cost floor")
    if post_floor < policy.pair_cost_min_floor:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"pair floor {post_floor} below {policy.pair_cost_min_floor}",
            pre_hedge_floor=pre_floor,
            post_hedge_floor=post_floor,
            hedge_floor_improvement=improvement,
        )
    decision = _buy_amount(
        ActionType.BUY_HEDGE,
        CampaignState.HEDGE_PENDING,
        opposite,
        ask,
        amount,
        policy,
        f"pair-cost completion at {pair_cost}",
    )
    return replace(
        decision,
        pre_hedge_floor=pre_floor,
        post_hedge_floor=post_floor,
        hedge_floor_improvement=improvement,
    )


def final_window_decision(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig | None = None) -> StrategyDecision:
    """Handle the last 30/15 seconds before settlement."""

    policy = config or StrategyConfig()
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "last 15 seconds: all trading disabled")
    if remaining > policy.final_hold_seconds:
        return StrategyDecision(ActionType.HOLD, campaign.state, "not in final window")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, why)
    if not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, CampaignState.SETTLEMENT, "no position to manage")
    side = quote.leader
    if quote.stable_final:
        return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "stable direction: hold through settlement", side)
    if side is not None and quote.bid(side) is not None and quote.bid(side) >= policy.final_hold_min_price and not quote.flip_confirmed and not quote.reference_recross:
        return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "leader remains stable in final 30 seconds", side)
    return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "final window is not stable; reconcile only")


def rescue_unwind_decision(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig | None = None) -> StrategyDecision:
    """Trim 25% of a cheap failed entry after a confirmed reversal.

    This is a reduce-only safety response for ``quality_hold_v4_rescue``.
    It deliberately does not buy the opposite outcome: at the observed
    0.70-0.80 entry prices, a reversal-side BUY would usually create a
    negative pair-cost floor.  The existing ``loser_unwind_*`` counters make
    the action restart-safe and cap total reduction at 50% of the initial leg.
    """

    policy = config or StrategyConfig()
    if not policy.direct_rescue_enabled:
        return StrategyDecision(ActionType.HOLD, campaign.state, "direct rescue disabled")
    if campaign.hedge_used:
        return StrategyDecision(ActionType.HOLD, campaign.state, "direct rescue is for an unhedged initial leg")
    if campaign.initial_outcome is None or not campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial leg for direct rescue")

    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.final_hold_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "final hold window: no rescue sell")
    if remaining <= policy.rescue_min_remaining_seconds:
        return StrategyDecision(ActionType.HOLD, campaign.state, "rescue window has closed")

    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )

    side = campaign.initial_outcome
    available = campaign.position.shares(side)
    original = campaign.position.initial_shares(side) or available
    if available <= ZERO or original <= ZERO:
        return StrategyDecision(ActionType.HOLD, campaign.state, "no initial shares remain for rescue")
    try:
        average_entry = campaign.position.cost(side) / original
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.HOLD, campaign.state, "rescue entry-price guard unavailable")
    if not average_entry.is_finite() or average_entry > policy.rescue_entry_price_max:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"rescue applies only when average entry <= {policy.rescue_entry_price_max}",
        )

    held_bid = quote.bid(side)
    if held_bid is None:
        return StrategyDecision(ActionType.HOLD, campaign.state, "held-side bid unavailable for rescue")
    if held_bid > policy.rescue_bid_trigger:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"held-side bid has not reached rescue trigger {policy.rescue_bid_trigger}",
        )
    if held_bid < policy.rescue_min_sell_price:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.INITIAL_POSITION,
            f"held-side bid below rescue sell floor {policy.rescue_min_sell_price}",
        )

    winner = quote.leader
    if winner is not side.opposite:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "opposite side is not leading for rescue")
    winner_bid = quote.bid(winner)
    if winner_bid is None or winner_bid < policy.rescue_confirm_bid:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.WAIT_CONFIRM,
            f"reversal winner bid is below {policy.rescue_confirm_bid}",
        )
    if quote.leader_duration_ms < policy.rescue_min_leader_duration_ms:
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.WAIT_CONFIRM,
            f"reversal leader has not held for {policy.rescue_min_leader_duration_ms // 1000} seconds",
        )
    if not quote.flip_confirmed or not quote.btc_crossed_reference:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "rescue requires two-feed reversal confirmation")
    if quote.reference_recross or _btc_direction(quote) is not winner:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "BTC direction is not stable for rescue")

    if campaign.loser_unwind_count >= policy.max_loser_unwinds:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "rescue reduction limit reached")
    cap = original * policy.max_loser_unwind_fraction - campaign.loser_unwind_shares
    shares = min(available * policy.loser_unwind_fraction, available, cap)
    if shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "no permitted rescue shares remain")

    try:
        before = calculate_pnl(campaign, quote)
        projected = replace(campaign.position)
        projected.add_sell(side, shares, shares * held_bid)
        after = calculate_pnl(projected, quote)
        winner_before = before.if_up if winner is OutcomeSide.UP else before.if_down
        winner_after = after.if_up if winner is OutcomeSide.UP else after.if_down
        improvement = winner_after - winner_before
        if improvement < policy.rescue_min_pnl_improvement:
            return StrategyDecision(
                ActionType.HOLD,
                CampaignState.INITIAL_POSITION,
                f"rescue PnL improvement {improvement} below {policy.rescue_min_pnl_improvement}",
            )
        if min(after.if_up, after.if_down) < policy.rescue_outcome_floor:
            return StrategyDecision(
                ActionType.HOLD,
                CampaignState.INITIAL_POSITION,
                f"rescue outcome floor {min(after.if_up, after.if_down)} below {policy.rescue_outcome_floor}",
            )
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "rescue PnL guard unavailable")

    percent = policy.loser_unwind_fraction * Decimal("100")
    return StrategyDecision(
        ActionType.SELL_LOSER,
        CampaignState.UNWIND_LOSER,
        f"confirmed reversal: reduce {percent.normalize():f}% of low-entry leg",
        side,
        OrderSide.SELL,
        shares,
        held_bid,
        policy.hedge_ttl_ms,
        True,
    )


def loser_unwind_decision(campaign: Campaign, quote: QuoteSnapshot, now_ms: int, config: StrategyConfig | None = None) -> StrategyDecision:
    """After a hedge, trim the losing leg by 25% at most twice."""

    policy = config or StrategyConfig()
    if not campaign.hedge_used:
        return StrategyDecision(ActionType.HOLD, campaign.state, "loser unwind requires hedge")
    if campaign.remaining_seconds(now_ms) <= policy.final_hold_seconds:
        return StrategyDecision(ActionType.HOLD, CampaignState.FINAL_HOLD, "final 30 seconds: no loser sell")
    if campaign.hedged_at_ms is None or now_ms - campaign.hedged_at_ms < policy.min_hedged_wait_ms:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "wait after hedge before choosing a leg")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD, campaign.state, why)
    winner = quote.leader
    if winner is None or quote.bid(winner) is None or quote.bid(winner) < policy.loser_confirm_price:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "winner is not confirmed at 0.80")
    if quote.leader_duration_ms < policy.min_direction_confirm_ms:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "winner has not led for five seconds")
    if quote.reference_recross or not quote.btc_crossed_reference:
        return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "BTC direction is not stable")
    loser = winner.opposite
    loser_bid = quote.bid(loser)
    if loser_bid is None or loser_bid < policy.loser_min_sell_price:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "loser bid below 0.25; do not force a sale")
    if campaign.loser_unwind_count >= policy.max_loser_unwinds:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "loser unwind limit reached")
    original = campaign.position.initial_shares(loser) or campaign.position.shares(loser)
    available = campaign.position.shares(loser)
    cap = original * policy.max_loser_unwind_fraction - campaign.loser_unwind_shares
    shares = min(available * policy.loser_unwind_fraction, available, cap)
    if shares <= ZERO:
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "no permitted loser shares remain")
    try:
        before = calculate_pnl(campaign, quote)
        projected = replace(campaign.position)
        projected.add_sell(loser, shares, shares * loser_bid)
        after = calculate_pnl(projected, quote)
        winner_before = before.if_up if winner is OutcomeSide.UP else before.if_down
        winner_after = after.if_up if winner is OutcomeSide.UP else after.if_down
        improvement = winner_after - winner_before
        if improvement < policy.loser_unwind_min_pnl_improvement:
            return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, f"loser unwind PnL improvement {improvement} below {policy.loser_unwind_min_pnl_improvement}")
        if min(after.if_up, after.if_down) < policy.loser_outcome_floor:
            return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, f"loser outcome floor {min(after.if_up, after.if_down)} below {policy.loser_outcome_floor}")
    except (ArithmeticError, ValueError, TypeError):
        return StrategyDecision(ActionType.HOLD, CampaignState.HEDGED, "loser unwind PnL guard unavailable")
    return StrategyDecision(ActionType.SELL_LOSER, CampaignState.UNWIND_LOSER, "direction confirmed: trim 25% of losing leg", loser, OrderSide.SELL, shares, loser_bid, policy.hedge_ttl_ms, True)


def _v7_payload(quote: QuoteSnapshot) -> Mapping[str, object]:
    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    payload = raw.get("regime_value_v7") if isinstance(raw, Mapping) else None
    return payload if isinstance(payload, Mapping) else {}


def _v7_decimal(payload: Mapping[str, object], key: str) -> Decimal | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    try:
        number = as_decimal(value)
    except (ArithmeticError, TypeError, ValueError):
        return None
    return number if number.is_finite() else None


def _v7_int(payload: Mapping[str, object], key: str) -> int:
    try:
        return max(0, int(payload.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _v7_bool(payload: Mapping[str, object], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _v7_side_probability(payload: Mapping[str, object], side: OutcomeSide) -> Decimal | None:
    return _v7_decimal(payload, "p_up" if side is OutcomeSide.UP else "p_down")


def _v7_clean_directional_quote(
    quote: QuoteSnapshot,
    side: OutcomeSide,
    payload: Mapping[str, object],
    policy: StrategyConfig,
) -> tuple[bool, str]:
    if quote.flip_confirmed or quote.btc_crossed_reference or quote.reference_recross:
        return False, "V7 directional entry blocked by fresh flip/cross"
    if _v7_bool(payload, "recent_cross"):
        return False, "V7 directional entry is inside BTC cross cooldown"
    bid = quote.bid(side)
    ask = quote.ask(side)
    if bid is None or ask is None or bid <= ZERO or ask <= ZERO:
        return False, "V7 executable bid/ask is unavailable"
    spread = ask - bid
    if not spread.is_finite() or spread > policy.v7_max_book_spread:
        return False, f"V7 book spread {spread} exceeds {policy.v7_max_book_spread}"
    return True, "ok"


def regime_value_v7_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Choose V7's conservative fair-value entry and A+ add.

    The model probabilities are produced by the worker from the persisted
    BTC spot history.  This pure function validates that the prediction-book
    ask still leaves enough edge; it never treats a high book price alone as
    proof of direction.  V7 deliberately holds the selected side to
    settlement and abstains in high volatility until atomic pair execution is
    available in the Live executor.
    """

    policy = config or StrategyConfig.for_profile(REGIME_VALUE_V7_PROFILE)
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "V7 final 15 seconds: no trading")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )
    payload = _v7_payload(quote)
    samples = _v7_int(payload, "sample_count")
    z_score = _v7_decimal(payload, "z_score")
    sigma = _v7_decimal(payload, "sigma_bps_sqrt_second")
    recent_cross = _v7_bool(payload, "recent_cross")
    high_volatility = _v7_bool(payload, "high_volatility") or (
        sigma is not None and sigma >= policy.v7_high_volatility_bps_sqrt_second
    )

    if campaign.position.has_any:
        side = campaign.initial_outcome
        if side is None:
            return StrategyDecision(ActionType.HOLD, campaign.state, "V7 position has no initial-side provenance")
        if campaign.buy_count >= 2 or campaign.scale_in_attempts >= policy.max_scale_in_attempts:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 maximum two-unit position reached; hold to settlement")
        if campaign.initial_filled_at_ms is None:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 add awaits initial fill timestamp")
        if now_ms - int(campaign.initial_filled_at_ms) < policy.v7_scale_min_hold_seconds * 1000:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add minimum hold not reached")
        if not (policy.v7_scale_min_remaining_seconds <= remaining <= policy.v7_scale_max_remaining_seconds):
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add is outside remaining-time window")
        if samples < policy.v7_min_feature_samples or z_score is None:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add requires model feature history")
        if high_volatility or recent_cross or quote.flip_confirmed or quote.btc_crossed_reference or quote.reference_recross:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add blocked by volatility or flip/cross")
        if quote.leader is not side or not _btc_aligned(side, quote, policy):
            return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, "V7 A+ add requires BTC/book alignment with initial side")
        clean, reason = _v7_clean_directional_quote(quote, side, payload, policy)
        if not clean:
            return StrategyDecision(ActionType.HOLD, CampaignState.WAIT_CONFIRM, reason)
        probability = _v7_side_probability(payload, side)
        ask = quote.ask(side)
        z_delta = _v7_decimal(payload, "z_delta")
        if probability is None or ask is None or z_delta is None:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add model value is unavailable")
        edge = probability - ask
        if edge < policy.v7_scale_min_edge or z_delta < policy.v7_scale_min_z_delta:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add lacks strengthened value edge")
        shares = campaign.position.initial_shares(side)
        cost = campaign.position.cost(side)
        if shares <= ZERO or cost <= ZERO:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add initial value basis is unavailable")
        initial_average = cost / shares
        if ask > initial_average + policy.v7_scale_max_entry_premium:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add ask exceeds initial value guard")
        if campaign.total_invested + policy.max_buy_usdt > policy.max_market_buy_usdt:
            return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 A+ add would exceed two-USDT market cap")
        return _buy_amount(
            ActionType.BUY_ADD,
            CampaignState.INITIAL_POSITION,
            side,
            ask,
            policy.max_buy_usdt,
            policy,
            "V7 A+ add: edge >= 0.08, z strengthens, no recross, initial-value guard holds",
        )

    if campaign.initial_outcome is not None or campaign.buy_count:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V7 initial attempt already exists")
    if campaign.initial_attempts >= policy.max_initial_attempts:
        return StrategyDecision(ActionType.PAUSE, CampaignState.PAUSED, "V7 initial attempt limit reached")
    if samples < policy.v7_min_feature_samples or z_score is None:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V7 waits for persisted BTC volatility history")
    if high_volatility:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V7 high-volatility regime: directional entry disabled pending atomic pair execution")
    side = quote.leader
    if side is None or not _btc_aligned(side, quote, policy):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V7 requires BTC/book directional alignment")
    probability = _v7_side_probability(payload, side)
    ask = quote.ask(side)
    if probability is None or ask is None or ask <= ZERO or not ask.is_finite():
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V7 model probability or executable ask is unavailable")
    clean, reason = _v7_clean_directional_quote(quote, side, payload, policy)
    if not clean:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, reason)
    edge = probability - ask
    abs_z = abs(z_score)
    if (
        policy.v7_trend_min_remaining_seconds <= remaining <= policy.v7_trend_max_remaining_seconds
        and ask <= policy.v7_trend_max_ask
        and edge >= policy.v7_trend_min_edge
        and abs_z >= policy.v7_trend_min_abs_z
        and quote.leader_duration_ms >= policy.v7_trend_min_leader_duration_seconds * 1000
    ):
        return _buy(
            ActionType.BUY_INITIAL,
            CampaignState.INITIAL_PENDING,
            side,
            ask,
            policy,
            "V7 trend value: model edge >= 0.05, ask <= 0.75, mature clean leader",
        )
    if (
        policy.v7_late_min_remaining_seconds <= remaining <= policy.v7_late_max_remaining_seconds
        and probability >= policy.v7_late_min_probability
        and ask <= policy.v7_late_max_ask
        and edge >= policy.v7_late_min_edge
        and abs_z >= policy.v7_late_min_abs_z
        and quote.leader_duration_ms >= policy.v7_late_min_leader_duration_seconds * 1000
    ):
        return _buy(
            ActionType.BUY_INITIAL,
            CampaignState.INITIAL_PENDING,
            side,
            ask,
            policy,
            "V7 late lock: p >= 0.94, edge >= 0.04, clean no-recrossover confirmation",
        )
    return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V7 model/book value gate not met")


def regime_value_v8_calibrated_decision(
    campaign: Campaign,
    quote: QuoteSnapshot,
    now_ms: int,
    config: StrategyConfig | None = None,
) -> StrategyDecision:
    """Trade only a conservative, cost-adjusted version of V7 probability.

    This Shadow-only policy freezes its calibration before collection: raw V7
    probability is shrunk toward 0.5, then an uncertainty margin and all-in
    execution reserve are deducted.  It never scale-ins or hedges, which keeps
    each resolved market an independent one-unit observation.
    """

    policy = config or StrategyConfig.for_profile(REGIME_VALUE_V8_CALIBRATED_PROFILE)
    remaining = campaign.remaining_seconds(now_ms)
    if remaining <= policy.trading_cutoff_seconds:
        return StrategyDecision(ActionType.RECONCILE, CampaignState.SETTLEMENT, "V8 final 15 seconds: no trading")
    if campaign.position.has_any:
        return StrategyDecision(ActionType.HOLD, CampaignState.INITIAL_POSITION, "V8 one-unit position: hold to settlement")
    if campaign.initial_outcome is not None or campaign.buy_count or campaign.initial_attempts >= policy.max_initial_attempts:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V8 initial attempt already used")
    usable, why = _quote_usable(campaign, quote, now_ms, policy)
    if not usable:
        return StrategyDecision(
            ActionType.RECONCILE if "stale" in why or "unknown" in why else ActionType.HOLD,
            campaign.state,
            why,
        )
    if not (policy.v8_min_remaining_seconds <= remaining <= policy.v8_max_remaining_seconds):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V8 outside frozen 60-120 second window")

    payload = _v7_payload(quote)
    samples = _v7_int(payload, "sample_count")
    z_score = _v7_decimal(payload, "z_score")
    sigma = _v7_decimal(payload, "sigma_bps_sqrt_second")
    if samples < policy.v7_min_feature_samples or z_score is None:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V8 waits for persisted BTC feature history")
    if _v7_bool(payload, "high_volatility") or (
        sigma is not None and sigma >= policy.v7_high_volatility_bps_sqrt_second
    ):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V8 high-volatility abstention")

    side = quote.leader
    if side is None or not _btc_aligned(side, quote, policy):
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V8 requires BTC/book alignment")
    clean, reason = _v7_clean_directional_quote(quote, side, payload, policy)
    if not clean:
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, reason)
    probability = _v7_side_probability(payload, side)
    ask = quote.ask(side)
    if probability is None or ask is None or ask <= ZERO or not ask.is_finite():
        return StrategyDecision(ActionType.HOLD, CampaignState.OBSERVE, "V8 model probability or ask unavailable")

    calibrated = Decimal("0.5") + (probability - Decimal("0.5")) * policy.v8_probability_shrink
    probability_floor = calibrated - policy.v8_uncertainty_margin
    execution_reserve = ask * policy.v8_total_cost_bps / Decimal("10000")
    conservative_edge = probability_floor - ask - execution_reserve
    if (
        ask > policy.v8_max_ask
        or abs(z_score) < policy.v8_min_abs_z
        or quote.leader_duration_ms < policy.v8_min_leader_duration_seconds * 1000
        or conservative_edge < policy.v8_min_conservative_edge
    ):
        return StrategyDecision(
            ActionType.HOLD,
            CampaignState.OBSERVE,
            f"V8 conservative edge {conservative_edge} below frozen gate",
        )
    return _buy_amount(
        ActionType.BUY_INITIAL,
        CampaignState.INITIAL_PENDING,
        side,
        ask,
        policy.max_buy_usdt,
        policy,
        f"V8 calibrated value: p_floor={probability_floor}, cost_edge={conservative_edge}",
    )


def v3_net_edge_source_valid(quote: QuoteSnapshot, now_ms: int, max_age_ms: int = 1_500) -> bool:
    """Common source-quality contract for the paired V3 research lanes."""
    values = (quote.up_bid, quote.up_ask, quote.down_bid, quote.down_ask,
              quote.btc_spot, quote.reference_price)
    if not all(isinstance(value, Decimal) and value.is_finite() for value in values):
        return False
    return bool(
        quote.feed_ok
        and ZERO <= quote.up_bid <= quote.up_ask < Decimal("1")
        and ZERO <= quote.down_bid <= quote.down_ask < Decimal("1")
        and quote.up_ask > ZERO and quote.down_ask > ZERO
        and quote.btc_spot > ZERO and quote.reference_price > ZERO
        and 0 < quote.observed_at_ms <= now_ms
        and 0 < quote.spot_observed_at_ms <= now_ms
        and now_ms - quote.observed_at_ms <= max_age_ms
        and now_ms - quote.spot_observed_at_ms <= max_age_ms
    )


def v3_momentum30_gate(
    current_quote: QuoteSnapshot,
    historical_quotes: Iterable[QuoteSnapshot],
    *,
    leader_side: OutcomeSide | str,
    now_ms: int,
    profile: str = QUALITY_HOLD_V3_MOMENTUM30_V1_PROFILE,
) -> dict[str, Any]:
    """Evaluate frozen sign-only hypotheses at the first complete V3 candidate.

    The caller must scope history to the same campaign and persist this result
    before any simulated fill. Source timestamps may be REST request starts;
    BTC collected during that request can legitimately have a later timestamp.
    Completed availability, not request start, bounds historical freshness.
    This helper neither supplies calibrated probabilities nor retries entries.
    """
    result: dict[str, Any] = {
        "gate_version": profile, "allowed": False,
        "reason": "invalid momentum30 profile",
        "research_status": "unvalidated_hypothesis_not_calibrated_EV",
        "history_age_ms": None, "history_observed_at_ms": None,
        "history_available_at_ms": None, "history_candidate_count": 0,
        "signed_btc_change_bps": None, "same_side_ask_change": None,
        "candidate_policy": "first_complete_base_v3_candidate_never_retry",
    }
    if profile not in V3_MOMENTUM30_PROFILES:
        return result
    try:
        side = leader_side if isinstance(leader_side, OutcomeSide) else OutcomeSide(str(leader_side).upper())
    except (TypeError, ValueError):
        result["reason"] = "invalid candidate side"
        return result
    result["leader"] = side.value
    if not v3_net_edge_source_valid(current_quote, now_ms):
        result["reason"] = "current source invalid or stale at decision"
        return result

    def availability(quote: QuoteSnapshot) -> tuple[int | None, str | None]:
        raw = quote.raw if isinstance(quote.raw, Mapping) else {}
        book = raw.get("execution_book")
        if not isinstance(book, Mapping):
            return None, "missing execution book receipt metadata"
        try:
            received = int(book.get("received_at_ms") or 0)
            source = int(quote.observed_at_ms)
            spot_at = int(quote.spot_observed_at_ms)
        except (TypeError, ValueError, OverflowError):
            return None, "invalid source or receipt timestamp"
        if min(received, source, spot_at) <= 0:
            return None, "missing positive source or receipt timestamp"
        if received < source:
            return None, "book receipt precedes source timestamp"
        available = max(received, source, spot_at)
        if available > now_ms:
            return None, "source unavailable at decision"
        if not v3_net_edge_source_valid(quote, available):
            return None, "source invalid or stale at completed availability"
        return available, None

    current_available, error = availability(current_quote)
    result["current_available_at_ms"] = current_available
    if error:
        result["reason"] = f"current {error}"
        return result
    candidates: list[QuoteSnapshot] = []
    for candidate in historical_quotes:
        if not isinstance(candidate, QuoteSnapshot):
            continue
        try:
            age = current_quote.observed_at_ms - int(candidate.observed_at_ms)
        except (TypeError, ValueError, OverflowError):
            continue
        if V3_MOMENTUM30_MIN_HISTORY_AGE_MS <= age <= V3_MOMENTUM30_MAX_HISTORY_AGE_MS:
            candidates.append(candidate)
    result["history_candidate_count"] = len(candidates)
    if not candidates:
        result["reason"] = "missing historical quote in 30-45 second source-age window"
        return result
    historical = max(candidates, key=lambda item: item.observed_at_ms)
    result["history_observed_at_ms"] = historical.observed_at_ms
    result["history_age_ms"] = current_quote.observed_at_ms - historical.observed_at_ms
    historical_available, error = availability(historical)
    result["history_available_at_ms"] = historical_available
    if error:
        result["reason"] = f"historical {error}"
        return result
    if historical.reference_price != current_quote.reference_price:
        result["reason"] = "historical reference differs from current market reference"
        return result
    sign = Decimal("1") if side is OutcomeSide.UP else Decimal("-1")
    signed_move = sign * (current_quote.btc_spot - historical.btc_spot) / current_quote.reference_price * Decimal("10000")
    ask_change = current_quote.ask(side) - historical.ask(side)
    result["signed_btc_change_bps"] = str(signed_move)
    result["same_side_ask_change"] = str(ask_change)
    if profile == QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE:
        result["allowed"] = True
        result["reason"] = "matched history availability control accepted; no directional filter"
    elif signed_move < ZERO:
        result["reason"] = "signed BTC change is adverse"
    elif profile == QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE and ask_change < ZERO:
        result["reason"] = "same-side ask change is negative"
    else:
        result["allowed"] = True
        result["reason"] = "non-adverse BTC and same-side ask confirmed" if profile == QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE else "non-adverse BTC change accepted"
    return result


def v3_net_edge_cost_gate(
    decision: StrategyDecision,
    *,
    fee_bps: Any,
    slippage_bps: Any,
    max_cost_per_share: Decimal = V3_NET_EDGE_MAX_COST_PER_SHARE,
) -> dict[str, Any]:
    """Frozen payoff-cost hypothesis, NOT a calibrated expected-value test.

    Costs match the Shadow simulator: gross / ask shares and a separate
    cash reserve. This is not an executable-quote or guaranteed-profit claim.
    """
    result: dict[str, Any] = {
        "gate_version": QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE,
        "research_status": "unvalidated_cost_cap_hypothesis_not_calibrated_EV",
        "allowed": False,
        "reason": "invalid candidate or cost inputs",
        "max_cost_per_share": str(max_cost_per_share),
        "fee_bps": str(fee_bps),
        "slippage_bps": str(slippage_bps),
    }
    try:
        ask = Decimal(str(decision.limit_price))
        amount = Decimal(str(decision.amount))
        fee = Decimal(str(fee_bps))
        slip = Decimal(str(slippage_bps))
        cap = Decimal(str(max_cost_per_share))
        if not all(value.is_finite() for value in (ask, amount, fee, slip, cap)):
            return result
        if (decision.action is not ActionType.BUY_INITIAL or not decision.is_trade
                or decision.outcome not in (OutcomeSide.UP, OutcomeSide.DOWN)
                or not ZERO < ask < Decimal("1") or amount != Decimal("2")
                or not ZERO <= fee <= Decimal("10000")
                or not ZERO <= slip <= Decimal("10000")
                or not ZERO < cap < Decimal("1")):
            return result
        reserve = amount * (fee + slip) / Decimal("10000")
        shares = amount / ask
        cost = ask * (Decimal("1") + (fee + slip) / Decimal("10000"))
        result.update({
            "allowed": cost <= cap,
            "reason": "modeled cost cap accepted" if cost <= cap else "modeled cost cap exceeded",
            "gross_usdt": str(amount), "modeled_shares": str(shares),
            "modeled_cost_reserve_usdt": str(reserve),
            "modeled_total_cash_usdt": str(amount + reserve),
            "modeled_cost_per_share": str(cost),
            "modeled_break_even_win_rate": str(cost),
            "modeled_win_pnl_usdt": str(shares - amount - reserve),
            "modeled_loss_pnl_usdt": str(-amount - reserve),
        })
    except (ValueError, TypeError, ArithmeticError):
        pass
    return result


class PredictionStateMachine:
    """Read-only facade combining the pure policy decisions."""

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or StrategyConfig()

    def decide(self, campaign: Campaign, quote: QuoteSnapshot, now_ms: int) -> StrategyDecision:
        """Choose one action, with safety windows taking precedence."""

        if self.config.profile == QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE or self.config.profile in V3_MOMENTUM30_PROFILES:
            if campaign.position.has_any or campaign.buy_count or campaign.initial_attempts:
                return StrategyDecision(ActionType.HOLD, campaign.state, "V3 research one-shot: hold to settlement")
            # Reject malformed/future source data before Decimal comparisons.
            # The ordinary Base freshness and signal rules still run below.
            if not v3_net_edge_source_valid(quote, now_ms, self.config.max_quote_age_ms):
                return StrategyDecision(ActionType.HOLD, campaign.state, "V3 research source quote invalid or BTC stale")

        if self.config.profile in (*CONFIRM3_LANES, *VALUE9_LANES):
            return StrategyDecision(ActionType.HOLD,campaign.state,'Frozen research-only lane; no broker execution')
        if self.config.profile in NEXT5_LANES:
            return StrategyDecision(ActionType.HOLD,campaign.state,'Next5 v1 collection-only; model gate failed')
        if self.config.profile in REVERSAL5_LANES:
            return StrategyDecision(ActionType.HOLD,campaign.state,'Reversal5 requires Shadow worker')
        if self.config.profile in DIVERSE5_LANES:
            return StrategyDecision(ActionType.HOLD, campaign.state, "Diverse5 requires Shadow worker history")
        if self.config.profile in {C180_FAVORITE_HOLD_PROFILE, "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            return StrategyDecision(ActionType.HOLD, campaign.state,
                                    "C180 requires durable worker signal")
        if self.config.profile in {S3S5_PAIR_V1_PROFILE, "fav_only_v1", "fav_only_v2", "fav_only_v3", "fav_only_v4", "fav_p3"}:
            return StrategyDecision(ActionType.HOLD, campaign.state, "S3+S5 uses worker live path")
        if self.config.v8_enabled:
            return regime_value_v8_calibrated_decision(campaign, quote, now_ms, self.config)
        if self.config.v7_enabled:
            return regime_value_v7_decision(campaign, quote, now_ms, self.config)
        if campaign.position.has_any and self.config.protective_exit_enabled:
            protective = protective_exit_decision(campaign, quote, now_ms, self.config)
            if protective.is_trade:
                return protective
        if campaign.position.has_any and self.config.late_risk_exit_enabled:
            late_risk = late_risk_exit_decision(campaign, quote, now_ms, self.config)
            if late_risk.is_trade or late_risk.action is ActionType.RECONCILE:
                return late_risk
        if campaign.position.has_any and self.config.pnl_priority_enabled:
            pnl_exit = pnl_priority_exit_decision(campaign, quote, now_ms, self.config)
            if pnl_exit.is_trade or pnl_exit.action is ActionType.RECONCILE:
                return pnl_exit
        final = final_window_decision(campaign, quote, now_ms, self.config)
        if final.action is not ActionType.HOLD or final.state is CampaignState.FINAL_HOLD:
            return final
        if campaign.pending_unknown or campaign.pending_intent_id:
            return StrategyDecision(ActionType.RECONCILE, campaign.state, "pending/unknown order requires reconciliation")
        if not campaign.position.has_any:
            if self.config.pair_cost_enabled:
                return pair_cost_entry_decision(campaign, quote, now_ms, self.config)
            return entry_decision(campaign, quote, now_ms, self.config)
        if self.config.pair_cost_enabled:
            return pair_cost_hedge_decision(campaign, quote, now_ms, self.config)
        if self.config.direct_rescue_enabled:
            rescue = rescue_unwind_decision(campaign, quote, now_ms, self.config)
            if rescue.is_trade or rescue.action is ActionType.RECONCILE:
                return rescue
        if self.config.pnl_priority_enabled or self.config.pnl_scale_in_enabled:
            scale = pnl_scale_in_decision(campaign, quote, now_ms, self.config)
            if scale.is_trade or scale.action is ActionType.RECONCILE:
                return scale
            return scale
        if campaign.hedge_used:
            if self.config.flip1_enabled:
                return flip1_unwind_decision(campaign, quote, now_ms, self.config)
            return loser_unwind_decision(campaign, quote, now_ms, self.config)
        lock = profit_lock_decision(campaign, quote, now_ms, self.config)
        if lock.is_trade:
            return lock
        if not self.config.allow_hedge:
            return StrategyDecision(ActionType.HOLD, campaign.state, "hedge disabled for selected lane")
        hedge = (
            flip1_hedge_decision(campaign, quote, now_ms, self.config)
            if self.config.flip1_enabled
            else hedge_decision(campaign, quote, now_ms, self.config)
        )
        if hedge.is_trade:
            return hedge
        return lock if lock.reason != "profit lock already used" else hedge

    evaluate = decide


__all__ = [
    "DEFAULT_STRATEGY_PROFILE",
    "STABLE_ENTRY_HOLD_PROFILE",
    "SHADOW_LANE_A_PROFILE",
    "SHADOW_LANE_B_PROFILE",
    "BALANCED_HOLD_PROFILE",
    "QUALITY_HOLD_PROFILE",
    "QUALITY_HOLD_V2_PROFILE",
    "QUALITY_HOLD_V3_PROFIT1_PROFILE",
    "QUALITY_HOLD_V3_GATE_V1_PROFILE",
    "QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE",
    "QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE",
    "QUALITY_HOLD_V3_MOMENTUM30_V1_PROFILE",
    "QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE",
    "QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE",
    "V3_MOMENTUM30_PROFILES",
    "V3_MOMENTUM30_MIN_HISTORY_AGE_MS",
    "V3_MOMENTUM30_MAX_HISTORY_AGE_MS",
    "v3_momentum30_gate",
    "V3_NET_EDGE_MAX_COST_PER_SHARE",
    "v3_net_edge_cost_gate",
    "v3_net_edge_source_valid",
    "SHADOW_ONLY_PROFILES",
    "QUALITY_HOLD_V4_RESCUE_PROFILE",
    "QUALITY_HOLD_V5_PNL_PROFILE",
    "QUALITY_HOLD_V6_A_STAGED_PROFILE",
    "QUALITY_HOLD_V6_BALANCED_SHADOW_PROFILE",
    "QUALITY_HOLD_V6_B_45S_PROFILE",
    "QUALITY_HOLD_V6_C_45S_PROFILE",
    "QUALITY_HOLD_V3_SNIPER_PROFILE",
    "LATE_MATURITY_V1_PROFILE",
    "REGIME_VALUE_V7_PROFILE",
    "REGIME_VALUE_V8_CALIBRATED_PROFILE",
    "LATENCY_SNIPE_PROFILE",
    "PAIR_COST_ARB_PROFILE",
    "COMPLETE_SET_ARB_V1_PROFILE",
    "S3S5_PAIR_V1_PROFILE",
    "PnlResult",
    "PredictionStateMachine",
    "StrategyConfig",
    "StrategyDecision",
    "calculate_pnl",
    "entry_decision",
    "pair_cost_entry_decision",
    "protective_exit_decision",
    "late_risk_exit_decision",
    "profit_lock_decision",
    "pnl_priority_exit_decision",
    "pnl_scale_in_decision",
    "regime_value_v7_decision",
    "regime_value_v8_calibrated_decision",
    "hedge_decision",
    "flip1_hedge_decision",
    "flip1_unwind_decision",
    "pair_cost_hedge_decision",
    "loser_unwind_decision",
    "final_window_decision",
    "rescue_unwind_decision",
    "quality_hold_v3_gate",
]


"""Frozen exploratory policies. Pure decisions only: no orders, state, or calibrated EV.

Input quotes use QuoteSnapshot JSON fields including raw.execution_book.received_at_ms.
The caller must provide same-market history and enforce one fill per market/lane.
"""


import math
from collections.abc import Mapping

LANES = ("trend_continuation_v1", "reference_reversion_v1", "spot_book_lag_v1",
         "diffusion_value_v1", "late_distance_v1")
GROSS_USDT = 2.0
COST_BPS = 300


def _number(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not a number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite")
    return result


def _validated(payload, now_ms, current=False):
    if not isinstance(payload, Mapping) or payload.get("feed_ok") is not True:
        raise ValueError("bad feed")
    q = {k: _number(payload.get(k)) for k in
         ("up_bid", "up_ask", "down_bid", "down_ask", "btc_spot", "reference_price",
          "observed_at_ms", "spot_observed_at_ms")}
    q["received_at_ms"] = _number(payload["raw"]["execution_book"]["received_at_ms"])
    source, spot, receipt = (q[k] for k in
                             ("observed_at_ms", "spot_observed_at_ms", "received_at_ms"))
    available = max(source, spot, receipt)
    if min(source, spot, receipt) <= 0 or receipt < source or available > now_ms:
        raise ValueError("unavailable timestamp")
    if max(available-source, available-spot) > 1500:
        raise ValueError("stale at availability")
    if current and max(now_ms-source, now_ms-spot, now_ms-receipt) > 1500:
        raise ValueError("stale current")
    if min(q["btc_spot"], q["reference_price"]) <= 0:
        raise ValueError("bad underlying")
    for side in ("up", "down"):
        bid, ask = q[side+"_bid"], q[side+"_ask"]
        if not 0 <= bid <= ask < 1 or ask <= 0 or ask-bid > .040000000001:
            raise ValueError("invalid or wide book")
    q["available_at_ms"] = available
    return q


def evaluate_policies(current, history, *, now_ms, remaining_seconds):
    """Return {lane: {side, reason, diagnostics}}; None side means do not fill.

    Thresholds are prespecified research hypotheses, not optimized or validated.
    Diffusion uses past spot increments only; its probability is uncalibrated.
    """
    base = {"research_status": "uncalibrated_exploratory_hypothesis",
            "gross_usdt": GROSS_USDT, "cost_bps": COST_BPS}
    results = {lane: {"side": None, "reason": "invalid_current",
                      "diagnostics": dict(base)} for lane in LANES}
    try:
        now_ms = _number(now_ms)
        remaining = _number(remaining_seconds)
        q = _validated(current, now_ms, current=True)
    except (ValueError, TypeError, KeyError, OverflowError):
        return results
    past = []
    for payload in history:
        try:
            p = _validated(payload, now_ms)
            if (p["reference_price"] == q["reference_price"]
                    and p["observed_at_ms"] < q["observed_at_ms"]
                    and p["spot_observed_at_ms"] < q["spot_observed_at_ms"]):
                past.append(p)
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
    matches = [p for p in past if 30000 <= q["observed_at_ms"]-p["observed_at_ms"] <= 45000]
    h = max(matches, key=lambda p: p["observed_at_ms"]) if matches else None
    margin = (q["btc_spot"]/q["reference_price"]-1)*10000
    move = (q["btc_spot"]/h["btc_spot"]-1)*10000 if h else None
    # Duplicate spot timestamps are one observation. Bound to the preceding 180s.
    samples = {p["spot_observed_at_ms"]: p["btc_spot"] for p in sorted(
        past, key=lambda p: p["observed_at_ms"])
        if q["spot_observed_at_ms"]-p["spot_observed_at_ms"] <= 180000}
    samples[q["spot_observed_at_ms"]] = q["btc_spot"]
    points = sorted(samples.items())
    sigma = None
    if len(points) >= 4 and points[-1][0]-points[0][0] >= 60000:
        increments = [(10000*math.log(b[1]/a[1]), (b[0]-a[0])/1000)
                      for a, b in zip(points, points[1:])]
        sigma = max(.1, math.sqrt(sum(dx*dx for dx, dt in increments)/sum(dt for dx, dt in increments)))
    z = margin/(sigma*math.sqrt(remaining)) if sigma is not None and remaining > 0 else None
    for lane, result in results.items():
        d = result["diagnostics"]
        d.update(margin_bps=margin, move30_bps=move, sigma_bps_sqrt_second=sigma,
                 z_uncalibrated=z, history_source_ms=h["observed_at_ms"] if h else None,
                 spot_sample_count=len(points))
        late = lane == "late_distance_v1"
        if not ((20 <= remaining <= 60) if late else (60 <= remaining <= 180)):
            result["reason"] = "outside_entry_window"
            continue
        if h is None:
            result["reason"] = "missing_valid_30_45s_history"
            continue
        side = "UP" if margin > 0 else "DOWN"
        sign = 1 if side == "UP" else -1
        ask = q[side.lower()+"_ask"]
        passes = False
        if lane == "trend_continuation_v1":
            passes = abs(margin) >= 3 and sign*move >= 1 and ask <= .75
        elif lane == "reference_reversion_v1":
            side = "DOWN" if move > 0 else "UP"
            ask = q[side.lower()+"_ask"]
            passes = abs(margin) <= 3 and abs(move) >= 3 and ask <= .55
        elif lane == "spot_book_lag_v1":
            change = ask-h[side.lower()+"_ask"]
            d["same_side_ask_change"] = change
            passes = abs(margin) > 0 and sign*move >= 2 and change <= 0 and ask <= .8
        else:
            if z is None:
                result["reason"] = "insufficient_past_volatility_data"
                continue
            if late:
                passes = abs(z) >= 1.5 and ask <= .85
            else:
                p_up = .5+.8*(.5*(1+math.erf(z/math.sqrt(2)))-.5)
                options = [(p_up-.03-q["up_ask"]*1.03, "UP", p_up),
                           (1-p_up-.03-q["down_ask"]*1.03, "DOWN", 1-p_up)]
                edge, side, probability = max(options)
                ask = q[side.lower()+"_ask"]
                d.update(probability_uncalibrated=probability, conservative_cost_edge=edge)
                passes = edge >= .07 and ask <= .8
        d.update(selected_ask=ask, cost_per_share=ask*1.03)
        if passes:
            try:
                depth = current["raw"]["execution_book"][side]
                depth_ask = _number(depth["ask"])
                shares = _number(depth["top_ask_shares"])
                required = GROSS_USDT/ask
                d.update(top_ask_shares=shares, required_shares=required)
                if depth_ask != ask or shares < required:
                    result["reason"] = "insufficient_or_mismatched_selected_depth"
                    continue
            except (ValueError, TypeError, KeyError, OverflowError):
                result["reason"] = "missing_or_invalid_selected_depth"
                continue
        result.update(side=side if passes else None,
                      reason="hypothesis_pass" if passes else "threshold_rejected")
    return results


"""Frozen, pure Shadow-only early-entry/hedge experiment. No broker calls.

Quotes are 1-second observation samples, not exchange OHLC candles. A closed
5-second observation window needs >=3 samples, >=3 seconds span and <=2s gaps.
All 5 lanes share one entry and differ only in subsequent hedge treatment.
"""
from copy import deepcopy
from decimal import Decimal as D, ROUND_DOWN

REVERSAL5_LANES = (
    'open_hold_control_v1', 'open_first_flip_equal_v1',
    'open_first_flip_balanced_v1', 'open_profit_lock_v1',
    'open_profit_lock_rescue_v1',
)
REVERSAL5_VERSION = 'reversal5-v1-early30-obs5s-delay1s-cost300-stress500'
FEE = D('0.03')
STRESS = D('0.05')


def _d(value):
    v=D(str(value))
    if not v.is_finite(): raise ValueError('nonfinite')
    return v


def reversal5_valid(q, now_ms):
    try:
        if q.get('source') != 'binance_prediction_ws' or not q.get('feed_ok'):
            return False
        raw_ts=[_d(q[k]) for k in ('book_at_ms','spot_at_ms','received_at_ms')]
        if any(t!=t.to_integral_value() for t in raw_ts): return False
        ts=[int(t) for t in raw_ts]
        if any(t<=0 or t>now_ms or now_ms-t>1500 for t in ts): return False
        if q.get('orientation_verified') is not True: return False
        if _d(q['spot'])<=0 or _d(q['reference'])<=0: return False
        for side in ('UP','DOWN'):
            b=q[side]
            if not D(0)<_d(b['bid'])<=_d(b['ask'])<D(1): return False
            if _d(b['ask'])-_d(b['bid'])>D('0.04'): return False
            if _d(b['ask_shares'])<0: return False
        return True
    except (KeyError,TypeError,ValueError,ArithmeticError,OverflowError):
        return False


def _size(entry, side, ask, treatment):
    return D(2) if treatment=='equal' else (_d(entry['shares'])*ask).quantize(D('0.000001'),rounding=ROUND_DOWN)


def _floor(entry, amount, ask, rate=FEE):
    return min(_d(entry['shares']),amount/ask) - (_d(entry['gross'])+amount)*(1+rate)


def _possible(q, side, amount):
    ask=_d(q[side]['ask'])
    return D('1.5')<=amount<=D('2.5') and _d(q[side]['ask_shares'])>=amount/ask


def _bars(state, q, now):
    bucket=int(q['spot_at_ms'])//5000
    value=str(q['spot'])
    current=state.get('bar')
    closed=None
    if current and bucket!=current['bucket']:
        samples=current['samples']
        gaps=[b[0]-a[0] for a,b in zip(samples,samples[1:])]
        valid=(bucket==current['bucket']+1 and len(samples)>=3 and
               samples[-1][0]-samples[0][0]>=3000 and max(gaps,default=99999)<=2000 and
               (current['bucket']+1)*5000-samples[-1][0]<=1500)
        closed={'start_ms':current['bucket']*5000,'end_ms':(current['bucket']+1)*5000,'valid':valid,
                'move_bps':str((_d(samples[-1][1])/_d(samples[0][1])-1)*10000) if valid else None}
        state.setdefault('bars',[]).append(closed)
        state['bars']=state['bars'][-4:]
        current=None
    if current is None:
        current={'bucket':bucket,'samples':[]}
    if not current['samples'] or q['spot_at_ms']>current['samples'][-1][0]:
        current['samples'].append([int(q['spot_at_ms']),value])
    state['bar']=current
    return closed


def reversal5_step(previous, q, *, now_ms, start_ms, end_ms):
    """Return new JSON state and planned fills; caller must persist before fill.

The caller stores inflight plans before touching the shadow ledger. An
incomplete persisted execution is censored after restart, never replay-filled.
"""
    s=deepcopy(previous) if previous else {'version':REVERSAL5_VERSION,'entry_status':'waiting',
        'lanes':{lane:{'status':'waiting'} for lane in REVERSAL5_LANES}}
    if s.get('version')!=REVERSAL5_VERSION: raise ValueError('policy identity mismatch')
    identity=[start_ms,end_ms,str(q.get('reference'))]
    if s.get('identity') is not None and s['identity']!=identity:
        raise ValueError('market/reference identity mismatch')
    if s.get('censored') or not start_ms<=now_ms<end_ms: return s,[]
    if not reversal5_valid(q,now_ms):
        s['last_reason']='invalid_or_stale_source'; return s,[]
    s['identity']=identity
    token=(int(q['spot_at_ms']),int(q['book_at_ms']))
    if token==tuple(s.get('last_sample',[])): return s,[]
    previous_token=s.get('last_sample')
    if previous_token and (any(a<b for a,b in zip(token,previous_token)) or now_ms<s['last_valid_ms']):
        s['last_reason']='out_of_order_source';return s,[]
    last=s.get('last_valid_ms')
    gap=last is not None and now_ms-last>2500
    if gap:
        s['bar']=None; s['bars']=[]
        if s['entry_status']=='filled':
            for lane in REVERSAL5_LANES[1:3]:
                if s['lanes'][lane]['status']=='open':
                    s['lanes'][lane].update(status='hold_gap_censored',reason='first reversal unobservable after data gap')
    s['last_sample']=list(token);s['last_valid_ms']=now_ms
    closed=_bars(s,q,now_ms)
    elapsed=now_ms-start_ms
    plans=[]
    pending=s.get('entry_pending')
    if pending:
        if now_ms>pending['expires_ms'] or elapsed>30000:
            s['entry_status']='censored';s['entry_pending']=None
            s['last_reason']='entry_timeout';return s,[]
        if now_ms>=pending['ready_ms']:
            if int(q['book_at_ms'])<=pending['book_at_ms']: return s,[]
            side=pending['side'];ask=_d(q[side]['ask'])
            if not D('0.5')<=ask<=D(1)/D('1.8') or ask>_d(pending['limit']) or not _possible(q,side,D(2)):
                s['entry_status']='censored';s['entry_pending']=None
                s['last_reason']='entry_execution_rejected';return s,[]
            entry={'side':side,'ask':str(ask),'shares':str(D(2)/ask),'gross':'2','at_ms':now_ms}
            s['entry']=entry;s['entry_pending']=None;s['entry_status']='filled'
            for lane in REVERSAL5_LANES:
                s['lanes'][lane]={'status':'open'}
                plans.append({'lane':lane,'action':'BUY_INITIAL','side':side,'gross':'2','ask':str(ask),'reason':'shared_early_entry'})
            return s,plans
        return s,[]
    if s['entry_status']=='waiting':
        if elapsed>30000:
            s['entry_status']='missed';s['last_reason']='no_early_candidate';return s,[]
        if elapsed<5000 or not closed or not closed['valid']: return s,[]
        move=_d(closed['move_bps']);margin=(_d(q['spot'])/_d(q['reference'])-1)*10000
        sign=1 if move>0 else -1
        side='UP' if sign==1 else 'DOWN'
        ask=_d(q[side]['ask'])
        if abs(move)>=D(1) and margin*sign>=D('0.5') and D('0.5')<=ask<=D(1)/D('1.8') and _possible(q,side,D(2)):
            s['entry_status']='pending'
            s['entry_pending']={'side':side,'ready_ms':now_ms+1000,'expires_ms':now_ms+5000,
                                'book_at_ms':int(q['book_at_ms']),'limit':str(ask+D('0.005'))}
            s['last_reason']='shared_early_candidate'
        return s,[]
    if s['entry_status']!='filled': return s,[]
    entry=s['entry'];side='DOWN' if entry['side']=='UP' else 'UP'
    sign=1 if entry['side']=='UP' else -1
    reverse=bool(closed and closed['valid'] and closed['start_ms']>=entry['at_ms'] and _d(closed['move_bps'])*sign<=-D(1))
    after=[b for b in s.get('bars',[]) if b['start_ms']>=entry['at_ms']]
    confirmed=(len(after)>=2 and all(b['valid'] and _d(b['move_bps'])*sign<=-D(1) for b in after[-2:]) and
        after[-1]['end_ms']-after[-2]['end_ms']==5000 and
        (_d(q['spot'])/_d(q['reference'])-1)*10000*sign<=-D(2))
    for lane in REVERSAL5_LANES[1:]:
        ls=s['lanes'][lane]
        if ls['status'] not in ('open','pending'): continue
        ask=_d(q[side]['ask'])
        kind='equal' if lane==REVERSAL5_LANES[1] else 'balanced'
        amount=_size(entry,side,ask,kind)
        if ls['status']=='pending':
            p=ls['pending']
            if now_ms>p['expires_ms']:
                ls.update(status='hold_execution_censored',reason='hedge_timeout');continue
            if now_ms<p['ready_ms']: continue
            if int(q['book_at_ms'])<=p['book_at_ms']: continue
            good=_possible(q,side,amount) and ask<=_d(p['limit'])
            if p['trigger']=='profit_lock': good=good and _floor(entry,amount,ask,STRESS)>=D('0.02')
            if p['trigger']=='rescue': good=good and _floor(entry,amount,ask)>=-D('0.50')
            if not good:
                ls.update(status='hold_execution_censored',reason='hedge_execution_rejected');continue
            ls.update(status='hedged',floor_300bps=str(_floor(entry,amount,ask)),trigger=p['trigger'],hedged_at_ms=now_ms)
            plans.append({'lane':lane,'action':'BUY_HEDGE','side':side,'gross':str(amount),'ask':str(ask),'reason':p['trigger']})
            continue
        if end_ms-now_ms<20000: continue
        trigger=None
        if lane in REVERSAL5_LANES[1:3] and reverse: trigger='first_reverse'
        elif lane in REVERSAL5_LANES[3:] and _possible(q,side,amount) and _floor(entry,amount,ask,STRESS)>=D('0.02'):
            trigger='profit_lock'
        elif lane==REVERSAL5_LANES[4] and confirmed and _possible(q,side,amount) and _floor(entry,amount,ask)>=-D('0.50'):
            trigger='rescue'
        if not trigger: continue
        if not _possible(q,side,amount):
            ls.update(status='hold_execution_censored',reason='first_reverse_size_or_depth_rejected');continue
        ls.update(status='pending',pending={'trigger':trigger,'ready_ms':now_ms+1000,
                   'expires_ms':now_ms+5000,'book_at_ms':int(q['book_at_ms']),'limit':str(ask+D('0.005'))})
    return s,plans


"""Next5 pure research policy: independent entries, causal samples, no broker I/O.

The embedded model and gate verdict are frozen at build time. A failed model
gate collects observations only. Cost scenarios are NOT verified venue fees.
"""
import math
from copy import deepcopy

NEXT5_LANES = ('early_value_v1', 'trend_value_v1', 'reversion_value_v1',
               'late_oracle_watch_v1', 'passive_queue_watch_v1')
NEXT5_VERSION = 'next5-v1-causal-value-delay1s-observe-prerequisites'
NEXT5_WINDOWS = ((5, 40), (20, 180), (30, 210), (180, 270), (10, 180))
NEXT5_MODEL = {'schema': 'next5-logistic-distance-time-v1', 'coef': [-0.31110177289131064, 1.674447878734325, 0.2577636372162233], 'calibration': {'2': {'n': 43, 'rate': 0.5116279069767442, 'lower': 0.36752063766074083, 'upper': 0.6538279072224399}, '1': {'n': 24, 'rate': 0.2916666666666667, 'lower': 0.14914460334164278, 'upper': 0.49168063658709704}, '3': {'n': 11, 'rate': 0.9090909090909091, 'lower': 0.6226353745137962, 'upper': 0.9837682477371741}, '0': {'n': 3, 'rate': 0.3333333333333333, 'lower': 0.0614903152761605, 'upper': 0.7923450448735121}}, 'training_n': 172, 'calibration_n': 81, 'validation_n': 176, 'training_max_ms': 1788393600000, 'frozen_data_max_ms': 1788652800000, 'validation': {'n': 176, 'brier': 0.22551341909604322, 'book_brier': 0.21441107954545463, 'accuracy': 0.6590909090909091}, 'shadow_gate_passed': False, 'blockers': ['calibration_only_at_60_to_120s_not_lane_selected_entries', 'actual_routed_fee_and_minimum_size_not_verified', 'holdout_brier_not_better_than_book'], 'model_id': 'd42d7459e245b0ccd22fed3f269fb93818e3f62842fc12b328bf2c357d67c51c'}

def next5_features(spot, reference, remaining):
    spot, reference, remaining = map(float, (spot, reference, remaining))
    if not all(math.isfinite(x) and x > 0 for x in (spot, reference, remaining)):
        raise ValueError('invalid feature')
    distance = math.log(spot/reference)*10000
    z = distance/math.sqrt(max(10., remaining))
    # No fitted volatility is claimed: this is a scaled distance feature.
    return [1., max(-10., min(10., z)), remaining/300.]

def next5_probability(model, q, remaining):
    x = next5_features(q['spot'], q['reference'], remaining)
    score = sum(a*b for a, b in zip(model['coef'], x))
    raw = 1/(1+math.exp(-max(-30., min(30., score))))
    return raw

def next5_bounds(model, p, side):
    # Market-level empirical bin bounds, not a per-trade guarantee.
    bucket = min(4, int(p*5))
    row = model.get('calibration', {}).get(str(bucket))
    if not row or row['n'] < 20:
        return None
    return row['lower'] if side == 'UP' else 1-row['upper']

def next5_valid(q, now_ms):
    try:
        if q.get('source') != 'binance_prediction_ws' or not q.get('feed_ok') or q.get('orientation_verified') is not True:
            return False
        for key in ('book_at_ms', 'spot_at_ms', 'received_at_ms'):
            t = float(q[key])
            if not math.isfinite(t) or int(t) != t or not 0 < t <= now_ms or now_ms-t > 1500: return False
        next5_features(q['spot'], q['reference'], 300)
        for side in ('UP', 'DOWN'):
            bid, ask, depth = [float(q[side][k]) for k in ('bid', 'ask', 'ask_shares')]
            if not all(math.isfinite(x) for x in (bid, ask, depth)): return False
            if not 0 < bid <= ask < 1 or ask-bid > .040000001 or depth < 0: return False
        return True
    except (ValueError, TypeError, KeyError, OverflowError): return False

def next5_reason(ls, reason):
    ls['reason'] = reason
    counts = ls.setdefault('reasons', {})
    counts[reason] = counts.get(reason, 0)+1

def next5_step(previous, q, *, now_ms, start_ms, end_ms, model=None, delay_ms=1000):
    model = model or NEXT5_MODEL
    if model is None: raise ValueError('frozen model missing')
    s = deepcopy(previous) if previous else {'version': NEXT5_VERSION, 'model_id': model['model_id'],
        'lanes': {lane: {'status': 'waiting', 'buy_count': 0, 'valid_samples': 0,
                         'candidates': 0, 'signals': 0} for lane in NEXT5_LANES}, 'history': []}
    if s.get('version') != NEXT5_VERSION or s.get('model_id') != model['model_id']:
        raise ValueError('frozen identity mismatch')
    if now_ms < s.get('last_call_ms', 0): raise ValueError('observation time reversed')
    s['last_call_ms'] = now_ms
    elapsed = (now_ms-start_ms)/1000
    plans = []
    if not start_ms <= now_ms < end_ms: return s, plans
    valid = next5_valid(q, now_ms)
    token = [q.get('spot_at_ms'), q.get('book_at_ms')]
    if valid and 'last_token' in s and any(a < b for a,b in zip(token,s['last_token'])): valid = False
    fresh = valid and token != s.get('last_token')
    if fresh:
        s['last_token'] = token
        if s.get('reference') not in (None, str(q['reference'])): raise ValueError('reference changed')
        s['reference'] = str(q['reference'])
        sample = [now_ms, float(q['spot']), (float(q['UP']['bid'])+float(q['UP']['ask']))/2]
        s['history'].append(sample)
        s['history'] = [h for h in s['history'] if now_ms-h[0] <= 32000]
    hist = s['history']
    def lag(seconds):
        rows = [h for h in hist if seconds*1000 <= now_ms-h[0] <= seconds*1000+1500]
        if not rows: return None
        selected=rows[-1]
        interval=[h for h in hist if h[0]>=selected[0]]
        if any(b[0]-a[0]>2500 for a,b in zip(interval,interval[1:])): return None
        return selected
    old3, old10 = lag(3), lag(10)
    p = next5_probability(model,q,(end_ms-now_ms)/1000) if valid else None
    for i,lane in enumerate(NEXT5_LANES):
        ls=s['lanes'][lane]; low,high=NEXT5_WINDOWS[i]
        if ls['status'] in ('filled','expired','execution_rejected'): continue
        if elapsed > high:
            ls['status']='expired'
            next5_reason(ls,'source_missing' if not ls['valid_samples'] else 'window_complete_without_fill')
            continue
        if elapsed < low: continue
        ls['samples']=ls.get('samples',0)+1
        if not valid:
            next5_reason(ls,'invalid_or_stale_source'); continue
        ls['valid_samples']+=1
        ls.setdefault('first_valid_at_ms',now_ms)
        if i >= 3:
            ls['status']='observation_only'
            next5_reason(ls,'oracle_stream_not_verified' if i==3 else 'trade_tape_queue_not_observable')
            continue
        if not fresh: continue
        ls['p_up']=p
        side = 'UP' if p >= .5 else 'DOWN'
        move3 = math.log(float(q['spot'])/old3[1])*10000 if old3 else None
        move10 = math.log(float(q['spot'])/old10[1])*10000 if old10 else None
        delta = (float(q['UP']['bid'])+float(q['UP']['ask']))/2-old10[2] if old10 else None
        distance = math.log(float(q['spot'])/float(q['reference']))*10000
        if i==0:
            signal = move3 is not None and abs(move3) >= .05
            if signal: side='UP' if move3 > 0 else 'DOWN'
            band=(.45,.58)
        elif i==1:
            signal=move10 is not None and abs(move10)>=.1 and move10*distance>0
            if signal: side='UP' if move10>0 else 'DOWN'
            band=(.35,.80)
        else:
            # Flat underlying but large contract move, with recent spot flow
            # no longer accelerating in the repricing direction.
            signal=(move10 is not None and move3 is not None and abs(move10)<=.5 and
                    abs(delta)>=.03 and move3*delta<=0)
            if signal: side='DOWN' if delta>0 else 'UP'
            band=(.25,.75)
        pending=ls.get('pending')
        if pending:
            if now_ms>pending['expires_ms']:
                ls['status']='execution_rejected'; next5_reason(ls,'execution_timeout'); continue
            if now_ms<pending['ready_ms'] or q['book_at_ms']<=pending['book_at_ms']: continue
            side=pending['side']; ask=float(q[side]['ask'])
            lower=next5_bounds(model,p,side)
            good=(lower is not None and lower>ask*1.03+.03 and ask<=pending['limit'] and
                  band[0]<=ask<=band[1] and float(q[side]['ask_shares'])>=2/ask)
            # Conservative fill-or-kill top-level model. No invented partial
            # fills or queue position. The venue's actual min size is unverified.
            if not good:
                ls['status']='execution_rejected'; next5_reason(ls,'post_delay_price_edge_or_depth'); continue
            ls.update(status='filled',buy_count=1,pending=None)
            plans.append({'lane':lane,'action':'BUY_INITIAL','side':side,'gross':'2',
                          'ask':str(ask),'reason':'next5_cost300_scenario_not_actual_fee'})
            continue
        if not signal:
            next5_reason(ls,'no_distinct_signal'); continue
        ls['signals']+=1
        ls['last_signal']={'at_ms':now_ms,'side':side,'p_up':p,'move3_bps':move3,'move10_bps':move10,'book_delta':delta}
        ask=float(q[side]['ask'])
        lower=next5_bounds(model,p,side)
        if not model.get('shadow_gate_passed'):
            ls['status']='observation_only'; next5_reason(ls,'model_validation_failed'); continue
        if lower is None:
            next5_reason(ls,'insufficient_calibration_bin'); continue
        if not band[0]<=ask<=band[1]: next5_reason(ls,'price_band'); continue
        if lower<=ask*1.03+.03: next5_reason(ls,'insufficient_cost_adjusted_edge'); continue
        ls['candidates']+=1; ls['status']='pending'
        ls['pending']={'side':side,'ready_ms':now_ms+delay_ms,'expires_ms':now_ms+delay_ms+4000,
                       'book_at_ms':q['book_at_ms'],'limit':ask+.005}
        next5_reason(ls,'candidate_waiting_for_new_book')
    return s,plans


"""Three frozen research controls; no model promotion, no broker calls."""
from copy import deepcopy

CONFIRM3_LANES=('trend_control_1s_v1','reversion_control_1s_v1','reversion_control_3s_v1')
CONFIRM3_VERSION='confirm3-v1-first-eligible-signal-2usdt-cost-scenarios'
CONFIRM3_SPECS=(('trend_value_v1',1000,20,180,.35,.80),
                ('reversion_value_v1',1000,30,210,.25,.75),
                ('reversion_value_v1',3000,30,210,.25,.75))

def confirm3_step(previous,q,*,now_ms,start_ms,end_ms,model):
    if model.get('shadow_gate_passed') is not False:
        raise ValueError('Research controls require the old model gate to remain closed')
    identity={'version':CONFIRM3_VERSION,'model_id':model['model_id'],'start_ms':start_ms,'end_ms':end_ms}
    s=deepcopy(previous) if previous else {**identity,'signal_state':None,
        'lanes':{lane:{'status':'waiting','reason':'before_window','samples':0,'valid_samples':0,'signals':0,
                       'attempts':0,'fills':0,'invalid_samples':0,'max_valid_gap_ms':0} for lane in CONFIRM3_LANES}}
    if any(s.get(k)!=v for k,v in identity.items()):raise ValueError('Research identity changed')
    if now_ms<s.get('last_call_ms',0):raise ValueError('Time reversed')
    if now_ms==s.get('last_call_ms'):return s,[]
    s['last_call_ms']=now_ms
    if now_ms<start_ms:return s,[]
    s['signal_state'],unexpected=next5_step(s['signal_state'],q,now_ms=now_ms,start_ms=start_ms,end_ms=end_ms,model=model)
    if unexpected:raise RuntimeError('Old model gate generated a plan')
    plans=[]
    for lane,(source,delay,low,high,lower,upper) in zip(CONFIRM3_LANES,CONFIRM3_SPECS):
        ls=s['lanes'][lane];begin=start_ms+low*1000;end=start_ms+high*1000
        ls['signals']=s['signal_state']['lanes'][source]['signals']
        if begin<=now_ms<=end:
            ls['samples']+=1
            if next5_valid(q,now_ms):
                ls['valid_samples']+=1;ls.setdefault('first_valid_at_ms',now_ms)
                ls['max_valid_gap_ms']=max(ls['max_valid_gap_ms'],now_ms-ls.get('last_valid_at_ms',begin))
                ls['last_valid_at_ms']=now_ms
            else:ls['invalid_samples']+=1
        if now_ms>end and not ls.get('window_closed'):
            ls['max_valid_gap_ms']=max(ls['max_valid_gap_ms'],end-ls.get('last_valid_at_ms',begin))
            ls['window_closed']=True
        if ls['status'] in ('filled','rejected','expired'):continue
        if now_ms>end:
            ls['max_valid_gap_ms']=max(ls['max_valid_gap_ms'],end-ls.get('last_valid_at_ms',begin))
            ls.update(status='expired',reason='pending_timeout' if ls.get('pending') else
                      ('window_data_missing' if not ls['valid_samples'] else 'no_eligible_signal'))
            continue
        pending=ls.get('pending')
        if pending:
            if now_ms>pending['expires_ms']:
                ls.update(status='expired',reason='pending_timeout');continue
            if now_ms<pending['ready_ms']:continue
            if not next5_valid(q,now_ms):ls['reason']='waiting_fresh_book';continue
            if q['book_at_ms']<=pending['book_at_ms']:ls['reason']='waiting_new_book';continue
            ask=float(q[pending['side']]['ask']);depth=float(q[pending['side']]['ask_shares'])
            if ask>pending['limit'] or not lower<=ask<=upper:
                ls.update(status='rejected',reason='post_delay_price');continue
            if depth<2/ask:
                ls.update(status='rejected',reason='post_delay_depth');continue
            plan={'lane':lane,'action':'BUY_INITIAL','side':pending['side'],'ask':str(ask),'gross':'2',
                  'decision_ms':pending['decision_ms'],'fill_ms':now_ms,'reason':'research_counterfactual_fill',
                  'book_at_ms':q['book_at_ms'],'decision_book_at_ms':pending['book_at_ms'],
                  'cost_bps_scenarios':[0,300,500],'executable_profitability_verified':False}
            ls.update(status='filled',reason='hypothetical_fill',fills=1,fill=plan);plans.append(plan)
            continue
        if now_ms<begin:continue
        sig=s['signal_state']['lanes'][source].get('last_signal')
        if not sig or sig['at_ms']!=now_ms:
            ls['reason']='no_signal' if next5_valid(q,now_ms) else 'invalid_data';continue
        ask=float(q[sig['side']]['ask'])
        if not lower<=ask<=upper:ls['reason']='signal_outside_price_band';continue
        ls['pending']={'side':sig['side'],'decision_ms':now_ms,'ready_ms':now_ms+delay,
                       'expires_ms':min(now_ms+delay+4000,end),'book_at_ms':q['book_at_ms'],'limit':ask+.005}
        ls.update(status='pending',reason='waiting_delay',attempts=1)
    return s,plans


"""Value9: causal price-selective controls and hybrids for Shadow only.

Every lane takes at most one first eligible signal. Price selection is made
at signal time and checked again after the source-specific delay. A failed
post-delay execution is terminal, so later observations cannot replace it.
"""
VALUE9_VERSION = 'value9-v1-first-eligible-multisource-2usdt-cost300-stress500'

_VALUE9_TREND_CONTROL = (('trend_value_v1', 1000, 20, 180, {'UP': .35, 'DOWN': .35}, .80),)
_VALUE9_REVERSION_CONTROL_1S = (('reversion_value_v1', 1000, 30, 210, {'UP': .25, 'DOWN': .25}, .75),)
_VALUE9_REVERSION_CONTROL_3S = (('reversion_value_v1', 3000, 30, 210, {'UP': .25, 'DOWN': .25}, .75),)

VALUE9_SPECS = {
    'value9_trend_control_1s': _VALUE9_TREND_CONTROL,
    'value9_reversion_control_1s': _VALUE9_REVERSION_CONTROL_1S,
    'value9_reversion_control_3s': _VALUE9_REVERSION_CONTROL_3S,
    'value9_reversion_3s_p58': (
        ('reversion_value_v1', 3000, 30, 210, {'UP': .58, 'DOWN': .58}, .75),
    ),
    'value9_trend_1s_p68': (
        ('trend_value_v1', 1000, 20, 180, {'UP': .68, 'DOWN': .68}, .80),
    ),
    # Reversion is listed first only to resolve the rare same-millisecond tie.
    'value9_hybrid_58_68': (
        ('reversion_value_v1', 3000, 30, 210, {'UP': .58, 'DOWN': .58}, .75),
        ('trend_value_v1', 1000, 20, 180, {'UP': .68, 'DOWN': .68}, .80),
    ),
    'value9_hybrid_60_70': (
        ('reversion_value_v1', 3000, 30, 210, {'UP': .60, 'DOWN': .60}, .75),
        ('trend_value_v1', 1000, 20, 180, {'UP': .70, 'DOWN': .70}, .80),
    ),
    'value9_hybrid_62_72': (
        ('reversion_value_v1', 3000, 30, 210, {'UP': .62, 'DOWN': .62}, .75),
        ('trend_value_v1', 1000, 20, 180, {'UP': .72, 'DOWN': .72}, .80),
    ),
    'value9_hybrid_asym_58_58_68': (
        ('reversion_value_v1', 3000, 30, 210, {'UP': .58, 'DOWN': .58}, .75),
        ('trend_value_v1', 1000, 20, 180, {'UP': .68, 'DOWN': .58}, .80),
    ),
}


def value9_step(previous, q, *, now_ms, start_ms, end_ms, model):
    """Advance all nine frozen lanes using only information available now."""
    if model.get('shadow_gate_passed') is not False:
        raise ValueError('Value9 requires the old model gate to remain closed')
    identity = {
        'version': VALUE9_VERSION,
        'model_id': model['model_id'],
        'start_ms': start_ms,
        'end_ms': end_ms,
    }
    s = deepcopy(previous) if previous else {
        **identity,
        'signal_state': None,
        'lanes': {
            lane: {
                'status': 'waiting', 'reason': 'before_window', 'samples': 0,
                'valid_samples': 0, 'signals': 0, 'source_signals': {},
                'attempts': 0, 'fills': 0, 'invalid_samples': 0,
                'max_valid_gap_ms': 0,
            }
            for lane in VALUE9_LANES
        },
    }
    if any(s.get(key) != value for key, value in identity.items()):
        raise ValueError('Value9 research identity changed')
    if now_ms < s.get('last_call_ms', 0):
        raise ValueError('Time reversed')
    if now_ms == s.get('last_call_ms'):
        return s, []
    s['last_call_ms'] = now_ms
    if now_ms < start_ms:
        return s, []

    s['signal_state'], unexpected = next5_step(
        s['signal_state'], q, now_ms=now_ms, start_ms=start_ms,
        end_ms=end_ms, model=model,
    )
    if unexpected:
        raise RuntimeError('Old model gate generated a plan')

    valid_now = next5_valid(q, now_ms)
    plans = []
    for lane in VALUE9_LANES:
        rules = VALUE9_SPECS[lane]
        ls = s['lanes'][lane]
        lane_begin = min(start_ms + rule[2] * 1000 for rule in rules)
        lane_end = max(start_ms + rule[3] * 1000 for rule in rules)

        source_counts = {
            rule[0]: s['signal_state']['lanes'][rule[0]]['signals']
            for rule in rules
        }
        ls['source_signals'] = source_counts
        ls['signals'] = sum(source_counts.values())

        if lane_begin <= now_ms <= lane_end:
            ls['samples'] += 1
            if valid_now:
                ls['valid_samples'] += 1
                ls.setdefault('first_valid_at_ms', now_ms)
                ls['max_valid_gap_ms'] = max(
                    ls['max_valid_gap_ms'],
                    now_ms - ls.get('last_valid_at_ms', lane_begin),
                )
                ls['last_valid_at_ms'] = now_ms
            else:
                ls['invalid_samples'] += 1

        if now_ms > lane_end and not ls.get('window_closed'):
            ls['max_valid_gap_ms'] = max(
                ls['max_valid_gap_ms'],
                lane_end - ls.get('last_valid_at_ms', lane_begin),
            )
            ls['window_closed'] = True
        if ls['status'] in ('filled', 'rejected', 'expired'):
            continue
        if now_ms > lane_end:
            ls.update(
                status='expired',
                reason=('pending_timeout' if ls.get('pending') else
                        ('window_data_missing' if not ls['valid_samples'] else 'no_eligible_signal')),
            )
            continue

        pending = ls.get('pending')
        if pending:
            if now_ms > pending['expires_ms']:
                ls.update(status='expired', reason='pending_timeout')
                continue
            if now_ms < pending['ready_ms']:
                continue
            if not valid_now:
                ls['reason'] = 'waiting_fresh_book'
                continue
            if q['book_at_ms'] <= pending['book_at_ms']:
                ls['reason'] = 'waiting_new_book'
                continue
            ask = float(q[pending['side']]['ask'])
            depth = float(q[pending['side']]['ask_shares'])
            if ask > pending['limit'] or not pending['lower'] <= ask <= pending['upper']:
                ls.update(status='rejected', reason='post_delay_price')
                continue
            if depth < 2 / ask:
                ls.update(status='rejected', reason='post_delay_depth')
                continue
            plan = {
                'lane': lane, 'action': 'BUY_INITIAL', 'side': pending['side'],
                'ask': str(ask), 'gross': '2', 'decision_ms': pending['decision_ms'],
                'fill_ms': now_ms, 'reason': 'value9_research_counterfactual_fill',
                'signal_source': pending['source'], 'signal_price_floor': pending['lower'],
                'book_at_ms': q['book_at_ms'],
                'decision_book_at_ms': pending['book_at_ms'],
                'cost_bps_scenarios': [0, 300, 500],
                'executable_profitability_verified': False,
            }
            ls.update(status='filled', reason='hypothetical_fill', fills=1, fill=plan)
            plans.append(plan)
            continue
        if now_ms < lane_begin:
            continue

        candidates = []
        for priority, (source, delay, low, high, floors, upper) in enumerate(rules):
            begin = start_ms + low * 1000
            source_end = start_ms + high * 1000
            if not begin <= now_ms <= source_end:
                continue
            sig = s['signal_state']['lanes'][source].get('last_signal')
            if not sig or sig['at_ms'] != now_ms:
                continue
            side = sig['side']
            ask = float(q[side]['ask'])
            lower = floors[side]
            if lower <= ask <= upper:
                candidates.append((sig['at_ms'], priority, source, delay, source_end, side, ask, lower, upper))
        if not candidates:
            ls['reason'] = 'no_eligible_signal' if valid_now else 'invalid_data'
            continue

        _, _, source, delay, source_end, side, ask, lower, upper = min(candidates)
        ls['pending'] = {
            'source': source, 'side': side, 'lower': lower, 'upper': upper,
            'decision_ms': now_ms, 'ready_ms': now_ms + delay,
            'expires_ms': min(now_ms + delay + 4000, source_end),
            'book_at_ms': q['book_at_ms'], 'limit': ask + .005,
        }
        ls.update(status='pending', reason='waiting_delay', attempts=1)
    return s, plans

from scripts.prediction_paired8_policy import paired8_step as value9_step, PAIRED8_VERSION as VALUE9_VERSION
