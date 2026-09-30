"""P3 Live Lane Gate Evaluator.

Strict pre-signal microstructure gate for FAV_P3_LIVE:
1. pre_cross_count == 0 (real-time from pre-signal quote history)
2. same_side_seconds >= 30.0 (continuous duration since last cross to target side)
3. distance_from_reference_bps >= 3.0 (and spot on target side)
4. Strict feed freshness (fail-closed)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import logging
import time
from typing import Any, Mapping

from src.gridbot.prediction.models import OutcomeSide, QuoteSnapshot

LOGGER = logging.getLogger("cry3.prediction.p3_gate")

# P3 Gate Constants
P3_MIN_SAME_SIDE_SECONDS = 30.0
P3_MIN_DISTANCE_BPS = 3.0
P3_FIXED_ORDER_UNIT_USDT = Decimal("1.0")
P3_UNFILLED_TIMEOUT_SECONDS = 5.0
P3_MAX_SPOT_AGE_MS = 2500
P3_MAX_BOOK_AGE_MS = 2500


@dataclass
class P3MarketState:
    campaign_id: str
    target_outcome: OutcomeSide | None = None
    last_spot: float | None = None
    last_reference: float | None = None
    last_side: int | None = None  # +1 for spot >= ref, -1 for spot < ref
    pre_cross_count: int = 0
    same_side_start_ms: int | None = None
    last_quote_observed_ms: int = 0
    first_quote_observed_ms: int = 0
    quotes_seen: int = 0


@dataclass
class P3GateResult:
    allowed: bool
    reject_reason: str | None
    side: str | None
    cross_count: int
    same_side_seconds: float
    distance_bps: float
    spot_price: float
    reference_price: float
    ask_price: float | None
    feature_max_source_ts: int
    metrics: dict[str, Any] = field(default_factory=dict)


class P3LaneGateEvaluator:
    """Evaluates the strict P3 gate in real time without look-ahead bias."""

    def __init__(self) -> None:
        self._states: dict[str, P3MarketState] = {}

    def reset_market(self, campaign_id: str) -> None:
        self._states.pop(campaign_id, None)

    def update_quote(self, campaign_id: str, quote: QuoteSnapshot) -> P3MarketState:
        state = self._states.get(campaign_id)
        if state is None:
            state = P3MarketState(campaign_id=campaign_id)
            self._states[campaign_id] = state

        spot_raw = getattr(quote, "btc_spot", None)
        ref_raw = getattr(quote, "reference_price", None)
        obs_ms = int(getattr(quote, "observed_at_ms", 0) or 0)

        if spot_raw is None or ref_raw is None or obs_ms <= 0:
            return state

        try:
            spot = float(spot_raw)
            ref = float(ref_raw)
        except (ValueError, TypeError):
            return state

        if spot <= 0 or ref <= 0:
            return state

        if state.first_quote_observed_ms == 0:
            state.first_quote_observed_ms = obs_ms
        state.last_quote_observed_ms = max(state.last_quote_observed_ms, obs_ms)
        state.quotes_seen += 1
        state.last_spot = spot
        state.last_reference = ref

        curr_side = 1 if spot >= ref else -1
        if state.last_side is None:
            state.last_side = curr_side
            state.same_side_start_ms = obs_ms
        elif curr_side != state.last_side:
            # Cross occurred! Increment pre_cross_count and reset same_side timer
            state.pre_cross_count += 1
            state.last_side = curr_side
            state.same_side_start_ms = obs_ms

        return state

    def evaluate(
        self,
        campaign_id: str,
        quote: QuoteSnapshot,
        fav_side: OutcomeSide,
        ask_price: float | Decimal | None,
        now_ms: int,
    ) -> P3GateResult:
        """Evaluate whether a market currently passes the P3 gate."""
        state = self.update_quote(campaign_id, quote)

        feature_max_source_ts = state.last_quote_observed_ms
        # Strict anti-lookahead assertion
        assert feature_max_source_ts <= now_ms + 1000, (
            f"Future quote timestamp: {feature_max_source_ts} > {now_ms}"
        )

        spot = state.last_spot
        ref = state.last_reference
        side_str = fav_side.value.upper()

        if spot is None or ref is None or ref <= 0 or spot <= 0:
            return P3GateResult(
                allowed=False,
                reject_reason="P3_REJECT_STALE_DATA",
                side=side_str,
                cross_count=state.pre_cross_count,
                same_side_seconds=0.0,
                distance_bps=0.0,
                spot_price=spot or 0.0,
                reference_price=ref or 0.0,
                ask_price=float(ask_price) if ask_price is not None else None,
                feature_max_source_ts=feature_max_source_ts,
            )

        # 1. Check data freshness (fail-closed)
        spot_age_ms = max(0, now_ms - feature_max_source_ts)
        if spot_age_ms > P3_MAX_SPOT_AGE_MS:
            return P3GateResult(
                allowed=False,
                reject_reason="P3_REJECT_STALE_DATA",
                side=side_str,
                cross_count=state.pre_cross_count,
                same_side_seconds=0.0,
                distance_bps=0.0,
                spot_price=spot,
                reference_price=ref,
                ask_price=float(ask_price) if ask_price is not None else None,
                feature_max_source_ts=feature_max_source_ts,
                metrics={"spot_age_ms": spot_age_ms},
            )

        # 2. Compute Directional Distance
        # UP requires spot > ref; DOWN requires spot < ref
        is_on_target_side = (spot >= ref) if fav_side is OutcomeSide.UP else (spot <= ref)
        distance_bps = abs(spot - ref) / ref * 10000.0

        # 3. Compute continuous same-side seconds
        if state.same_side_start_ms is not None and is_on_target_side:
            same_side_seconds = max(0.0, (now_ms - state.same_side_start_ms) / 1000.0)
        else:
            same_side_seconds = 0.0

        # 4. Check P3 Gate Conditions
        # Condition A: pre_cross_count == 0
        if state.pre_cross_count > 0:
            return P3GateResult(
                allowed=False,
                reject_reason="P3_REJECT_CROSS",
                side=side_str,
                cross_count=state.pre_cross_count,
                same_side_seconds=same_side_seconds,
                distance_bps=distance_bps,
                spot_price=spot,
                reference_price=ref,
                ask_price=float(ask_price) if ask_price is not None else None,
                feature_max_source_ts=feature_max_source_ts,
            )

        # Condition B: distance_bps >= 3.0 on correct target side
        if not is_on_target_side or distance_bps < P3_MIN_DISTANCE_BPS:
            return P3GateResult(
                allowed=False,
                reject_reason="P3_REJECT_DISTANCE",
                side=side_str,
                cross_count=state.pre_cross_count,
                same_side_seconds=same_side_seconds,
                distance_bps=distance_bps,
                spot_price=spot,
                reference_price=ref,
                ask_price=float(ask_price) if ask_price is not None else None,
                feature_max_source_ts=feature_max_source_ts,
            )

        # Condition C: same_side_seconds >= 30.0
        if same_side_seconds < P3_MIN_SAME_SIDE_SECONDS:
            return P3GateResult(
                allowed=False,
                reject_reason="P3_REJECT_SAME_SIDE",
                side=side_str,
                cross_count=state.pre_cross_count,
                same_side_seconds=same_side_seconds,
                distance_bps=distance_bps,
                spot_price=spot,
                reference_price=ref,
                ask_price=float(ask_price) if ask_price is not None else None,
                feature_max_source_ts=feature_max_source_ts,
            )

        # All P3 conditions satisfied!
        return P3GateResult(
            allowed=True,
            reject_reason=None,
            side=side_str,
            cross_count=state.pre_cross_count,
            same_side_seconds=same_side_seconds,
            distance_bps=distance_bps,
            spot_price=spot,
            reference_price=ref,
            ask_price=float(ask_price) if ask_price is not None else None,
            feature_max_source_ts=feature_max_source_ts,
        )
