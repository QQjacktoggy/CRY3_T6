"""Production-facing Prediction market worker.

The worker is intentionally small and deterministic: synchronous Binance REST
calls run in ``asyncio.to_thread`` while strategy/risk/repository transitions
remain explicit and persisted.  It supports shadow collection immediately;
live order placement is enabled only after wallet, SAS, permission, balance,
and promotion prerequisites pass.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
import re
import traceback
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import inspect
import json
import logging
import math
import os
import time
from enum import Enum
from typing import Any, Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger("cry3.prediction.worker")

from .client import (
    BinancePredictionClient,
    PredictionAPIError,
    PredictionReadTimestampError,
    PredictionClientError,
    PredictionEntryNotSubmitted,
    entry_http_guard_scope, request_admission_guard_scope, request_timing_scope, request_stage,
    available_balance_display,
    normalize_amount_in,
)
from .models import (
    ActionType,
    Campaign,
    CampaignState,
    Fill,
    MarketInfo,
    OrderIntent,
    OrderSide,
    OutcomeSide,
    QuoteSnapshot,
    OrderRecord,
    as_decimal,
)
from .repository import PredictionRepository
from .p3_lane_gate import P3LaneGateEvaluator, P3_FIXED_ORDER_UNIT_USDT, P3_UNFILLED_TIMEOUT_SECONDS
from .rate_limit import request_budget_scope, SharedBudgetDeferred
from .rate_limit import PredictionRateLimiter
from .spot import Reversal5Feeds, reversal5_orientation
from .risk import RiskConfig, RiskEngine, RiskSnapshot
from .runtime import PredictionRuntimeManager, PromotionEvidence, PromotionGate
from .settings import PredictionSettings, RuntimeMode
from .strategy import (
    BALANCED_HOLD_PROFILE,
    COMPLETE_SET_ARB_V1_PROFILE,
    CONTROL_PROFILE,
    LATE_MATURITY_V1_PROFILE,
    LATENCY_SNIPE_PROFILE,
    PAIR_COST_ARB_PROFILE,
    QUALITY_HOLD_PROFILE,
    QUALITY_HOLD_V2_PROFILE,
    QUALITY_HOLD_V3_PROFIT1_PROFILE,
    QUALITY_HOLD_V3_GATE_V1_PROFILE,
    QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE,
    QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE,
    QUALITY_HOLD_V3_MOMENTUM30_V1_PROFILE,
    QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE,
    QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE,
    V3_MOMENTUM30_PROFILES,
    QUALITY_HOLD_V4_RESCUE_PROFILE,
    QUALITY_HOLD_V5_PNL_PROFILE,
    QUALITY_HOLD_V6_A_STAGED_PROFILE,
    QUALITY_HOLD_V6_BALANCED_SHADOW_PROFILE,
    QUALITY_HOLD_V6_B_45S_PROFILE,
    QUALITY_HOLD_V6_C_45S_PROFILE,
    QUALITY_HOLD_V3_SNIPER_PROFILE,
    REGIME_VALUE_V7_PROFILE,
    REGIME_VALUE_V8_CALIBRATED_PROFILE,
    PredictionStateMachine,
    SHADOW_LANE_A_PROFILE,
    SHADOW_LANE_B_PROFILE,
    StrategyConfig,
    StrategyDecision,
    calculate_pnl,
    quality_hold_v3_gate,
    v3_net_edge_cost_gate,
    v3_net_edge_source_valid,
    v3_momentum30_gate,
    reversal5_valid,
)


LIVE_PREFLIGHT_MAX_AGE_MS = 300_000
SIGNED_MUTATING_ENDPOINTS = frozenset({"place_order", "batch_cancel_orders", "batch_redeem"})
# Status-filtered order history can return these for some accounts; the
# caller then falls back to unfiltered pages (see _history_rows).
HISTORY_STATUS_FALLBACK = frozenset({400, 404, 500})


def durable_admission_sql(profile_count):
    """Pre-HTTP admission row; unknown-execution lookups use migration 028.

    CROSS JOIN fixes the join order so both lookups start from the partial
    unknown indexes; otherwise SQLite walks every LIVE campaign by loop.
    """
    placeholders = ",".join("?" for _ in range(profile_count))
    return f"""SELECT l.state,l.mode,l.strategy_profile,l.new_entries_stopped,l.hard_stop_latched,
                      r.config_value_json,lane.config_value_json,guard.config_value_json,
                      EXISTS(SELECT 1 FROM prediction_campaigns c
                        CROSS JOIN prediction_loops h
                        WHERE c.pending_unknown=1 AND h.loop_id=c.loop_id
                          AND h.mode='LIVE' AND h.strategy_profile IN ({placeholders}))
                      OR EXISTS(SELECT 1 FROM prediction_order_intents i
                        CROSS JOIN prediction_campaigns c CROSS JOIN prediction_loops h
                        WHERE i.unknown=1 AND c.campaign_id=i.campaign_id AND h.loop_id=c.loop_id
                          AND h.mode='LIVE' AND h.strategy_profile IN ({placeholders})),
                      legacy.config_value_json
               FROM prediction_loops l LEFT JOIN prediction_runtime_config r
                 ON r.config_key='prediction_risk_state'
               LEFT JOIN prediction_runtime_config legacy
                 ON legacy.config_key='prediction_hard_stop_latched'
               LEFT JOIN prediction_runtime_config lane ON lane.config_key=?
               LEFT JOIN prediction_runtime_config guard ON guard.config_key=?
               WHERE l.loop_id=?"""


ALLOWED_ORDER_UNITS_USDT = frozenset({Decimal("1"), Decimal("2"), Decimal("3")})
ORDER_UNIT_RISK_MULTIPLIER = Decimal("2")
REGIME_SENSITIVE_LANE_BREAK_EVEN_WR = {
    QUALITY_HOLD_V3_PROFIT1_PROFILE: Decimal("82"),
    QUALITY_HOLD_V4_RESCUE_PROFILE: Decimal("84"),
    QUALITY_HOLD_V5_PNL_PROFILE: Decimal("77"),
}
REGIME_ENTRY_GATE_PROFILES = frozenset(REGIME_SENSITIVE_LANE_BREAK_EVEN_WR)
LANE_READINESS_SAFETY_MARGIN_WR = Decimal("3")
ADAPTIVE_JUMP_STOP_MIN_LOOP_LOSSES = 2
ADAPTIVE_JUMP_STOP_PATH_BPS = Decimal("15")
ADAPTIVE_JUMP_STOP_LATEST_MAX_AGE_MS = 10 * 60_000
ADAPTIVE_JUMP_STOP_TWO_LOSS_WINDOW_MS = 15 * 60_000
ADAPTIVE_JUMP_STOP_COOLDOWN_MS = 15 * 60_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _data(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return payload.get("data", payload)
    return payload


def _book_level(level: Any) -> tuple[Decimal, Decimal] | None:
    if isinstance(level, Mapping):
        price = level.get("price") or level.get("px")
        quantity = level.get("quantity") or level.get("qty") or level.get("size") or "0"
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        price, quantity = level[0], level[1]
    else:
        return None
    try:
        return as_decimal(price), as_decimal(quantity)
    except ValueError:
        return None


def _book_top(payload: Any) -> tuple[Decimal | None, Decimal | None]:
    data = _data(payload)
    if isinstance(data, Mapping):
        bids = data.get("bids") or data.get("buy") or data.get("BUY") or []
        asks = data.get("asks") or data.get("sell") or data.get("SELL") or []
    else:
        bids, asks = [], []
    parsed_bids = [item for item in (_book_level(level) for level in bids) if item and item[1] > 0]
    parsed_asks = [item for item in (_book_level(level) for level in asks) if item and item[1] > 0]
    bid = max((price for price, _ in parsed_bids), default=None)
    ask = min((price for price, _ in parsed_asks), default=None)
    return bid, ask


def _v7_quote_features(
    state: dict[str, Any],
    *,
    now_ms: int,
    btc_spot: Decimal | None,
    spot_observed_at_ms: int,
    reference_price: Decimal | None,
    end_time_ms: int,
    leader: OutcomeSide | None,
    crossed_reference: bool,
    history_window_ms: int = 120_000,
    cross_cooldown_seconds: int = 30,
    high_volatility_bps_sqrt_second: Decimal = Decimal("0.95"),
) -> dict[str, object]:
    """Build V7's replayable BTC fair-value features from persisted samples.

    Values are intentionally advisory gates, not trade prices.  The final
    decision still validates a fresh executable Prediction-book quote.
    ``history`` lives in market state so a process restart cannot silently
    reset the model's warm-up period.
    """

    raw_history = state.get("v7_spot_history")
    history: list[tuple[int, Decimal]] = []
    if isinstance(raw_history, list):
        for item in raw_history:
            if not isinstance(item, Mapping):
                continue
            try:
                at_ms = int(item.get("at_ms") or 0)
                spot = as_decimal(item.get("spot"))
            except (ArithmeticError, TypeError, ValueError):
                continue
            if at_ms > 0 and spot > 0 and spot.is_finite():
                history.append((at_ms, spot))
    if btc_spot is not None and btc_spot > 0 and btc_spot.is_finite():
        observed = int(spot_observed_at_ms or now_ms)
        if not history or observed > history[-1][0]:
            history.append((observed, btc_spot))
        elif observed == history[-1][0]:
            history[-1] = (observed, btc_spot)
    floor = max(0, int(now_ms) - max(1, int(history_window_ms)))
    history = [(at_ms, spot) for at_ms, spot in history if at_ms >= floor]
    state["v7_spot_history"] = [
        {"at_ms": at_ms, "spot": str(spot)} for at_ms, spot in history
    ]
    if crossed_reference:
        state["v7_last_cross_at_ms"] = int(now_ms)
    try:
        last_cross_at_ms = int(state.get("v7_last_cross_at_ms") or 0)
    except (TypeError, ValueError):
        last_cross_at_ms = 0
    recent_cross = bool(last_cross_at_ms and now_ms - last_cross_at_ms <= cross_cooldown_seconds * 1000)

    normalized_returns: list[float] = []
    for (prior_at, prior_spot), (current_at, current_spot) in zip(history, history[1:]):
        dt_seconds = (current_at - prior_at) / 1000.0
        if dt_seconds <= 0 or prior_spot <= 0 or current_spot <= 0:
            continue
        try:
            return_bps = math.log(float(current_spot / prior_spot)) * 10_000.0
            normalized = return_bps / math.sqrt(dt_seconds)
        except (ArithmeticError, OverflowError, ValueError):
            continue
        if math.isfinite(normalized):
            normalized_returns.append(normalized)
    raw_sigma = (
        math.sqrt(sum(value * value for value in normalized_returns) / len(normalized_returns))
        if normalized_returns
        else None
    )
    sigma = max(0.35, raw_sigma) if raw_sigma is not None else None
    z_score: float | None = None
    p_up: float | None = None
    if (
        sigma is not None
        and btc_spot is not None
        and reference_price is not None
        and reference_price > 0
    ):
        remaining_seconds = max(1.0, (int(end_time_ms) - int(now_ms)) / 1000.0)
        try:
            displacement_bps = float((btc_spot - reference_price) / reference_price * Decimal("10000"))
            z_score = displacement_bps / (sigma * math.sqrt(remaining_seconds))
            z_score = max(-8.0, min(8.0, z_score))
            p_up = 0.5 * (1.0 + math.erf(z_score / math.sqrt(2.0)))
        except (ArithmeticError, OverflowError, ValueError, ZeroDivisionError):
            z_score = None
            p_up = None
    leader_z: float | None = None
    z_delta: float | None = None
    if leader is not None and z_score is not None:
        leader_z = z_score if leader is OutcomeSide.UP else -z_score
        prior_leader = str(state.get("v7_last_feature_leader") or "").upper()
        try:
            prior_leader_z = float(state.get("v7_last_leader_z"))
        except (TypeError, ValueError):
            prior_leader_z = None
        if prior_leader == leader.value and prior_leader_z is not None and math.isfinite(prior_leader_z):
            z_delta = leader_z - prior_leader_z
        state["v7_last_feature_leader"] = leader.value
        state["v7_last_leader_z"] = str(leader_z)
    else:
        state["v7_last_feature_leader"] = leader.value if leader is not None else None
        state["v7_last_leader_z"] = None
    return {
        "schema": "v7-btc-value-v1",
        "sample_count": len(history),
        "span_ms": max(0, history[-1][0] - history[0][0]) if len(history) >= 2 else 0,
        "sigma_bps_sqrt_second": str(sigma) if sigma is not None else None,
        "raw_sigma_bps_sqrt_second": str(raw_sigma) if raw_sigma is not None else None,
        "z_score": str(z_score) if z_score is not None else None,
        "p_up": str(p_up) if p_up is not None else None,
        "p_down": str(1.0 - p_up) if p_up is not None else None,
        "leader_z": str(leader_z) if leader_z is not None else None,
        "z_delta": str(z_delta) if z_delta is not None else None,
        "recent_cross": recent_cross,
        "high_volatility": bool(raw_sigma is not None and raw_sigma >= float(high_volatility_bps_sqrt_second)),
    }


class PredictionRateLimitDeferred(RuntimeError):
    """Local weight-budget backoff; it is not an exchange execution error."""

    def __init__(self, method_name: str, health: Mapping[str, Any]) -> None:
        self.method_name = method_name
        self.health = dict(health)
        super().__init__(f"prediction weight deferred for {method_name}")


@dataclass
class WorkerHeartbeat:
    started_at_ms: int
    last_loop_at_ms: int = 0
    last_api_ok_at_ms: int = 0
    last_feed_ok_at_ms: int = 0
    last_db_write_at_ms: int = 0
    last_error: str | None = None
    markets_seen: int = 0
    markets_completed: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "started_at_ms": self.started_at_ms,
            "last_loop_at_ms": self.last_loop_at_ms,
            "last_api_ok_at_ms": self.last_api_ok_at_ms,
            "last_feed_ok_at_ms": self.last_feed_ok_at_ms,
            "last_db_write_at_ms": self.last_db_write_at_ms,
            "last_error": self.last_error,
            "markets_seen": self.markets_seen,
            "markets_completed": self.markets_completed,
        }


class WorkerState(str, Enum):
    CREATED = "created"
    SHADOW = "shadow"
    LIVE = "live"
    PAUSED = "paused"
    STOPPED = "stopped"
    HARD_STOP = "hard_stop"
    DONE = "done"
    FAILED = "failed"


from .loop_market_worker import LoopMarketWorker


class PredictionWorker(LoopMarketWorker):
    """Finite market-loop controller for one isolated Prediction process."""

    worker_available = True
    fail_closed = False

    @staticmethod
    def _loop_origin_ms(
        row: Mapping[str, Any] | None,
        *,
        loop_id: str | None = None,
    ) -> int | None:
        """Return the durable start boundary used for first-market admission."""

        selected_id = str(loop_id or (row or {}).get("loop_id") or "")
        if selected_id.startswith("loop:"):
            try:
                value = int(selected_id.split(":", 1)[1])
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                return value
        try:
            value = int((row or {}).get("created_at_ms") or 0)
        except (TypeError, ValueError):
            value = 0
        return value if value > 0 else None

    def __init__(
        self,
        settings: PredictionSettings,
        repository: PredictionRepository,
        client: BinancePredictionClient,
        *,
        strategy: PredictionStateMachine | None = None,
        risk: RiskEngine | None = None,
        now_ms: Callable[[], int] = _now_ms,
        spot_source: Callable[..., Any] | None = None,
        rate_limiter: PredictionRateLimiter | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.client = client
        self._required_balance_floor_usdt = as_decimal(
            getattr(settings, "required_balance_usdt", Decimal("2"))
        )
        self._selected_order_unit_usdt = self._normalize_order_unit_usdt(
            getattr(settings, "order_unit_usdt", Decimal("1"))
        )
        self._selected_order_unit_loaded = False
        self._pending_order_unit_usdt: Decimal | None = None
        selected_profile = str(
            getattr(settings, "strategy_profile", "balanced_hold") or "balanced_hold"
        ).strip().lower()
        selected_strategy = strategy or PredictionStateMachine(
            self._sized_strategy_config(selected_profile, self._selected_order_unit_usdt)
        )
        self.strategy = selected_strategy
        self._selected_strategy_profile = selected_profile
        self._selected_strategy_loaded = False
        self._pending_strategy_profile: str | None = None
        self.risk_engine = risk or RiskEngine(
            config=self._sized_risk_config(RiskConfig(), self._selected_order_unit_usdt)
        )
        self.settings = replace(
            settings,
            order_unit_usdt=self._selected_order_unit_usdt,
            required_balance_usdt=max(
                self._required_balance_floor_usdt,
                self._selected_order_unit_usdt * ORDER_UNIT_RISK_MULTIPLIER,
            ),
        )
        client_unit_setter = getattr(self.client, "set_order_unit_usdt", None)
        if callable(client_unit_setter):
            client_unit_setter(self._selected_order_unit_usdt)
        self._now_ms = now_ms
        self.heartbeat = WorkerHeartbeat(now_ms())
        self._task: asyncio.Task[Any] | None = None
        # The Shadow observer is a separate lifecycle from the finite Live
        # loop.  It is allowed to run while the operator is idle, but yields
        # whenever a Live loop is active so the two paths never double-poll a
        # market or compete for execution-critical API budget.
        self._shadow_observer_task: asyncio.Task[Any] | None = None
        self._shadow_observer_campaign: Campaign | None = None
        self._shadow_observer_last_error: str | None = None
        self._shadow_observer_last_tick_at_ms = 0
        self._shadow_observer_discovery_at_ms = 0
        self._shadow_observer_discovery_cache: Any = None
        self._target_markets = 0
        self._loop_id: str | None = None
        self._loop_created_at_ms: int | None = None
        self._initial_market_wait_until_ms: int | None = None
        self._ignored_discovery_topic_ids: set[str] = set()
        self._accept_new_markets = True
        self._allow_new_orders = True
        self._allow_new_buys = True
        self._allow_reductions = True
        self._hard_stop_latched = False
        self._cancel_requested = False
        self._loop_loss_limit_reached = False
        self._adaptive_jump_stop_latched = False
        self._effective_mode = RuntimeMode.SHADOW
        self._regime_entry_gate_snapshot: dict[str, Any] = {
            "status": "NOT_CHECKED",
            "profile": selected_profile,
            "mode": RuntimeMode.SHADOW.value,
            "live_only": True,
            "applicable": False,
            "allowed": True,
            "blocked": False,
            "gate_block_new_entries": False,
            "gate_reason": "regime entry gate has not been checked",
            "gate_profiles": sorted(REGIME_ENTRY_GATE_PROFILES),
        }
        self._regime_entry_block_keys: set[tuple[str, str, str, str, str]] = set()
        self._promotion_decision: Mapping[str, Any] | None = None
        self._active_campaigns: dict[str, Campaign] = {}
        self._order_fill_progress: dict[str, tuple[Decimal, Decimal]] = {}
        self._last_risk: Any = None
        self._lock = asyncio.Lock()
        self._shadow_reasons = self._live_prerequisite_reasons()
        self._spot_source = spot_source or self.settings.spot_source
        self._last_leader: OutcomeSide | None = None
        self._prior_leader: OutcomeSide | None = None
        self._leader_quotes = 0
        self._leader_since_ms = 0
        self._cumulative_fills: dict[str, Decimal] = {}
        self._cumulative_gross: dict[str, Decimal] = {}
        self._cumulative_fees: dict[str, Decimal] = {}
        # A reduce-only exit may need one fresh held-side book (weight 200)
        # before the lightweight quote/order pair.  Keep enough emergency
        # budget reserved so a normal data burst can never strand an exit.
        self._rate_limiter = rate_limiter or PredictionRateLimiter(
            clock_ms=now_ms,
            reserve_weight=300,
        )
        self._discovery_cache: tuple[int, Any] | None = None
        self._detail_cache: dict[str, tuple[int, Any]] = {}
        self._discovery_cache_ttl_ms = 60_000
        self._detail_cache_ttl_ms = 300_000
        # Binance can publish a new five-minute topic before variantData.startPrice
        # is populated.  Such a response is not tradeable metadata and must be
        # retried promptly instead of being held for the full market lifetime.
        self._incomplete_detail_cache_ttl_ms = 5_000
        self._latest_quotes: dict[str, QuoteSnapshot] = {}
        self._s3s5_engine: dict[int, dict] = {}
        self._s3s5_accounting_lock = asyncio.Lock()
        self._observability_events = []
        self._observability_dropped = 0
        self._observability_task = None
        self._observability_campaigns = {}
        self._observability_flush_lock = asyncio.Lock()
        self._s3s5_snapshot: dict | None = None
        self._market_states: dict[str, dict[str, Any]] = {}
        self._settlement_attempt_at_ms: dict[str, int] = {}
        # One live settlement pass can consume position, settled-history and
        # PnL weight. Polling every worker tick would exceed the one-minute
        # budget before Binance publishes the resolved position.
        self._settlement_poll_cadence_ms = 5_000
        # Empty OBSERVE markets that cannot obtain official resolution (often
        # because Prediction weight budget deferred get_market_detail) must not
        # block admit-next forever. After this grace past end_time, finalize as
        # zero-PnL NO_FILL with a clear last_error. Never applies when buys or
        # filled exposure exist.
        self._empty_observe_auto_close_grace_ms = 45_000
        configured_cadence = max(0, int(getattr(self.settings, "quote_cadence_ms", 1_000)))
        # A pair of books costs two market-data weights.  Leave the reserve
        # untouched and spread refreshes over the remaining 1200/min budget;
        # discovery/detail overhead is accounted for at their cache cadence.
        normal_budget = max(1, self._rate_limiter.limit - self._rate_limiter.reserve_weight)
        discovery_per_minute = self._endpoint_weight("list_prediction_markets") * 60_000 / self._discovery_cache_ttl_ms
        detail_per_minute = self._endpoint_weight("get_market_detail") * 60_000 / self._detail_cache_ttl_ms
        book_budget = max(1, int(normal_budget - discovery_per_minute - detail_per_minute))
        sustainable_pair_ms = int((60_000 * (2 * self._endpoint_weight("query_order_book")) + book_budget - 1) / book_budget)
        self._quote_cadence_ms = max(configured_cadence, sustainable_pair_ms, 36_364)
        self._shadow_config_hash: str | None = None
        self._shadow_window: dict[str, Any] | None = None
        self._shadow_campaign_ids: dict[str, str] = {}
        self._shadow_decision_counts: dict[str, int] = {}
        self._shadow_lane_strategies: dict[str, PredictionStateMachine] = {}
        self._shadow_lane_config_hashes: dict[str, str] = {}
        self._shadow_lane_windows: dict[str, dict[str, Any]] = {}
        self._shadow_lane_campaigns: dict[str, dict[str, Campaign]] = {}
        self._shadow_lane_shadow_ids: dict[tuple[str, str], str] = {}
        self._shadow_lane_restored: set[tuple[str, str]] = set()
        # The V3 gate keeps only the short entry-history window in memory.
        # ``prediction_quotes`` remains the durable source for restart recovery
        # and for the existing regime analytics; it is not globally pruned.
        self._v3_gate_quote_history: dict[str, list[QuoteSnapshot]] = {}
        self._v3_gate_history_loaded: set[tuple[str, bool]] = set()
        self._shadow_tick_generation = 0
        self._shadow_regime_cache_generation = -1
        self._shadow_regime_cache: Mapping[str, Any] | None = None
        self._p3_gate_evaluator = P3LaneGateEvaluator()
        self._p3_active_campaigns: dict[str, Campaign] = {}
        self._fav_p3_arm_override: str | None = None
        from src.gridbot.prediction.jev_gate import JevGateEvaluator
        self._jev_gate = JevGateEvaluator(
            enabled=getattr(self.settings, "jev_gate_enabled", True),
            runtime_dir=getattr(self.settings, "jev_gate_runtime_dir", "/home/jack_shih/cry3/jev_shadow_lane/runtime"),
            max_age_ms=getattr(self.settings, "jev_gate_max_age_ms", 20000),
            fail_open_on_stale=getattr(self.settings, "jev_gate_fail_open", True),
        )
        self._refresh_shadow_lane_strategies()

    def _normalize_fav_p3_arm(self, value: Any) -> str:
        raw = str(value or "off").strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return "live"
        if raw in {"off", "live", "shadow"}:
            return raw
        return "off"

    def _fav_p3_arm(self) -> str:
        """Independent FAV_P3 lane arm: off | live | shadow. Never gates Baseline."""
        if self._fav_p3_arm_override is not None:
            return self._normalize_fav_p3_arm(self._fav_p3_arm_override)
        raw = getattr(self.settings, "fav_p3_arm", "off")
        return self._normalize_fav_p3_arm(raw)

    def _fav_p3_live_orders_enabled(self) -> bool:
        return (getattr(self, "_selected_strategy_profile", "") not in ("regime_target6_7_v1", "regime_target6_7a_v1", "regime_target6_7b_v1", "regime_target6_7c_v1", "regime_target6_7d_v1", "regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1", "regime_target6_9a_v1")
                and self._fav_p3_arm() == "live")

    def _is_fav_p3_profile(self) -> bool:
        """Selectable P3 lane: FAV through P3 gates only; no R3."""
        return str(getattr(self, "_selected_strategy_profile", "") or "").strip().lower() == "fav_p3"

    def _suppress_fav_baseline_live(self) -> bool:
        # fav_p3 profile always suppresses naked FAV; arm=live also suppresses on other profiles.
        return self._is_fav_p3_profile() or self._fav_p3_live_orders_enabled()

    async def set_fav_p3_arm(self, arm: str) -> dict[str, Any]:
        """Telegram/operator toggle for independent FAV_P3 Live lane."""
        selected = self._normalize_fav_p3_arm(arm)
        self._fav_p3_arm_override = selected
        try:
            await self.repository.set_runtime_config(
                "prediction_fav_p3_arm",
                {"arm": selected, "at_ms": self._now_ms(), "source": "telegram"},
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "fav_p3_arm": selected,
                "error": type(exc).__name__,
                "strategy_profile": getattr(self, "_selected_strategy_profile", None),
            }
        return {
            "ok": True,
            "fav_p3_arm": selected,
            "fav_p3_live_orders": selected == "live",
            "strategy_profile": getattr(self, "_selected_strategy_profile", None),
            "note": "Prefer strategy_profile=fav_p3 (FAV+P3 gates, no R3). arm=live alone only suppresses Baseline FAV.",
        }

    def _configured_shadow_lanes(self) -> tuple[str, ...]:
        raw = getattr(self.settings, "shadow_lanes", ())
        if isinstance(raw, str):
            raw = raw.split(",")
        return tuple(dict.fromkeys(str(item).strip().lower() for item in (raw or ()) if str(item).strip()))

    @staticmethod
    def _normalize_order_unit_usdt(value: Any) -> Decimal:
        selected = as_decimal(value)
        if selected not in ALLOWED_ORDER_UNITS_USDT:
            raise ValueError("Prediction order unit must be exactly 1, 2 or 3 USDT")
        return selected

    @staticmethod
    def _sized_strategy_config(profile: str, order_unit_usdt: Decimal) -> StrategyConfig:
        selected = PredictionWorker._normalize_order_unit_usdt(order_unit_usdt)
        config = StrategyConfig.for_profile(profile)
        if config.profile in {"regime_target6_v1", "regime_target6_1_v1"}:
            return config
        if config.profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            return replace(config, max_buy_usdt=selected, max_market_buy_usdt=selected,
                           max_scale_in_attempts=0, max_hedge_attempts=0,
                           pnl_scale_in_enabled=False)
        if config.profile == QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE or config.profile in V3_MOMENTUM30_PROFILES:
            # Frozen experiment unit: two USDT gross plus modeled costs.
            return config
        if config.v7_enabled or config.v8_enabled:
            # V7 is 1+1; V8 is a single 1-USDT observation.  The operator's
            # account envelope must never convert its first leg into 2 USDT.
            return replace(
                config,
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=(Decimal("1") if config.v8_enabled else Decimal("2")),
            )
        if config.pnl_scale_in_enabled:
            # V6A defaults to staged 1 USDT + 1 USDT.  When the operator
            # selects 2 USDT, that selection means a single 2-USDT initial
            # entry—not an implicit 2 + 2 position.  Keep the market cap at
            # 2 USDT and disable the add leg for this explicit mode.
            if selected == Decimal("2"):
                return replace(
                    config,
                    max_buy_usdt=Decimal("2"),
                    max_market_buy_usdt=Decimal("2"),
                    pnl_scale_in_enabled=False,
                    max_scale_in_attempts=0,
                )
            return replace(
                config,
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=Decimal("2"),
            )
        if config.pnl_priority_enabled:
            return replace(
                config,
                max_buy_usdt=Decimal("1"),
                max_market_buy_usdt=selected,
            )
        return replace(
            config,
            max_buy_usdt=selected,
            max_market_buy_usdt=selected * ORDER_UNIT_RISK_MULTIPLIER,
        )

    @staticmethod
    def _shadow_lane_profile(lane: str) -> str | None:
        return {
            "lane_a": SHADOW_LANE_A_PROFILE,
            "lane_b": SHADOW_LANE_B_PROFILE,
            "balanced_hold": BALANCED_HOLD_PROFILE,
            "quality_hold": QUALITY_HOLD_PROFILE,
            "quality_hold_v2": QUALITY_HOLD_V2_PROFILE,
            "quality_hold_v3_profit1": QUALITY_HOLD_V3_PROFIT1_PROFILE,
            "quality_hold_v3_gate_v1": QUALITY_HOLD_V3_GATE_V1_PROFILE,
            "quality_hold_v3_loss_guard_v2": QUALITY_HOLD_V3_LOSS_GUARD_V2_PROFILE,
            "quality_hold_v3_net_edge_v1": QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE,
            "quality_hold_v3_momentum30_v1": QUALITY_HOLD_V3_MOMENTUM30_V1_PROFILE,
            "quality_hold_v3_confirm30_v1": QUALITY_HOLD_V3_CONFIRM30_V1_PROFILE,
            "quality_hold_v3_history30_control_v1": QUALITY_HOLD_V3_HISTORY30_CONTROL_V1_PROFILE,
            "quality_hold_v4_rescue": QUALITY_HOLD_V4_RESCUE_PROFILE,
            "quality_hold_v5_pnl": QUALITY_HOLD_V5_PNL_PROFILE,
            "quality_hold_v6_a_staged": QUALITY_HOLD_V6_A_STAGED_PROFILE,
            "quality_hold_v6_balanced_shadow": QUALITY_HOLD_V6_BALANCED_SHADOW_PROFILE,
            "quality_hold_v6_b_45s": QUALITY_HOLD_V6_B_45S_PROFILE,
            "quality_hold_v6_c_45s": QUALITY_HOLD_V6_C_45S_PROFILE,
            "quality_hold_v3_sniper": QUALITY_HOLD_V3_SNIPER_PROFILE,
            "late_maturity_v1": LATE_MATURITY_V1_PROFILE,
            "regime_value_v7": REGIME_VALUE_V7_PROFILE,
            "regime_value_v8_calibrated": REGIME_VALUE_V8_CALIBRATED_PROFILE,
            "control": CONTROL_PROFILE,
            "latency_snipe": LATENCY_SNIPE_PROFILE,
            "pair_cost_arb": PAIR_COST_ARB_PROFILE,
            "complete_set_arb_v1": COMPLETE_SET_ARB_V1_PROFILE,
        }.get(str(lane or "").strip().lower())

    @staticmethod
    def _is_v3_gate_lane(lane: str) -> bool:
        return str(lane or "").strip().lower() == QUALITY_HOLD_V3_GATE_V1_PROFILE

    def _begin_shadow_tick(self) -> None:
        """Reset once-per-worker-tick Shadow gate reads."""

        self._shadow_tick_generation += 1
        self._shadow_regime_cache_generation = -1
        self._shadow_regime_cache = None

    def _remember_v3_gate_quote(self, campaign_id: str, quote: QuoteSnapshot) -> None:
        """Keep only the bounded 30--60s gate history in process memory."""

        if not ({QUALITY_HOLD_V3_GATE_V1_PROFILE, *V3_MOMENTUM30_PROFILES} & self._shadow_lane_strategies.keys()):
            return
        campaign_key = str(campaign_id)
        history = self._v3_gate_quote_history.setdefault(campaign_key, [])
        history[:] = [
            item
            for item in history
            if item.observed_at_ms != quote.observed_at_ms
        ]
        history.append(quote)
        history.sort(key=lambda item: item.observed_at_ms)
        floor = int(quote.observed_at_ms) - 120_000
        history[:] = [item for item in history if item.observed_at_ms >= floor][-64:]

    def _clear_v3_gate_history(self, campaign_id: str) -> None:
        campaign_key = str(campaign_id)
        self._v3_gate_quote_history.pop(campaign_key, None)
        self._v3_gate_history_loaded = {
            key for key in self._v3_gate_history_loaded if key[0] != campaign_key
        }

    @staticmethod
    def _quote_row_value(row: Mapping[str, Any], key: str) -> Any:
        value = row.get(key)
        if value is not None:
            return value
        payload = row.get("payload")
        if isinstance(payload, Mapping):
            return payload.get(key)
        return None

    async def _historical_v3_gate_quotes(
        self,
        campaign: Campaign,
        current_quote: QuoteSnapshot,
        *,
        observer: bool,
    ) -> list[QuoteSnapshot | Mapping[str, Any]]:
        """Load bounded candidate history; pure gate code selects the quote."""

        campaign_key = str(campaign.campaign_id)
        history = self._v3_gate_quote_history.setdefault(campaign_key, [])
        self._remember_v3_gate_quote(campaign_key, current_quote)
        candidates = [
            item
            for item in history
            if 30_000 <= int(current_quote.observed_at_ms) - int(item.observed_at_ms) <= 60_000
        ]
        if candidates:
            return list(candidates)

        load_key = (campaign_key, bool(observer))
        if load_key in self._v3_gate_history_loaded:
            return []
        self._v3_gate_history_loaded.add(load_key)
        if observer:
            fallback = getattr(self.repository, "get_shadow_observer_quotes", None)
        else:
            fallback = getattr(self.repository, "get_quotes", None)
        if not callable(fallback):
            return []
        try:
            rows = await fallback(
                campaign_key,
                since_ms=int(current_quote.observed_at_ms) - 60_000,
                limit=64,
            )
        except TypeError:
            try:
                rows = await fallback(campaign_key, limit=64)
            except Exception:
                return []
        except Exception:
            return []
        return [row for row in (rows or []) if isinstance(row, Mapping)]

    async def _historical_v3_gate_quote(
        self,
        campaign: Campaign,
        current_quote: QuoteSnapshot,
        *,
        observer: bool,
    ) -> QuoteSnapshot | Mapping[str, Any] | None:
        """Compatibility reader returning the nearest bounded candidate."""

        candidates = await self._historical_v3_gate_quotes(
            campaign, current_quote, observer=observer
        )
        target = int(current_quote.observed_at_ms) - 45_000
        valid: list[tuple[int, QuoteSnapshot | Mapping[str, Any]]] = []
        for item in candidates:
            try:
                observed_at = int(
                    item.observed_at_ms
                    if isinstance(item, QuoteSnapshot)
                    else item.get("observed_at_ms") or 0
                )
            except (TypeError, ValueError):
                continue
            age = int(current_quote.observed_at_ms) - observed_at
            if 30_000 <= age <= 60_000:
                valid.append((observed_at, item))
        return min(valid, key=lambda item: (abs(item[0] - target), item[0]))[1] if valid else None

    @staticmethod
    def _momentum30_history_quote(row: QuoteSnapshot | Mapping[str, Any]) -> QuoteSnapshot:
        """Restore the persisted book/spot source clocks without refreshing them."""

        if isinstance(row, QuoteSnapshot):
            return row
        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            try:
                payload = json.loads(str(row.get("payload_json") or "{}"))
            except (TypeError, ValueError):
                payload = {}
        values = {**(dict(payload) if isinstance(payload, Mapping) else {}), **dict(row)}
        # Rows from both repository forms retain the original quote.raw;
        # never substitute a decision/DB-write timestamp for a source clock.
        raw = values.get("raw")
        if not isinstance(raw, Mapping):
            raw = {}
        try:
            observed = int(values.get("observed_at_ms") or 0)
        except (TypeError, ValueError, OverflowError):
            observed = 0
        try:
            return QuoteSnapshot.from_books(
                observed_at_ms=observed,
                **{key: values.get(key) for key in ("up_bid", "up_ask", "down_bid", "down_ask", "leader")},
                btc_spot=as_decimal(values["btc_spot"]) if values.get("btc_spot") is not None else None,
                reference_price=as_decimal(values["reference_price"]) if values.get("reference_price") is not None else None,
                spot_observed_at_ms=int(values.get("spot_observed_at_ms") or 0),
                feed_ok=values.get("feed_ok") in (True, 1),
                raw=raw,
            )
        except (TypeError, ValueError, ArithmeticError):
            # Keep the invalid newest sample visible to the pure gate.  It
            # must not silently fall back to an older favorable observation.
            return QuoteSnapshot(observed_at_ms=observed, feed_ok=False, raw=raw)

    async def _historical_momentum30_quotes(
        self, campaign: Campaign, current_quote: QuoteSnapshot, *, observer: bool,
    ) -> list[QuoteSnapshot]:
        # These research arms recover only their dedicated observer stream;
        # the live/primary campaign quote ledger is never a substitute.
        rows = await self._historical_v3_gate_quotes(campaign, current_quote, observer=True)
        quotes = [self._momentum30_history_quote(row) for row in rows]
        for quote in quotes:
            self._remember_v3_gate_quote(campaign.campaign_id, quote)
        # Both arms see the same recovered samples even though the repository
        # history is loaded only once per campaign after a process restart.
        return quotes

    async def _shadow_regime_for_tick(self) -> Mapping[str, Any]:
        """Load the expensive regime monitor at most once per worker tick."""

        if self._shadow_regime_cache_generation == self._shadow_tick_generation:
            return self._shadow_regime_cache or {}
        self._shadow_regime_cache_generation = self._shadow_tick_generation
        getter = getattr(self.repository, "get_market_regime_monitor", None)
        if not callable(getter):
            self._shadow_regime_cache = {"status": None}
            return self._shadow_regime_cache
        try:
            value = await getter(limit=20)
        except Exception:
            value = {"status": None}
        self._shadow_regime_cache = dict(value) if isinstance(value, Mapping) else {"status": None}
        return self._shadow_regime_cache

    @staticmethod
    def _shadow_gate_bypass_telemetry(
        lane: str,
        *,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "gate_version": QUALITY_HOLD_V3_GATE_V1_PROFILE if PredictionWorker._is_v3_gate_lane(lane) else None,
            "allowed": None,
            "reason": reason,
            "regime_status": None,
            "median_path_bps": None,
            "fast_median_path_bps": None,
            "ask_move": None,
            "history_age_ms": None,
            "signed_btc_margin_bps": None,
            "leader": None,
        }

    def _refresh_shadow_lane_strategies(self) -> None:
        """Size every virtual lane from the operator's current 1/2-USDT unit."""

        selected = self._selected_order_unit_usdt
        strategies: dict[str, PredictionStateMachine] = {}
        for lane in self._configured_shadow_lanes():
            profile = self._shadow_lane_profile(lane)
            if profile is not None:
                strategies[lane] = PredictionStateMachine(
                    self._sized_strategy_config(profile, selected)
                )
        self._shadow_lane_strategies = strategies
        # Sizing is part of strategy provenance.  Drop only in-memory Shadow
        # state so the next sample gets a fresh immutable configuration hash.
        self._shadow_config_hash = None
        self._shadow_window = None
        self._shadow_lane_config_hashes.clear()
        self._shadow_lane_windows.clear()
        self._shadow_lane_campaigns.clear()
        self._shadow_lane_shadow_ids.clear()
        self._shadow_lane_restored.clear()
        self._v3_gate_quote_history.clear()
        self._v3_gate_history_loaded.clear()

    @staticmethod
    def _refresh_pnl_btc_peak(
        campaign: Campaign,
        quote: QuoteSnapshot,
        machine: PredictionStateMachine,
    ) -> None:
        """Persist the best post-entry BTC displacement for the V5 exit."""

        if not machine.config.pnl_priority_enabled or campaign.initial_outcome is None:
            return
        spot = quote.btc_spot
        reference = quote.reference_price
        if not quote.feed_ok or spot is None or reference is None or reference <= 0:
            return
        move = (spot - reference) / reference * Decimal("10000")
        if campaign.initial_outcome is OutcomeSide.DOWN:
            move = -move
        if not move.is_finite():
            return
        if campaign.pnl_btc_peak_bps is None or move > campaign.pnl_btc_peak_bps:
            campaign.pnl_btc_peak_bps = move

    def _sized_risk_config(self, config: RiskConfig, order_unit_usdt: Decimal) -> RiskConfig:
        selected = self._normalize_order_unit_usdt(order_unit_usdt)
        profile = str(getattr(self, "_selected_strategy_profile", "") or "").strip().lower()
        from src.gridbot.prediction import s3s5_pair as s3

        if s3.uses_loop_risk_guards(profile):

            env = s3.risk_envelope()
            return replace(
                config,
                loop_loss_limit=Decimal(str(env["loop_loss_limit"])),
                daily_loss_limit=Decimal(str(env["daily_loss_limit"])),
                consecutive_loss_limit=int(env["consecutive_loss_limit"]),
            )
        loss_limit = -(selected * ORDER_UNIT_RISK_MULTIPLIER)
        return replace(
            config,
            loop_loss_limit=loss_limit,
            daily_loss_limit=loss_limit,
        )

    def _apply_order_unit_runtime(self, order_unit_usdt: Decimal) -> None:
        selected = self._normalize_order_unit_usdt(order_unit_usdt)
        self._selected_order_unit_usdt = selected
        self.settings = replace(
            self.settings,
            order_unit_usdt=selected,
            required_balance_usdt=max(
                self._required_balance_floor_usdt,
                selected * ORDER_UNIT_RISK_MULTIPLIER,
            ),
        )
        self.strategy = PredictionStateMachine(
            self._sized_strategy_config(self._selected_strategy_profile, selected)
        )
        self._refresh_shadow_lane_strategies()
        self.risk_engine.config = self._sized_risk_config(self.risk_engine.config, selected)
        setter = getattr(self.client, "set_order_unit_usdt", None)
        if callable(setter):
            setter(selected)

    def _shadow_lane_experiment_enabled(self) -> bool:
        # Virtual lanes are telemetry sidecars.  They may observe a Live
        # campaign, but their decisions are persisted only to the immutable
        # Shadow ledger and can never reach a signed exchange endpoint.
        return (getattr(self, "_selected_strategy_profile", "") not in ("regime_target6_7_v1", "regime_target6_7a_v1", "regime_target6_7b_v1", "regime_target6_7c_v1", "regime_target6_7d_v1", "regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1", "regime_target6_9a_v1")
                and bool(self._shadow_lane_strategies))

    async def _shadow_lane_at_exact_target(self, lane: str) -> bool:
        """Return whether one immutable lane has reached its frozen target."""

        target = getattr(self.settings, "shadow_exact_target", None)
        if target is None:
            return False
        await self._ensure_shadow_window()
        counter = getattr(self.repository, "count_shadow_resolved_campaigns", None)
        if not callable(counter):
            return False
        window = self._shadow_lane_windows.get(lane) or self._shadow_window or {}
        config_hash = str(
            self._shadow_lane_config_hashes.get(lane)
            or self._shadow_lane_config_hash(lane)
        )
        count = await counter(
            config_hash=config_hash,
            window_start_ms=int(window.get("window_start_ms", 0)),
            window_end_ms=int(window.get("window_end_ms", 0)),
        )
        return int(count) >= int(target)

    async def _shadow_exact_target_complete(self) -> bool:
        """Freeze observer collection only after every configured lane is full."""

        target = getattr(self.settings, "shadow_exact_target", None)
        if target is None:
            return False
        lanes = tuple(self._shadow_lane_strategies)
        if not lanes:
            return False
        await self._ensure_shadow_window()
        config_hashes = {
            lane: str(
                self._shadow_lane_config_hashes.get(lane)
                or self._shadow_lane_config_hash(lane)
            )
            for lane in lanes
        }
        marker = await self.repository.get_runtime_config(
            "prediction_shadow_exact_complete", {}
        )
        if (
            isinstance(marker, Mapping)
            and bool(marker.get("complete"))
            and int(marker.get("target") or 0) == int(target)
            and marker.get("config_hashes") == config_hashes
        ):
            return True
        complete = True
        for lane in lanes:
            if not await self._shadow_lane_at_exact_target(lane):
                complete = False
                break
        if complete:
            await self.repository.set_runtime_config(
                "prediction_shadow_exact_complete",
                {
                    "complete": True,
                    "target": int(target),
                    "lanes": list(lanes),
                    "config_hashes": config_hashes,
                    "at_ms": self._now_ms(),
                },
            )
        return complete

    @property
    def effective_config_hash(self) -> str:
        """Hash the configuration that will actually drive this worker.

        The selected Telegram lane is runtime state, not a mutable
        ``PredictionSettings`` field.  It must nevertheless be part of the
        live/shadow provenance envelope so a restart or lane switch cannot
        reuse evidence produced by another strategy.
        """

        payload = getattr(self.settings, "config_payload", None)
        if not isinstance(payload, Mapping):
            return str(getattr(self.settings, "config_hash", "") or "")
        effective = dict(payload)
        effective["strategy_profile"] = self._selected_strategy_profile
        effective["strategy"] = self.strategy.config.provenance_payload
        effective["order_unit_usdt"] = str(self._selected_order_unit_usdt)
        effective["risk"] = {
            "loop_loss_limit": str(self.risk_engine.config.loop_loss_limit),
            "daily_loss_limit": str(self.risk_engine.config.daily_loss_limit),
        }
        canonical = json.dumps(effective, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _balance_account_type(settings: Any) -> str:
        return (
            "CeDeFi"
            if str(getattr(settings, "funding_source", "MPC") or "MPC").strip().upper() == "MPC"
            else str(getattr(settings, "account_type", "SPOT") or "SPOT").strip().upper()
        )

    async def restore_selected_strategy(self) -> str:
        """Restore the last operator-selected lane once per process."""

        if getattr(self, "_selected_strategy_loaded", False):
            return str(getattr(self, "_selected_strategy_profile", "balanced_hold"))
        if not hasattr(self, "_selected_strategy_profile"):
            settings = getattr(self, "settings", None)
            self._selected_strategy_profile = str(
                getattr(settings, "strategy_profile", "balanced_hold") or "balanced_hold"
            ).strip().lower()
        self._selected_strategy_loaded = True
        getter = getattr(getattr(self, "repository", None), "get_runtime_config", None)
        if not callable(getter):
            return self._selected_strategy_profile
        try:
            persisted = await getter("prediction_selected_strategy", {})
        except Exception:
            persisted = {}
        profile = str(persisted.get("profile") or "").strip().lower() if isinstance(persisted, Mapping) else ""
        if profile in {
            "balanced_hold",
            QUALITY_HOLD_PROFILE,
            QUALITY_HOLD_V2_PROFILE,
            QUALITY_HOLD_V3_PROFIT1_PROFILE,
            QUALITY_HOLD_V4_RESCUE_PROFILE,
            QUALITY_HOLD_V5_PNL_PROFILE,
            QUALITY_HOLD_V6_A_STAGED_PROFILE,
            REGIME_VALUE_V7_PROFILE,
            CONTROL_PROFILE,
            "s3s5_pair_v1",
            "fav_only_v1",
            "fav_only_v2",
            "fav_only_v3",
            "fav_only_v4",
            "c180_favorite_hold_v1",
            "regime_target6_v1",
            "regime_target6_1_v1",
            "regime_target6_2_v1",
            "fav_p3", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            try:
                self.strategy = PredictionStateMachine(
                    self._sized_strategy_config(
                        profile,
                        getattr(self, "_selected_order_unit_usdt", Decimal("1")),
                    )
                )
                self._selected_strategy_profile = profile
            except ValueError:
                pass
        self.risk_engine.config = self._sized_risk_config(
            self.risk_engine.config,
            getattr(self, "_selected_order_unit_usdt", Decimal("1")),
        )
        return self._selected_strategy_profile

    async def restore_order_unit(self) -> Decimal:
        """Restore the durable 1/2-USDT execution unit once per process."""

        if not hasattr(self, "_selected_order_unit_usdt"):
            settings = getattr(self, "settings", None)
            self._selected_order_unit_usdt = self._normalize_order_unit_usdt(
                getattr(settings, "order_unit_usdt", Decimal("1"))
            )
        if not hasattr(self, "_pending_order_unit_usdt"):
            self._pending_order_unit_usdt = None
        if getattr(self, "_selected_order_unit_loaded", False):
            return self._selected_order_unit_usdt
        self._selected_order_unit_loaded = True
        getter = getattr(getattr(self, "repository", None), "get_runtime_config", None)
        if not callable(getter):
            return self._selected_order_unit_usdt
        try:
            persisted = await getter("prediction_selected_order_unit", {})
        except Exception:
            persisted = {}
        raw = persisted.get("order_unit_usdt") if isinstance(persisted, Mapping) else None
        try:
            selected = self._normalize_order_unit_usdt(
                self._selected_order_unit_usdt if raw in (None, "") else raw
            )
        except ValueError:
            selected = self._selected_order_unit_usdt
        self._selected_order_unit_usdt = selected
        if all(
            hasattr(self, name)
            for name in ("settings", "strategy", "risk_engine", "client", "_selected_strategy_profile")
        ):
            if not hasattr(self, "_required_balance_floor_usdt"):
                self._required_balance_floor_usdt = as_decimal(
                    getattr(self.settings, "required_balance_usdt", Decimal("2"))
                )
            self._apply_order_unit_runtime(selected)
        return selected

    @staticmethod
    def _selectable_strategy_profiles() -> set[str]:
        return {"s3s5_pair_v1", "fav_only_v1", "fav_only_v2", "fav_only_v3", "fav_only_v4", "fav_p3", "c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}

    async def _load_pending_strategy(self) -> str | None:
        getter = getattr(getattr(self, "repository", None), "get_runtime_config", None)
        if not callable(getter):
            return getattr(self, "_pending_strategy_profile", None)
        try:
            persisted = await getter("prediction_pending_strategy", {})
        except Exception:
            persisted = {}
        profile = str(persisted.get("profile") or "").strip().lower() if isinstance(persisted, Mapping) else ""
        self._pending_strategy_profile = profile if profile in self._selectable_strategy_profiles() else None
        return self._pending_strategy_profile

    async def _activate_strategy(self, selected: str) -> bool:
        """Activate one validated strategy only at an idle loop boundary.

        A strategy change invalidates the previous Live authorization.  The
        operator must explicitly confirm Live again for the new strategy
        hash before any signed endpoint can be reached.
        """

        if selected in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            await self._activate_order_unit(Decimal("1"))
        config = self._sized_strategy_config(selected, self._selected_order_unit_usdt)
        previous = str(getattr(self, "_selected_strategy_profile", "") or "").strip().lower()
        changed = previous != selected
        now = self._now_ms()
        self.strategy = PredictionStateMachine(config)
        self._selected_strategy_profile = selected
        self.risk_engine.config = self._sized_risk_config(
            self.risk_engine.config, self._selected_order_unit_usdt
        )
        self._selected_strategy_loaded = True
        self._pending_strategy_profile = None
        if changed:
            self._s3s5_engine = {}
            self._effective_mode = RuntimeMode.SHADOW
        await self.repository.set_runtime_config(
            "prediction_selected_strategy",
            {"profile": selected, "at_ms": now},
        )
        await self.repository.set_runtime_config(
            "prediction_pending_strategy",
            {"profile": None, "cleared_at_ms": now},
        )
        return changed and bool(getattr(getattr(self, "settings", None), "is_live_requested", False))

    async def _activate_pending_strategy_if_idle(self) -> str | None:
        existing = await self.repository.get_active_loop()
        task = getattr(self, "_task", None)
        if existing or (task is not None and not task.done()):
            return None
        selected = await self._load_pending_strategy()
        if selected is None:
            return None
        await self._activate_strategy(selected)
        return selected

    async def _load_pending_order_unit(self) -> Decimal | None:
        getter = getattr(getattr(self, "repository", None), "get_runtime_config", None)
        if not callable(getter):
            return self._pending_order_unit_usdt
        try:
            persisted = await getter("prediction_pending_order_unit", {})
        except Exception:
            persisted = {}
        raw = persisted.get("order_unit_usdt") if isinstance(persisted, Mapping) else None
        try:
            self._pending_order_unit_usdt = (
                None if raw in (None, "") else self._normalize_order_unit_usdt(raw)
            )
        except ValueError:
            self._pending_order_unit_usdt = None
        return self._pending_order_unit_usdt

    async def _activate_order_unit(self, selected: Decimal) -> bool:
        selected = self._normalize_order_unit_usdt(selected)
        if self._selected_strategy_profile in {"regime_target6_v1", "regime_target6_1_v1"} and selected != Decimal("1"):
            raise ValueError("Regime T6/T6.1/T6.2 requires exactly 1 USDT")
        previous = self._selected_order_unit_usdt
        changed = previous != selected
        now = self._now_ms()
        self._apply_order_unit_runtime(selected)
        self._selected_order_unit_loaded = True
        self._pending_order_unit_usdt = None
        if changed:
            self._effective_mode = RuntimeMode.SHADOW
        await self.repository.set_runtime_config(
            "prediction_selected_order_unit",
            {
                "order_unit_usdt": str(selected),
                "loop_loss_limit": str(self.risk_engine.config.loop_loss_limit),
                "daily_loss_limit": str(self.risk_engine.config.daily_loss_limit),
                "at_ms": now,
            },
        )
        await self.repository.set_runtime_config(
            "prediction_pending_order_unit",
            {"order_unit_usdt": None, "cleared_at_ms": now},
        )
        return changed and bool(getattr(self.settings, "is_live_requested", False))

    async def _activate_pending_order_unit_if_idle(self) -> Decimal | None:
        existing = await self.repository.get_active_loop()
        task = getattr(self, "_task", None)
        if existing or (task is not None and not task.done()):
            return None
        selected = await self._load_pending_order_unit()
        if selected is None:
            return None
        await self._activate_order_unit(selected)
        return selected

    def _shadow_lane_config_hash(self, lane: str) -> str:
        """Derive an immutable hash for one isolated Shadow lane."""

        frozen_baseline_hashes = {
            "quality_hold_v3_gate_v1": "cb98bc44e1edca4f1273b5b0b2fc74ff03c2f9a7cc8b100698195d41001599a2",
            "quality_hold_v3_profit1": "68fe8624c2b257ac2c867e25230f53af0ecd4df2ed147ce895c93228b826a4b7",
            "quality_hold_v6_a_staged": "755a3120e5370e3f0fbb5cf59927c162d10907e409f9561e92804fc1313652d1",
            "quality_hold_v6_b_45s": "100cd1141d0a872b8b209890ed00ee23aaa66c90a51407909d68c2324003c646",
            "quality_hold_v6_c_45s": "b76876c03113a5484f5c8fac40f73380c798f90662ed19da09b0672b8afa80d6",
            "quality_hold_v3_sniper": "47090d463c51f4cc83f3cf1c857579aa6a95cf51b60df243868b58345ded08cc",
            "quality_hold_v4_rescue": "edddde6bd97ce79024ad96cb9494d86131411e28e3649fa039dc731c4d2dc4de",
            "quality_hold_v5_pnl": "05a0333313dcc9109d1c1cc267822f5b3d476e7d35ad7d6098b9b11658f8faf2",
            "quality_hold_v2": "f46d0a4eddf9e9cdf7d5e4289784f706af1f6ecf812c5badfa7db6d82420ca48",
            "regime_value_v7": "dc3b41782d10d0e55942372d7237e8b0917cd958d1244ff1f54fb060e7f68901",
        }
        legacy_cost_model = (
            getattr(self.settings, "shadow_exact_target", None) is None
            and int(getattr(self.settings, "shadow_fee_bps", 10)) == 10
            and int(getattr(self.settings, "shadow_slippage_bps", 0)) == 0
        )
        if lane in frozen_baseline_hashes and legacy_cost_model:
            return frozen_baseline_hashes[lane]

        configured = self._configured_shadow_lanes()
        if not configured:
            return str(getattr(self.settings, "config_hash", "") or "")
        strategy = self._shadow_lane_strategies.get(lane)
        payload = {
            "experiment": "prediction-shadow-ab-v1",
            "base_config_hash": str(getattr(self.settings, "config_hash", "") or ""),
            "configured_lanes": list(configured),
            "lane": lane,
            "strategy": strategy.config.provenance_payload if strategy is not None else self.strategy.config.provenance_payload,
        }
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def hard_stop_latched(self) -> bool:
        return self._hard_stop_latched

    @property
    def orders_enabled(self) -> bool:
        # Backwards-compatible status field: BUY capability.  A hard stop
        # must never revoke the process' ability to reduce an existing
        # position or cancel/settle it.
        return self.live_capability and not self._hard_stop_latched and self._allow_new_buys

    @property
    def live_capability(self) -> bool:
        """Whether the worker may reach the signed trade endpoints.

        This is deliberately independent from the BUY gate.  Hard-stop and
        pause are risk/control states, not a reason to strand an open SELL.
        """

        return self._effective_mode is RuntimeMode.LIVE

    @property
    def mode(self) -> str:
        return self._effective_mode.value

    def _live_prerequisite_reasons(self) -> list[str]:
        reasons: list[str] = []
        if not self.settings.live_enabled:
            reasons.append("live trading is not enabled by settings")
        if not self.settings.wallet_address:
            reasons.append("wallet address is not configured")
        if not self.settings.wallet_id:
            reasons.append("wallet id is not configured")
        if not bool(getattr(self.settings, "sas_verified", False)):
            reasons.append("SAS trade authorization has not been verified for this wallet")
        return reasons

    @staticmethod
    def _strict_true(value: Any) -> bool:
        """Accept only an actual JSON boolean true for security gates."""

        return value is True

    def _validate_persisted_live_preflight(self, value: Any, *, now_ms: int | None = None) -> list[str]:
        """Validate the controller's persisted official live-capability proof.

        The worker is the final authority before signed order execution.  A
        settings flag, a stale controller object, or a loosely typed JSON
        value must never grant live capability after a restart.
        """

        reasons: list[str] = []
        if not isinstance(value, Mapping):
            return ["persisted official live preflight is missing or not a mapping"]
        if not self._strict_true(value.get("checked")):
            reasons.append("persisted live preflight was not checked")
        if not self._strict_true(value.get("passed")):
            reasons.append("persisted live preflight did not pass")
        if str(value.get("mode") or "").upper() != RuntimeMode.LIVE.value.upper():
            reasons.append("persisted live preflight mode is not LIVE")
        if not self._strict_true(value.get("requested_live")):
            reasons.append("persisted live preflight is not for a live request")

        checked_at = value.get("checked_at_ms", value.get("generated_at_ms", value.get("timestamp_ms")))
        try:
            checked_at_ms = int(checked_at)
        except (TypeError, ValueError):
            checked_at_ms = -1
            reasons.append("persisted live preflight timestamp is missing or invalid")
        if checked_at_ms >= 0:
            current = self._now_ms() if now_ms is None else int(now_ms)
            if checked_at_ms > current + 5_000:
                reasons.append("persisted live preflight timestamp is in the future")
            elif current - checked_at_ms > LIVE_PREFLIGHT_MAX_AGE_MS:
                reasons.append("persisted live preflight is stale")

        expected_hash = str(self.effective_config_hash or "")
        actual_hash = str(value.get("config_hash") or value.get("configuration_hash") or "")
        if not actual_hash:
            reasons.append("persisted live preflight config hash is missing")
        elif expected_hash and actual_hash != expected_hash:
            reasons.append("persisted live preflight config hash does not match settings")

        capability = value.get("permission_capability")
        if not isinstance(capability, Mapping):
            capability = {}
            reasons.append("persisted live preflight permission capability is missing")
        if not self._strict_true(value.get("permission_verified")):
            reasons.append("persisted live preflight permission_verified is not true")
        if not self._strict_true(capability.get("verified")):
            reasons.append("persisted live preflight permission capability is not verified")
        security_type = str(value.get("security_type") or "").upper()
        capability_security = str(capability.get("security_type") or "").upper()
        if security_type != "PREDICTION_TRADE" or capability_security != "PREDICTION_TRADE":
            reasons.append("persisted live preflight security type is not PREDICTION_TRADE")
        if not self._strict_true(value.get("signed")) or not self._strict_true(capability.get("signed")):
            reasons.append("persisted live preflight is not signed")
        if not self._strict_true(value.get("authenticated")) or not self._strict_true(capability.get("authenticated")):
            reasons.append("persisted live preflight is not authenticated")

        expected_address = str(getattr(self.settings, "wallet_address", "") or "").strip().lower()
        actual_address = str(value.get("wallet_address") or value.get("walletAddress") or "").strip().lower()
        if not expected_address or actual_address != expected_address:
            reasons.append("persisted live preflight wallet address does not match settings")
        expected_wallet_id = str(getattr(self.settings, "wallet_id", "") or "").strip()
        actual_wallet_id = str(value.get("wallet_id") or value.get("walletId") or "").strip()
        if not expected_wallet_id or actual_wallet_id != expected_wallet_id:
            reasons.append("persisted live preflight wallet id does not match settings")
        if not self._strict_true(value.get("wallet_match")):
            reasons.append("persisted live preflight wallet match is not verified")
        expected_account = str(getattr(self.settings, "account_type", "SPOT") or "").upper()
        actual_account = str(value.get("account_type") or value.get("accountType") or "").upper()
        if actual_account != expected_account or actual_account != "SPOT":
            reasons.append("persisted live preflight account type does not match SPOT settings")
        expected_funding = str(getattr(self.settings, "funding_source", "MPC") or "MPC").upper()
        actual_funding = str(value.get("funding_source") or value.get("fundingSource") or "").upper()
        if actual_funding != expected_funding:
            reasons.append("persisted live preflight funding source does not match settings")
        expected_balance_account = self._balance_account_type(self.settings)
        actual_balance_account = str(value.get("balance_account_type") or "").strip()
        if actual_balance_account != expected_balance_account:
            reasons.append("persisted live preflight balance account does not match funding source")
        return list(dict.fromkeys(reasons))

    async def _refresh_live_prerequisites(self) -> list[str]:
        reasons = self._live_prerequisite_reasons()
        try:
            preflight = await self.repository.get_runtime_config("prediction_preflight")
        except Exception as exc:  # noqa: BLE001 - missing proof is fail-closed
            preflight = None
            reasons.append(f"persisted official live preflight unavailable: {exc}")
        reasons.extend(self._validate_persisted_live_preflight(preflight))
        if reasons:
            self._shadow_reasons = list(dict.fromkeys(reasons))
            return self._shadow_reasons
        try:
            balance_payload = await self._call_api(
                "query_payment_option_balances",
                recv_window=self.settings.recv_window,
            )
            available = available_balance_display(balance_payload, account_type=self._balance_account_type(self.settings))
            if available is None:
                reasons.append("availableBalanceDisplay is missing")
            elif available < self.settings.required_balance_usdt:
                reasons.append(
                    f"availableBalanceDisplay {available} < required {self.settings.required_balance_usdt}"
                )
        except Exception as exc:  # noqa: BLE001 - missing preflight is shadow-only
            reasons.append(f"live balance preflight failed: {exc}")
        self._shadow_reasons = reasons
        return reasons

    def _trace_event(self, event, *, campaign_id=None, **fields):
        # No API arguments/results: wallet, signatures and credentials stay out.
        queue = getattr(self, '_observability_events', None)
        if queue is None:
            return
        if len(queue) >= 2000:
            self._observability_dropped += 1
            return
        attempt = getattr(self, "_entry_attempts", {}).get(campaign_id)
        if attempt is not None:
            fields = {"attempt_id": attempt["id"], "profile": self._selected_strategy_profile,
                      "elapsed_ns": time.monotonic_ns()-attempt["started_ns"],
                      "remaining_ms": attempt["expires_at_ms"]-self._now_ms(), **fields}
        queue.append((event,campaign_id,dict(version='observability-v1',at_ms=self._now_ms(),monotonic_ns=time.monotonic_ns(),**fields)))

    def _start_entry_attempt(self, campaign, ready):
        self._entry_attempts = getattr(self, "_entry_attempts", {})
        self._finish_entry_attempt(campaign.campaign_id, "superseded_before_execution")
        self._entry_attempts[campaign.campaign_id] = {
            "id": str(uuid4()), "started_ns": time.monotonic_ns(),
            "expires_at_ms": ready.execution.expires_at_ms}
        self._trace_event("entry_selected", campaign_id=campaign.campaign_id)

    def _finish_entry_attempt(self, campaign_id, reason, **fields):
        if campaign_id in getattr(self, "_entry_attempts", {}):
            self._trace_event("entry_finished", campaign_id=campaign_id, reason=reason, **fields)
            self._entry_attempts.pop(campaign_id, None)

    @staticmethod
    def _post_claim_admission_denials(*, checked, ready, intent, bound, risk_state,
                                     live_capability, allow_new_buys, hard_stop_latched, now_ms):
        """Name existing rejection gates without recording API or wallet values."""
        execution = checked.execution
        denials = (
            ("signal_recheck_denied", not checked.allowed),
            ("frozen_signal_changed", checked.signal is not None and checked.signal != ready.signal),
            ("execution_missing", execution is None),
            ("worst_ask_missing", execution is not None and execution.worst_ask_limit is None),
            ("worst_ask_exceeds_intent_limit", execution is not None
             and execution.worst_ask_limit is not None and execution.worst_ask_limit > intent.limit_price),
            ("live_capability_disabled", not live_capability),
            ("new_buys_disabled", not allow_new_buys),
            ("worker_hard_stop", bool(hard_stop_latched)),
            ("risk_hard_stop", bool(risk_state.get("hard_stop_latched"))),
            ("loop_missing", not bound),
            ("loop_not_running", bool(bound) and bound.get("state") != "RUNNING"),
            ("loop_entries_stopped", bool(bound) and bool(bound.get("new_entries_stopped"))),
            ("loop_hard_stop", bool(bound) and bool(bound.get("hard_stop_latched"))),
            ("execution_deadline_reached", now_ms >= ready.execution.expires_at_ms),
        )
        return tuple(reason for reason, denied in denials if denied)

    def _entry_durable_http_guard(self, loop_id, profile):
        import sqlite3
        from contextlib import closing
        from .regime_lane import STATE_KEY as lane_key
        from .regime_live_ledger import RISK_PROFILES
        regime_entry = profile in RISK_PROFILES
        guard_key = profile.removesuffix("_v1")+"_loop_risk:"+str(loop_id) if regime_entry else ""
        try:
            with closing(sqlite3.connect(self.repository.db_path.resolve().as_uri()+"?mode=ro",
                                         uri=True, timeout=0.05)) as db:
                row = db.execute(durable_admission_sql(len(RISK_PROFILES)),
                    (*RISK_PROFILES, *RISK_PROFILES, lane_key if regime_entry else "",
                     guard_key, loop_id)).fetchone()
                if (not row or row[:3] != ("RUNNING", "LIVE", profile) or row[3] or row[4]
                        or (row[5] and json.loads(row[5]).get("hard_stop_latched"))
                        or (row[9] and json.loads(row[9]).get("latched"))
                        or (regime_entry and (row[8] or any(value and json.loads(value).get("halt_reason")
                                                          for value in row[6:8])))):
                    raise PredictionEntryNotSubmitted("durable_admission_rejected_before_http")
        except PredictionEntryNotSubmitted:
            raise
        except Exception as exc:
            raise PredictionEntryNotSubmitted("durable_admission_unavailable_before_http") from exc

    def _entry_http_started(self, campaign_id, intent_id, at_ms, stamp):
        self._trace_event("entry_http_start", campaign_id=campaign_id, intent_id=intent_id,
                          http_at_ms=at_ms, http_monotonic_ns=stamp)
        self._finish_entry_attempt(campaign_id, "post_started")

    @contextmanager
    def _entry_stage(self, campaign_id, stage):
        began = time.monotonic_ns()
        try:
            yield
        finally:
            if campaign_id in getattr(self, "_entry_attempts", {}):
                self._trace_event("entry_stage", campaign_id=campaign_id, stage=stage,
                                  duration_ns=time.monotonic_ns()-began)

    async def _entry_signal_within_window(self, bridge, campaign, start):
        # Only local, read-only readiness is retried. No schedule/risk/API work
        # is repeated and a frozen rejection never gets another selection.
        missing = {"regime_features_missing_skip", "regime_initial_book_missing_skip",
                   "t67a_features_missing", "t67b_features_missing", "t67c_features_missing", "t67d_features_missing", "t68_features_missing", "t68a_features_missing", "t69_features_missing", "t69a_features_missing"}
        if self._selected_strategy_profile == "regime_target6_7c_v1":
            from .regime_t67c_bridge import TRANSIENT_BOOK_REASONS
            missing |= TRANSIENT_BOOK_REASONS
        if self._selected_strategy_profile == "regime_target6_9_v1":
            from .regime_t69_bridge import TRANSIENT_BOOK_REASONS as T69_TRANSIENT
            missing |= T69_TRANSIENT
        if self._selected_strategy_profile == "regime_target6_9a_v1":
            from .regime_t69a_bridge import TRANSIENT_BOOK_REASONS as T69A_TRANSIENT
            missing |= T69A_TRANSIENT
        for attempt in range(9):
            ready = bridge.check_signal(
                market=campaign.market, unit_usdt=self._selected_order_unit_usdt,
                at_ms=self._now_ms(), last_seen_book_at_ms=start +
                (59999 if self._selected_strategy_profile == "regime_target6_7_v1" else 120000))
            if (ready.allowed or ready.reason not in missing or attempt == 8
                    or not start+124000 <= self._now_ms() < start+125900):
                return ready
            # Never call readiness again after the original initial window,
            # including an oversleep or a delayed event-loop wakeup.
            await asyncio.sleep(min(.1, max(0, (start+126000-self._now_ms())/1000)))
            if self._now_ms() >= start+126000:
                return ready
        return ready

    async def _entry_refresh_within_deadline(self, bridge, campaign, ready, *, attempts=8):
        from .c180_worker_bridge import C180Ready
        from .regime_t67c_bridge import TRANSIENT_BOOK_REASONS
        retryable = {"quote_not_new_after_ready"}
        if self._selected_strategy_profile == "regime_target6_7c_v1":
            retryable |= TRANSIENT_BOOK_REASONS
        if self._selected_strategy_profile == "regime_target6_9_v1":
            from .regime_t69_bridge import TRANSIENT_BOOK_REASONS as T69_TRANSIENT
            retryable |= T69_TRANSIENT
        if self._selected_strategy_profile == "regime_target6_9a_v1":
            from .regime_t69a_bridge import TRANSIENT_BOOK_REASONS as T69A_TRANSIENT
            retryable |= T69A_TRANSIENT
        expires = ready.execution.expires_at_ms
        for attempt in range(attempts):
            now = self._now_ms()
            if now >= expires:
                return C180Ready(False, "execution_expired_during_book_wait")
            checked = bridge.check_signal(market=campaign.market,
                unit_usdt=self._selected_order_unit_usdt, at_ms=now,
                last_seen_book_at_ms=ready.book_at_ms)
            if checked.allowed or checked.reason not in retryable or attempt == attempts-1:
                return checked
            await asyncio.sleep(min(.1, max(0, (expires-self._now_ms())/1000)))
        return checked

    async def _observation_repository(self):
        # Never share the execution transaction connection: its commit/rollback
        # boundary belongs exclusively to order/fill/accounting transitions.
        repo = getattr(self,'_observability_repository',None)
        if repo is None:
            import aiosqlite
            repo=PredictionRepository(self.repository.db_path)
            repo._operation_gate = self.repository._operation_gate
            repo._conn=await aiosqlite.connect('file:'+str(repo.db_path)+'?mode=rw',uri=True,timeout=.1,isolation_level=None)
            repo._conn.row_factory=aiosqlite.Row
            await repo._conn.execute('PRAGMA busy_timeout=100')
            self._observability_repository=repo
        return repo

    async def _flush_observability(self):
        async with self._observability_flush_lock:
            try:
                observer_repo=await self._observation_repository()
            except Exception:
                return
            if self._observability_dropped:
                count,self._observability_dropped = self._observability_dropped,0
                self._trace_event('dropped',count=count)
            # One small telemetry transaction, outside the selected-entry path.
            batch = list(self._observability_events[:32])
            try:
                await observer_repo.record_execution_timings(batch)
            except Exception:
                return  # Retain events until a later flush succeeds.
            del self._observability_events[:len(batch)]

    def _observe_campaign(self, campaign):
        if not self._s3s5_profile_active():
            return
        self._observability_campaigns[campaign.campaign_id] = campaign
        if self._observability_task is None or self._observability_task.done():
            self._observability_task = asyncio.create_task(self._observe_quotes())

    async def _observe_quotes(self):
        # Samples the already-selected websocket only. Never requests REST or
        # changes the market subscription, strategy state, pending order or TTL.
        try:
            while self._observability_campaigns:
                now=self._now_ms()
                for cid,c in list(self._observability_campaigns.items()):
                    if now >= c.market.end_time_ms:
                        self._trace_event('collector_window_ended',campaign_id=cid,end_ms=c.market.end_time_ms)
                        self._observability_campaigns.pop(cid,None)
                        continue
                    feeds=getattr(self,'_s3s5_feeds',None)
                    if feeds is None or getattr(feeds,'_closed',False):
                        self._trace_event('collector_unavailable',campaign_id=cid,reason='feed_not_ready')
                        continue
                    try:
                        q=feeds.snapshot(c.market,now)
                        # Keep invalid snapshots and original clocks for gap diagnosis.
                        quote=QuoteSnapshot.from_books(observed_at_ms=now,
                            up_bid=as_decimal(q['UP']['bid']) if q['UP']['bid'] is not None else None,
                            up_ask=as_decimal(q['UP']['ask']) if q['UP']['ask'] is not None else None,
                            down_bid=as_decimal(q['DOWN']['bid']) if q['DOWN']['bid'] is not None else None,
                            down_ask=as_decimal(q['DOWN']['ask']) if q['DOWN']['ask'] is not None else None,
                            btc_spot=as_decimal(q['spot']) if q.get('spot') is not None else None,
                            reference_price=c.market.reference_price,spot_observed_at_ms=int(q.get('spot_at_ms') or 0),
                            feed_ok=bool(q.get('feed_ok')) and bool(reversal5_valid(q,now)),
                            raw={'reversal5':q,'observer_only':True})
                        started=time.monotonic_ns()
                        observer_repo=await self._observation_repository()
                        await observer_repo.save_quote(cid,quote)
                        self._trace_event('quote_persisted',campaign_id=cid,observed_at_ms=now,
                            duration_ns=time.monotonic_ns()-started,feed_ok=quote.feed_ok,
                            pending=bool(c.pending_intent_id or c.pending_unknown))
                    except Exception as exc:
                        self._trace_event('collector_error',campaign_id=cid,error_type=type(exc).__name__)
                await self._flush_observability()
                await asyncio.sleep(1)
        finally:
            await self._flush_observability()

    async def _call_api(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        entry_deadline = kwargs.pop('_entry_deadline_ms', None)
        entry_buy = bool(kwargs.pop('_entry_buy', False)) or entry_deadline is not None
        if method_name in SIGNED_MUTATING_ENDPOINTS and not self.live_capability:
            if entry_buy:
                raise PredictionEntryNotSubmitted("live_capability_revoked_before_http")
            raise RuntimeError(f"{method_name} is blocked while effective mode is {self.mode}")
        trace_cid = kwargs.pop('_trace_campaign_id', None)
        if method_name in ("query_active_orders", "query_positions"):
            trace_cid = trace_cid or getattr(self, "_entry_exposure_campaign_id", None)
        trace_iid = kwargs.pop('_trace_intent_id', None)
        fallback_statuses = frozenset(kwargs.pop('_fallback_statuses', ()))
        trace_attempt = getattr(self, "_entry_attempts", {}).get(trace_cid)
        trace_fields = {"attempt_id": trace_attempt["id"]} if trace_attempt else {}
        entry_book_at = kwargs.pop('_entry_book_at_ms', None)
        entry_loop = kwargs.pop('_entry_loop_id', getattr(self, "_loop_id", None))
        entry_profile = kwargs.pop('_entry_profile', getattr(self, "_selected_strategy_profile", None))
        book_max_age_ms = 1000 if entry_profile in {
            "regime_target6_3_v1", "regime_target6_3a_v1", "regime_target6_3b_v1",
            "regime_target6_5_v1", "regime_target6_7_v1", "regime_target6_7a_v1", "regime_target6_7b_v1", "regime_target6_7c_v1", "regime_target6_7d_v1", "regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1", "regime_target6_9a_v1"} else 2000
        method = getattr(self.client, method_name)
        emergency = bool(kwargs.pop("_emergency", False))
        management = bool(kwargs.pop("_management", False))
        shared_prepaid = bool(kwargs.pop("_shared_pre_acquired", False))
        weight_pre_acquired = bool(kwargs.pop("_weight_pre_acquired", False))
        # Weight is deliberately conservative for the endpoints that can be
        # called from a tight management loop.  A single process-wide budget
        # prevents discovery/detail/quote storms across campaigns.
        weight = self._endpoint_weight(method_name)
        allowed = (self._rate_limiter.can_send_prepaid() if weight_pre_acquired else
                   self._rate_limiter.acquire(weight, block=False, emergency=emergency, management=management))
        if not allowed:
            health = self._rate_limiter.health()
            self._rate_limiter.note_deferred(weight, error=f"{method_name} deferred by local weight budget")
            raise PredictionRateLimitDeferred(method_name, health.as_dict())
        pre_http_spans = []
        trace_start = time.monotonic_ns()
        self._trace_event('api_start',campaign_id=trace_cid,method=method_name,intent_id=trace_iid,**trace_fields)
        try:
            loop = asyncio.get_running_loop()
            def entry_guard():
                if not entry_buy:
                    return
                if (not self.live_capability or not self._allow_new_buys or self._hard_stop_latched
                        or self._loop_id != entry_loop or self._selected_strategy_profile != entry_profile):
                    raise PredictionEntryNotSubmitted("entry_control_changed_before_http")
                clock_now = self._now_ms()
                if entry_deadline is None:
                    return
                if clock_now >= entry_deadline:
                    raise PredictionEntryNotSubmitted(
                        f"entry_deadline_expired_before_http late_ms={clock_now-entry_deadline}")
                if entry_book_at is None or not 0 <= clock_now-entry_book_at <= book_max_age_ms:
                    age = None if entry_book_at is None else clock_now-entry_book_at
                    raise PredictionEntryNotSubmitted(
                        f"entry_book_stale_before_http book_age_ms={age} max_ms={book_max_age_ms}")
            def admission_guard():
                entry_guard()
                if entry_buy:
                    with request_stage("durable_buy_admission"):
                        self._entry_durable_http_guard(entry_loop, entry_profile)
                    entry_guard()  # Recheck controls/deadline after the durable read.
            def http_guard():
                admission_guard()
                cooldown_guard()
                if entry_buy:
                    # Capture the actual boundary time, not later telemetry flush.
                    at_ms, stamp = self._now_ms(), time.monotonic_ns()
                    loop.call_soon_threadsafe(lambda: self._entry_http_started(trace_cid, trace_iid, at_ms, stamp))
            def cooldown_guard():
                if not self._rate_limiter.can_send_prepaid():
                    raise PredictionRateLimitDeferred(method_name, self._rate_limiter.health().as_dict())
            def invoke():
                callback = lambda stage, ns: pre_http_spans.append((stage, ns))
                with request_timing_scope(callback if entry_buy else None):
                    admission_guard()  # Also guards injected/custom clients before invocation.
                    cooldown_guard()
                    with request_admission_guard_scope(cooldown_guard), entry_http_guard_scope(http_guard if entry_buy else None):
                        return method(*args, **kwargs)
            with request_budget_scope("exit" if emergency else ("management" if management else "normal"), shared_prepaid):
                result = await asyncio.to_thread(invoke)
        except SharedBudgetDeferred as exc:
            self._trace_event('api_deferred', campaign_id=trace_cid, method=method_name,
                              intent_id=trace_iid, duration_ns=time.monotonic_ns()-trace_start,
                              error_type='SharedBudgetDeferred', **trace_fields)
            raise PredictionRateLimitDeferred(method_name, exc.health) from exc
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            # A local pre-send refusal and an expected history-filter fallback
            # are not exchange API failures; keep api_error for real ones.
            if isinstance(exc, PredictionEntryNotSubmitted):
                event, extra = 'entry_not_submitted', {"reason": str(exc)}
            elif status is not None and status in fallback_statuses:
                event, extra = 'history_fallback', {"status_code": status}
            else:
                event, extra = 'api_error', {"status_code": status}
            self._trace_event(event,campaign_id=trace_cid,method=method_name,intent_id=trace_iid,duration_ns=time.monotonic_ns()-trace_start,error_type=type(exc).__name__,**extra,**trace_fields)
            if status in (418, 429):
                self._rate_limiter.note_response(status, getattr(exc, "headers", None), error=str(exc))
            elif "429" in str(exc) or "rate limit" in str(exc).lower():
                self._rate_limiter.note_rate_limit(error=str(exc))
            raise
        finally:
            # Flush only after the blocking client has returned; no telemetry
            # persistence or event-loop callback delays the HTTP boundary.
            for stage, ns in pre_http_spans:
                self._trace_event('entry_pre_http_stage', campaign_id=trace_cid,
                    intent_id=trace_iid, stage=stage, duration_ns=ns, **trace_fields)
        self._trace_event('api_ack',campaign_id=trace_cid,method=method_name,intent_id=trace_iid,duration_ns=time.monotonic_ns()-trace_start,**trace_fields)
        self._rate_limiter.note_success()
        now = self._now_ms()
        self.heartbeat.last_api_ok_at_ms = now
        if str(self.heartbeat.last_error or "").startswith("READ_TIMESTAMP_DEFERRED:"):
            self.heartbeat.last_error = None
        return result

    @staticmethod
    def _endpoint_weight(method_name: str) -> int:
        # Binance Prediction catalog, audited 2026-09-15 (Trade/Market/Position).
        # Keep an explicit list; unreviewed methods retain a conservative cost.
        return {
            name: 1 for name in (
                "list_prediction_markets", "get_market_detail", "query_order_book",
                "get_quote", "place_order", "batch_cancel_orders", "batch_redeem",
                "get_redeem_status", "get_position_by_token", "query_positions",
                "query_settled_position_history", "query_pnl", "query_order_history",
                "query_active_orders",
            )
        }.get(method_name, 10)

    async def _persist_heartbeat(self) -> None:
        now = self._now_ms()
        self.heartbeat.last_db_write_at_ms = now
        await self.repository.set_runtime_config("prediction_heartbeat", self.heartbeat.as_dict())
        tight_window = any(not c.pending_intent_id and not c.position.has_any
                           and (int(c.market.start_time_ms)+123000 <= now < int(c.market.start_time_ms)+126000
                                or (self._selected_strategy_profile in ("regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1")
                                    and not c.buy_count and not c.initial_attempts
                                    and int(c.market.start_time_ms)+179000 <= now < int(c.market.start_time_ms)+183500))
                           for c in self._active_campaigns.values())
        if hasattr(self,'_observability_flush_lock') and not tight_window and not getattr(self, "_entry_attempts", {}):
            await self._flush_observability()

    def _status(self) -> dict[str, Any]:
        task_running = self._task is not None and not self._task.done()
        observer_running = self._shadow_observer_task is not None and not self._shadow_observer_task.done()
        gate_snapshot = getattr(self, "_regime_entry_gate_snapshot", None)
        gate_snapshot = dict(gate_snapshot) if isinstance(gate_snapshot, Mapping) else None
        return {
            "state": (
                "cancelling"
                if task_running and self._cancel_requested
                else ("running" if task_running else ("hard_stop" if self._hard_stop_latched else "idle"))
            ),
            "mode": self.mode,
            "effective_mode": self.mode,
            "market_symbol": str(getattr(self.settings, "market_symbol", "BTCUSDT") or "BTCUSDT").upper(),
            "shared_request_budget": self.client.request_budget.health() if getattr(self.client, "request_budget", None) else None,
            "requested_live": bool(getattr(self.settings, "is_live_requested", False)),
            "live_armed": self.live_capability,
            "strategy_profile": self._selected_strategy_profile,
            "wallet_address": getattr(self.settings, "wallet_address", None),
            "wallet_id": getattr(self.settings, "wallet_id", None),
            "account_type": getattr(self.settings, "account_type", "SPOT"),
            "funding_source": getattr(self.settings, "funding_source", "MPC"),
            "balance_account_type": self._balance_account_type(self.settings),
            "config_hash": self.effective_config_hash,
            "sas_verified": bool(getattr(self.settings, "sas_verified", False)),
            "order_unit_usdt": str(self._selected_order_unit_usdt),
            "initial_order_usdt": str(getattr(self.strategy.config, "max_buy_usdt", self._selected_order_unit_usdt)),
            "market_buy_cap_usdt": str(getattr(self.strategy.config, "max_market_buy_usdt", self._selected_order_unit_usdt)),
            "scale_in_enabled": bool(getattr(self.strategy.config, "pnl_scale_in_enabled", False)),
            "required_balance_usdt": str(getattr(self.settings, "required_balance_usdt", "2")),
            "configured_live_enabled": bool(getattr(self.settings, "live_enabled", False)),
            "worker_available": True,
            "orders_enabled": self.orders_enabled,
            "live_capability": self.live_capability,
            "fail_closed": bool(getattr(self.settings, "is_live_requested", False) and not self.live_capability),
            "accept_new_markets": self._accept_new_markets,
            "allow_new_orders": self._allow_new_orders,
            "allow_new_buys": self._allow_new_buys,
            "allow_reductions": self._allow_reductions,
            "loop_cancel_requested": self._cancel_requested,
            "target_markets": self._target_markets,
            "loop_loss_limit": str(getattr(getattr(self.risk_engine, "config", None), "loop_loss_limit", "-2")),
            "daily_loss_limit": str(getattr(getattr(self.risk_engine, "config", None), "daily_loss_limit", "-2")),
            "loop_loss_limit_reached": self._loop_loss_limit_reached,
            "active_campaigns": len(self._active_campaigns),
            "heartbeat": self.heartbeat.as_dict(),
            "shadow_reasons": list(self._shadow_reasons),
            "hard_stop_latched": self._hard_stop_latched,
            "rate_limit": self._rate_limiter.health().as_dict(),
            "shadow_observer_running": observer_running,
            "shadow_observer_last_tick_at_ms": self._shadow_observer_last_tick_at_ms or None,
            "shadow_observer_last_error": self._shadow_observer_last_error,
            "regime_entry_gate": gate_snapshot,
            "regime_entry_gate_snapshot": dict(gate_snapshot) if gate_snapshot is not None else None,
        }

    def pending_pnl_status(self, confirmed_loop_pnl: Any = "0") -> dict[str, Any]:
        """Return a conservative, non-booked view of active campaign PnL.

        Confirmed accounting remains settlement-only.  This snapshot exists
        for operator visibility while Binance has not yet exposed a terminal
        position: it never writes a settlement or changes the durable risk
        ledger.  ``_run_market_once`` already blocks admission while any
        active campaign remains, so ``pending_settlement_hold`` describes an
        enforced safety boundary rather than a cosmetic Telegram flag.
        """

        now = self._now_ms()
        active = [
            campaign
            for campaign in self._active_campaigns.values()
            if campaign.state not in {CampaignState.DONE, CampaignState.CANCELLED}
        ]
        positioned = [campaign for campaign in active if campaign.position.has_any]
        pending_worst_case = sum(
            (calculate_pnl(campaign).worst_case for campaign in positioned),
            Decimal("0"),
        )
        ended = [campaign for campaign in active if now >= int(campaign.market.end_time_ms)]
        wait_seconds = max(
            (max(0, now - int(campaign.market.end_time_ms)) // 1000 for campaign in ended),
            default=0,
        )
        confirmed = as_decimal(confirmed_loop_pnl)
        return {
            "confirmed_loop_pnl": str(confirmed),
            "pending_position_count": len(positioned),
            "pending_settlement_count": len(ended),
            "pending_estimated_pnl": str(pending_worst_case),
            "loop_pnl_with_pending": str(confirmed + pending_worst_case),
            "pending_settlement_wait_seconds": int(wait_seconds),
            "pending_settlement_hold": bool(active),
        }

    @staticmethod
    def _enrich_lane_readiness(
        monitor: Mapping[str, Any],
        regime: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Attach advisory READY/WATCH/AVOID states to V3–V5 evidence."""

        enriched = dict(monitor)
        regime_status = str((regime or {}).get("status") or "WAIT_DATA").upper()
        lanes: list[dict[str, Any]] = []
        for raw in monitor.get("lanes", ()) if isinstance(monitor.get("lanes"), (list, tuple)) else ():
            if not isinstance(raw, Mapping):
                continue
            item = dict(raw)
            profile = str(item.get("profile") or "").strip().lower()
            floor = REGIME_SENSITIVE_LANE_BREAK_EVEN_WR.get(profile)
            if floor is None:
                item.update(
                    {
                        "readiness": "OBSERVE",
                        "readiness_reason": "research lane; no live threshold approved",
                        "required_win_rate": item.get("break_even_win_rate"),
                    }
                )
                lanes.append(item)
                continue
            try:
                dynamic_break_even = (
                    Decimal(str(item.get("break_even_win_rate")))
                    if item.get("break_even_win_rate") not in (None, "")
                    else None
                )
                required = max(floor, dynamic_break_even or floor)
                win_rate = (
                    Decimal(str(item.get("win_rate")))
                    if item.get("win_rate") not in (None, "")
                    else None
                )
                pnl = Decimal(str(item.get("pnl_usdt") or "0"))
                runs = int(item.get("runs") or 0)
                decisive = int(item.get("wins") or 0) + int(item.get("losses") or 0)
            except (ArithmeticError, TypeError, ValueError):
                required = floor
                win_rate = None
                pnl = Decimal("0")
                runs = decisive = 0
            target = min(Decimal("99"), required + LANE_READINESS_SAFETY_MARGIN_WR)
            if regime_status == "RED":
                readiness, reason = "AVOID", "whipsaw market alert is red"
            elif runs < 20 or decisive < 5 or win_rate is None:
                readiness, reason = "WAIT_DATA", "recent 20-run evidence is incomplete"
            elif pnl <= 0:
                readiness, reason = "AVOID", "recent PnL is not positive"
            elif win_rate < required:
                readiness, reason = "AVOID", "win rate is below break-even"
            elif regime_status in {"YELLOW", "WAIT_DATA"}:
                readiness, reason = "WATCH", "market regime is not green"
            elif win_rate < target:
                readiness, reason = "WATCH", "win rate lacks the 3-point safety margin"
            else:
                readiness, reason = "READY", "market and lane evidence pass"
            item.update(
                {
                    "readiness": readiness,
                    "readiness_reason": reason,
                    "required_win_rate": str(required.quantize(Decimal("0.01"))),
                    "target_win_rate": str(target.quantize(Decimal("0.01"))),
                }
            )
            lanes.append(item)
        enriched["lanes"] = lanes
        enriched["market_status"] = regime_status
        enriched["advisory_only"] = True
        return enriched

    async def _risk_snapshot(self, campaign: Campaign | None = None) -> RiskSnapshot:
        state = await self.repository.get_runtime_config("prediction_risk_state", {})
        state = state if isinstance(state, Mapping) else {}
        taipei = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
        persisted_day = str(state.get("day") or taipei)
        hard_stop = bool(state.get("hard_stop_latched")) if persisted_day == taipei else False
        daily = await self.repository.get_daily_pnl(taipei)
        if (
            str(state.get("hard_stop_reset_day") or "") == taipei
            and int(state.get("hard_stop_reset_count") or 0) > 0
        ):
            daily -= as_decimal(state.get("hard_stop_reset_baseline_pnl", "0"))
        loop_net_pnl = as_decimal(state.get("loop_net_pnl", "0"))
        get_loop_pnl = getattr(self.repository, "get_loop_pnl", None)
        if self._loop_id and callable(get_loop_pnl):
            try:
                loop_net_pnl = as_decimal(await get_loop_pnl(self._loop_id))
            except Exception:
                pass
        return RiskSnapshot(
            daily_net_pnl=daily,
            loop_net_pnl=loop_net_pnl,
            consecutive_losses=int(state.get("consecutive_losses", 0)),
            order_attempts=campaign.order_attempts if campaign else int(state.get("order_attempts", 0)),
            buy_count=campaign.buy_count if campaign else int(state.get("buy_count", 0)),
            hard_stop_latched=hard_stop,
            soft_cooldown_until_ms=(int(state["soft_cooldown_until_ms"]) if state.get("soft_cooldown_until_ms") is not None else None),
            campaign_id=campaign.campaign_id if campaign else None,
        )

    async def _persist_risk_state(self, snapshot: RiskSnapshot) -> None:
        day = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
        payload = {"day": day, "daily_net_pnl": str(snapshot.daily_net_pnl),
                   "loop_net_pnl": str(snapshot.loop_net_pnl),
                   "consecutive_losses": snapshot.consecutive_losses,
                   "order_attempts": snapshot.order_attempts, "buy_count": snapshot.buy_count,
                   "hard_stop_latched": bool(snapshot.hard_stop_latched),
                   "soft_cooldown_until_ms": snapshot.soft_cooldown_until_ms}
        saver = getattr(self.repository, "save_risk_snapshot", None)
        if callable(saver):
            await saver(payload)
            return
        existing = await self.repository.get_runtime_config("prediction_risk_state", {})
        existing = existing if isinstance(existing, Mapping) else {}
        for key in ("hard_stop_reset_day", "hard_stop_reset_at_ms", "hard_stop_reset_count",
                    "hard_stop_reset_baseline_pnl", "raw_daily_net_pnl"):
            if key in existing:
                payload[key] = existing[key]
        if existing.get("day") == day and existing.get("hard_stop_latched"):
            payload["hard_stop_latched"] = True
        await self.repository.set_runtime_config("prediction_risk_state", payload)

    async def status(self) -> dict[str, Any]:
        await self.restore_order_unit()
        await self.restore_selected_strategy()
        await self.restore_loop_market()
        result = self._status()
        pending_market = await self.repository.get_runtime_config("prediction_pending_market", {})
        result["next_market_symbol"] = pending_market.get("symbol") or self.settings.market_symbol
        result.update(await self._lane_mask_labels())
        try:
            result["next_lane_mask"] = list(await self._pending_lane_mask())
        except ValueError:
            pass
        if self._selected_strategy_profile in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            from src.gridbot.prediction.regime_lane import STATE_KEY
            result["regime_lane_risk"] = await self.repository.get_runtime_config(STATE_KEY, None)
        # Telegram status must use the durable loop cursor. The process-local
        # heartbeat can lag after a restart and, before this read-through,
        # the formatter had no top-level completed/target fields to render.
        loop = await self.repository.get_active_loop()
        if loop:
            loop_id = str(loop.get("loop_id") or "")
            loop_state = str(loop.get("state") or "")
            loop_strategy = str(loop.get("strategy_profile") or self._selected_strategy_profile).strip().lower()
            result.update(
                {
                    "loop_id": loop_id or None,
                    "loop_state": loop_state,
                    "loop_active": loop_state.upper() == "RUNNING",
                    "target_markets": int(loop.get("target") or result.get("target_markets") or 0),
                    "markets_completed": int(loop.get("completed") or 0),
                    "loop_net_pnl": str(loop.get("net_pnl") or "0"),
                    "strategy_profile": loop_strategy,
                    "loop_strategy_profile": loop_strategy,
                    "loop_terminal_reason": str(loop.get("terminal_reason") or ""),
                    "loop_new_entries_stopped": bool(loop.get("new_entries_stopped")),
                }
            )
            get_loop_pnl = getattr(self.repository, "get_loop_pnl", None)
            if callable(get_loop_pnl) and loop_id:
                try:
                    result["loop_net_pnl"] = str(await get_loop_pnl(loop_id))
                except Exception:
                    pass
            await self._attach_loop_risk_status(result, loop_id)
            if loop_strategy in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
                from src.gridbot.prediction.regime_lane import STATE_KEY
                regime_risk = await self.repository.get_runtime_config(STATE_KEY, None)
                result["regime_lane_risk"] = regime_risk
                result["order_unit_usdt"] = (str(self._selected_order_unit_usdt)
                                             if loop_strategy in ("regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1') else "1")
            if loop_strategy == "c180_favorite_hold_v1":
                try:
                    gate_state = await self.repository.get_runtime_config(
                        "c180_batch_gate_runtime_v1", None,
                    )
                    if isinstance(gate_state, Mapping) and gate_state.get("loop_id") == loop_id:
                        result["c180_policy_version"] = str(gate_state.get("policy_version") or "1.0")
                        result["c180_loop_loss_latched"] = bool(gate_state.get("loop_loss_latched"))
                except Exception:
                    result["c180_policy_version"] = "unknown"
            result["state"] = "running" if loop_state.upper() == "RUNNING" else result["state"]
        else:
            # A completed loop is history, not the current loop.  Older code
            # fell back to ``self._loop_id`` here and therefore kept showing
            # the previous DONE 10/10 as "current" until another process
            # restart or loop was created.
            result.update(
                {
                    "loop_id": None,
                    "loop_state": "IDLE",
                    "loop_active": False,
                    "target_markets": 0,
                    "markets_completed": 0,
                    "loop_net_pnl": "0",
                }
            )
            latest_loop = None

        if getattr(self, "_jev_gate", None):
            has_active_loop = bool(loop and str(loop.get("state") or "").upper() == "RUNNING")
            self._jev_gate.sync_daemon_state(has_active_loop)
            get_latest_loop = getattr(self.repository, "get_latest_loop", None)
            if callable(get_latest_loop):
                latest_loop = await get_latest_loop()
            elif self._loop_id:
                latest_loop = await self.repository.get_loop(self._loop_id)
            if latest_loop:
                last_loop_id = str(latest_loop.get("loop_id") or "")
                last_loop_pnl = str(latest_loop.get("net_pnl") or "0")
                get_loop_pnl = getattr(self.repository, "get_loop_pnl", None)
                if callable(get_loop_pnl) and last_loop_id:
                    try:
                        last_loop_pnl = str(await get_loop_pnl(last_loop_id))
                    except Exception:
                        pass
                result.update(
                    {
                        "last_loop_id": last_loop_id or None,
                        "last_loop_state": str(latest_loop.get("state") or ""),
                        "last_loop_target": int(latest_loop.get("target") or 0),
                        "last_loop_completed": int(latest_loop.get("completed") or 0),
                        "last_loop_net_pnl": last_loop_pnl,
                    }
                )
        # Reuse the exact same settled-trade statistics as /predict_loop_pnl
        # so Telegram status cannot drift to a different WR denominator.
        performance_loop_id = str(result.get("loop_id") or result.get("last_loop_id") or "")
        get_loop_summary = getattr(self.repository, "get_loop_pnl_summary", None)
        if performance_loop_id and callable(get_loop_summary):
            try:
                summary = await get_loop_summary()
                records = summary.get("loops", ()) if isinstance(summary, Mapping) else ()
                performance = next(
                    (
                        item
                        for item in records
                        if isinstance(item, Mapping)
                        and str(item.get("loop_id") or "") == performance_loop_id
                    ),
                    None,
                )
                if isinstance(performance, Mapping):
                    prefix = "loop_" if result.get("loop_active") else "last_loop_"
                    result.update(
                        {
                            f"{prefix}wins": int(performance.get("wins") or 0),
                            f"{prefix}losses": int(performance.get("losses") or 0),
                            f"{prefix}breakevens": int(performance.get("breakevens") or 0),
                            f"{prefix}no_trades": int(performance.get("no_trades") or 0),
                            f"{prefix}win_rate": performance.get("win_rate"),
                        }
                    )
            except Exception:
                # Status remains available even if the optional performance
                # summary cannot be read; PnL/risk fields still fail safe.
                pass
        regime_monitor: Mapping[str, Any] = {}
        get_market_regime_monitor = getattr(self.repository, "get_market_regime_monitor", None)
        if callable(get_market_regime_monitor):
            try:
                regime_monitor = await get_market_regime_monitor(limit=20)
                result["market_regime_monitor"] = regime_monitor
            except Exception as exc:  # noqa: BLE001 - status is best-effort telemetry
                result["market_regime_monitor_error"] = type(exc).__name__
        get_observer_status = getattr(self.repository, "get_shadow_observer_status", None)
        if callable(get_observer_status):
            try:
                observer_status = await get_observer_status()
                observer_status = dict(observer_status) if isinstance(observer_status, Mapping) else {}
                observer_status.update(
                    {
                        "running": self._shadow_observer_task is not None and not self._shadow_observer_task.done(),
                        "last_tick_at_ms": self._shadow_observer_last_tick_at_ms or None,
                        "runtime_error": self._shadow_observer_last_error,
                    }
                )
                result["shadow_observer"] = observer_status
            except Exception as exc:  # noqa: BLE001 - status is best-effort telemetry
                result["shadow_observer_error"] = type(exc).__name__
        get_lane_wr_monitor = getattr(self.repository, "get_lane_wr_monitor", None)
        if callable(get_lane_wr_monitor):
            monitor_profiles = tuple(
                dict.fromkeys(
                    (
                        self._selected_strategy_profile,
                        *self._shadow_lane_strategies.keys(),
                    )
                )
            )
            shadow_hashes = {
                profile: str(
                    self._shadow_lane_config_hashes.get(profile)
                    or self._shadow_lane_config_hash(profile)
                )
                for profile in self._shadow_lane_strategies
            }
            try:
                lane_monitor = await get_lane_wr_monitor(
                    active_profile=self._selected_strategy_profile,
                    profiles=monitor_profiles,
                    shadow_config_hashes=shadow_hashes,
                    order_unit_usdt=self._selected_order_unit_usdt,
                    limit=20,
                )
                result["lane_wr_monitor"] = self._enrich_lane_readiness(
                    lane_monitor,
                    regime_monitor,
                )
            except Exception as exc:  # noqa: BLE001 - status is best-effort telemetry
                result["lane_wr_monitor_error"] = type(exc).__name__
        try:
            jump_stop = await self.repository.get_runtime_config(
                "prediction_adaptive_jump_stop",
                {},
            )
            if isinstance(jump_stop, Mapping):
                active_loop_id = str(result.get("loop_id") or "")
                jump_loop_id = str(jump_stop.get("loop_id") or "")
                jump_active = bool(jump_stop.get("active")) and bool(active_loop_id) and jump_loop_id == active_loop_id
                result["adaptive_jump_stop"] = dict(jump_stop)
                result["adaptive_jump_stop_active"] = jump_active
        except Exception as exc:  # noqa: BLE001 - status remains best effort
            result["adaptive_jump_stop_error"] = type(exc).__name__
        heartbeat = result.get("heartbeat")
        if isinstance(heartbeat, Mapping):
            result["markets_seen"] = int(heartbeat.get("markets_seen") or 0)
            result.setdefault("last_error", heartbeat.get("last_error"))
        pending_strategy = await self._load_pending_strategy()
        result["next_strategy_profile"] = pending_strategy or str(result.get("strategy_profile") or "")
        # Restore Telegram P3 arm override if persisted
        try:
            if self._fav_p3_arm_override is None:
                stored = await self.repository.get_runtime_config("prediction_fav_p3_arm", {})
                if isinstance(stored, Mapping) and stored.get("arm") is not None:
                    self._fav_p3_arm_override = self._normalize_fav_p3_arm(stored.get("arm"))
        except Exception:
            pass
        result["fav_p3_arm"] = self._fav_p3_arm()
        result["fav_p3_live_orders"] = self._is_fav_p3_profile() or self._fav_p3_live_orders_enabled()
        result["fav_p3_lane"] = self._is_fav_p3_profile()
        if self._is_fav_p3_profile():
            # Status must read as P3 lane (FAV+gates, no R3), not s3s5 footnote.
            result["strategy_display"] = "fav_p3"
        if getattr(self, "_jev_gate", None):
            result["jev_gate_enabled"] = self._jev_gate.enabled
            result["jev_gate_reject_count"] = self._jev_gate.reject_count
            result["jev_gate_allow_count"] = self._jev_gate.allow_count
        pending_order_unit = await self._load_pending_order_unit()
        result["next_order_unit_usdt"] = str(pending_order_unit or self._selected_order_unit_usdt)
        result.update(self.pending_pnl_status(result.get("loop_net_pnl", "0")))
        return result

    async def select_strategy(self, profile: str) -> dict[str, Any]:
        """Select one operator-approved strategy for the next run.

        An active loop keeps its immutable strategy provenance.  A tap while
        that loop is running is persisted as the next-loop strategy instead
        of being rejected.  If the operator already stopped the loop and
        reconciliation proves zero exposure, the stopped cursor is closed and
        the new strategy becomes active immediately for a fresh loop.
        """

        await self.restore_order_unit()
        await self.restore_selected_strategy()
        selected = str(profile or "").strip().lower()
        if selected not in self._selectable_strategy_profiles():
            return {**self._status(), "action_denied": True, "reason": "strategy is not selectable"}
        try:
            StrategyConfig.for_profile(selected)
        except ValueError:
            return {**self._status(), "action_denied": True, "reason": "strategy is unavailable"}

        existing = await self.repository.get_active_loop()
        task = getattr(self, "_task", None)
        task_running = task is not None and not task.done()
        if existing:
            loop_id = str(existing.get("loop_id") or "")
            loop_strategy = str(existing.get("strategy_profile") or self._selected_strategy_profile).strip().lower()
            if selected == loop_strategy:
                self._pending_strategy_profile = None
                await self.repository.set_runtime_config(
                    "prediction_pending_strategy",
                    {"profile": None, "cleared_at_ms": self._now_ms()},
                )
                return {
                    **self._status(),
                    "strategy_profile": loop_strategy,
                    "next_strategy_profile": loop_strategy,
                    "strategy_changed": False,
                    "strategy_queued": False,
                    "reason": "strategy is already selected",
                }

            operator_stopped = bool(existing.get("new_entries_stopped")) and str(
                existing.get("terminal_reason") or ""
            ).upper() == "OPERATOR_STOP"
            if operator_stopped and not task_running:
                reconciliation = await self.reconcile()
                unresolved = await self.repository.load_unresolved_intents()
                active_campaigns = [
                    campaign
                    for campaign in self._active_campaigns.values()
                    if campaign.state not in {CampaignState.DONE, CampaignState.CANCELLED}
                ]
                if (
                    bool(reconciliation.get("known"))
                    and int(reconciliation.get("orders") or 0) == 0
                    and not unresolved
                    and not active_campaigns
                ):
                    closer = getattr(self.repository, "close_operator_stopped_loop_for_strategy_switch", None)
                    if callable(closer):
                        closed = await closer(loop_id)
                    else:
                        closed = {"closed": True, "row": await self.repository.stop_loop(loop_id, state="STOPPED")}
                    if bool(closed.get("closed")):
                        previous_completed = int(existing.get("completed") or 0)
                        previous_target = int(existing.get("target") or 0)
                        self._loop_id = None
                        self._target_markets = 0
                        live_rearm_required = await self._activate_strategy(selected)
                        return {
                            **self._status(),
                            "strategy_profile": selected,
                            "next_strategy_profile": selected,
                            "strategy_changed": True,
                            "strategy_queued": False,
                            "live_rearm_required": live_rearm_required,
                            "previous_loop_closed": True,
                            "previous_loop_id": loop_id,
                            "previous_loop_completed": previous_completed,
                            "previous_loop_target": previous_target,
                            "reconciliation": reconciliation,
                        }

        if existing or task_running:
            now = self._now_ms()
            self._pending_strategy_profile = selected
            await self.repository.set_runtime_config(
                "prediction_pending_strategy",
                {
                    "profile": selected,
                    "at_ms": now,
                    "after_loop_id": str(existing.get("loop_id") or "") if existing else None,
                },
            )
            current_strategy = str(
                (existing or {}).get("strategy_profile") or self._selected_strategy_profile
            ).strip().lower()
            return {
                **self._status(),
                "strategy_profile": current_strategy,
                "next_strategy_profile": selected,
                "strategy_changed": False,
                "strategy_queued": True,
                "live_rearm_required": bool(
                    getattr(getattr(self, "settings", None), "is_live_requested", False)
                ),
                "reason": "strategy queued for the next loop",
            }

        previous_profile = str(getattr(self, "_selected_strategy_profile", "") or "").strip().lower()
        live_rearm_required = await self._activate_strategy(selected)
        return {
            **self._status(),
            "strategy_profile": selected,
            "next_strategy_profile": selected,
            "strategy_changed": previous_profile != selected,
            "strategy_queued": False,
            "live_rearm_required": live_rearm_required,
        }

    async def select_order_unit(self, value: Decimal | str | int) -> dict[str, Any]:
        """Select 1/2/3 USDT for the next immutable loop boundary."""

        await self.restore_order_unit()
        await self.restore_selected_strategy()
        try:
            selected = self._normalize_order_unit_usdt(value)
        except ValueError:
            return {
                **self._status(),
                "action_denied": True,
                "reason": "order unit must be exactly 1, 2 or 3 USDT",
            }

        if self._selected_strategy_profile in {"regime_target6_v1", "regime_target6_1_v1"} and selected != Decimal("1"):
            return {**self._status(), "action_denied": True,
                    "reason": "Regime T6/T6.1 固定每筆 1 USDT，不支援加碼"}

        existing = await self.repository.get_active_loop()
        task = getattr(self, "_task", None)
        task_running = task is not None and not task.done()
        current = self._selected_order_unit_usdt
        if existing and selected == current:
            self._pending_order_unit_usdt = None
            await self.repository.set_runtime_config(
                "prediction_pending_order_unit",
                {"order_unit_usdt": None, "cleared_at_ms": self._now_ms()},
            )
            return {
                **self._status(),
                "order_unit_usdt": str(current),
                "next_order_unit_usdt": str(current),
                "order_unit_changed": False,
                "order_unit_queued": False,
                "reason": "order unit is already selected",
            }

        if existing:
            loop_id = str(existing.get("loop_id") or "")
            operator_stopped = bool(existing.get("new_entries_stopped")) and str(
                existing.get("terminal_reason") or ""
            ).upper() == "OPERATOR_STOP"
            if operator_stopped and not task_running:
                reconciliation = await self.reconcile()
                unresolved = await self.repository.load_unresolved_intents()
                active_campaigns = [
                    campaign
                    for campaign in self._active_campaigns.values()
                    if campaign.state not in {CampaignState.DONE, CampaignState.CANCELLED}
                ]
                if (
                    bool(reconciliation.get("known"))
                    and int(reconciliation.get("orders") or 0) == 0
                    and not unresolved
                    and not active_campaigns
                ):
                    closer = getattr(
                        self.repository,
                        "close_operator_stopped_loop_for_strategy_switch",
                        None,
                    )
                    closed = (
                        await closer(loop_id)
                        if callable(closer)
                        else {"closed": True, "row": await self.repository.stop_loop(loop_id, state="STOPPED")}
                    )
                    if bool(closed.get("closed")):
                        previous_completed = int(existing.get("completed") or 0)
                        previous_target = int(existing.get("target") or 0)
                        self._loop_id = None
                        self._target_markets = 0
                        live_rearm_required = await self._activate_order_unit(selected)
                        return {
                            **self._status(),
                            "order_unit_usdt": str(selected),
                            "next_order_unit_usdt": str(selected),
                            "order_unit_changed": current != selected,
                            "order_unit_queued": False,
                            "live_rearm_required": live_rearm_required,
                            "previous_loop_closed": True,
                            "previous_loop_id": loop_id,
                            "previous_loop_completed": previous_completed,
                            "previous_loop_target": previous_target,
                            "reconciliation": reconciliation,
                        }

        if existing or task_running:
            now = self._now_ms()
            self._pending_order_unit_usdt = selected
            await self.repository.set_runtime_config(
                "prediction_pending_order_unit",
                {
                    "order_unit_usdt": str(selected),
                    "loop_loss_limit": str(-(selected * ORDER_UNIT_RISK_MULTIPLIER)),
                    "daily_loss_limit": str(-(selected * ORDER_UNIT_RISK_MULTIPLIER)),
                    "at_ms": now,
                    "after_loop_id": str((existing or {}).get("loop_id") or "") or None,
                },
            )
            return {
                **self._status(),
                "order_unit_usdt": str(current),
                "next_order_unit_usdt": str(selected),
                "next_loop_loss_limit": str(-(selected * ORDER_UNIT_RISK_MULTIPLIER)),
                "order_unit_changed": False,
                "order_unit_queued": True,
                "live_rearm_required": bool(getattr(self.settings, "is_live_requested", False)),
                "reason": "order unit queued for the next loop",
            }

        live_rearm_required = await self._activate_order_unit(selected)
        return {
            **self._status(),
            "order_unit_usdt": str(selected),
            "next_order_unit_usdt": str(selected),
            "order_unit_changed": current != selected,
            "order_unit_queued": False,
            "live_rearm_required": live_rearm_required,
        }

    async def start_loop(self, count: int = 10, *, expected_loop_id: str | None = None) -> dict[str, Any]:
        maximum = max(1, int(getattr(self.settings, "max_loop_limit", 50)))
        count = int(count)
        if count < 1 or count > maximum:
            return {**self._status(), "action_denied": True,
                    "reason": f"requested loop count {count} is outside allowed range 1..{maximum}",
                    "requested_loop_count": count, "max_loop_limit": maximum}
        if self._hard_stop_latched:
            return {**self._status(), "action_denied": True, "reason": "hard stop is latched"}
        persisted_risk = await self._risk_snapshot()
        if persisted_risk.hard_stop_latched:
            self._hard_stop_latched = True
            return {**self._status(), "action_denied": True, "reason": "same-day persisted hard stop is latched"}
        await self.restore_order_unit()
        await self.restore_selected_strategy()
        await self._activate_pending_strategy_if_idle()
        await self._activate_pending_order_unit_if_idle()
        await self.restore_loop_market()
        if self.settings.is_live_requested:
            await self._refresh_live_prerequisites()
        async with self._lock:
            market_error = await self._loop_market_start_guard(count)
            if market_error:
                return {**self._status(), "action_denied": True, "reason": market_error}
            if self._effective_mode is RuntimeMode.SHADOW or self._shadow_lane_experiment_enabled():
                await self._ensure_shadow_window()
            await self.reconcile()
            existing = await self.repository.get_active_loop()
            if expected_loop_id is not None and (
                not existing or str(existing.get('loop_id')) != expected_loop_id
                or existing.get('state') != 'RUNNING'
                or str(existing.get('mode') or '').upper() != self.mode.upper()
                or existing.get('strategy_profile') != self._selected_strategy_profile
                or bool(existing.get('new_entries_stopped'))
                or int(existing.get('target', 0)) != count
                or int(existing.get('completed', 0)) >= count
            ):
                return {**self._status(), 'action_denied': True,
                        'reason': 'authorized existing loop identity/state/target changed'}
            active_metadata = getattr(self.repository, "get_active_campaign_metadata", None)
            if callable(active_metadata):
                records = await active_metadata()
            else:
                records = []
            if existing:
                if bool(existing.get("new_entries_stopped")):
                    mask_error = await self._resume_lane_mask_error()
                    if mask_error:
                        return {**self._status(), "action_denied": True, "reason": mask_error}
                self._loop_id = str(existing["loop_id"])
                self._loop_created_at_ms = self._loop_origin_ms(existing)
                if bool(existing.get("new_entries_stopped")):
                    resume_loop = getattr(self.repository, "resume_operator_stopped_loop", None)
                    if not callable(resume_loop):
                        return {
                            **self._status(),
                            "action_denied": True,
                            "reason": "stopped loop cannot be resumed",
                        }
                    resumed = await resume_loop(self._loop_id)
                    if not bool(resumed.get("resumed")):
                        return {
                            **self._status(),
                            "action_denied": True,
                            "reason": str(resumed.get("reason") or "loop resume denied"),
                        }
                    existing = dict(resumed.get("row") or existing)
                foreign = [
                    row for row in records
                    if str(row.get("loop_id") or "") != self._loop_id
                ]
                if foreign:
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "active campaign requires reconciliation before starting another loop",
                        "orphan_campaigns": [str(row.get("campaign_id") or "") for row in foreign],
                    }
                count = max(count, int(existing.get("target", count)))
                # A restart may intentionally extend the active finite loop
                # (for example 50 -> 200).  Persist the larger target so the
                # durable loop cursor and the in-memory worker agree.
                # A resumed loop keeps its own bound lane mask, never the queued one.
                from .regime_t69a_lane_mask import to_text as lane_mask_text
                await self.repository.start_loop(
                    self._loop_id,
                    count,
                    mode=self.mode,
                    strategy_profile=self._selected_strategy_profile,
                    **self._loop_market_start_kwargs(lane_mask_text(await self._active_lane_mask())),
                )
                self.heartbeat.markets_completed = int(existing.get("completed", 0))
            else:
                if records:
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "orphan active campaign requires reconciliation before starting a loop",
                        "orphan_campaigns": [str(row.get("campaign_id") or "") for row in records],
                    }
                self._loop_id = f"loop:{self._now_ms()}"
                from .regime_t69a_lane_mask import to_text as lane_mask_text
                start_kwargs = self._loop_market_start_kwargs(lane_mask_text(await self._pending_lane_mask()))
                if start_kwargs:
                    # The queued mask is bound and cleared in the same transaction.
                    start_kwargs["consume_pending_lane_mask"] = True
                created = await self.repository.start_loop(
                    self._loop_id,
                    count,
                    mode=self.mode,
                    strategy_profile=self._selected_strategy_profile,
                    **start_kwargs,
                )
                self._loop_created_at_ms = self._loop_origin_ms(created, loop_id=self._loop_id)
                self._initial_market_wait_until_ms = None
                # Every finite loop owns a fresh progress cursor.  Do not let
                # the previous loop's 10/10 heartbeat leak into the new
                # Telegram acknowledgement before the worker's first tick.
                self.heartbeat.markets_completed = 0
                self.heartbeat.markets_seen = 0
                self.heartbeat.last_error = None
            if self._effective_mode is RuntimeMode.SHADOW and self._loop_id and hasattr(self.repository, "reconcile_shadow_loop_completion"):
                reconciled = await self.repository.reconcile_shadow_loop_completion(self._loop_id)
                self.heartbeat.markets_completed = int(reconciled.get("completed", self.heartbeat.markets_completed))
            self._target_markets = count
            self._loop_loss_limit_reached = False
            self._cancel_requested = False
            self._settlement_only_recovery = False
            self._accept_new_markets = True
            self._allow_new_orders = True
            self._allow_new_buys = True
            self._allow_reductions = True
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run_loop(), name="prediction-market-loop")
        # Read through the durable cursor so Telegram immediately receives
        # the newly inserted loop ID and 0/N progress.
        return await self.status()

    async def one_run(self) -> dict[str, Any]:
        """Start exactly one market cycle and let the loop auto-stop."""

        await self.restore_order_unit()
        await self.restore_selected_strategy()
        if self._hard_stop_latched:
            return {**self._status(), "action_denied": True, "reason": "hard stop is latched"}
        if self._task is not None and not self._task.done():
            return {**self._status(), "action_denied": True, "reason": "a prediction loop is already running"}
        existing = await self.repository.get_active_loop()
        if existing:
            return {**self._status(), "action_denied": True, "reason": "a persisted prediction loop is already running"}
        result = await self.start_loop(1)
        return {**result, "one_run": True, "one_run_target": 1}

    async def _ensure_shadow_window(self) -> dict[str, Any]:
        """Restore or create the immutable SHADOW provenance boundary."""

        if self._shadow_window is not None:
            return self._shadow_window
        configured_lanes = self._configured_shadow_lanes()
        config_hash = self._shadow_lane_config_hash("control") if configured_lanes else self.effective_config_hash
        if not config_hash:
            raise RuntimeError("shadow configuration hash is unavailable")
        getter = getattr(self.repository, "get_shadow_window", None)
        window: Mapping[str, Any] | None = None
        now = self._now_ms()
        configured_start = getattr(self.settings, "shadow_window_start_ms", None)
        configured_end = getattr(self.settings, "shadow_window_end_ms", None)
        requested_start = int(configured_start) if configured_start is not None else None
        requested_end = int(configured_end) if configured_end is not None else None
        if requested_start is not None and requested_end is None:
            requested_end = requested_start + 86_400_000
        if callable(getter):
            window = await getter(
                config_hash=config_hash,
                window_start_ms=requested_start,
                window_end_ms=requested_end,
            )
        if window is None:
            runtime = await self.repository.get_runtime_config("prediction_shadow_window", {})
            runtime_matches = (
                isinstance(runtime, Mapping)
                and str(runtime.get("config_hash") or "") == config_hash
                and (
                    requested_start is None
                    or int(runtime.get("window_start_ms") or 0) == requested_start
                )
                and (
                    requested_end is None
                    or int(runtime.get("window_end_ms") or 0) == requested_end
                )
            )
            if runtime_matches:
                window = runtime
        start_default = now if requested_start is None else requested_start
        start = int((window or {}).get("window_start_ms", (window or {}).get("start_ms", start_default)))
        end_default = start + 86_400_000 if requested_end is None else requested_end
        end = int((window or {}).get("window_end_ms", (window or {}).get("end_ms", end_default)))
        if callable(getattr(self.repository, "ensure_shadow_window", None)):
            window = await self.repository.ensure_shadow_window(
                config_hash=config_hash,
                window_start_ms=start,
                window_end_ms=end,
            )
        else:
            window = {"config_hash": config_hash, "window_start_ms": start, "window_end_ms": end}
        self._shadow_config_hash = config_hash
        self._shadow_window = dict(window)
        self._shadow_lane_config_hashes["control"] = config_hash
        self._shadow_lane_windows["control"] = dict(window)
        if configured_lanes:
            # All lanes observe exactly the same immutable 24-hour boundary;
            # only their strategy/config identity is separated.  This keeps
            # the A/B comparison chronological and prevents old baseline rows
            # from being mixed into the new 200-market run.
            for lane in configured_lanes:
                if lane == "control":
                    continue
                lane_hash = self._shadow_lane_config_hash(lane)
                lane_window = None
                if callable(getter):
                    lane_window = await getter(
                        config_hash=lane_hash,
                        window_start_ms=int(window.get("window_start_ms", start)),
                        window_end_ms=int(window.get("window_end_ms", end)),
                    )
                if lane_window is None:
                    lane_window = await self.repository.ensure_shadow_window(
                        config_hash=lane_hash,
                        window_start_ms=int(window.get("window_start_ms", start)),
                        window_end_ms=int(window.get("window_end_ms", end)),
                    )
                self._shadow_lane_config_hashes[lane] = lane_hash
                self._shadow_lane_windows[lane] = dict(lane_window)
        await self.repository.set_runtime_config(
            "prediction_shadow_window",
            {
                "config_hash": config_hash,
                "window_start_ms": int(window.get("window_start_ms", start)),
                "window_end_ms": int(window.get("window_end_ms", end)),
            },
        )
        if configured_lanes:
            await self.repository.set_runtime_config(
                "prediction_shadow_lanes",
                {
                    lane: {
                        "config_hash": self._shadow_lane_config_hashes.get(lane),
                        "window_start_ms": int(self._shadow_lane_windows[lane].get("window_start_ms", start)),
                        "window_end_ms": int(self._shadow_lane_windows[lane].get("window_end_ms", end)),
                    }
                    for lane in configured_lanes
                    if lane in self._shadow_lane_windows
                },
            )
        return self._shadow_window

    async def hard_stop(self, reason: str = "manual hard stop") -> dict[str, Any]:
        """Latch hard stop while retaining safe reductions/settlement."""

        self._hard_stop_latched = True
        self._accept_new_markets = False
        self._allow_new_orders = False
        self._allow_new_buys = False
        self._allow_reductions = True
        existing = await self.repository.get_runtime_config("prediction_risk_state", {})
        existing = dict(existing) if isinstance(existing, Mapping) else {}
        await self.repository.set_runtime_config(
            "prediction_risk_state",
            {
                **existing,
                "day": datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat(),
                "hard_stop_latched": True,
                "reason": reason,
                "at_ms": self._now_ms(),
            },
        )
        return {**self._status(), "hard_stop": True, "reason": reason}

    @staticmethod
    def _official_position_rows(value: Any) -> list[Mapping[str, Any]]:
        """Flatten official position-list wrappers without accepting scalars."""

        if isinstance(value, Mapping):
            if any(key in value for key in ("currentShares", "current_shares", "shares")):
                return [value]
            rows: list[Mapping[str, Any]] = []
            for key in ("data", "positions", "items", "results"):
                if key in value:
                    rows.extend(PredictionWorker._official_position_rows(value.get(key)))
            return rows
        if isinstance(value, (list, tuple)):
            rows: list[Mapping[str, Any]] = []
            for item in value:
                rows.extend(PredictionWorker._official_position_rows(item))
            return rows
        return []

    @staticmethod
    def _official_position_shares(row: Mapping[str, Any]) -> Decimal:
        """Read the live position-list `shares` schema and older API variants."""

        for key in ("currentShares", "current_shares", "shares"):
            if key in row:
                shares = as_decimal(row[key])
                if not shares.is_finite() or shares < 0:
                    raise ValueError("official position shares invalid")
                return shares
        raise ValueError("official position shares missing")

    @staticmethod
    def _official_order_rows(value: Any) -> list[Mapping[str, Any]]:
        """Flatten active-order response wrappers without accepting scalars."""

        if isinstance(value, Mapping):
            if any(key in value for key in ("orderId", "order_id", "id")):
                return [value]
            rows: list[Mapping[str, Any]] = []
            for key in ("orders", "data", "items", "results"):
                if key in value:
                    rows.extend(PredictionWorker._official_order_rows(value.get(key)))
            return rows
        if isinstance(value, (list, tuple)):
            rows: list[Mapping[str, Any]] = []
            for item in value:
                rows.extend(PredictionWorker._official_order_rows(item))
            return rows
        return []

    async def _query_active_order_rows(self) -> list[Mapping[str, Any]]:
        """Read and normalize the wallet's currently active orders."""

        if not self.settings.wallet_address:
            return []
        payload = await self._call_api(
            "query_active_orders",
            wallet_address=self.settings.wallet_address,
        )
        return self._official_order_rows(payload)

    async def reset_hard_stop_once(self, reason: str = "telegram operator hard-stop reset") -> dict[str, Any]:
        """Reset a latched hard stop after proving there is no exposure.

        Historical PnL remains immutable.  The current daily risk baseline is
        persisted so only settlements after this reset count toward the next
        daily hard-stop edge.  Trading stays paused until an explicit Resume.
        """

        async with self._lock:
            self._accept_new_markets = False
            self._allow_new_orders = False
            self._allow_new_buys = False
            self._allow_reductions = True
            today = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
            state = await self.repository.get_runtime_config("prediction_risk_state", {})
            state = dict(state) if isinstance(state, Mapping) else {}
            legacy = await self.repository.get_runtime_config("prediction_hard_stop_latched", {})
            legacy_latched = bool(legacy.get("latched")) if isinstance(legacy, Mapping) else False
            if not (self._hard_stop_latched or bool(state.get("hard_stop_latched")) or legacy_latched):
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": "hard stop is not latched",
                }

            reconciliation = await self.reconcile()
            unresolved = await self.repository.load_unresolved_intents()
            active_campaigns = [
                campaign
                for campaign in self._active_campaigns.values()
                if campaign.state not in {CampaignState.DONE, CampaignState.CANCELLED}
            ]
            if (
                not bool(reconciliation.get("known"))
                or int(reconciliation.get("orders") or 0) > 0
                or unresolved
                or active_campaigns
            ):
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": "hard stop reset requires zero exposure and clean reconciliation",
                    "reconciliation": reconciliation,
                }

            # The market task does not take the Telegram control lock.  Let it
            # finish its current read-only tick before changing the risk epoch;
            # otherwise a snapshot captured before Reset can write the old
            # latch back after Reset completes.
            task = getattr(self, "_task", None)
            if task is not None and task is not asyncio.current_task() and not task.done():
                timeout = max(
                    5.0,
                    min(15.0, float(getattr(self.settings, "poll_interval_seconds", 0) or 0) + 3.0),
                )
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
                except TimeoutError:
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "worker is still finalizing; retry hard stop reset",
                    }
                reconciliation = await self.reconcile()
                unresolved = await self.repository.load_unresolved_intents()
                active_campaigns = [
                    campaign
                    for campaign in self._active_campaigns.values()
                    if campaign.state not in {CampaignState.DONE, CampaignState.CANCELLED}
                ]
                if (
                    not bool(reconciliation.get("known"))
                    or int(reconciliation.get("orders") or 0) > 0
                    or unresolved
                    or active_campaigns
                ):
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "hard stop reset requires zero exposure after worker quiescence",
                        "reconciliation": reconciliation,
                    }

            if self.settings.wallet_address:
                if not hasattr(self.client, "query_positions"):
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "official position check is unavailable",
                    }
                try:
                    positions_payload = await self._call_api(
                        "query_positions",
                        wallet_address=self.settings.wallet_address,
                    )
                    open_positions = [
                        row
                        for row in self._official_position_rows(positions_payload)
                        if self._official_position_shares(row) > 0
                    ]
                except Exception as exc:  # noqa: BLE001 - reset must fail closed
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": f"official position check failed: {type(exc).__name__}",
                    }
                if open_positions:
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "hard stop reset denied while an official position is open",
                        "open_position_count": len(open_positions),
                    }

            now = self._now_ms()
            raw_daily = await self.repository.get_daily_pnl(today)
            previous_reset_day = str(state.get("hard_stop_reset_day") or "")
            previous_reset_count = int(state.get("hard_stop_reset_count") or 0)
            reset_count = previous_reset_count + 1 if previous_reset_day == today else 1
            next_state = {
                **state,
                "day": today,
                "daily_net_pnl": "0",
                "raw_daily_net_pnl": str(raw_daily),
                "consecutive_losses": 0,
                "hard_stop_latched": False,
                "hard_stop_reset_day": today,
                "hard_stop_reset_at_ms": now,
                "hard_stop_reset_count": reset_count,
                "hard_stop_reset_baseline_pnl": str(raw_daily),
                "hard_stop_reset_reason": str(reason),
            }
            active_loop = await self.repository.get_active_loop()
            if active_loop and hasattr(self.repository, "request_operator_stop"):
                await self.repository.request_operator_stop(str(active_loop.get("loop_id") or ""))
            await self.repository.set_runtime_config("prediction_risk_state", next_state)
            await self.repository.set_runtime_config(
                "prediction_hard_stop_latched",
                {"latched": False, "reason": str(reason), "at_ms": now},
            )
            await self.repository.record_risk_event(
                "HARD_STOP_RESET",
                "WARNING",
                "operator reset the hard stop after a clean exposure check",
                payload={
                    "day": today,
                    "prior_daily_net_pnl": str(raw_daily),
                    "reset_count_today": reset_count,
                    "resume_required": True,
                },
            )
            if active_loop and callable(getattr(self.repository, "_execute", None)):
                await self.repository._execute(
                    "UPDATE prediction_loops SET hard_stop_latched=0 WHERE loop_id=?",
                    (str(active_loop['loop_id']),),
                )
            reset_risk_engine = getattr(self.risk_engine, "reset_daily", None)
            if callable(reset_risk_engine):
                reset_risk_engine()
            self._hard_stop_latched = False
            self.heartbeat.last_error = None
            return {
                **self._status(),
                "hard_stop_reset": True,
                "hard_stop_latched": False,
                "reset_count_today": reset_count,
                "prior_daily_net_pnl": str(raw_daily),
                "daily_net_pnl": "0",
                "resume_required": True,
                "reconciliation": reconciliation,
            }

    async def reset_hard_stop(self, reason: str = "telegram operator hard-stop reset") -> dict[str, Any]:
        """Publicly named alias for the guarded, repeatable hard-stop reset."""

        return await self.reset_hard_stop_once(reason)

    async def reset_regime_risk(self, reason: str = "telegram operator T6 MDD reset") -> dict[str, Any]:
        """Reset the shared T6 20-run MDD halt after a zero-exposure check.

        History stays untouched: only settlements from the next market slot
        count again.  Any running loop is stopped; a new loop is required.
        """

        async with self._lock:
            reconciliation = await self.reconcile()
            unresolved = await self.repository.load_unresolved_intents()
            active_campaigns = [
                campaign
                for campaign in self._active_campaigns.values()
                if campaign.state not in {CampaignState.DONE, CampaignState.CANCELLED}
            ]
            if (
                not bool(reconciliation.get("known"))
                or int(reconciliation.get("orders") or 0) > 0
                or unresolved
                or active_campaigns
            ):
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": "T6 MDD reset requires zero exposure and clean reconciliation",
                    "reconciliation": reconciliation,
                }
            if self.settings.wallet_address:
                try:
                    positions_payload = await self._call_api(
                        "query_positions",
                        wallet_address=self.settings.wallet_address,
                    )
                    open_positions = [
                        row
                        for row in self._official_position_rows(positions_payload)
                        if self._official_position_shares(row) > 0
                    ]
                except Exception as exc:  # noqa: BLE001 - reset must fail closed
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": f"official position check failed: {type(exc).__name__}",
                    }
                if open_positions:
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "T6 MDD reset denied while an official position is open",
                        "open_position_count": len(open_positions),
                    }
            from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger

            # Stop the old loop before the halt clears so it can never enter
            # in the window between the reset commit and the stop.
            active_loop = await self.repository.get_active_loop()
            if active_loop and hasattr(self.repository, "request_operator_stop"):
                await self.repository.request_operator_stop(str(active_loop.get("loop_id") or ""))
            result = await RegimeLiveLedger(self.repository).reset_shared_risk(
                now_ms=self._now_ms(), reason=str(reason))
            if not result.get("reset"):
                return {**self._status(), "action_denied": True,
                        "reason": str(result.get("reason")), "regime_risk_reset": result}
            try:
                await self.repository.record_risk_event(
                    "REGIME_T6_MDD_RESET",
                    "WARNING",
                    "operator reset the shared T6 20-run MDD halt after a clean exposure check",
                    payload=result,
                )
            except Exception:  # noqa: BLE001 - reset already committed; audit stays in risk_resets
                LOGGER.exception("regime_t6_mdd_reset_event_failed")
                result = {**result, "risk_event_recorded": False}
            return {**self._status(), "regime_risk_reset": result}

    async def cancel_loop(self, reason: str = "telegram operator cancelled loop") -> dict[str, Any]:
        """Cancel the current loop after closing every known execution edge.

        Cancellation is terminal for the loop cursor, but it is never a
        shortcut around live reconciliation.  The worker first stops market
        admission, waits for its market task to quiesce, cancels known open
        orders, verifies the official wallet has no active order or position,
        and only then marks campaigns and the loop as ``CANCELLED``.  Any
        uncertainty keeps Hard Stop latched and leaves the durable loop
        recoverable for a later retry.
        """

        async with self._lock:
            self._accept_new_markets = False
            self._allow_new_orders = False
            self._allow_new_buys = False
            self._allow_reductions = True
            self._cancel_requested = True

            async def deny(message: str, **extra: Any) -> dict[str, Any]:
                try:
                    await self.hard_stop(message)
                except Exception:
                    # The local flag still prevents admission if persistence
                    # is unavailable; the caller will retry reconciliation.
                    self._hard_stop_latched = True
                # A denied cancellation must keep existing execution alive
                # for reconciliation/settlement under the latched Hard Stop.
                # Releasing this flag never enables admission or BUY.
                self._cancel_requested = False
                # A restarted process may not have adopted the durable loop
                # yet. Recover only campaigns owned by this exact loop, and
                # include fully reduced fills still awaiting final settlement.
                task = getattr(self, "_task", None)
                if (loop_id and self._loop_id in (None, loop_id)
                        and (task is None or task.done())):
                    metadata = await self.repository.get_active_campaign_metadata()
                    owned = {str(row['campaign_id']) for row in metadata
                             if str(row.get('loop_id') or '') == loop_id}
                    needs_management = False
                    for campaign in self._active_campaigns.values():
                        if (campaign.campaign_id not in owned or
                                campaign.state in {CampaignState.DONE, CampaignState.CANCELLED}):
                            continue
                        if (campaign.position.has_any or campaign.pending_intent_id
                                or campaign.pending_unknown
                                or await self.repository.get_fills(campaign.campaign_id)):
                            needs_management = True
                            break
                    if needs_management:
                        self._loop_id = loop_id
                        self._target_markets = int(active_loop['target'])
                        self._loop_created_at_ms = self._loop_origin_ms(active_loop, loop_id=loop_id)
                        self.heartbeat.markets_completed = int(active_loop['completed'])
                        self._settlement_only_recovery = True
                        self._task = asyncio.create_task(self._run_loop(), name="prediction-market-loop")
                return {**self._status(), "action_denied": True, "reason": message, **extra}

            active_loop = await self.repository.get_active_loop()
            if not active_loop:
                self._cancel_requested = False
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": "no active prediction loop",
                }
            loop_id = str(active_loop.get("loop_id") or "")
            if not loop_id:
                return await deny("active loop has no durable loop id")

            if hasattr(self.repository, "request_operator_stop"):
                await self.repository.request_operator_stop(loop_id)

            task = getattr(self, "_task", None)
            if task is not None and task is not asyncio.current_task() and not task.done():
                timeout = max(
                    5.0,
                    min(15.0, float(getattr(self.settings, "poll_interval_seconds", 0) or 0) + 3.0),
                )
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
                except TimeoutError:
                    return await deny("worker did not quiesce before loop cancellation timeout")
                except Exception as exc:  # noqa: BLE001 - cancellation remains fail-closed
                    return await deny(f"worker failed while stopping loop: {type(exc).__name__}")
            self._task = None

            reconciliation = await self.reconcile()
            if not bool(reconciliation.get("known")):
                return await deny(
                    "loop cancellation requires a clean exchange reconciliation",
                    reconciliation=reconciliation,
                )

            metadata_getter = getattr(self.repository, "get_active_campaign_metadata", None)
            metadata = await metadata_getter() if callable(metadata_getter) else []
            loop_campaign_ids = {
                str(row.get("campaign_id") or "")
                for row in metadata
                if str(row.get("loop_id") or "") == loop_id and row.get("campaign_id")
            }

            order_rows_getter = getattr(self.repository, "get_loop_order_rows", None)
            local_order_rows = await order_rows_getter(loop_id) if callable(order_rows_getter) else []
            local_order_ids = {
                str(row.get("order_id") or row.get("orderId") or row.get("id") or "")
                for row in local_order_rows
                if row.get("order_id") or row.get("orderId") or row.get("id")
            }
            loop_campaign_ids.update(
                str(row.get("campaign_id") or "")
                for row in local_order_rows
                if row.get("campaign_id")
            )

            unresolved = await self.repository.load_unresolved_intents()
            order_intents: dict[str, tuple[Campaign, OrderIntent]] = {}
            unmatched_without_order_id: list[tuple[Campaign, OrderIntent]] = []
            for row in unresolved:
                campaign_id = str(row.get("campaign_id") or "")
                if not campaign_id:
                    return await deny("unresolved intent has no campaign id")
                campaign_row = await self.repository.get_campaign(campaign_id)
                if not campaign_row or str(campaign_row.get("loop_id") or "") != loop_id:
                    continue
                loop_campaign_ids.add(campaign_id)
                campaign = self._active_campaigns.get(campaign_id)
                if campaign is None:
                    campaign = await self.repository.load_campaign(campaign_id)
                if campaign is None:
                    return await deny("loop cancellation found an unknown campaign")
                intent = self._intent_from_row(row)
                order_id = str(row.get("order_id") or "")
                if not order_id:
                    try:
                        match = await self.reconcile_intent_without_order_id(campaign, intent)
                    except Exception as exc:  # noqa: BLE001 - never guess an execution result
                        return await deny(
                            f"cannot reconcile intent without order id: {type(exc).__name__}",
                            reconciliation=reconciliation,
                        )
                    if match is not None:
                        order_id = str(match.get("orderId") or match.get("order_id") or match.get("id") or "")
                        if order_id:
                            await self.repository.update_intent(
                                intent.intent_id,
                                order_id=order_id,
                                status="SUBMITTED",
                                unknown=False,
                            )
                            campaign.pending_intent_id = intent.intent_id
                            campaign.pending_unknown = False
                            await self.repository.save_campaign(campaign)
                    if not order_id:
                        unmatched_without_order_id.append((campaign, intent))
                        continue
                order_intents[order_id] = (campaign, intent)

            if self.settings.wallet_address:
                try:
                    active_order_rows = await self._query_active_order_rows()
                except Exception as exc:  # noqa: BLE001 - unknown exchange state is fail-closed
                    return await deny(
                        f"active-order check failed: {type(exc).__name__}",
                        reconciliation=reconciliation,
                    )
            else:
                active_order_rows = []
            active_order_ids = {
                str(row.get("orderId") or row.get("order_id") or row.get("id") or "")
                for row in active_order_rows
                if row.get("orderId") or row.get("order_id") or row.get("id")
            }
            owned_order_ids = local_order_ids | set(order_intents)
            unowned_order_ids = active_order_ids - owned_order_ids
            if unowned_order_ids:
                return await deny(
                    "active orders cannot be attributed safely to this loop",
                    active_order_count=len(active_order_ids),
                )

            if active_order_ids:
                if not self.settings.wallet_id or not hasattr(self.client, "batch_cancel_orders"):
                    return await deny("loop has active orders but cancellation capability is unavailable")
                for order_id, (_, intent) in order_intents.items():
                    if order_id in active_order_ids:
                        await self.repository.mark_cancel_requested(intent.intent_id, at_ms=self._now_ms())
                try:
                    await self._call_api(
                        "batch_cancel_orders",
                        wallet_address=self.settings.wallet_address,
                        wallet_id=self.settings.wallet_id,
                        order_ids=sorted(active_order_ids),
                        _emergency=True,
                    )
                except Exception as exc:  # noqa: BLE001 - never mark a possibly-open order cancelled
                    return await deny(
                        f"loop order cancellation failed: {type(exc).__name__}",
                        active_order_count=len(active_order_ids),
                    )

                try:
                    remaining_order_rows = await self._query_active_order_rows()
                except Exception as exc:  # noqa: BLE001
                    return await deny(f"post-cancel active-order check failed: {type(exc).__name__}")
                remaining_order_ids = {
                    str(row.get("orderId") or row.get("order_id") or row.get("id") or "")
                    for row in remaining_order_rows
                    if row.get("orderId") or row.get("order_id") or row.get("id")
                }
                if remaining_order_ids:
                    return await deny(
                        "active orders remain after cancellation; reconciliation is incomplete",
                        active_order_count=len(remaining_order_ids),
                    )

            # A known order may have disappeared from the active list before
            # the cancel call. Poll its official history so a late fill is
            # applied to the position ledger before cancellation is decided.
            for order_id, (campaign, intent) in list(order_intents.items()):
                try:
                    await self._poll_order_terminal(campaign, intent, order_id, attempts=2)
                except Exception as exc:  # noqa: BLE001
                    return await deny(f"known order history check failed: {type(exc).__name__}")

            if self.settings.wallet_address:
                if not hasattr(self.client, "query_positions"):
                    return await deny("official position check is unavailable")
                try:
                    positions_payload = await self._call_api(
                        "query_positions",
                        wallet_address=self.settings.wallet_address,
                    )
                    open_positions = [
                        row
                        for row in self._official_position_rows(positions_payload)
                        if self._official_position_shares(row) > 0
                    ]
                except Exception as exc:  # noqa: BLE001
                    return await deny(f"official position check failed: {type(exc).__name__}")
                if open_positions:
                    return await deny(
                        "loop cancellation denied while an official position is open",
                        open_position_count=len(open_positions),
                    )

            # No-order UNKNOWN intents are safe to close only when a fresh
            # history read still has no match and the campaign has no fill.
            # This is the exact recovery path for a submit timeout where the
            # exchange confirmed neither an order id nor a position.
            for campaign, intent in unmatched_without_order_id:
                try:
                    match = await self.reconcile_intent_without_order_id(campaign, intent)
                except Exception as exc:  # noqa: BLE001
                    return await deny(f"final intent reconciliation failed: {type(exc).__name__}")
                if match is not None:
                    return await deny("loop cancellation found a late order-history match")
                if await self.repository.get_fills(campaign.campaign_id):
                    return await deny("loop cancellation found fills on an unresolved campaign")
                await self.repository.update_intent(
                    intent.intent_id,
                    status="CANCELLED",
                    unknown=False,
                    payload_json={
                        "operator_cancelled": True,
                        "reason": str(reason),
                        "prior_status": "UNKNOWN",
                    },
                )
                campaign.pending_intent_id = None
                campaign.pending_unknown = False
                await self.repository.save_campaign(campaign)

            campaigns_to_cancel: dict[str, Campaign] = {}
            for campaign_id in loop_campaign_ids:
                campaign = self._active_campaigns.get(campaign_id)
                if campaign is None:
                    campaign = await self.repository.load_campaign(campaign_id)
                if campaign is None or campaign.state in {CampaignState.DONE, CampaignState.CANCELLED}:
                    continue
                if campaign.position.has_any or await self.repository.get_fills(campaign.campaign_id):
                    return await deny("loop cancellation found campaign exposure or fills")
                campaigns_to_cancel[campaign_id] = campaign

            remaining_unresolved = [
                row
                for row in await self.repository.load_unresolved_intents()
                if str(row.get("campaign_id") or "") in loop_campaign_ids
            ]
            if remaining_unresolved:
                return await deny(
                    "loop cancellation left unresolved intents",
                    unresolved=len(remaining_unresolved),
                )

            for campaign in campaigns_to_cancel.values():
                campaign.pending_intent_id = None
                campaign.pending_unknown = False
                campaign.state = CampaignState.CANCELLED
                campaign.last_error = str(reason)
                await self.repository.save_campaign(campaign)
                self._active_campaigns.pop(campaign.campaign_id, None)

            cancelled_loop = await self.repository.cancel_loop(loop_id, reason="OPERATOR_CANCEL")
            if not cancelled_loop or str(cancelled_loop.get("state") or "").upper() != "CANCELLED":
                return await deny("durable loop cancellation could not be committed")
            try:
                await self.repository.record_risk_event(
                    "LOOP_CANCELLED",
                    "WARNING",
                    "operator cancelled the complete loop after clean reconciliation",
                    payload={
                        "loop_id": loop_id,
                        "campaign_count": len(campaigns_to_cancel),
                        "order_count": len(active_order_ids),
                        "reason": str(reason),
                    },
                )
            except Exception:
                pass
            self._loop_id = None
            self._loop_created_at_ms = None
            self._target_markets = 0
            self._initial_market_wait_until_ms = None
            self._cancel_requested = False
            return {
                **(await self.status()),
                "loop_cancelled": True,
                "cancelled_loop_id": loop_id,
                "cancelled_campaigns": len(campaigns_to_cancel),
                "cancelled_orders": len(active_order_ids),
                "cancelled_intents": len(unmatched_without_order_id),
                "reconciliation": reconciliation,
            }

    async def stop_loop(self) -> dict[str, Any]:
        # Stop only discovery/new orders. Existing campaigns continue through
        # management and settlement in the background loop.
        self._accept_new_markets = False
        self._allow_new_orders = False
        self._allow_new_buys = False
        self._allow_reductions = True
        active_loop = await self.repository.get_active_loop()
        if active_loop and hasattr(self.repository, "request_operator_stop"):
            await self.repository.request_operator_stop(str(active_loop.get("loop_id") or ""))
        return {**self._status(), "stop_requested": True}

    async def pause(self) -> dict[str, Any]:
        self._accept_new_markets = False
        self._allow_new_orders = False
        self._allow_new_buys = False
        self._allow_reductions = True
        return {**self._status(), "paused": True}

    async def resume(self) -> dict[str, Any]:
        persisted_risk = await self._risk_snapshot()
        if self._hard_stop_latched or persisted_risk.hard_stop_latched:
            self._hard_stop_latched = True
            return {**self._status(), "action_denied": True, "reason": "hard stop is latched"}
        existing = await self.repository.get_active_loop()
        if not existing:
            self._settlement_only_recovery = False
            self._accept_new_markets = True
            self._allow_new_orders = True
            self._allow_new_buys = True
            self._allow_reductions = True
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run_loop(), name="prediction-market-loop")
            return {**self._status(), "resumed": True}
        reconciliation = await self.reconcile()
        unresolved = await self.repository.load_unresolved_intents()
        if not bool(reconciliation.get("known")) or int(reconciliation.get("orders") or 0) > 0 or unresolved:
            return {
                **self._status(),
                "action_denied": True,
                "reason": "resume requires clean reconciliation",
                "reconciliation": reconciliation,
            }
        # A paused but still-running loop keeps its bound lanes; only a stopped
        # loop could be resumed under a different queued choice.
        mask_error = await self._resume_lane_mask_error() if bool(existing.get("new_entries_stopped")) else None
        if mask_error:
            return {**self._status(), "action_denied": True, "reason": mask_error}
        self._loop_id = str(existing.get("loop_id") or "") or None
        self._loop_created_at_ms = self._loop_origin_ms(existing, loop_id=self._loop_id)
        self._target_markets = int(existing.get("target") or self._target_markets)
        stop_reason = str(existing.get("terminal_reason") or "").upper()
        allow_adaptive = stop_reason == "ADAPTIVE_JUMP_STOP"
        if allow_adaptive:
            jump_stop = await self.repository.get_runtime_config(
                "prediction_adaptive_jump_stop",
                {},
            )
            jump_stop = jump_stop if isinstance(jump_stop, Mapping) else {}
            resume_after_ms = int(jump_stop.get("resume_after_ms") or 0)
            if self._now_ms() < resume_after_ms:
                wait_seconds = max(1, (resume_after_ms - self._now_ms() + 999) // 1000)
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": f"adaptive jump stop cooldown has {wait_seconds}s remaining",
                }
            get_regime = getattr(self.repository, "get_market_regime_monitor", None)
            if callable(get_regime):
                regime = await get_regime(limit=20)
                if str(regime.get("status") or "").upper() == "RED":
                    return {
                        **self._status(),
                        "action_denied": True,
                        "reason": "adaptive jump stop cannot resume while market regime is RED",
                        "market_regime_monitor": regime,
                    }
        resume_loop = getattr(self.repository, "resume_operator_stopped_loop", None)
        if callable(resume_loop) and self._loop_id:
            resumed = (
                await resume_loop(self._loop_id, allow_adaptive=True)
                if allow_adaptive
                else await resume_loop(self._loop_id)
            )
            if not bool(resumed.get("resumed")):
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": str(resumed.get("reason") or "loop resume denied"),
                }
        if allow_adaptive:
            prior = await self.repository.get_runtime_config(
                "prediction_adaptive_jump_stop",
                {},
            )
            prior = dict(prior) if isinstance(prior, Mapping) else {}
            await self.repository.set_runtime_config(
                "prediction_adaptive_jump_stop",
                {
                    **prior,
                    "active": False,
                    "resumed_at_ms": self._now_ms(),
                },
            )
            self._adaptive_jump_stop_latched = False
        self._settlement_only_recovery = False
        self._accept_new_markets = True
        self._allow_new_orders = True
        self._allow_new_buys = True
        self._allow_reductions = True
        self._cancel_requested = False
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run_loop(), name="prediction-market-loop")
        return {**(await self.status()), "resumed": True, "reconciliation": reconciliation}

    async def set_shadow_mode(
        self,
        enabled: bool,
        *,
        preflight_evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if enabled:
            self._effective_mode = RuntimeMode.SHADOW
            return self._status()
        if preflight_evidence is not None:
            preflight_reasons = self._validate_persisted_live_preflight(preflight_evidence)
            if preflight_reasons:
                self._shadow_reasons = preflight_reasons
                return {
                    **self._status(),
                    "action_denied": True,
                    "reason": "signed live preflight evidence is invalid",
                    "preflight_reasons": tuple(preflight_reasons),
                }
            await self.repository.set_runtime_config("prediction_preflight", dict(preflight_evidence))
        # Re-read the current official payment-source balance immediately
        # before arming Live.  This check is mandatory even when historical
        # Shadow promotion is explicitly skipped; a stale controller result
        # must never grant signed trade capability.
        live_reasons = await self._refresh_live_prerequisites()
        if live_reasons:
            return {
                **self._status(),
                "action_denied": True,
                "reason": "current live preflight failed",
                "preflight_reasons": tuple(live_reasons),
            }
        if getattr(self.settings, "live_skip_shadow_promotion", False):
            self._promotion_decision = {
                "passed": True,
                "eligible": True,
                "reasons": ("Shadow promotion skipped by explicit live configuration",),
            }
        else:
            gate = await self.promotion_gate()
            if not gate.get("passed"):
                return {**self._status(), "action_denied": True, "promotion": gate}
        self._effective_mode = RuntimeMode.LIVE
        return self._status()

    async def promotion_gate(self) -> dict[str, Any]:
        # Promotion is always an authority transition, even when a caller
        # constructed the worker in SHADOW mode.  Re-read the persisted
        # official proof so a restart cannot promote from an in-memory flag.
        await self._refresh_live_prerequisites()
        evidence = await self.repository.get_runtime_config("promotion_evidence")
        reasons = list(self._shadow_reasons)
        if not evidence:
            reasons.append("promotion evidence is not recorded")
        else:
            try:
                decision = PromotionGate().evaluate(PromotionEvidence.from_mapping(evidence))
                reasons.extend(decision.reasons)
            except (TypeError, ValueError, ArithmeticError) as exc:
                reasons.append(f"invalid promotion evidence: {exc}")
        passed = not reasons
        self._promotion_decision = {"passed": passed, "eligible": passed, "reasons": tuple(reasons)}
        return dict(self._promotion_decision)

    async def risk(self) -> dict[str, Any]:
        await self.restore_order_unit()
        await self.restore_selected_strategy()
        active = next(iter(self._active_campaigns.values()), None)
        snapshot = await self._risk_snapshot(active)
        self._hard_stop_latched = self._hard_stop_latched or snapshot.hard_stop_latched
        snapshot.hard_stop_latched = self._hard_stop_latched
        loop = await self.repository.get_loop(self._loop_id) if self._loop_id else None
        c180_risk = (self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                     and (not self._loop_id or (loop and
                          str(loop.get("strategy_profile") or "").lower() in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                          and str(loop.get("mode") or "").upper() == "LIVE")))
        assessed = (replace(snapshot, daily_net_pnl=Decimal("0"),
                            loop_net_pnl=Decimal("0"), consecutive_losses=0,
                            soft_cooldown_until_ms=None) if c180_risk else snapshot)
        decision = self.risk_engine.evaluate(assessed, now_ms=self._now_ms(), action=ActionType.BUY_INITIAL)
        self._last_risk = decision
        if decision.mode.value == "HARD_STOP" and (not c180_risk or assessed.hard_stop_latched):
            self._hard_stop_latched = True
        snapshot.hard_stop_latched = self._hard_stop_latched or assessed.hard_stop_latched
        await self._persist_risk_state(snapshot)
        result = {
            "mode": decision.mode.value,
            "allow_trading": decision.allow_trading,
            "action": decision.action.value,
            "reasons": decision.reasons,
            "daily_net_pnl": str(snapshot.daily_net_pnl),
            "loop_net_pnl": str(snapshot.loop_net_pnl),
            "order_unit_usdt": str(self._selected_order_unit_usdt),
            "loop_loss_limit": str(self.risk_engine.config.loop_loss_limit),
            "daily_loss_limit": str(self.risk_engine.config.daily_loss_limit),
            "consecutive_losses": snapshot.consecutive_losses,
            "order_attempts": snapshot.order_attempts,
            "buy_count": snapshot.buy_count,
            "hard_stop_latched": self._hard_stop_latched,
        }
        if self._selected_strategy_profile in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            from src.gridbot.prediction.regime_lane import STATE_KEY
            lane_risk = await self.repository.get_runtime_config(STATE_KEY, None)
            result["regime_lane_risk"] = lane_risk
            result["strategy_profile"] = self._selected_strategy_profile
            if not lane_risk or lane_risk.get("halt_reason"):
                result["allow_trading"] = False
                result["reasons"] = (*result["reasons"], (lane_risk or {}).get("halt_reason") or "regime_not_initialized")
        result.update(self.pending_pnl_status(snapshot.loop_net_pnl))
        return result

    async def reconcile(self) -> dict[str, Any]:
        campaigns = await self.repository.load_active_campaigns()
        self._active_campaigns = {
            campaign.campaign_id: campaign
            for campaign in campaigns
            if not str(campaign.campaign_id or "").endswith("::p3")
            and "::shadow::" not in str(campaign.campaign_id or "")
        }
        # A restarted worker has already seen every campaign it restores.
        # Preserve that fact in the process-local heartbeat so health checks
        # do not report a false discovery failure after a clean restart.
        self.heartbeat.markets_seen = max(int(self.heartbeat.markets_seen), len(campaigns))
        result: dict[str, Any] = {"active_campaigns": len(campaigns), "known": True, "orders": 0, "unresolved": 0, "matched": 0, "settled": 0}
        unresolved = await self.repository.load_unresolved_intents()
        result["unresolved"] = len(unresolved)
        for row in unresolved:
            campaign_id = str(row.get("campaign_id") or "")
            if campaign_id.endswith("::p3") or "::shadow::" in campaign_id:
                continue
            campaign = self._active_campaigns.get(campaign_id)
            if campaign is None and campaign_id:
                campaign = await self.repository.load_campaign(campaign_id)
                if campaign is not None:
                    self._active_campaigns[campaign_id] = campaign
            if campaign is None:
                self._hard_stop_latched = True
                result["known"] = False
                continue
            intent = self._intent_from_row(row)
            order_id = str(row.get("order_id") or "")
            if not order_id:
                match = await self.reconcile_intent_without_order_id(campaign, intent)
                if match is not None:
                    order_id = str(match.get("orderId") or match.get("order_id") or match.get("id") or "")
                    if order_id:
                        await self.repository.update_intent(intent.intent_id, order_id=order_id, status="SUBMITTED", unknown=False)
                        campaign.pending_unknown = False
                        campaign.pending_intent_id = intent.intent_id
                        await self.repository.save_campaign(campaign)
                        result["matched"] += 1
            if order_id:
                try:
                    terminal_status = await self._poll_order_terminal(campaign, intent, order_id, attempts=1)
                    if terminal_status is None and campaign.pending_unknown:
                        result["known"] = False
                except Exception as exc:  # unknown execution remains fail-closed
                    campaign.pending_unknown = True
                    campaign.last_error = str(exc)
                    await self.repository.save_campaign(campaign)
                    result["known"] = False
        # A process can be restarted after a market has ended but before the
        # normal loop tick removes its campaign.  Reconcile those campaigns
        # now, including Shadow-only campaigns with no intent/order row, so a
        # completed old loop cannot strand an INITIAL_PENDING record and block
        # the next One Run.
        for campaign in list(self._active_campaigns.values()):
            if self._now_ms() < campaign.market.end_time_ms:
                continue
            if campaign.pending_intent_id or campaign.pending_unknown:
                continue
            try:
                settlement = await self.settle_campaign(campaign)
            except Exception as exc:  # unknown live state remains fail-closed
                campaign.last_error = str(exc)
                await self.repository.save_campaign(campaign)
                result["known"] = False
                continue
            if str(settlement.get("status", "")).upper() == "SETTLED":
                self._active_campaigns.pop(campaign.campaign_id, None)
                result["settled"] += 1
        if self.settings.wallet_address:
            try:
                result["orders"] = len(await self._query_active_order_rows())
            except Exception as exc:  # noqa: BLE001 - unknown exchange state is fail-closed
                self._hard_stop_latched = True
                result.update({"known": False, "error": str(exc)})
        await self._persist_heartbeat()
        return result

    @staticmethod
    def _intent_from_row(row: Mapping[str, Any]) -> OrderIntent:
        action_raw = str(row.get("action") or ActionType.RECONCILE.value)
        try:
            action = ActionType(action_raw)
        except ValueError:
            action = ActionType.RECONCILE
        try:
            outcome = OutcomeSide(str(row.get("outcome") or "UP").upper())
        except ValueError:
            outcome = OutcomeSide.UP
        try:
            side = OrderSide(str(row.get("order_side") or "BUY").upper())
        except ValueError:
            side = OrderSide.BUY
        return OrderIntent(
            intent_id=str(row.get("intent_id") or ""),
            campaign_id=str(row.get("campaign_id") or ""),
            action=action,
            outcome=outcome,
            order_side=side,
            amount=as_decimal(row.get("amount")),
            limit_price=as_decimal(row.get("limit_price")),
            created_at_ms=int(row.get("submission_at_ms") or row.get("created_at_ms") or 0),
            ttl_ms=int(row.get("ttl_ms") or 0),
            attempt=int(row.get("attempt") or 1),
            order_id=(str(row["order_id"]) if row.get("order_id") else None),
            unknown=bool(row.get("unknown")),
            tier=row.get("tier"),
        )

    async def health(self) -> dict[str, Any]:
        now = self._now_ms()
        hb = self.heartbeat
        max_age = max(10_000, int(self.settings.poll_interval_seconds * 5_000))
        return {
            "worker_heartbeat": hb.last_loop_at_ms > 0 and now - hb.last_loop_at_ms <= max_age,
            "api": hb.last_api_ok_at_ms > 0 and now - hb.last_api_ok_at_ms <= max_age,
            "feed": hb.last_feed_ok_at_ms > 0 and now - hb.last_feed_ok_at_ms <= max_age,
            "db_write": hb.last_db_write_at_ms > 0 and now - hb.last_db_write_at_ms <= max_age,
            "clock": now >= hb.started_at_ms,
            "last_error": hb.last_error,
            "rate_limit": self._rate_limiter.health().as_dict(),
        }

    async def _finalize_settlement_with_c180(self, campaign: Campaign,
                                                    settlement: Mapping[str, Any]) -> dict[str, Any]:
        row = await self.repository.finalize_settlement(campaign, settlement)
        if str(row.get("status") or "").upper() != "SETTLED":
            return row
        loop_id = str(row.get("loop_id") or "")
        loop = await self.repository.get_loop(loop_id) if loop_id else None
        if (not isinstance(loop, Mapping)
                or loop.get("strategy_profile") not in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                or str(loop.get("mode") or "").upper() != "LIVE"):
            return row
        bridge = self._c180_bridge_for_worker(profile=loop.get("strategy_profile"))
        if bridge is None:
            raise RuntimeError("C180 settlement ledger unavailable")
        fills = await self.repository.get_fills(campaign.campaign_id)
        if any(str(fill.get("order_side") or "").upper() == "BUY" for fill in fills):
            await bridge.ledger.observe_settlement(
                loop_id=loop_id, settlement_id=str(row["settlement_id"]))
        else:
            # Attest a completed zero-exposure slot only after an official
            # payment-source read; UNKNOWN or unresolved intents remain held.
            if self._now_ms() < int(campaign.market.end_time_ms):
                raise RuntimeError("C180 cannot attest empty before market end")
            await self._call_api("query_payment_option_balances",
                                 recv_window=self.settings.recv_window)
            await bridge.ledger.attest_empty(
                loop_id=loop_id, market_start_ms=campaign.market.start_time_ms,
                wallet_reconciled_at_ms=self._now_ms(), now_ms=self._now_ms())
        return row

    async def _recheck_cancelled_before_settlement(self, campaign: Campaign) -> bool:
        """A cancel snapshot is provisional until rechecked at market end.

        This read runs only for submitted terminal cancellations, never for
        locally rejected/unsubmitted intents, and never authorizes another BUY.
        """
        orders = await self.repository.submitted_cancelled_orders(campaign.campaign_id)
        if not orders:
            return True
        if self._now_ms() < campaign.market.end_time_ms:
            return False
        for item in orders:
            intent = OrderIntent(
                intent_id=item['intent_id'], campaign_id=campaign.campaign_id,
                action=ActionType(item['action']), outcome=OutcomeSide(item['outcome']),
                order_side=OrderSide(item['order_side']), amount=Decimal(item['amount']),
                limit_price=Decimal(item['limit_price']), created_at_ms=item['created_at_ms'],
                ttl_ms=item['ttl_ms'], order_id=item['order_id'], tier=item.get('tier'))
            try:
                rows = await self._history_rows(intent, statuses=('CLOSED',),
                                                target_order_id=item['order_id'])
            except (PredictionClientError, PredictionRateLimitDeferred):
                return False
            matches = [r for r in rows if str(r.get('orderId') or r.get('order_id') or '') == item['order_id']]
            if not matches:
                return False
            row = max(matches, key=lambda r: (
                Decimal(str(r.get('filledShareQty') or r.get('filledShares') or '0')),
                int(r.get('modifyTime') or r.get('terminalTime') or 0)))
            old = json.loads(item['order_payload_json'])
            if any(str(row.get(k) or '') != str(old.get(k) or '')
                   for k in ('marketId', 'marketTopicId', 'side', 'outcome')):
                return False
            if str(row.get('status') or '').upper() not in {
                    'FILLED', 'CLOSED', 'CANCELLED', 'CANCELED', 'EXPIRED', 'FAILED'}:
                return False
            await self.apply_order_snapshot(campaign, row, outcome=intent.outcome, intent=intent)
        return True

    async def settle_campaign(self, campaign: Campaign) -> dict[str, Any]:
        """Persist settlement/PnL and redeem only after a confirmed close."""

        execution_mode = await self.repository.get_campaign_execution_mode(campaign.campaign_id)
        if execution_mode == "SHADOW":
            return await self._settle_shadow_campaign(campaign)
        if execution_mode != "LIVE":
            return {"campaign_id": campaign.campaign_id, "status": "SETTLEMENT_PROVENANCE_UNKNOWN"}
        if self._effective_mode is not RuntimeMode.LIVE:
            # A restart/Live-off revokes signing permission, not ownership of
            # real fills. Keep the campaign recoverable until Live is armed.
            return {"campaign_id": campaign.campaign_id, "status": "LIVE_SETTLEMENT_PENDING_AUTHORIZATION"}
        if not self.settings.wallet_address:
            return {"campaign_id": campaign.campaign_id, "status": "SHADOW_PENDING"}
        prior = await self.repository.get_settlement(campaign.campaign_id)
        # Already-finalized history is changed only by the explicit atomic
        # repair tool; applying a late fill here would leave stale zero PnL.
        if not (isinstance(prior, Mapping) and str(prior.get('status', '')).upper() == 'SETTLED'):
            if not await self._recheck_cancelled_before_settlement(campaign):
                return {"campaign_id": campaign.campaign_id,
                        "status": "CANCELLED_FILL_RECHECK_PENDING"}
        if isinstance(prior, Mapping) and str(prior.get("status", "")).upper() == "SETTLED":
            # Rebuild any C180 gate observation left incomplete by a crash
            # after the atomic finalizer committed and before the next line.
            if self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
                await self._finalize_settlement_with_c180(campaign, prior)
            campaign.state = CampaignState.DONE
            return dict(prior)
        if isinstance(prior, Mapping) and str(prior.get("status", "")).upper() in {"CLOSED_PENDING_REDEEM", "PENDING"}:
            prior_payload = prior.get("payload") if isinstance(prior.get("payload"), Mapping) else {}
            if not prior_payload and prior.get("payload_json"):
                try:
                    parsed = json.loads(str(prior.get("payload_json")))
                    prior_payload = parsed if isinstance(parsed, Mapping) else {}
                except (TypeError, ValueError, json.JSONDecodeError):
                    prior_payload = {}
            tx_hash = prior.get("tx_hash") or prior.get("txHash") or prior_payload.get("tx_hash") or prior_payload.get("txHash")
            if tx_hash and hasattr(self.client, "get_redeem_status"):
                confirmed = await self._poll_redeem_confirmation(str(tx_hash))
                if str(confirmed.get("status", "")).upper() == "CONFIRMED":
                    prior = {
                        **prior,
                        "status": "SETTLED",
                        "redeem": confirmed,
                        "tx_hash": tx_hash,
                        "redeem_status": "CONFIRMED",
                        "redeem_error": None,
                    }
                    await self._finalize_settlement_with_c180(campaign, prior)
                    campaign.state = CampaignState.DONE
                else:
                    # Redeem status can disagree with the exact position after
                    # funds were claimed. Reconcile the asset before returning
                    # forever on an old transaction; never retry redemption here.
                    now = self._now_ms()
                    state = str(confirmed.get("status", "PENDING")).upper()
                    prior = {
                        **prior_payload,
                        **{k: v for k, v in prior.items() if k not in {"payload", "payload_json"}},
                        "redeem": confirmed,
                        "status": "CLOSED_PENDING_REDEEM",
                        "redeem_status": state,
                        "redeem_error": "redeem transaction failed" if state == "FAILED" else "redeem transaction not confirmed",
                        "redeem_checked_at_ms": now,
                        "redeem_wait_ms": max(0, now - int(prior.get("settled_at_ms") or now)),
                    }
                    # Persist FAILED before a position lookup can be deferred.
                    # The existing finalizer keeps accounting atomic/idempotent.
                    await self._finalize_settlement_with_c180(campaign, prior)
                    claimed = await self._pending_redeem_claimed_evidence(campaign, prior)
                    if claimed:
                        prior = {
                            **prior, "status": "SETTLED", "redeem_status": "CLAIMED",
                            "redeem_error": None,
                            "redeem_reconciliation": {
                                "source": "official_exact_token_position",
                                "at_ms": now, "positions": claimed,
                                "transaction_status": state,
                            },
                        }
                        await self._finalize_settlement_with_c180(campaign, prior)
                        campaign.state = CampaignState.DONE
                        await self.repository.record_risk_event(
                            "REDEEM_RECONCILED", "INFO",
                            "Exact token already claimed despite unconfirmed redeem status",
                            campaign_id=campaign.campaign_id,
                            payload=prior["redeem_reconciliation"],
                        )
                return prior
        # A market with no position, no recorded fill, and no unresolved
        # intent has nothing to redeem.  Finalize it locally as a zero-PnL
        # NO_FILL so an expired campaign cannot consume position-query weight
        # forever and starve discovery of the next market.  UNKNOWN execution
        # remains fail-closed because it is included in unresolved intents.
        if not campaign.position.has_any and not campaign.pending_intent_id and not campaign.pending_unknown:
            fills = await self.repository.get_fills(campaign.campaign_id)
            unresolved = await self.repository.load_unresolved_intents()
            campaign_unresolved = any(
                str(item.get("campaign_id") or "") == str(campaign.campaign_id)
                for item in unresolved
            )
            if not fills and not campaign_unresolved:
                official_winner: OutcomeSide | None = None
                profile = str(getattr(self, "_selected_strategy_profile", "")).strip().lower()
                if profile in {REGIME_VALUE_V7_PROFILE, "s3s5_pair_v1", "fav_only_v1", "fav_only_v2", "fav_only_v3", "fav_only_v4"}:
                    # V7/s3s5 prefer official resolution for abstention labels.
                    # If weight-deferred detail reads keep returning None (or
                    # the winner is not published yet), auto-close an empty
                    # OBSERVE after a short grace so admit-next is not blocked
                    # forever. Campaigns with buys/positions never take this path.
                    loaded = await self._get_market_detail_with_cache(
                        campaign.market.market_topic_id,
                        now_ms=self._now_ms(),
                    )
                    official_winner = None
                    if loaded is not None:
                        detail, _ = loaded
                        official_winner = self._official_resolution(detail)
                    if official_winner is None:
                        now_ms = self._now_ms()
                        grace_ms = int(getattr(self, "_empty_observe_auto_close_grace_ms", 45_000))
                        past_grace = now_ms >= int(campaign.market.end_time_ms) + grace_ms
                        empty_observe = (
                            int(getattr(campaign, "buy_count", 0) or 0) == 0
                            and not bool(getattr(campaign.position, "has_any", False))
                        )
                        if past_grace and empty_observe:
                            auto_reason = (
                                "empty market expired with no fills; official winner not yet available"
                            )
                            campaign.last_error = None
                            try:
                                await self.repository.record_risk_event(
                                    "EMPTY_OBSERVE_AUTO_CLOSE",
                                    "INFO",
                                    auto_reason,
                                    campaign_id=campaign.campaign_id,
                                    payload={
                                        "no_exposure": True,
                                        "resolution_pending": True,
                                        "grace_ms": grace_ms,
                                        "end_time_ms": int(campaign.market.end_time_ms),
                                        "buy_count": int(getattr(campaign, "buy_count", 0) or 0),
                                        "order_attempts": int(getattr(campaign, "order_attempts", 0) or 0),
                                    },
                                )
                            except Exception:
                                pass
                            settlement = {
                                "settlement_id": campaign.campaign_id,
                                "campaign_id": campaign.campaign_id,
                                "settled_at_ms": now_ms,
                                "winner": None,
                                "status": "SETTLED",
                                "result": "NO_FILL",
                                "reason": auto_reason,
                                "gross_pnl": "0",
                                "realized_pnl": "0",
                                "net_pnl": "0",
                                "fees": str(campaign.position.fees),
                                "closed_orders": 0,
                                "token_ids": [],
                                "auto_closed_empty_observe": True,
                            }
                            await self._finalize_settlement_with_c180(campaign, settlement)
                            campaign.state = CampaignState.DONE
                            return settlement
                        return {
                            "campaign_id": campaign.campaign_id,
                            "status": "PENDING_RESOLUTION",
                            "reason": "official no-fill outcome is not published yet",
                        }
                settlement = {
                    "settlement_id": campaign.campaign_id,
                    "campaign_id": campaign.campaign_id,
                    "settled_at_ms": self._now_ms(),
                    "winner": official_winner.value if official_winner is not None else None,
                    "status": "SETTLED",
                    "result": "NO_FILL",
                    "reason": (
                        "V7 official outcome recorded for market closed with no position or fill"
                        if official_winner is not None
                        else "market closed with no position, fill, or unresolved intent"
                    ),
                    "gross_pnl": "0",
                    "realized_pnl": "0",
                    "net_pnl": "0",
                    "fees": str(campaign.position.fees),
                    "closed_orders": 0,
                    "token_ids": [],
                }
                await self._finalize_settlement_with_c180(campaign, settlement)
                campaign.state = CampaignState.DONE
                return settlement

        # Use official positions, constrained to every token the campaign
        # actually holds.  Unrelated wallet positions must never make this
        # campaign claimable.
        held_tokens = {
            str(campaign.market.up_token_id): OutcomeSide.UP,
            str(campaign.market.down_token_id): OutcomeSide.DOWN,
        }
        held_tokens = {token: side for token, side in held_tokens.items() if campaign.position.shares(side) > 0}
        # A protective SELL can fully reduce a position before the market
        # closes.  In that case there is no winning token left to redeem, but
        # the campaign still needs a terminal SETTLED row so the loop cursor
        # advances.  The previous path treated this as
        # CLOSED_PENDING_REDEEM forever because it only considered official
        # wallet position rows.
        campaign_fills: list[Mapping[str, Any]] = []
        locally_closed_position = False
        if not held_tokens:
            campaign_fills = await self.repository.get_fills(campaign.campaign_id)
            locally_closed_position = any(
                str(
                    item.get("order_side")
                    or item.get("orderSide")
                    or item.get("side")
                    or ""
                ).upper() == OrderSide.SELL.value
                for item in campaign_fills
            )
        records: list[Mapping[str, Any]] = []
        for method_name in ("query_positions", "query_settled_position_history"):
            if not hasattr(self.client, method_name):
                continue
            payload = await self._call_api(method_name, wallet_address=self.settings.wallet_address)
            data = _data(payload)
            rows = data.get("positions", data.get("items", data.get("results", data))) if isinstance(data, Mapping) else data
            if isinstance(rows, Mapping):
                rows = [rows]
            if isinstance(rows, list):
                records.extend(item for item in rows if isinstance(item, Mapping))
        # The wallet-wide list endpoints can lag or paginate away a newly
        # resolved 5-minute position. Query every locally held token exactly;
        # Binance exposes PENDING_CLAIM here before it appears in settled
        # history, which is the authoritative redeem trigger.
        if hasattr(self.client, "get_position_by_token"):
            for token_id in held_tokens:
                payload = await self._call_api(
                    "get_position_by_token",
                    wallet_address=self.settings.wallet_address,
                    token_id=token_id,
                )
                data = _data(payload)
                rows = data.get("position", data.get("positions", data)) if isinstance(data, Mapping) else data
                if isinstance(rows, Mapping):
                    records.append(rows)
                elif isinstance(rows, list):
                    records.extend(item for item in rows if isinstance(item, Mapping))
        expected_market_ids = {str(campaign.market.market_id), str(campaign.market.market_topic_id), str(campaign.market.up_market_id or ""), str(campaign.market.down_market_id or "")}
        def matches(item: Mapping[str, Any]) -> bool:
            token = str(item.get("tokenId") or item.get("token_id") or "")
            market_id = str(item.get("marketId") or item.get("market_id") or item.get("marketTopicId") or item.get("market_topic_id") or "")
            return token in held_tokens and (not market_id or market_id in expected_market_ids)
        closed = [
            item for item in records
            if matches(item)
            and (str(item.get("positionStatus", item.get("status", ""))).upper() in {"CLOSED", "SETTLED", "RESOLVED", "PENDING_CLAIM", "CLAIMABLE", "CLAIMED", "REDEEMED"} or item.get("isWinner") is True)
        ]
        winner_item = next((item for item in closed if item.get("isWinner") is True), None)
        winner = str((winner_item or (closed[0] if closed else {})).get("finalOutcome") or (winner_item or {}).get("winner") or (winner_item or {}).get("outcome") or "").upper() or None
        winning_tokens = [str(item.get("tokenId") or item.get("token_id")) for item in closed if item.get("isWinner") is True and str(item.get("tokenId") or item.get("token_id")) in held_tokens]
        if not winning_tokens and winner in {OutcomeSide.UP.value, OutcomeSide.DOWN.value}:
            selected = campaign.market.up_token_id if winner == OutcomeSide.UP.value else campaign.market.down_token_id
            if selected in held_tokens:
                winning_tokens = [str(selected)]
        # PnL is a market-position query, not a wallet-wide aggregate.  Keep
        # all three identifiers exact so unrelated wallet activity cannot be
        # attributed to this campaign.  One call per held token is required
        # because the endpoint accepts a singular tokenId.
        pnl_payloads: list[tuple[str, OutcomeSide, Any]] = []
        for token_id, side in held_tokens.items():
            pnl_payloads.append(
                (
                    token_id,
                    side,
                    await self._call_api(
                        "query_pnl",
                        wallet_address=self.settings.wallet_address,
                        marketTopicId=campaign.market.market_topic_id,
                        marketId=campaign.market.market_id_for(side),
                        tokenId=token_id,
                    ),
                )
            )
        pnl = calculate_pnl(campaign).worst_case
        pnl_values: list[Decimal] = []
        exact_pnl_losers: dict[str, dict[str, str | bool]] = {}
        for token_id, side, pnl_payload in pnl_payloads:
            pnl_data = _data(pnl_payload)
            if isinstance(pnl_data, Mapping):
                nested_pnl = pnl_data.get("pnl")
                pnl_record = nested_pnl if isinstance(nested_pnl, Mapping) else pnl_data
                raw_pnl = (
                    pnl_data.get("totalPnl")
                    or pnl_data.get("totalRealizedPnl")
                    or pnl_data.get("netPnl")
                    or pnl_data.get("net_pnl")
                    or pnl_record.get("totalPnl")
                    or pnl_record.get("netPnl")
                    or pnl_record.get("net_pnl")
                    or pnl_record.get("realizedPnl")
                    or pnl_record.get("realized_pnl")
                    or (nested_pnl if not isinstance(nested_pnl, Mapping) else None)
                )
                if raw_pnl is not None:
                    pnl_values.append(as_decimal(raw_pnl))
                # The token-scoped PnL endpoint can become authoritative
                # before wallet position lists expose a resolved 5-minute
                # market.  Accept it as terminal loss evidence only when the
                # response echoes every requested identifier, isWinner is the
                # literal boolean False, shares remain positive, and there is
                # no redeem.  This prevents a stale or unrelated wallet PnL
                # row from advancing the campaign.
                response_token = str(pnl_record.get("tokenId") or pnl_record.get("token_id") or "")
                response_topic = str(pnl_record.get("marketTopicId") or pnl_record.get("market_topic_id") or "")
                response_market = str(pnl_record.get("marketId") or pnl_record.get("market_id") or "")
                raw_shares = pnl_record.get("currentShares", pnl_record.get("current_shares"))
                raw_redeem_count = pnl_record.get("redeemCount", pnl_record.get("redeem_count"))
                try:
                    current_shares = as_decimal(raw_shares) if raw_shares is not None else Decimal("0")
                    redeem_count = as_decimal(raw_redeem_count) if raw_redeem_count is not None else Decimal("-1")
                except (ArithmeticError, TypeError, ValueError):
                    current_shares = Decimal("0")
                    redeem_count = Decimal("-1")
                expected_market_id = str(campaign.market.market_id_for(side))
                if (
                    response_token == token_id
                    and response_topic == str(campaign.market.market_topic_id)
                    and response_market == expected_market_id
                    and pnl_record.get("isWinner") is False
                    and current_shares > 0
                    and redeem_count == 0
                    and raw_pnl is not None
                ):
                    exact_pnl_losers[token_id] = {
                        "token_id": token_id,
                        "market_topic_id": response_topic,
                        "market_id": response_market,
                        "is_winner": False,
                        "current_shares": str(current_shares),
                        "redeem_count": str(redeem_count),
                        "total_pnl": str(as_decimal(raw_pnl)),
                    }
        if pnl_values:
            pnl = sum(pnl_values, Decimal("0"))
        pnl_confirms_all_held_tokens_lost = bool(held_tokens) and set(exact_pnl_losers) == set(held_tokens)
        if pnl_confirms_all_held_tokens_lost and len(held_tokens) == 1 and winner is None:
            losing_side = next(iter(held_tokens.values()))
            winner = OutcomeSide.DOWN.value if losing_side is OutcomeSide.UP else OutcomeSide.UP.value
        redeem_result: Any = None
        redeem_results: list[Mapping[str, Any]] = []
        redeem_error: str | None = None
        token_ids = list(dict.fromkeys(winning_tokens))
        claimed_rows = [
            item for item in closed
            if str(item.get("tokenId") or item.get("token_id")) in token_ids
        ]
        claimed = bool(token_ids) and bool(claimed_rows) and all(
            str(
                item.get("redeemStatus")
                or item.get("claimStatus")
                or item.get("positionStatus")
                or item.get("status")
                or ""
            ).upper() in {"CLAIMED", "REDEEMED", "CONFIRMED"}
            for item in claimed_rows
        )
        tx_hash: str | None = next(
            (
                str(item.get("lastEventTxHash") or item.get("txHash") or item.get("transactionHash"))
                for item in claimed_rows
                if item.get("lastEventTxHash") or item.get("txHash") or item.get("transactionHash")
            ),
            None,
        )
        batch_id: str | None = None
        if closed and token_ids and not claimed:
            claimable = [
                item for item in closed
                if bool(item.get("canClaim"))
                or str(item.get("positionStatus", item.get("status", ""))).upper() in {"PENDING_CLAIM", "CLAIMABLE"}
                or str(item.get("redeemStatus", item.get("claimStatus", ""))).upper() in {"CLAIMABLE", "UNCLAIMED", ""}
            ]
            if not self.settings.wallet_id or not claimable or not hasattr(self.client, "batch_redeem"):
                redeem_error = "redeem prerequisites unavailable"
            else:
                try:
                    redeem_result = await self._call_api(
                        "batch_redeem",
                        wallet_address=self.settings.wallet_address,
                        wallet_id=self.settings.wallet_id,
                        token_ids=token_ids,
                        chain_id=campaign.market.chain_id,
                    )
                    if isinstance(redeem_result, Mapping):
                        redeem_data = _data(redeem_result)
                        if isinstance(redeem_data, Mapping):
                            raw_results = redeem_data.get("results")
                            if isinstance(raw_results, list):
                                redeem_results = [item for item in raw_results if isinstance(item, Mapping)]
                            tx_hash = redeem_data.get("txHash") or redeem_data.get("transactionHash") or redeem_data.get("tx_hash")
                            batch_id = redeem_data.get("batchId") or redeem_data.get("batch_id")
                            if not tx_hash and redeem_results:
                                tx_hash = redeem_results[0].get("txHash") or redeem_results[0].get("transactionHash") or redeem_results[0].get("tx_hash")
                            if not batch_id and redeem_results:
                                batch_id = redeem_results[0].get("batchId") or redeem_results[0].get("batch_id")
                    if tx_hash and hasattr(self.client, "get_redeem_status"):
                        confirmation = await self._poll_redeem_confirmation(str(tx_hash))
                        redeem_result = {"results": redeem_results or [confirmation], **dict(confirmation), "txHash": tx_hash}
                        if str(confirmation.get("status", "")).upper() != "CONFIRMED":
                            redeem_error = "redeem transaction not confirmed"
                    elif not tx_hash:
                        redeem_error = "redeem response has no transaction hash"
                except Exception as exc:  # noqa: BLE001 - settlement must remain recoverable
                    redeem_error = str(exc)
        # A confirmed losing position has no winning token and therefore no
        # payout to redeem.  Treating that as CLOSED_PENDING_REDEEM leaves the
        # campaign active forever and blocks discovery of every later market.
        # Only bypass redeem when the official winner is known and the wallet
        # does not hold that winner token; an unresolved/ambiguous close stays
        # fail-closed.
        winning_token = None
        if winner == OutcomeSide.UP.value:
            winning_token = str(campaign.market.up_token_id)
        elif winner == OutcomeSide.DOWN.value:
            winning_token = str(campaign.market.down_token_id)
        redemption_not_applicable = bool(
            (closed and winning_token and winning_token not in held_tokens)
            or pnl_confirms_all_held_tokens_lost
            or locally_closed_position
        )
        settled = bool(
            redemption_not_applicable
            or (closed and token_ids and redeem_error is None and (claimed or tx_hash))
        )
        settlement = {
            "settlement_id": campaign.campaign_id,
            "campaign_id": campaign.campaign_id,
            "loop_id": self._loop_id,
            "settled_at_ms": self._now_ms(),
            "winner": winner or (campaign.winner_candidate.value if campaign.winner_candidate else None),
            "status": "SETTLED" if settled else "CLOSED_PENDING_REDEEM",
            "gross_pnl": str(pnl),
            "realized_pnl": str(pnl),
            "net_pnl": str(pnl),
            "fees": str(campaign.position.fees),
            "closed_orders": len(closed),
            "token_ids": token_ids,
        }
        # Official token PnL is already the booked amount. Local fill fees
        # are diagnostic and must NOT be deducted from that number again.
        if self._s3s5_profile_active():
            local_payout = (
                campaign.position.shares(OutcomeSide(winner))
                if winner in {"UP", "DOWN"} else None
            )
            local_net = (
                local_payout + campaign.position.realized_cash
                - campaign.position.total_buy_cost - campaign.position.fees
                if local_payout is not None else None
            )
            settlement["accounting"] = {
                "pnl_source": "official_token_query" if pnl_values else "local_worst_case",
                "official_token_count": len(pnl_values),
                "held_token_count": len(held_tokens),
                "local_fill_estimate_net": str(local_net) if local_net is not None else None,
                "booked_minus_local": str(pnl - local_net) if local_net is not None else None,
                "local_fees_deducted_again": False,
            }
        if redeem_result is not None:
            settlement["redeem"] = redeem_result
            if isinstance(redeem_result, Mapping):
                data = redeem_result.get("data", redeem_result)
                if isinstance(data, Mapping):
                    settlement["tx_hash"] = data.get("txHash") or data.get("transactionHash") or data.get("tx_hash") or tx_hash
                    settlement["redeem_status"] = data.get("status")
                    settlement["results"] = data.get("results", redeem_results or [])
        if tx_hash:
            settlement["tx_hash"] = tx_hash
        if batch_id:
            settlement["batch_id"] = batch_id
        if redemption_not_applicable:
            settlement["redeem_status"] = "NOT_APPLICABLE"
            if pnl_confirms_all_held_tokens_lost:
                settlement["redeem_reason"] = "official token-scoped PnL confirms every held token is a loser"
                settlement["loss_evidence"] = list(exact_pnl_losers.values())
            elif locally_closed_position:
                settlement["redeem_reason"] = "all locally tracked shares were reduced before market close"
            else:
                settlement["redeem_reason"] = "official winner token is not held"
        if redeem_error:
            settlement["redeem_error"] = redeem_error
        if settlement["status"] != "SETTLED":
            await self.repository.record_settlement(settlement)
            return settlement
        await self._finalize_settlement_with_c180(campaign, settlement)
        if settlement["status"] == "SETTLED":
            campaign.state = CampaignState.DONE
        return settlement

    @staticmethod
    def _official_resolution(payload: Any) -> OutcomeSide | None:
        def exact_side(value: Any) -> OutcomeSide | None:
            if isinstance(value, Mapping):
                value = value.get("name") or value.get("outcome") or value.get("winner") or value.get("side")
            text = str(value or "").strip().upper()
            return OutcomeSide(text) if text in {OutcomeSide.UP.value, OutcomeSide.DOWN.value} else None

        data = _data(payload)
        if isinstance(data, Mapping):
            for key in ("resolvedOutcome", "finalOutcome", "winner", "outcome", "resolvedSide", "result"):
                side = exact_side(data.get(key))
                if side is not None:
                    return side
            markets = data.get("markets")
            if isinstance(markets, list):
                # Current Binance binary topics resolve on the nested
                # outcome object: markets[].outcomes[].winner.  Require one
                # and only one exact UP/DOWN winner; ambiguity fails closed.
                nested_winners: list[OutcomeSide] = []
                for market in markets:
                    if not isinstance(market, Mapping):
                        continue
                    outcomes = market.get("outcomes")
                    if not isinstance(outcomes, list):
                        continue
                    for outcome in outcomes:
                        if not isinstance(outcome, Mapping):
                            continue
                        if outcome.get("winner") is True or outcome.get("isWinner") is True:
                            side = exact_side(outcome.get("name") or outcome.get("title") or outcome.get("outcome") or outcome.get("side"))
                            if side is None:
                                return None
                            nested_winners.append(side)
                if len(nested_winners) == 1:
                    return nested_winners[0]
                if nested_winners:
                    return None
                legacy_winners: list[OutcomeSide] = []
                for item in markets:
                    if not isinstance(item, Mapping):
                        continue
                    if item.get("isWinner") is True or item.get("winner") is True:
                        side = exact_side(item.get("title") or item.get("outcome") or item.get("side"))
                        if side is None:
                            return None
                        legacy_winners.append(side)
                if len(legacy_winners) == 1:
                    return legacy_winners[0]
        return None

    @staticmethod
    def _official_shadow_resolution(payload: Any) -> str | None:
        """Accept a directional result or the explicit official 50/50 payout.

        Two winner flags alone are ambiguous.  A draw additionally requires a
        terminal market and both named outcomes carrying the 0.5 payout.
        Live order/redeem callers keep the narrower _official_resolution API.
        """

        winner = PredictionWorker._official_resolution(payload)
        if winner is not None:
            return winner.value
        data = _data(payload)
        if not isinstance(data, Mapping):
            return None
        markets = data.get("markets")
        if not isinstance(markets, list) or len(markets) != 1:
            return None
        market = markets[0]
        if not isinstance(market, Mapping):
            return None
        if str(market.get("status") or data.get("status") or "").upper() not in {"RESOLVED", "SETTLED"}:
            return None
        outcomes = market.get("outcomes")
        if not isinstance(outcomes, list) or len(outcomes) != 2:
            return None
        sides: set[str] = set()
        for outcome in outcomes:
            if not isinstance(outcome, Mapping):
                return None
            side = str(outcome.get("name") or outcome.get("outcome") or "").upper()
            if side not in {"UP", "DOWN"} or side in sides:
                return None
            if outcome.get("winner") is not True and outcome.get("isWinner") is not True:
                return None
            try:
                if as_decimal(outcome.get("price")) != Decimal("0.5"):
                    return None
            except (ValueError, ArithmeticError):
                return None
            sides.add(side)
        return "DRAW" if sides == {"UP", "DOWN"} else None

    async def _settle_shadow_campaign(self, campaign: Campaign) -> dict[str, Any]:
        """Resolve official market outcome and append shadow settlement/evidence."""

        if await self.repository.get_campaign_execution_mode(campaign.campaign_id) != "SHADOW":
            return {"campaign_id": campaign.campaign_id, "status": "SETTLEMENT_PROVENANCE_UNKNOWN"}
        await self._ensure_shadow_window()
        existing_shadow: Mapping[str, Any] | None = None
        existing_getter = getattr(self.repository, "get_shadow_campaign_for_campaign", None)
        if callable(existing_getter):
            candidate = await existing_getter(campaign.campaign_id)
            if isinstance(candidate, Mapping):
                existing_shadow = candidate
        detail: Any = campaign.market.raw
        if hasattr(self.client, "get_market_detail"):
            try:
                detail = await self._call_api("get_market_detail", campaign.market.market_topic_id)
            except PredictionRateLimitDeferred:
                return {"campaign_id": campaign.campaign_id, "status": "SHADOW_PENDING_RESOLUTION"}
        winner = self._official_resolution(detail)
        # A restart may lose the in-memory campaign cache after the official
        # result was already persisted into the immutable Shadow identity.
        # Reuse only an exact UP/DOWN value from that row; an absent or
        # ambiguous outcome remains pending and is never guessed.
        if winner is None and existing_shadow is not None:
            try:
                persisted_winner = OutcomeSide(str(existing_shadow.get("resolved_outcome") or "").upper())
            except ValueError:
                persisted_winner = None
            if persisted_winner in {OutcomeSide.UP, OutcomeSide.DOWN}:
                winner = persisted_winner
        if winner is None:
            return {"campaign_id": campaign.campaign_id, "status": "SHADOW_PENDING_RESOLUTION"}
        fees = campaign.position.fees
        pnl = campaign.position.realized_cash - campaign.position.total_buy_cost - fees + campaign.position.shares(winner)
        config_hash = str(self._shadow_config_hash or self.effective_config_hash)
        window = self._shadow_window or {}
        start = int(window.get("window_start_ms", self._now_ms()))
        end = int(window.get("window_end_ms", start + 86_400_000))
        shadow_id = self._shadow_campaign_ids.get(campaign.campaign_id)
        if not shadow_id:
            if existing_shadow is not None:
                shadow_id = str(existing_shadow.get("shadow_campaign_id") or "") or None
                # Reuse the immutable provenance attached to the existing row.
                # This prevents a restart from double-booking the same
                # campaign under a new config/window identity.
                config_hash = str(existing_shadow.get("config_hash") or config_hash)
                start = int(existing_shadow.get("window_start_ms") or start)
                end = int(existing_shadow.get("window_end_ms") or end)
                if shadow_id:
                    self._shadow_campaign_ids[campaign.campaign_id] = shadow_id
        if not shadow_id:
            shadow = await self.repository.record_shadow_campaign(
                campaign,
                config_hash=config_hash,
                window_start_ms=start,
                window_end_ms=end,
                resolved_outcome=winner.value,
                simulated_fees=fees,
                simulated_pnl=pnl,
                expected_fill_count=1 if self._configured_shadow_lanes() else 0,
                resolved_at_ms=self._now_ms(),
                payload={"official_resolution": winner.value},
            )
            shadow_id = str(shadow.get("shadow_campaign_id") or "")
            self._shadow_campaign_ids[campaign.campaign_id] = shadow_id
        settlement = await self.repository.record_shadow_settlement(
            shadow_campaign_id=shadow_id,
            resolved_outcome=winner.value,
            simulated_fees=fees,
            simulated_pnl=pnl,
            settled_at_ms=self._now_ms(),
            payload={"official_resolution": winner.value, "after_fee_pnl": str(pnl)},
        )
        if self._loop_id and hasattr(self.repository, "reconcile_shadow_loop_completion"):
            reconciled = await self.repository.reconcile_shadow_loop_completion(self._loop_id)
            self.heartbeat.markets_completed = int(reconciled.get("completed", self.heartbeat.markets_completed))
        evidence: Mapping[str, Any] | None = None
        evidence_error: str | None = None
        getter = getattr(self.repository, "get_shadow_promotion_evidence", None)
        if callable(getter):
            try:
                evidence = await getter(config_hash=config_hash, window_start_ms=start, window_end_ms=end)
                await self.repository.set_runtime_config("promotion_evidence", evidence)
            except Exception as exc:  # evidence can be retried without losing settlement
                evidence_error = str(exc)
        campaign.state = CampaignState.DONE
        await self.repository.save_campaign(campaign, loop_id=self._loop_id)
        result = {
            "campaign_id": campaign.campaign_id,
            "status": "SETTLED",
            "winner": winner.value,
            "net_pnl": str(pnl),
            "fees": str(fees),
            "shadow_settlement_id": settlement.get("shadow_settlement_id"),
        }
        if evidence is not None:
            result["promotion_evidence"] = evidence
        if evidence_error:
            result["evidence_error"] = evidence_error
        return result

    async def _pending_redeem_claimed_evidence(
        self, campaign: Campaign, prior: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Fail closed unless every expected winning token is explicitly claimed."""
        if campaign.pending_unknown or campaign.pending_intent_id:
            return []
        if not hasattr(self.client, "get_position_by_token"):
            return []
        winner = str(prior.get("winner") or "").upper()
        if winner not in {"UP", "DOWN"}:
            return []
        token = str(campaign.market.up_token_id if winner == "UP" else campaign.market.down_token_id)
        shares = campaign.position.up_shares if winner == "UP" else campaign.position.down_shares
        tokens = prior.get("token_ids")
        if not token or shares <= 0 or not isinstance(tokens, list) or tokens != [token]:
            return []
        if self._now_ms() < campaign.market.end_time_ms:
            return []
        response = await self._call_api(
            "get_position_by_token", wallet_address=self.settings.wallet_address, token_id=token,
        )
        data = response.get("data", response) if isinstance(response, Mapping) else response
        rows = data.get("position", data.get("positions", data)) if isinstance(data, Mapping) else data
        if isinstance(rows, Mapping):
            rows = [rows]
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
            return []
        item = rows[0]
        market = str(campaign.market.up_market_id if winner == "UP" else campaign.market.down_market_id)
        market = market if market not in {"", "None"} else str(campaign.market.market_id)
        if not market or str(item.get("tokenId") or "") != token or str(item.get("marketId") or "") != market:
            return []
        if item.get("marketTopicId") is not None and str(item["marketTopicId"]) != str(campaign.market.market_topic_id):
            return []
        if item.get("walletAddress") is not None and str(item["walletAddress"]).lower() != str(self.settings.wallet_address).lower():
            return []
        if str(item.get("positionStatus") or "").upper() not in {"CLAIMED", "REDEEMED", "CONFIRMED"}:
            return []
        if item.get("canClaim") is not False or item.get("isWinner") is not True:
            return []
        if str(item.get("finalOutcome") or "").upper() != winner:
            return []
        return [{"tokenId": token, "marketId": market,
                 "positionStatus": str(item["positionStatus"]).upper(), "canClaim": False}]

    async def _poll_redeem_confirmation(self, tx_hash: str, attempts: int = 3) -> Mapping[str, Any]:
        last: Mapping[str, Any] = {"status": "PENDING", "txHash": tx_hash}
        for _ in range(max(1, attempts)):
            result = await self._call_api("get_redeem_status", wallet_address=self.settings.wallet_address, tx_hash=tx_hash)
            if isinstance(result, Mapping):
                data = result.get("data", result)
                if isinstance(data, Mapping):
                    last = data
                    if str(data.get("status", "")).upper() in {"CONFIRMED", "SUCCESS"}:
                        return {**data, "status": "CONFIRMED"}
                    if str(data.get("status", "")).upper() in {"FAILED", "REVERTED"}:
                        return {**data, "status": "FAILED"}
            await asyncio.sleep(max(0.0, float(self.settings.poll_interval_seconds)))
        return last

    async def apply_order_snapshot(
        self,
        campaign: Campaign,
        payload: Mapping[str, Any],
        *,
        outcome: OutcomeSide,
        intent: OrderIntent | None = None,
    ) -> dict[str, Any]:
        """Apply only the monotonic fill delta from an OPEN→CLOSED history.

        Binance history can expose the same cumulative ``filledShareQty`` on
        partial and CLOSED snapshots.  The progress map and repository fill
        id make replay/restart idempotent instead of double-counting shares,
        costs, or fees.
        """

        self._trace_event('order_snapshot_seen',campaign_id=campaign.campaign_id,
            intent_id=intent.intent_id if intent else None,
            filled_shares=str(payload.get('filledShareQty') or payload.get('filledShares') or '0'),
            terminal_time_ms=payload.get('terminalTime'),status=str(payload.get('status') or ''))
        record = OrderRecord.from_api(payload)
        if not record.order_id:
            raise ValueError("order snapshot has no orderId")
        # The repository owns the durable cumulative cursor and transaction.
        # Only copy the candidate campaign back after commit succeeds.
        intent = intent or OrderIntent(
            intent_id=str(campaign.pending_intent_id or f"reconcile:{record.order_id}"),
            campaign_id=campaign.campaign_id,
            action=ActionType.RECONCILE,
            outcome=outcome,
            order_side=record.side,
            amount=record.filled_usdt_amount,
            limit_price=record.avg_price or Decimal("0"),
            created_at_ms=self._now_ms(),
            ttl_ms=0,
        )
        result = await self.repository.apply_order_snapshot_atomic(
            campaign,
            intent,
            record.order_id,
            payload,
            outcome=outcome,
        )
        candidate = result.get("campaign")
        if isinstance(candidate, Campaign):
            campaign.__dict__.update(candidate.__dict__)
        return {key: value for key, value in result.items() if key != "campaign"}

    async def _check_loop_loss_guard(self) -> bool:
        """Stop admitting new markets when this loop reaches its loss cap."""
        if self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'} and self._loop_id:
            loop = await self.repository.get_loop(self._loop_id)
            if (loop and str(loop.get("strategy_profile") or "").lower() in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                    and str(loop.get("mode") or "").upper() == "LIVE"):
                # The durable C180 20-run peak-MDD gate owns new BUY admission.
                return False

        if not self._loop_id or self._loop_loss_limit_reached:
            return self._loop_loss_limit_reached
        limit = Decimal(str(getattr(getattr(self.risk_engine, "config", None), "loop_loss_limit", "-2")))
        if limit >= 0:
            return False
        getter = getattr(self.repository, "get_loop_pnl", None)
        if not callable(getter):
            return False
        try:
            loop_pnl = as_decimal(await getter(self._loop_id))
        except Exception as exc:
            self._accept_new_markets = False
            self._allow_new_orders = False
            self._allow_new_buys = False
            self._allow_reductions = True
            self.heartbeat.last_error = f"loop PnL check failed: {type(exc).__name__}"
            return True
        if loop_pnl > limit:
            return False
        self._loop_loss_limit_reached = True
        self._accept_new_markets = False
        self._allow_new_orders = False
        self._allow_new_buys = False
        self._allow_reductions = True
        self.heartbeat.last_error = f"loop loss limit reached: {loop_pnl} <= {limit}"
        try:
            await self.repository.set_runtime_config(
                "prediction_loop_loss_guard",
                {
                    "loop_id": self._loop_id,
                    "loop_net_pnl": str(loop_pnl),
                    "loss_limit": str(limit),
                    "at_ms": self._now_ms(),
                },
            )
        except Exception:
            pass
        stopper = getattr(self.repository, "stop_loop", None)
        if callable(stopper):
            try:
                await stopper(self._loop_id, state="LOSS_LIMIT")
            except Exception:
                pass
        return True

    async def _check_adaptive_jump_stop(self) -> bool:
        """Soft-stop new entries after a fresh loss cluster or whipsaw loss."""
        if self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'} and self._loop_id:
            loop = await self.repository.get_loop(self._loop_id)
            if (loop and str(loop.get("strategy_profile") or "").lower() in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                    and str(loop.get("mode") or "").upper() == "LIVE"):
                return False

        if not self._loop_id or self._hard_stop_latched:
            return False
        persisted_guard = await self.repository.get_runtime_config(
            "prediction_adaptive_jump_stop",
            {},
        )
        if isinstance(persisted_guard, Mapping) and bool(persisted_guard.get("active")):
            if str(persisted_guard.get("loop_id") or "") == self._loop_id:
                self._adaptive_jump_stop_latched = True
                self._accept_new_markets = False
                self._allow_new_orders = False
                self._allow_new_buys = False
                self._allow_reductions = True
                return True
        getter = getattr(self.repository, "get_recent_live_risk_window", None)
        stopper = getattr(self.repository, "request_adaptive_jump_stop", None)
        if not callable(getter) or not callable(stopper):
            return False
        window = await getter(self._loop_id, limit=5, now_ms=self._now_ms())
        latest = window.get("latest") if isinstance(window, Mapping) else None
        if not isinstance(latest, Mapping):
            return False
        latest_age_ms = int(window.get("latest_age_ms") or 0)
        if latest_age_ms > ADAPTIVE_JUMP_STOP_LATEST_MAX_AGE_MS:
            return False
        unit = self._selected_order_unit_usdt
        loop_losses = int(window.get("losses_in_loop") or 0)
        losses_30m = int(window.get("losses_in_30m") or 0)
        trigger = ""
        if loop_losses >= ADAPTIVE_JUMP_STOP_MIN_LOOP_LOSSES:
            trigger = "HEAVY_WHIPSAW_LOSS"
        elif losses_30m >= 3:
            trigger = "THREE_LOSSES_IN_30M"
        if not trigger:
            return False
        now = self._now_ms()
        regime: Mapping[str, Any] = {}
        get_regime = getattr(self.repository, "get_market_regime_monitor", None)
        if callable(get_regime):
            try:
                regime = await get_regime(limit=20)
            except Exception:
                regime = {}
        evidence = {
            "trigger": trigger,
            "trigger_campaign_id": str(latest.get("campaign_id") or ""),
            "order_unit_usdt": str(unit),
            "loop_loss_count": loop_losses,
            "loop_loss_threshold": ADAPTIVE_JUMP_STOP_MIN_LOOP_LOSSES,
            "path_threshold_bps": str(ADAPTIVE_JUMP_STOP_PATH_BPS),
            "resume_after_ms": now + ADAPTIVE_JUMP_STOP_COOLDOWN_MS,
            "risk_window": dict(window),
            "market_regime": dict(regime),
        }
        stopped = await stopper(self._loop_id, evidence)
        if not bool(stopped.get("applied")):
            return False
        self._adaptive_jump_stop_latched = True
        self._accept_new_markets = False
        self._allow_new_orders = False
        self._allow_new_buys = False
        self._allow_reductions = True
        self.heartbeat.last_error = f"adaptive jump stop: {trigger}"
        try:
            await self.repository.record_risk_event(
                "ADAPTIVE_JUMP_STOP",
                "WARNING",
                "new Prediction markets stopped after a fresh loss cluster",
                payload=evidence,
            )
        except Exception:
            pass
        return True


    async def _run_loop(self) -> None:
        # Continue the persisted loop counter after a process restart; a
        # local zero would overrun the target and double-count markets.
        completed = int(self.heartbeat.markets_completed)
        while (self._accept_new_markets or self._active_campaigns) and not self._cancel_requested:
            self._begin_shadow_tick()
            self.heartbeat.last_loop_at_ms = self._now_ms()
            try:
                # The DB is the loop cursor.  Reading it at every tick makes
                # restart and an external completion event converge without
                # relying on a process-local counter.
                persisted = await self.repository.get_loop(self._loop_id) if self._loop_id and hasattr(self.repository, "get_loop") else None
                if persisted:
                    completed = int(persisted.get("completed", completed))
                    self.heartbeat.markets_completed = completed
                    persisted_state = str(persisted.get("state", "")).upper()
                    if persisted_state != "RUNNING":
                        self._accept_new_markets = False
                        self._allow_new_orders = False
                        self._allow_new_buys = False
                        self._allow_reductions = True
                    elif bool(persisted.get("new_entries_stopped")):
                        self._accept_new_markets = False
                        self._allow_new_orders = False
                        self._allow_new_buys = False
                        self._allow_reductions = True
                await self._check_adaptive_jump_stop()
                await self._check_loop_loss_guard()
                if self._hard_stop_latched:
                    # A hard stop is a terminal admission guard.  Keep the
                    # worker alive only long enough to reconcile/settle any
                    # existing campaign; never spin a RUNNING loop forever
                    # after all exposure has gone away.
                    self._accept_new_markets = False
                    self._allow_new_orders = False
                    self._allow_new_buys = False
                    self._allow_reductions = True
                if completed >= self._target_markets and not self._active_campaigns:
                    self._accept_new_markets = False
                await self._manage_active_campaigns()
                # A settlement during management can move loop PnL through
                # the -2 USDT limit.  Re-check before admission in the same
                # tick so a losing market can never be followed immediately
                # by a new BUY.
                await self._check_adaptive_jump_stop()
                await self._check_loop_loss_guard()
                if self._hard_stop_latched:
                    self._accept_new_markets = False
                    self._allow_new_orders = False
                    self._allow_new_buys = False
                    self._allow_reductions = True
                    if not self._active_campaigns and self._loop_id:
                        stopper = getattr(self.repository, "stop_loop", None)
                        if callable(stopper):
                            try:
                                await stopper(self._loop_id, state="HARD_STOP")
                            except Exception as exc:  # noqa: BLE001 - safe shutdown path
                                self.heartbeat.last_error = f"hard-stop loop close failed: {exc}"
                        await self._persist_heartbeat()
                        break
                persisted = await self.repository.get_loop(self._loop_id) if self._loop_id else None
                if persisted:
                    completed = int(persisted.get("completed", completed))
                    self.heartbeat.markets_completed = completed
                    if str(persisted.get("state", "")).upper() != "RUNNING" or completed >= self._target_markets:
                        self._accept_new_markets = False
                        self._allow_new_orders = False
                        self._allow_new_buys = False
                if self._accept_new_markets and not self._hard_stop_latched and completed < self._target_markets:
                    await self._run_market_once()
                persisted = await self.repository.get_loop(self._loop_id) if self._loop_id and hasattr(self.repository, "get_loop") else None
                if persisted:
                    completed = int(persisted.get("completed", completed))
                    self.heartbeat.markets_completed = completed
                    if str(persisted.get("state", "")).upper() != "RUNNING":
                        self._accept_new_markets = False
                if completed >= self._target_markets and not self._active_campaigns:
                    self._accept_new_markets = False
                await self._persist_heartbeat()
            except PredictionReadTimestampError as exc:
                # Only the client's allowlisted signed GET paths can raise
                # this type. Stop this tick before admission and recheck all
                # risk/identity/depth gates after a bounded cooldown. Existing
                # HS/unknown execution is never cleared by this read failure.
                self.heartbeat.last_error = "READ_TIMESTAMP_DEFERRED: " + str(exc)
                details = {"read_only": True, "recoverable": True, "path": exc.path,
                           "attempts": exc.attempts, "timings": list(exc.timings), "at_ms": self._now_ms()}
                await self.repository.record_risk_event(
                    "READ_TIMESTAMP_DEFERRED", "WARN", str(exc), payload=details)
                await self.repository.set_runtime_config("prediction_read_timestamp_deferred", details)
                await self._persist_heartbeat()
                await asyncio.sleep(5.0)
                continue
            except PredictionRateLimitDeferred as exc:
                # Local budget exhaustion is ordinary backpressure.  Defer
                # the tick and keep reductions/hard-stop state intact; it is
                # not an unknown exchange execution and must not hard-stop
                # the worker.
                self.heartbeat.last_error = str(exc)
                try:
                    await self.repository.set_runtime_config(
                        "prediction_rate_limit_deferred",
                        {"method": exc.method_name, "health": exc.health, "at_ms": self._now_ms()},
                    )
                    await self._persist_heartbeat()
                except Exception:
                    pass
                now = self._now_ms()
                reset_at = int(exc.health.get("window_started_at_ms", now)) + int(getattr(self._rate_limiter, "window_ms", 60_000))
                delay = min(1.0, max(0.01, (reset_at - now) / 1000.0))
                await asyncio.sleep(delay)
                continue
            except Exception as exc:  # noqa: BLE001 - loop never retries unknown state blindly
                self.heartbeat.last_error = str(exc)
                # NESTED_TXN_RETRY: shared-connection leftover txn is recoverable.
                # Do not latch Hard Stop for this SQLite race; roll forward next tick.
                msg = str(exc)
                msg_l = msg.lower()
                recoverable = (
                    "cannot start a transaction within a transaction" in msg_l
                    or "urlopen error timed out" in msg_l
                    or "prediction http request failed" in msg_l and "timed out" in msg_l
                    or "temporarily unavailable" in msg_l
                    or "connection reset by peer" in msg_l
                    or "broken pipe" in msg_l
                )
                if recoverable:
                    tag = "HTTP_TIMEOUT_RETRY" if "timed out" in msg_l or "http request failed" in msg_l else "NESTED_TXN_RETRY"
                    try:
                        await self.repository.record_risk_event(
                            "WORKER_ERROR",
                            "WARN",
                            f"{tag}: {exc}",
                            payload={"recoverable": True},
                        )
                    except Exception:
                        pass
                    # brief backoff on transport flakes; nested-txn can be shorter
                    await asyncio.sleep(1.0 if "timed out" in msg_l else 0.2)
                    continue
                # Shutdown can close SQLite while a control-plane task is
                # between ticks.  Do not turn that expected teardown race
                # into an un-retrieved task exception.
                try:
                    await self.repository.record_risk_event(
                        "WORKER_ERROR", "ERROR", str(exc),
                        payload={"exception_type": type(exc).__name__,
                                 "sqlite_errorcode": getattr(exc, "sqlite_errorcode", None),
                                 "stack": traceback.format_tb(exc.__traceback__)},
                    )
                except Exception:
                    pass
                self._hard_stop_latched = True
                try:
                    await self.repository.set_runtime_config("prediction_hard_stop_latched", {"latched": True, "reason": str(exc), "at_ms": self._now_ms()})
                    state = await self.repository.get_runtime_config("prediction_risk_state", {})
                    await self.repository.set_runtime_config(
                        "prediction_risk_state", {**state, "hard_stop_latched": True})
                    if self._loop_id:
                        await self.repository._execute(
                            "UPDATE prediction_loops SET hard_stop_latched=1 WHERE loop_id=?",
                            (self._loop_id,),
                        )
                except Exception:
                    pass
                break
            if (self._accept_new_markets or self._active_campaigns) and completed < self._target_markets:
                await asyncio.sleep(self._entry_tick_delay())

    def _entry_tick_delay(self):
        """Keep the new 180s selection punctual; attempted markets never re-enter."""
        delay = max(0.0, float(self.settings.poll_interval_seconds))
        if self._selected_strategy_profile not in ("regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1"):
            return delay
        now = self._now_ms()
        for campaign in self._active_campaigns.values():
            if (campaign.pending_intent_id or campaign.pending_unknown
                    or campaign.position.has_any or campaign.buy_count or campaign.initial_attempts):
                continue
            beginning = int(campaign.market.start_time_ms)+179000
            if now < beginning:
                # A long configured poll must not sleep through selection.
                delay = min(delay, max(0.1, (beginning-now)/1000))
            elif now < int(campaign.market.start_time_ms)+183500:
                delay = min(delay, 0.1)
        return delay

    async def _manage_active_campaigns(self) -> None:
        """Refresh every active campaign, including while BUY is paused."""
        for campaign in list(self._active_campaigns.values()):
            await self.manage_campaign(campaign)

    async def _load_market_state(self, campaign: Campaign, *, observer: bool = False) -> dict[str, Any]:
        state = self._market_states.get(campaign.campaign_id)
        if state is None:
            if observer:
                getter = getattr(self.repository, "get_shadow_observer_state", None)
                persisted = await getter(campaign.campaign_id) if callable(getter) else None
            else:
                persisted = await self.repository.get_market_state(campaign.campaign_id)
            state = dict(persisted or {})
            payload = state.get("payload") if isinstance(state.get("payload"), Mapping) else {}
            state.update(payload)
            self._market_states[campaign.campaign_id] = state
        return state

    async def _get_market_detail_with_cache(
        self,
        market_topic_id: str,
        *,
        now_ms: int,
    ) -> tuple[Any, MarketInfo] | None:
        """Return parsed detail while retrying incomplete start-price metadata."""

        topic_id = str(market_topic_id)
        cached = self._detail_cache.get(topic_id)
        if cached is not None:
            cached_at_ms, payload = cached
            parsed = MarketInfo.from_api(payload)
            ttl_ms = (
                self._detail_cache_ttl_ms
                if parsed.reference_price is not None
                else self._incomplete_detail_cache_ttl_ms
            )
            if now_ms - cached_at_ms < ttl_ms:
                return payload, parsed
        try:
            payload = await self._call_api("get_market_detail", topic_id)
        except PredictionRateLimitDeferred:
            return None
        parsed = MarketInfo.from_api(payload)
        self._detail_cache[topic_id] = (now_ms, payload)
        return payload, parsed

    async def _refresh_campaign_reference(
        self,
        campaign: Campaign,
        *,
        now_ms: int,
        persist: bool = True,
    ) -> None:
        """Enrich a restart-restored campaign whose original detail lacked startPrice."""

        if campaign.market.reference_price is not None:
            return
        loaded = await self._get_market_detail_with_cache(
            campaign.market.market_topic_id,
            now_ms=now_ms,
        )
        if loaded is None:
            return
        _, refreshed = loaded
        if refreshed.reference_price is None:
            return
        campaign.market = replace(
            campaign.market,
            reference_price=refreshed.reference_price,
            raw=refreshed.raw,
        )
        if persist:
            await self.repository.save_campaign(campaign)

    def _defer_pre_entry_quote_collection(self, campaign: Campaign, *, now_ms: int) -> bool:
        """Avoid spending book weight before a staged entry can qualify.

        V6A-style profiles need a fresh quote in the 120--240 second entry
        window and 60 seconds of leader stability.  Collecting a pair of
        books from the first tick onward used the same one-minute budget that
        the entry execution needed, so the useful quote was stale or its
        order path was deferred.  Start the first observation only when
        enough time remains to build the configured stability window.

        This is deliberately limited to staged profiles with an explicit
        remaining-time window.  Profiles without that contract keep their
        existing quote collection behavior, and any campaign with exposure
        continues to receive normal management quotes.
        """

        if campaign.position.has_any or campaign.pending_intent_id or campaign.initial_attempts:
            return False
        policy = getattr(self.strategy, "config", None)
        if policy is None:
            return False
        entry_max_remaining = getattr(policy, "entry_max_remaining_seconds", None)
        min_leader_seconds = max(0, int(getattr(policy, "min_entry_leader_duration_seconds", 0) or 0))
        entry_start_seconds = getattr(policy, "entry_start_seconds", None)
        if entry_max_remaining is None or entry_start_seconds is None or min_leader_seconds <= 0:
            return False
        seed_elapsed_seconds = max(0, int(entry_start_seconds) - min_leader_seconds)
        return campaign.elapsed_seconds(now_ms) < seed_elapsed_seconds

    def _regime_entry_profile(self) -> str:
        selected = str(getattr(self, "_selected_strategy_profile", "") or "").strip().lower()
        if selected:
            return selected
        config = getattr(getattr(self, "strategy", None), "config", None)
        return str(getattr(config, "profile", "") or "").strip().lower()

    async def _record_regime_entry_blocked(
        self,
        campaign: Campaign | None,
        snapshot: Mapping[str, Any],
    ) -> None:
        """Persist one audit event for one unchanged blocked entry state."""

        profile = str(snapshot.get("profile") or "")
        status = str(snapshot.get("status") or "WAIT_DATA")
        reason = str(snapshot.get("gate_reason") or "")
        latest_settled = str(snapshot.get("latest_settled_at_ms") or "")
        campaign_id = str(getattr(campaign, "campaign_id", "") or "")
        key = (campaign_id, profile, status, latest_settled, reason)
        seen = getattr(self, "_regime_entry_block_keys", None)
        if not isinstance(seen, set):
            seen = set()
            self._regime_entry_block_keys = seen
        if key in seen:
            return
        # Keep the deduplication cache bounded for a long-running worker.
        if len(seen) >= 256:
            seen.pop()
        seen.add(key)
        recorder = getattr(self.repository, "record_risk_event", None)
        if not callable(recorder):
            return
        payload = dict(snapshot)
        payload["action"] = ActionType.BUY_INITIAL.value
        payload["campaign_id"] = campaign_id or None
        payload["live_only"] = True
        try:
            await recorder(
                "REGIME_ENTRY_BLOCKED",
                "WARNING",
                f"{status} market regime blocked V3-V5 initial buy",
                campaign_id=campaign_id or None,
                payload=payload,
            )
        except Exception:
            # An audit write must not turn a bounded entry block into a worker
            # failure or stop settlement/reduction processing.
            return

    async def _check_regime_entry_gate(self, campaign: Campaign | None = None) -> bool:
        """Check the LIVE-only initial-entry regime gate immediately pre-order."""

        profile = self._regime_entry_profile()
        effective_mode = getattr(self, "_effective_mode", RuntimeMode.SHADOW)
        mode = str(getattr(effective_mode, "value", effective_mode) or RuntimeMode.SHADOW.value).upper()
        live_mode = mode == RuntimeMode.LIVE.value.upper()
        checked_at_ms = self._now_ms()
        base = {
            "checked_at_ms": checked_at_ms,
            "profile": profile,
            "mode": mode,
            "live_only": True,
            "gate_profiles": sorted(REGIME_ENTRY_GATE_PROFILES),
        }

        if not live_mode or profile not in REGIME_ENTRY_GATE_PROFILES:
            snapshot = {
                **base,
                "status": "BYPASS",
                "applicable": False,
                "allowed": True,
                "blocked": False,
                "gate_block_new_entries": False,
                "gate_reason": (
                    "LIVE-only regime gate is not applicable"
                    if not live_mode
                    else "strategy profile is outside the V3-V5 regime gate"
                ),
            }
            self._regime_entry_gate_snapshot = snapshot
            return True

        getter = getattr(self.repository, "get_market_regime_monitor", None)
        monitor: Mapping[str, Any] = {}
        monitor_error: str | None = None
        if not callable(getter):
            monitor_error = "market regime monitor is unavailable"
        else:
            try:
                value = await getter(limit=20)
                if not isinstance(value, Mapping):
                    monitor_error = "market regime monitor returned a non-mapping"
                else:
                    monitor = value
            except Exception as exc:  # noqa: BLE001 - this gate fails closed
                monitor_error = f"market regime monitor error: {type(exc).__name__}"

        if monitor_error:
            status = "WAIT_DATA"
            allowed = False
            gate_reason = monitor_error
        else:
            raw_status = str(monitor.get("status") or "WAIT_DATA").strip().upper()
            status = raw_status if raw_status in {"RED", "YELLOW", "GREEN", "WAIT_DATA"} else "WAIT_DATA"
            allowed = status in {"YELLOW", "GREEN"}
            gate_reason = str(
                monitor.get("gate_reason")
                or (
                    "RED market regime blocks V3-V5 initial buys"
                    if status == "RED"
                    else "WAIT_DATA market regime blocks V3-V5 initial buys"
                    if status == "WAIT_DATA"
                    else f"{status} market regime allows V3-V5 initial buys"
                )
            )
        blocked = not allowed
        snapshot = {
            **base,
            "status": status,
            "applicable": True,
            "allowed": allowed,
            "blocked": blocked,
            "gate_block_new_entries": blocked,
            "gate_reason": gate_reason,
            "median_path_bps": monitor.get("median_path_bps"),
            "fast_median_path_bps": monitor.get("fast_median_path_bps"),
            "valid_slow_slots": monitor.get("valid_slow_slots"),
            "expected_slow_slots": monitor.get("expected_slow_slots", 20),
            "valid_fast_slots": monitor.get("valid_fast_slots"),
            "expected_fast_slots": monitor.get("expected_fast_slots", 5),
            "latest_settled_at_ms": monitor.get("latest_settled_at_ms"),
            "latest_settled_age_ms": monitor.get("latest_settled_age_ms"),
            "latest_settled_fresh": monitor.get("latest_settled_fresh"),
            "source": monitor.get("source"),
        }
        if monitor_error:
            snapshot["monitor_error"] = monitor_error
        self._regime_entry_gate_snapshot = snapshot
        if blocked:
            await self._record_regime_entry_blocked(campaign, snapshot)
        return not blocked

    def _can_prioritize_initial_entry(self, campaign: Campaign, decision: StrategyDecision) -> bool:
        """Allow a no-exposure first entry to consume its reserved weight.

        The local limiter reserves emergency capacity for reductions.  An
        initial BUY has no held position to protect yet, so it can use that
        capacity when the normal data budget has already been consumed.  Do
        not do this if another active campaign has exposure; exits remain the
        higher-priority operation in that recovery case.
        """

        if decision.action is not ActionType.BUY_INITIAL or campaign.position.has_any:
            return False
        return not any(
            other.campaign_id != campaign.campaign_id
            and other.state not in {CampaignState.DONE, CampaignState.CANCELLED}
            and other.position.has_any
            for other in self._active_campaigns.values()
        )

    def _s3s5_profile_active(self) -> bool:
        from src.gridbot.prediction import s3s5_pair as s3

        return s3.uses_loop_risk_guards(getattr(self, "_selected_strategy_profile", ""))

    async def _s3s5_ws_quote_for_campaign(self, campaign: Campaign) -> QuoteSnapshot | None:
        """Dense Live decide quotes via signed prediction WSS (no book REST weight).

        REST paired books cost weight 400 and the sustainable cadence collapses
        to ~36s (~3–7 quotes/5m).  Shadow journal density is ~1Hz via the same
        Reversal5Feeds path.  Prefer WSS while an s3s5 campaign is active;
        return None when orientation/book/spot are not ready so the caller can
        fall back to REST (still subject to deferral).
        """

        if not self._s3s5_profile_active():
            return None
        if not reversal5_orientation(campaign.market):
            loaded = await self._get_market_detail_with_cache(
                campaign.market.market_topic_id,
                now_ms=self._now_ms(),
            )
            if loaded is not None:
                _detail, market = loaded
                original = campaign.market
                keys = (
                    "market_topic_id",
                    "start_time_ms",
                    "end_time_ms",
                    "reference_price",
                    "up_token_id",
                    "down_token_id",
                    "up_market_id",
                    "down_market_id",
                )
                if all(getattr(original, k) == getattr(market, k) for k in keys):
                    campaign.market = market
            if not reversal5_orientation(campaign.market):
                return None
        feeds = getattr(self, "_s3s5_feeds", None)
        if feeds is None or getattr(feeds, "_closed", False):
            key = getattr(self.client, "api_key", None) or getattr(self.settings, "api_key", None)
            secret = getattr(self.client, "api_secret", None) or getattr(self.settings, "api_secret", None)
            if not key or not secret:
                return None
            feeds = Reversal5Feeds(key, secret, clock_ms=self._now_ms, symbol=self.settings.market_symbol)
            feeds.prewarm_spot()
            self._s3s5_feeds = feeds
        await feeds.select(campaign.market.up_market_id)
        now = self._now_ms()
        q = feeds.snapshot(campaign.market, now)
        # snapshot sets a coarse feed_ok; reverseal5_valid adds spread/orientation checks
        q["feed_ok"] = bool(q.get("feed_ok")) and bool(reversal5_valid(q, now))
        if not q.get("feed_ok"):
            return None
        execution = {
            "source": q["source"],
            "sampled_at_ms": now,
            "received_at_ms": q["received_at_ms"],
            "book_source_times_ms": [q["book_at_ms"], q["book_at_ms"]],
            "source_timestamp_available": int(q.get("book_at_ms") or 0) > 0,
            "atomic_execution": False,
            "orientation_verified": q.get("orientation_verified"),
        }
        for side in ("UP", "DOWN"):
            execution[side] = {
                "ask": q[side]["ask"],
                "top_ask_shares": q[side]["ask_shares"],
            }
        quote = QuoteSnapshot.from_books(
            observed_at_ms=now,
            up_bid=as_decimal(q["UP"]["bid"]) if q["UP"]["bid"] is not None else None,
            up_ask=as_decimal(q["UP"]["ask"]) if q["UP"]["ask"] is not None else None,
            down_bid=as_decimal(q["DOWN"]["bid"]) if q["DOWN"]["bid"] is not None else None,
            down_ask=as_decimal(q["DOWN"]["ask"]) if q["DOWN"]["ask"] is not None else None,
            btc_spot=as_decimal(q["spot"]) if q["spot"] is not None else None,
            reference_price=campaign.market.reference_price,
            spot_observed_at_ms=int(q.get("spot_at_ms") or now),
            feed_ok=True,
            raw={"execution_book": execution, "reversal5": q, "stream_health": dict(feeds.health)},
        )
        campaign.last_quote_at_ms = now
        self._latest_quotes[campaign.campaign_id] = quote
        self.heartbeat.last_feed_ok_at_ms = now
        try:
            await self.repository.save_quote(campaign.campaign_id, quote)
        except Exception:
            pass
        return quote

    async def _quote_for_campaign(
        self,
        campaign: Campaign,
        *,
        force: bool = False,
        observer: bool = False,
    ) -> QuoteSnapshot:
        now = self._now_ms()
        state = await self._load_market_state(campaign, observer=observer)
        latest = self._latest_quotes.get(campaign.campaign_id)
        last_quote_at = int(state.get("last_quote_at_ms") or campaign.last_quote_at_ms or 0)
        # s3s5 REST is the degraded path when WSS is cold. Cap cadence at 1s so
        # fallback samples can still form features; local weight deferral remains
        # the hard budget guard (global sustainable floor stays ~36s otherwise).
        effective_cadence = int(self._quote_cadence_ms or 0)
        if self._s3s5_profile_active() and not observer:
            pending_live = False
            engine = getattr(self, "_s3s5_engine", None)
            if isinstance(engine, dict):
                start_key = int(campaign.market.start_time_ms)
                legs = (engine.get(start_key) or {}).get("legs") or {}
                pending_live = any(
                    isinstance(leg, Mapping) and leg.get("status") == "pending"
                    for leg in legs.values()
                )
            effective_cadence = 0 if (force or pending_live) else min(effective_cadence, 1_000)
        if not force and latest is not None and effective_cadence and now - last_quote_at < effective_cadence:
            return latest
        await self._refresh_campaign_reference(campaign, now_ms=now, persist=not observer)
        market = campaign.market
        # Reserve both reads together: never spend half the budget on an
        # unusable one-sided sample. Concurrent reads reduce cross-leg skew,
        # but are NOT atomic execution and never imply complete-set profit.
        pair_weight = 2 * self._endpoint_weight("query_order_book")
        if not self._rate_limiter.acquire(pair_weight, block=False):
            self._rate_limiter.note_deferred(pair_weight, error="paired book sample deferred")
            raise PredictionRateLimitDeferred("paired book sample deferred", self._rate_limiter.health().as_dict())
        request_started_ms = self._now_ms()
        up_book, down_book = await asyncio.gather(
            self._call_api("query_order_book", market.market_id_for(OutcomeSide.UP), market.up_token_id or "", vendor=market.vendor, _weight_pre_acquired=True),
            self._call_api("query_order_book", market.market_id_for(OutcomeSide.DOWN), market.down_token_id or "", vendor=market.vendor, _weight_pre_acquired=True),
        )
        received_at_ms = self._now_ms()
        # A REST receipt is not an exchange update. Keep the conservative
        # request-start age when the provider omits a source timestamp.
        book_times = []
        for book in (up_book, down_book):
            data = _data(book)
            source_ms = data.get("updateTimestampMs") if isinstance(data, Mapping) else None
            try:
                if source_ms is None:
                    parsed_ms = request_started_ms
                else:
                    value = Decimal(str(source_ms))
                    parsed_ms = int(value) if value.is_finite() and value == value.to_integral_value() else 0
            except (TypeError, ValueError, ArithmeticError):
                parsed_ms = 0
            book_times.append(parsed_ms if 0 < parsed_ms <= received_at_ms else 0)
        source_observed_ms = min(request_started_ms, *book_times)
        now = received_at_ms
        def safe_book_top(book: Any) -> tuple[Decimal | None, Decimal | None]:
            try:
                bid, ask = _book_top(book)
            except (TypeError, ValueError, ArithmeticError):
                return None, None
            if any(value is not None and not value.is_finite() for value in (bid, ask)):
                return None, None
            return bid, ask

        up_bid, up_ask = safe_book_top(up_book)
        down_bid, down_ask = safe_book_top(down_book)
        book_leader: OutcomeSide | None = None
        if up_bid is not None and (down_bid is None or up_bid >= down_bid):
            book_leader = OutcomeSide.UP
        elif down_bid is not None:
            book_leader = OutcomeSide.DOWN
        prior_leader_raw = state.get("last_leader") or campaign.last_leader
        prior_leader = OutcomeSide(str(prior_leader_raw)) if prior_leader_raw else None

        btc_spot: Decimal | None = None
        spot_observed_at_ms = now
        spot_fresh = False
        if self._spot_source is not None:
            try:
                supplied = self._spot_source(market, now_ms=now)
                if inspect.isawaitable(supplied):
                    supplied = await supplied
                now = self._now_ms()
                if isinstance(supplied, Mapping):
                    btc_spot = as_decimal(supplied.get("price") or supplied.get("spot") or supplied.get("btc_spot"))
                    # An explicit zero/invalid source time must not be
                    # replaced by a fresh receipt timestamp.
                    supplied_at = supplied.get("observed_at_ms")
                    spot_observed_at_ms = int(supplied_at) if supplied_at is not None else received_at_ms
                else:
                    btc_spot = as_decimal(supplied)
                max_age_ms = max(0, int(getattr(self.settings, "spot_max_age_ms", 1_500)))
                spot_fresh = btc_spot is not None and btc_spot.is_finite() and btc_spot > 0 and 0 < spot_observed_at_ms <= now and now - spot_observed_at_ms <= max_age_ms
                if not spot_fresh:
                    btc_spot = None
                    self.heartbeat.last_error = "BTCUSDT spot feed is stale"
            except Exception as exc:  # stale/missing public feed is fail-closed
                btc_spot = None
                self.heartbeat.last_error = str(exc)
        # A leader flip is pending until two consecutive fresh quotes agree.
        # The pending candidate never replaces the committed leader early.
        leader = book_leader
        flip_confirmed = False
        if prior_leader is None:
            if leader is not None:
                state["last_leader"] = leader.value
                state["leader_since_ms"] = now
                state["leader_quotes"] = 1
        elif book_leader is not prior_leader:
            pending = str(state.get("pending_leader") or "")
            pending_quotes = int(state.get("pending_leader_quotes") or 0)
            if spot_fresh and book_leader is not None:
                if pending == book_leader.value:
                    pending_quotes += 1
                else:
                    pending = book_leader.value
                    pending_quotes = 1
            else:
                pending_quotes = 0
            state["pending_leader"] = pending or None
            state["pending_leader_quotes"] = pending_quotes
            if book_leader is not None and pending_quotes >= 2:
                state["prior_leader"] = prior_leader.value
                state["last_leader"] = book_leader.value
                state["leader_since_ms"] = now
                state["leader_quotes"] = 1
                state["pending_leader"] = None
                state["pending_leader_quotes"] = 0
                leader = book_leader
                flip_confirmed = True
            else:
                leader = prior_leader
        elif prior_leader is not None:
            state["pending_leader"] = None
            state["pending_leader_quotes"] = 0
            if spot_fresh:
                state["leader_quotes"] = int(state.get("leader_quotes") or 0) + 1
        leader_since = int(state.get("leader_since_ms") or now)
        leader_duration = max(0, now - leader_since)
        reference = market.reference_price
        if reference is not None and (not reference.is_finite() or reference <= 0):
            reference = None
        prior_spot_raw = state.get("last_spot")
        try:
            prior_spot = as_decimal(prior_spot_raw) if prior_spot_raw not in (None, "") else None
        except (TypeError, ValueError, ArithmeticError):
            prior_spot = None
        if prior_spot is not None and (not prior_spot.is_finite() or prior_spot <= 0):
            prior_spot = None
        crossed = False
        recross = False
        if btc_spot is not None and reference is not None and prior_spot is not None:
            was_up = prior_spot >= reference
            is_up = btc_spot >= reference
            crossed = was_up != is_up
            recross = crossed and int(state.get("cross_count") or 0) > 0
            if crossed:
                state["cross_count"] = int(state.get("cross_count") or 0) + 1
        state["prior_spot"] = str(prior_spot) if prior_spot is not None else None
        state["last_spot"] = str(btc_spot) if btc_spot is not None else None
        state["last_spot_at_ms"] = spot_observed_at_ms
        state["last_quote_at_ms"] = now
        aligned = btc_spot is not None and reference is not None and leader is not None and ((leader is OutcomeSide.UP and btc_spot >= reference) or (leader is OutcomeSide.DOWN and btc_spot <= reference))
        def valid_book(bid: Decimal | None, ask: Decimal | None) -> bool:
            return bool(bid is not None and ask is not None and bid.is_finite() and ask.is_finite() and 0 < bid <= ask < 1)

        feed_ok = (valid_book(up_bid, up_ask) and valid_book(down_bid, down_ask)
                   and spot_fresh and btc_spot is not None and reference is not None
                   and 0 < source_observed_ms <= now
                   and now - source_observed_ms <= 1_500)
        execution_book = {"source": "binance_rest", "request_started_ms": request_started_ms,
                          "received_at_ms": received_at_ms, "book_source_times_ms": book_times,
                          "source_timestamp_available": all(isinstance(_data(b), Mapping) and _data(b).get("updateTimestampMs") is not None for b in (up_book, down_book)),
                          "atomic_execution": False}
        # Both comparison lanes consume the identical depth-tested sample.
        # Only visible top-ask liquidity is assumed; no queue-fill guarantee.
        for side, book, ask in (("UP", up_book, up_ask), ("DOWN", down_book, down_ask)):
            data = _data(book)
            levels = (data.get("asks") or data.get("sell") or data.get("SELL") or []) if isinstance(data, Mapping) else []
            parsed = [level for level in (_book_level(item) for item in levels) if level]
            top_size = sum((size for price, size in parsed if price.is_finite() and size.is_finite() and ask is not None and price == ask and size > 0), Decimal("0"))
            if top_size <= 0 and parsed:
                best_ask = min(
                    (price for price, size in parsed if price.is_finite() and size.is_finite() and size > 0),
                    default=None,
                )
                if best_ask is not None:
                    top_size = sum((size for price, size in parsed if price == best_ask and size > 0), Decimal("0"))
            execution_book[side] = {
                "ask": str(ask) if ask is not None else None,
                "top_ask_shares": str(top_size),
                "visible_ask_shares": str(top_size),
            }
            if observer and leader is not None and leader.value == side:
                # Fixed-2-USDT research lanes must not borrow a smaller
                # operator unit when testing visible liquidity.
                gross_required = max(
                    (machine.config.max_buy_usdt for machine in self._shadow_lane_strategies.values()),
                    default=self._selected_order_unit_usdt,
                )
                required = gross_required / ask if ask is not None and ask.is_finite() and ask > 0 else Decimal("Infinity")
                feed_ok = feed_ok and top_size >= required
        v7_policy = StrategyConfig.for_profile(REGIME_VALUE_V7_PROFILE)
        v7_features = _v7_quote_features(
            state,
            now_ms=now,
            btc_spot=btc_spot,
            spot_observed_at_ms=spot_observed_at_ms,
            reference_price=reference,
            end_time_ms=market.end_time_ms,
            leader=leader,
            crossed_reference=crossed,
            cross_cooldown_seconds=v7_policy.v7_cross_cooldown_seconds,
            high_volatility_bps_sqrt_second=v7_policy.v7_high_volatility_bps_sqrt_second,
        )
        quote = QuoteSnapshot.from_books(
            observed_at_ms=source_observed_ms,
            up_bid=up_bid,
            up_ask=up_ask,
            down_bid=down_bid,
            down_ask=down_ask,
            leader=leader,
            reference_price=reference,
            btc_spot=btc_spot,
            spot_observed_at_ms=spot_observed_at_ms,
            flip_confirmed=flip_confirmed,
            btc_crossed_reference=crossed,
            reference_recross=recross,
            leader_duration_ms=leader_duration,
            stable_final=bool(leader is not None and leader_duration >= 5_000 and not recross),
            feed_ok=feed_ok,
            raw={"regime_value_v7": v7_features, "execution_book": execution_book},
        )
        campaign.prior_spot = prior_spot
        campaign.last_spot = btc_spot
        campaign.last_spot_at_ms = spot_observed_at_ms
        campaign.prior_leader = prior_leader
        campaign.leader_since_ms = leader_since
        campaign.leader_quotes = int(state.get("leader_quotes") or 0)
        campaign.reference_cross_count = int(state.get("cross_count") or 0)
        campaign.last_quote_at_ms = now
        campaign.update_leader(leader)
        if observer:
            saver = getattr(self.repository, "save_shadow_observer_state", None)
            quote_saver = getattr(self.repository, "save_shadow_observer_quote", None)
            if callable(saver):
                await saver(campaign.campaign_id, state)
            if callable(quote_saver):
                await quote_saver(campaign.campaign_id, quote)
            market_updater = getattr(self.repository, "update_shadow_observer_market", None)
            if callable(market_updater):
                await market_updater(
                    campaign.campaign_id,
                    last_quote_at_ms=now,
                    last_seen_at_ms=now,
                    last_error=None,
                )
        else:
            await self.repository.save_market_state(campaign.campaign_id, state)
            await self.repository.save_quote(campaign.campaign_id, quote)
        self._latest_quotes[campaign.campaign_id] = quote
        self._remember_v3_gate_quote(campaign.campaign_id, quote)
        if quote.feed_ok:
            self.heartbeat.last_feed_ok_at_ms = now
        return quote

    async def _s3s5_load_book(self):
        from . import s3s5_pair as s3
        async with self._s3s5_accounting_lock:
            book = await self.repository.get_runtime_config('s3s5_playbook_v2', None)
            if not isinstance(book, Mapping):
                old = await self.repository.get_runtime_config('s3s5_playbook_v1', {})
                book = await self.repository.reconcile_s3s5_book(old)
                book['migration_hold'] = s3.playbook_snapshot(old,now_ms=self._now_ms())['mode'] != 'LIVE'
                book['migrated_at_ms'] = self._now_ms()
                await self.repository.set_runtime_config('s3s5_playbook_v2',book)
            elif not getattr(self,'_s3s5_book_restored',False):
                before=s3.playbook_snapshot(book,now_ms=self._now_ms())
                book=await self.repository.reconcile_s3s5_book(book)
                after=s3.playbook_snapshot(book,now_ms=self._now_ms())
                if before['mode'] != 'LIVE' and after['mode'] == 'LIVE':
                    book['migration_hold']=True
                await self.repository.set_runtime_config('s3s5_playbook_v2',book)
            self._s3s5_book_restored=True
            return book

    async def _s3s5_playbook_status(self) -> dict:
        from src.gridbot.prediction import s3s5_pair as s3
        getter = getattr(self.repository, "get_runtime_config", None)
        payload = await self._s3s5_load_book()
        return s3.playbook_snapshot(payload, now_ms=self._now_ms())

    async def _s3s5_official_winner(self, campaign) -> str:
        loaded = await self._get_market_detail_with_cache(
            campaign.market.market_topic_id,
            now_ms=self._now_ms(),
        )
        if loaded is None:
            return ""
        detail, _parsed = loaded
        side = self._official_resolution(detail)
        return side.value if side is not None else ""

    async def _s3s5_on_settled(self, campaign, settlement) -> None:
        from . import s3s5_pair as s3
        await self._s3s5_load_book()
        async with self._s3s5_accounting_lock:
            payload = await self.repository.get_runtime_config('s3s5_playbook_v2', {})
            before = s3.playbook_snapshot(payload,now_ms=self._now_ms())
            payload = await self.repository.reconcile_s3s5_book(payload, campaign_id=campaign.campaign_id)
            start = int(campaign.market.start_time_ms)
            state = getattr(self, '_s3s5_engine', {}).get(start)
            for row in payload['markets']:
                if row['start_ms'] == start:
                    paper = s3.paper_pnl_from_state(state,row.get('outcome'))
                    if paper is not None:
                        row['pnl'] = str(paper)
                    row['paper_status'] = 'scored' if row.get('pnl') is not None else 'unknown'
            after=s3.playbook_snapshot(payload,now_ms=self._now_ms())
            if before['mode'] != 'LIVE' and after['mode']=='LIVE' and before.get('halt_batch')==after.get('halt_batch') and not after.get('recovery_batch'):
                payload['migration_hold']=True
            await self.repository.set_runtime_config('s3s5_playbook_v2', payload)

    async def _attach_loop_risk_status(self, result: dict[str, Any], loop_id: str) -> None:
        from src.gridbot.prediction import s3s5_pair as s3
        from src.gridbot.prediction.loss_cooldown_guard import (
            evaluate_loss_cooldown,
            filled_losses_since_win,
            loop_peak_drawdown,
        )

        unit = getattr(self, "_selected_order_unit_usdt", Decimal("1"))
        result["loop_mdd_limit"] = str(s3.loop_mdd_limit(unit))
        getter = getattr(self.repository, "get_loop_settled_trades", None)
        if not callable(getter) or not loop_id:
            return
        try:
            settled = await getter(loop_id)
        except Exception:
            return
        peak, current, dd = loop_peak_drawdown(settled)
        losses, _last = filled_losses_since_win(settled)
        _allow, cd_reason, remaining_s = evaluate_loss_cooldown(settled, self._now_ms())
        result.update(
            {
                "loop_peak_pnl": str(peak),
                "loop_drawdown": str(dd),
                "loop_filled_pnl": str(current),
                "consecutive_filled_losses": losses,
                "loss_cooldown_reason": cd_reason,
                "loss_cooldown_remaining_s": remaining_s,
            }
        )

    async def _s3s5_entry_risk_hold(self, campaign, now_ms: int):
        """HOLD new entries on 2-loss cooldown or loop peak MDD. Never blocks exits."""

        from src.gridbot.prediction import s3s5_pair as s3
        from src.gridbot.prediction.loss_cooldown_guard import (
            evaluate_loss_cooldown,
            loop_peak_drawdown,
        )

        getter = getattr(self.repository, "get_loop_settled_trades", None)
        loop_id = str(getattr(self, "_loop_id", "") or "")
        if not callable(getter) or not loop_id:
            return None
        try:
            settled = await getter(loop_id)
        except Exception:
            return StrategyDecision(
                ActionType.HOLD, campaign.state, "s3s5 loop risk ledger unavailable"
            )
        allowed, cd_reason, remaining_s = evaluate_loss_cooldown(settled, int(now_ms))
        if not allowed:
            await self.repository.set_runtime_config(
                f"loss_cooldown_excluded:{campaign.campaign_id}",
                {"reason": cd_reason, "at_ms": int(now_ms), "remaining_seconds": remaining_s},
            )
            return StrategyDecision(ActionType.HOLD, campaign.state, f"s3s5 {cd_reason}")
        unit = getattr(self, "_selected_order_unit_usdt", Decimal("1"))
        limit = s3.loop_mdd_limit(unit)
        peak, current, dd = loop_peak_drawdown(settled)
        if dd <= limit:
            await self.repository.set_runtime_config(
                f"loop_mdd_excluded:{campaign.campaign_id}",
                {
                    "reason": "loop_mdd_halted",
                    "at_ms": int(now_ms),
                    "peak": str(peak),
                    "pnl": str(current),
                    "dd": str(dd),
                    "limit": str(limit),
                },
            )
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 loop_mdd_halted")
        if str(getattr(self, "_selected_strategy_profile", "")).strip().lower() == s3.FAV_V4_PROFILE:
            worst_case_pnl = current - Decimal("1.0")
            if worst_case_pnl < limit:
                await self.repository.set_runtime_config(
                    f"loop_mdd_excluded:{campaign.campaign_id}",
                    {
                        "reason": "eth_v4_pretrade_mdd",
                        "at_ms": int(now_ms),
                        "peak": str(peak),
                        "pnl": str(current),
                        "worst_case_pnl": str(worst_case_pnl),
                        "limit": str(limit),
                    },
                )
                return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 eth_v4_pretrade_mdd")
        return None

    def _c180_bridge_for_worker(self, profile=None):
        if (profile or self._selected_strategy_profile) in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
            bridge = getattr(self, "_regime_worker_bridge", None)
            asset = self.settings.market_symbol
            checked_asset = asset if (profile or self._selected_strategy_profile) in ("regime_target6_7c_v1", "regime_target6_9_v1", "regime_target6_9a_v1") else None
            if bridge is None or bridge.profile != (profile or self._selected_strategy_profile) or getattr(bridge, "symbol", None) != checked_asset:
                path = os.environ.get("PREDICTION_C180_SIGNAL_DB", "").strip()
                if not path:
                    return None
                from .loop_market import data_paths
                feature_path, signal_path = data_paths(self.repository.db_path, asset)
                if asset != "BTCUSDT":
                    path = str(signal_path)
                bridge = RegimeWorkerBridge(self.repository, path, feature_db=feature_path, symbol=checked_asset,
                    exposure_checker=self._c180_recovery_exposure_clear,
                    profile=(profile or self._selected_strategy_profile))
                self._regime_worker_bridge = bridge
            return bridge
        from src.gridbot.prediction.c180_worker_bridge import C180WorkerBridge
        bridge = getattr(self, "_c180_worker_bridge", None)
        if bridge is None:
            path = os.environ.get("PREDICTION_C180_SIGNAL_DB", "").strip()
            if not path:
                return None
            bridge = C180WorkerBridge(self.repository, path,
                                      exposure_checker=self._c180_recovery_exposure_clear)
            self._c180_worker_bridge = bridge
        return bridge

    async def _c180_recovery_exposure_clear(self) -> bool:
        """Never promote paper evidence while the official wallet is exposed."""
        if not self.settings.wallet_address:
            return False
        try:
            if getattr(self.client, "concurrent_reads", False) is True:
                results = await asyncio.gather(
                    self._query_active_order_rows(),
                    self._call_api("query_positions", wallet_address=self.settings.wallet_address),
                    return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
                orders, positions = results
            else:
                # Session-based/custom transports have not opted into threading.
                orders = await self._query_active_order_rows()
                positions = await self._call_api(
                    "query_positions", wallet_address=self.settings.wallet_address)
        except (PredictionRateLimitDeferred, PredictionClientError):
            return False
        finally:
            self._entry_exposure_campaign_id = None
        return not orders and not any(
            self._official_position_shares(item) > 0
            for item in self._official_position_rows(positions)
        )

    async def _c180_decide(self, campaign, now_ms):
        def hold(reason):
            self._finish_entry_attempt(campaign.campaign_id, reason)
            return StrategyDecision(ActionType.HOLD, campaign.state, "c180 " + reason)
        bridge = self._c180_bridge_for_worker()
        if bridge is None or not self._loop_id:
            return hold("bridge or loop unavailable")
        registered = await bridge.register_market(
            loop_id=self._loop_id, market=campaign.market, now_ms=now_ms,
            unit_usdt=self._selected_order_unit_usdt,
        )
        if not registered.allowed:
            return hold(registered.reason)
        if campaign.pending_intent_id or campaign.pending_unknown:
            return hold("pending order")
        if campaign.position.has_any:
            # Frozen C180_FAVORITE_HOLD retains all filled shares to official
            # resolution; old R3/FAV protective exits are not applicable.
            return hold("hold to official settlement")
        if campaign.buy_count or campaign.initial_attempts:
            return hold("market already attempted")
        start = int(campaign.market.start_time_ms)
        # Registration can cross the 124-second boundary.  The caller's
        # pre-registration timestamp must not decide the entry window.
        now_ms = self._now_ms()
        begin, end = (60000, 270000) if self._selected_strategy_profile == "regime_target6_7_v1" else (124000, 136000)
        if self._selected_strategy_profile in ("regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1") and start+180000 <= now_ms < start+183500:
            begin, end = 180000, 183500
        if not start + begin <= now_ms < start + end:
            return hold("outside execution window")
        if not self.live_capability:
            return hold("LIVE not armed")
        regime_profile = self._selected_strategy_profile in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
        ready = None
        if regime_profile:
            # Freeze the original T+124..126 signal before the wallet/network
            # checks.  This is read-only and cannot submit an order.  The
            # durable claim still rechecks risk and fresh execution depth.
            ready = await self._entry_signal_within_window(bridge, campaign, start)
            if not ready.allowed or ready.signal is None or ready.execution is None:
                return hold(ready.reason)
            self._start_entry_attempt(campaign, ready)
            self._entry_exposure_campaign_id = campaign.campaign_id
            try:
                with self._entry_stage(campaign.campaign_id, "prepare_risk_and_official_exposure"):
                    gate = await bridge.prepare_market(
                        loop_id=self._loop_id, market=campaign.market, now_ms=self._now_ms(),
                        unit_usdt=self._selected_order_unit_usdt, already_registered=True,
                        trace=lambda stage, duration_ns: self._trace_event("entry_stage",
                            campaign_id=campaign.campaign_id, stage=stage, duration_ns=duration_ns))
            finally:
                self._entry_exposure_campaign_id = None
        else:
            gate = await bridge.prepare_market(
                loop_id=self._loop_id, market=campaign.market, now_ms=now_ms,
                unit_usdt=self._selected_order_unit_usdt,
            )
        if not gate.allowed:
            return hold(gate.reason)
        if ready is None:
            ready = bridge.check_signal(
                market=campaign.market, unit_usdt=self._selected_order_unit_usdt,
                at_ms=self._now_ms(), last_seen_book_at_ms=start + (59999 if self._selected_strategy_profile == "regime_target6_7_v1" else 120000),
            )
        if not ready.allowed or ready.signal is None or ready.execution is None:
            return hold(ready.reason)
        if self._now_ms() >= ready.execution.expires_at_ms:
            return hold("execution window expired after gate")
        if ready.execution.worst_ask_limit is None or ready.signal.entry is None:
            return hold("execution limit missing")
        self._c180_ready = getattr(self, "_c180_ready", {})
        self._c180_ready[campaign.campaign_id] = ready
        side = OutcomeSide.UP if ready.signal.entry.side == "UP" else OutcomeSide.DOWN
        return StrategyDecision(
            ActionType.BUY_INITIAL, CampaignState.INITIAL_PENDING,
            "c180 durable entry", outcome=side, order_side=OrderSide.BUY,
            amount=self._selected_order_unit_usdt,
            limit_price=ready.execution.worst_ask_limit,
            ttl_ms=max(1, ready.execution.expires_at_ms - self._now_ms()),
            trade_allowed=True,
        )

    async def _s3s5_pair_decide(self, campaign, quote, now_ms):
        from src.gridbot.prediction import s3s5_pair as s3
        from src.gridbot.prediction import r3_reversal_guard as guard
        if campaign.pending_intent_id or campaign.pending_unknown:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 wait pending order")
        if campaign.position.has_any:
            r3_exit_dec = await self._r3_late_exit_decide(campaign, quote, now_ms)
            if r3_exit_dec.action is ActionType.SELL_PROTECTIVE:
                return r3_exit_dec
            fav_exit_dec = await self._fav_late_exit_decide(campaign, quote, now_ms)
            if fav_exit_dec.action is ActionType.SELL_PROTECTIVE:
                return fav_exit_dec
            return r3_exit_dec
        getter = getattr(self.repository, "get_runtime_config", None)
        if not callable(getter):
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 durable early guard unavailable")
        if await getter(guard.early_key(campaign.campaign_id), False):
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 R3 early market excluded")
        if await getter(s3.fav_exclusion_key(campaign.campaign_id), False):
            remaining_s = (campaign.market.end_time_ms - now_ms) / 1000.0
            if 18.0 <= remaining_s <= 50.0:
                sniper_dec = await self._s3s5_late_sniper_decide(campaign, quote, now_ms)
                if sniper_dec is not None:
                    return sniper_dec
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 FAV market excluded")
        if campaign.position.has_any or campaign.buy_count or campaign.pending_intent_id or campaign.pending_unknown:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 market already owned")
        has_buy = getattr(self.repository, "has_market_buy", None)
        if not callable(has_buy):
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 durable entry guard unavailable")
        if await has_buy(campaign.campaign_id):
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 market already owned")
        if not hasattr(self, "_s3s5_engine") or not isinstance(self._s3s5_engine, dict):
            self._s3s5_engine = {}
        start = int(campaign.market.start_time_ms)
        mapped = s3.quote_from_snapshot(quote, now_ms)
        payload = await self._s3s5_load_book()
        snap = s3.playbook_snapshot(payload, now_ms=now_ms)
        self._s3s5_snapshot = snap
        allow, why = s3.allow_live_entry(snap, start)
        if mapped is None:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 quote unusable")
        try:
            state, plans = s3.step(
                self._s3s5_engine.get(start),
                mapped,
                at=now_ms,
                start=start,
                live=True,
                profile=self._selected_strategy_profile,
                order_unit_usdt=self._selected_order_unit_usdt,
            )
        except ValueError as exc:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5: " + type(exc).__name__)
        self._s3s5_engine[start] = state
        self._trace_event('strategy_evaluated',campaign_id=campaign.campaign_id,signal=bool(plans),
            observed_at_ms=quote.observed_at_ms,remaining_ms=campaign.market.end_time_ms-now_ms)
        if plans:
            self._trace_event('signal',campaign_id=campaign.campaign_id,strategy=plans[0]['strategy'],side=plans[0]['side'])
        if state.get("fav_excluded"):
            if now_ms - start >= 90_000:
                await self.repository.set_runtime_config(s3.fav_exclusion_key(campaign.campaign_id),
                    {"version": s3.FAV_VERSION, "at_ms": now_ms, "reason": "fav_distance_below_2bps"})
            remaining_s = (campaign.market.end_time_ms - now_ms) / 1000.0
            if 18.0 <= remaining_s <= 50.0:
                sniper_dec = await self._s3s5_late_sniper_decide(campaign, quote, now_ms)
                if sniper_dec is not None:
                    return sniper_dec
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 FAV distance below 2bps")
        if plans and plans[0]["strategy"] == "FAV":
            proof = s3.fav_snapshot_approval(quote, plans[0], now_ms, start, profile=self._selected_strategy_profile)
            if proof is None:
                await self.repository.set_runtime_config(s3.fav_exclusion_key(campaign.campaign_id),
                    {"version": s3.FAV_VERSION, "at_ms": now_ms, "reason": "fav_original_snapshot_rejected"})
                return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 FAV market excluded")
            await self.repository.set_runtime_config(s3.fav_approval_key(campaign.campaign_id), proof)
        if plans and now_ms - start < guard.EARLY_MS:
            await self.repository.set_runtime_config(
                guard.early_key(campaign.campaign_id),
                {"version": guard.VERSION, "at_ms": now_ms, "reason": "first executable intent before 45s"},
            )
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 R3 early market excluded")
        if not allow:
            return StrategyDecision(ActionType.HOLD, campaign.state, why)
        if self.live_capability and not self.orders_enabled:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 paper only; live not armed")
        if campaign.pending_intent_id or campaign.pending_unknown:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 wait pending order")
        if not plans:
            remaining_s = (campaign.market.end_time_ms - now_ms) / 1000.0
            if 18.0 <= remaining_s <= 50.0:
                sniper_dec = await self._s3s5_late_sniper_decide(campaign, quote, now_ms)
                if sniper_dec is not None:
                    return sniper_dec
            reason = "s3s5 waiting/reconfirm"
            for _name, leg in (state.get("legs") or {}).items():
                if not isinstance(leg, Mapping):
                    continue
                st = str(leg.get("status") or "")
                detail = str(leg.get("reason") or "")
                if st == "pending":
                    reason = "s3s5 pending_delay"
                    break
                if st == "ordering":
                    reason = "s3s5 ordering"
                    break
                if st in {"expired", "rejected"} and detail:
                    reason = f"s3s5 {st}:{detail}"
                    break
            return StrategyDecision(ActionType.HOLD, campaign.state, reason)
        blocked = await self._s3s5_entry_risk_hold(campaign, now_ms)
        if blocked is not None:
            return blocked
        if str(getattr(self, "_selected_strategy_profile", "")).strip().lower() == s3.FAV_V4_PROFILE:
            return StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 eth_v4 live orders disabled")
        plan = plans[0]
        # Jev System One Gate check (Option A: FAV + Jev Gate)
        if getattr(self, "_jev_gate", None) and self._jev_gate.enabled:
            verdict = self._jev_gate.evaluate(
                symbol=getattr(self.settings, "market_symbol", "BTCUSDT"),
                fav_side=plan["side"],
                now_ms=now_ms,
                buy_price=float(plan["ask"]),
            )
            if not verdict.allowed:
                log_line = (
                    f"[JEV_GATE] market={campaign.campaign_id} strategy={plan.get('strategy')} "
                    f"side={plan['side']} ACTION=REJECT reason={verdict.reject_reason} "
                    f"regime={verdict.regime} conf={verdict.confidence or 0.0:.2f}"
                )
                print(log_line, flush=True)
                LOGGER.info("%s", log_line)
                if hasattr(self.repository, "record_risk_event"):
                    try:
                        await self.repository.record_risk_event(
                            event_type="JEV_GATE_REJECT",
                            severity="INFO",
                            message=f"Jev Gate blocked {plan.get('strategy')} {plan.get('side')}: {verdict.reject_reason}",
                            campaign_id=campaign.campaign_id,
                            event_time_ms=now_ms,
                        )
                    except Exception:
                        pass
                if hasattr(self, "_s3s5_engine") and isinstance(self._s3s5_engine, dict):
                    st = self._s3s5_engine.get(start)
                    if isinstance(st, dict) and "legs" in st:
                        leg_obj = st["legs"].get(plan.get("strategy"))
                        if isinstance(leg_obj, dict) and leg_obj.get("status") == "ordering":
                            leg_obj.pop("fill", None)
                            leg_obj["status"] = "waiting"
                return StrategyDecision(ActionType.HOLD, campaign.state, f"jev_gate {verdict.reject_reason}")
            else:
                log_line = (
                    f"[JEV_GATE] market={campaign.campaign_id} strategy={plan.get('strategy')} "
                    f"side={plan['side']} ACTION=ALLOW regime={verdict.regime} conf={verdict.confidence or 0.0:.2f}"
                )
                print(log_line, flush=True)
                LOGGER.info("%s", log_line)

        action = ActionType.BUY_INITIAL
        # Rule set C' ops: mid-ask TTL 12s (was 5s). One clear constant for smoke safety.
        return StrategyDecision(
            action,
            campaign.state,
            "s3s5 " + plan["strategy"] + " " + plan["side"],
            OutcomeSide(plan["side"]),
            OrderSide.BUY,
            Decimal(str(plan["gross"])),
            Decimal(str(plan["ask"])),
            12000,
            True,
        )

    async def _r3_late_exit_decide(self, campaign, quote, now_ms):
        from src.gridbot.prediction import r3_reversal_guard as guard
        hold = StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 R3 position held")
        if campaign.pending_intent_id or campaign.pending_unknown or not guard.late_window(campaign, now_ms):
            return hold
        if await self.repository.market_entry_tier(campaign.campaign_id) != "R3":
            return hold
        sides = [side for side in OutcomeSide if campaign.position.shares(side) > 0]
        if len(sides) != 1:
            return hold
        if not quote.feed_ok or not 0 < quote.observed_at_ms <= now_ms or now_ms - quote.observed_at_ms > guard.MAX_AGE_MS:
            return hold
        side = sides[0]
        bid = quote.up_bid if side is OutcomeSide.UP else quote.down_bid
        if bid is None or not bid.is_finite() or not 0 < bid < 1:
            return hold
        shares = campaign.position.shares(side)
        latched = await self.repository.get_runtime_config(guard.exit_key(campaign.campaign_id), False)
        if not latched and guard.projected_net(campaign.position, shares * bid) < guard.MIN_NET:
            return hold
        # Preliminary trigger only; fresh full-size depth is checked at execution.
        return StrategyDecision(ActionType.SELL_PROTECTIVE, campaign.state, guard.EXIT_REASON,
                                side, OrderSide.SELL, shares, bid, 2000, True)

    async def _fav_late_exit_decide(self, campaign, quote, now_ms):
        hold = StrategyDecision(ActionType.HOLD, campaign.state, "s3s5 FAV position held")
        if campaign.pending_intent_id or campaign.pending_unknown:
            return hold
        remaining_s = (campaign.market.end_time_ms - now_ms) / 1000.0
        if remaining_s > 75.0 or remaining_s <= 3.0:
            return hold
        sides = [side for side in OutcomeSide if campaign.position.shares(side) > 0]
        if len(sides) != 1:
            return hold
        side = sides[0]
        bid = quote.up_bid if side is OutcomeSide.UP else quote.down_bid
        if bid is None or not bid.is_finite() or bid < Decimal("0.20"):
            return hold
        shares = campaign.position.shares(side)
        if shares <= 0:
            return hold

        should_exit = False
        exit_reason = "fav_late_exit"
        if getattr(self, "_jev_gate", None) and getattr(self._jev_gate, "enabled", False):
            spot_f = float(quote.btc_spot or 0.0)
            ref_f = float(quote.reference_price or 0.0)
            bid_f = float(bid)
            should_exit, reason_str = self._jev_gate.evaluate_late_exit(
                symbol=getattr(self.settings, "market_symbol", "BTCUSDT"),
                position_side=side.value,
                current_spot=spot_f,
                reference_price=ref_f,
                remaining_seconds=remaining_s,
                current_bid=bid_f,
            )
            if should_exit:
                exit_reason = f"fav_late_exit:{reason_str}"
        else:
            spot_diff = float((quote.btc_spot or 0.0) - (quote.reference_price or 0.0))
            if (side is OutcomeSide.UP and spot_diff < 0) or (side is OutcomeSide.DOWN and spot_diff > 0):
                should_exit = True
                exit_reason = "fav_late_exit:spot_crossed"

        if should_exit:
            LOGGER.info(
                "[FAV_LATE_EXIT] Triggering protective exit market=%s side=%s bid=%s reason=%s",
                campaign.campaign_id, side.value, bid, exit_reason,
            )
            return StrategyDecision(
                ActionType.SELL_PROTECTIVE,
                campaign.state,
                exit_reason,
                side,
                OrderSide.SELL,
                shares,
                bid,
                3000,
                True,
            )
        return hold

    async def _s3s5_late_sniper_decide(self, campaign: Campaign, quote: Quote, now_ms: int) -> StrategyDecision | None:
        """Late sniper sleeve in the final 40 to 18 seconds for unfilled markets.

        Requires:
        1. 18.0 <= remaining_s <= 50.0
        2. abs(spot - reference) >= 8.0 USD
        3. 0.65 <= ask <= 0.85 for the leading side
        4. JEV gate agrees (direction matches, reversal_prob <= 0.25)
        """
        remaining_s = (campaign.market.end_time_ms - now_ms) / 1000.0
        if not (18.0 <= remaining_s <= 50.0):
            return None
        if campaign.position.has_any or campaign.buy_count > 0:
            return None
        if campaign.pending_intent_id or campaign.pending_unknown:
            return None

        has_buy = getattr(self.repository, "has_market_buy", None)
        if callable(has_buy) and await has_buy(campaign.campaign_id):
            return None

        spot = float(quote.btc_spot or 0.0)
        ref = float(quote.reference_price or 0.0)
        if spot <= 0 or ref <= 0:
            return None

        diff = spot - ref
        if abs(diff) < 8.0:
            return None

        lead_side = OutcomeSide.UP if diff > 0 else OutcomeSide.DOWN
        lead_str = lead_side.value
        ask = quote.up_ask if lead_side is OutcomeSide.UP else quote.down_ask
        if ask is None or not ask.is_finite():
            return None

        ask_f = float(ask)
        if not (0.65 <= ask_f <= 0.85):
            return None

        # Check JEV gate if enabled
        if getattr(self, "_jev_gate", None) and getattr(self._jev_gate, "enabled", False):
            cache = self._jev_gate.read_cache(getattr(self.settings, "market_symbol", "BTCUSDT"))
            if cache and isinstance(cache, dict):
                pred = cache.get("prediction")
                if isinstance(pred, dict):
                    jev_dir = str(pred.get("direction") or "").upper()
                    reversal_prob = float(pred.get("reversal_prob") or 0.0)
                    if jev_dir and jev_dir != lead_str and float(pred.get("direction_confidence") or 0.0) >= 0.65:
                        LOGGER.info("[LATE_SNIPER] Skip market=%s JEV dir conflict (lead=%s jev=%s)", campaign.campaign_id, lead_str, jev_dir)
                        return None
                    if reversal_prob > 0.25:
                        LOGGER.info("[LATE_SNIPER] Skip market=%s high reversal hazard (%s)", campaign.campaign_id, reversal_prob)
                        return None

        order_unit = Decimal(str(self._selected_order_unit_usdt or "1"))
        LOGGER.info(
            "[LATE_SNIPER] Triggering late sniper market=%s side=%s ask=%.3f diff=%.2f remaining=%.1fs",
            campaign.campaign_id, lead_str, ask_f, diff, remaining_s,
        )
        return StrategyDecision(
            ActionType.BUY_INITIAL,
            campaign.state,
            f"s3s5 LATE_SNIPER {lead_str}",
            lead_side,
            OrderSide.BUY,
            order_unit,
            ask,
            3000,
            True,
        )

    async def manage_campaign(self, campaign: Campaign) -> bool:
        """Run one management tick for an existing campaign.

        Market/spot refresh and strategy evaluation are always performed;
        control flags only gate BUY execution.  This keeps SELL, CANCEL and
        settlement available during pause/stop/hard-stop recovery.
        """
        self._observe_campaign(campaign)
        self._trace_event('strategy_tick',campaign_id=campaign.campaign_id,pending=bool(campaign.pending_intent_id or campaign.pending_unknown))
        if campaign.pending_intent_id:
            row = await self.repository.get_intent(campaign.pending_intent_id)
            if row:
                intent = self._intent_from_row(row)
                if row.get("order_id"):
                    # Best-effort: space query_order_history while in-flight / freshly
                    # submitted so Live does not hammer weight (ORDER_HISTORY_DEFERRED).
                    if not hasattr(self, "_order_history_poll_at_ms") or not isinstance(
                        self._order_history_poll_at_ms, dict
                    ):
                        self._order_history_poll_at_ms = {}
                    now_hist = self._now_ms()
                    last_hist = int(self._order_history_poll_at_ms.get(campaign.campaign_id, 0) or 0)
                    created_ms = int(getattr(intent, "created_at_ms", 0) or 0)
                    deadline_ms = int(row.get("ttl_deadline_ms") or (created_ms + max(0, int(intent.ttl_ms))))
                    cancel_due = now_hist >= deadline_ms and not bool(row.get("cancel_requested"))
                    if cancel_due:
                        await self._cancel_due_before_history(campaign, intent, str(row["order_id"]))
                    fresh_ms = 1000
                    age_ms = max(0, now_hist - created_ms)
                    cadence_ms = 1000 if age_ms < 5000 else (2500 if age_ms < 15000 else 5000)
                    if bool(row.get("cancel_requested")):
                        cadence_ms = min(cadence_ms, 1000)
                    if cancel_due:
                        self._order_history_poll_at_ms[campaign.campaign_id] = now_hist
                        await self._poll_order_terminal(campaign, intent, str(row["order_id"]), attempts=1)
                    elif created_ms and now_hist - created_ms < fresh_ms:
                        pass  # give exchange a beat before first history poll
                    elif last_hist and now_hist - last_hist < cadence_ms:
                        pass  # cadence backoff
                    else:
                        self._order_history_poll_at_ms[campaign.campaign_id] = now_hist
                        await self._poll_order_terminal(campaign, intent, str(row["order_id"]), attempts=1)
                elif intent.unknown:
                    await self.reconcile_intent_without_order_id(campaign, intent)
                elif self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
                    await self._resolve_expired_c180_pre_submit(campaign, intent, row)
            # A known order is a durable per-campaign execution barrier.  If
            # the read-only history endpoint is temporarily unavailable, keep
            # reconciling this exact order_id and do not run strategy or mark
            # the expired campaign NO_FILL.  This also prevents a duplicate
            # order from being submitted on the next loop tick.
            if campaign.pending_intent_id or campaign.pending_unknown:
                return False
        # Never poll a closed order book or run entry/exit strategy after the
        # contract window.  Reconcile any pending execution above, then
        # resolve so old campaigns cannot starve discovery of the next topic.
        if self._now_ms() >= campaign.market.end_time_ms:
            settlement_now = self._now_ms()
            previous_attempt = int(self._settlement_attempt_at_ms.get(campaign.campaign_id, 0))
            if previous_attempt and settlement_now - previous_attempt < self._settlement_poll_cadence_ms:
                return False
            self._settlement_attempt_at_ms[campaign.campaign_id] = settlement_now
            try:
                settlement = await self.settle_campaign(campaign)
            except PredictionClientError as exc:
                # Position/PnL/redeem-status reads can temporarily return
                # Binance SYSTEM_ERROR while resolution is propagating.  No
                # new order is submitted by these reads, so keep the campaign
                # active and retry on the settlement cadence instead of
                # killing the whole loop.  batch_redeem itself persists an
                # explicit pending result inside settle_campaign.
                self.heartbeat.last_error = str(exc)
                await self.repository.record_risk_event(
                    "SETTLEMENT_DEFERRED",
                    "WARNING",
                    str(exc),
                    campaign_id=campaign.campaign_id,
                    payload={"retryable": True, "read_only": True},
                )
                return False
            self.heartbeat.last_error = None
            if settlement.get("status") == "SETTLED":
                if str(getattr(self, '_selected_strategy_profile', '')).strip().lower() in {'s3s5_pair_v1', 'fav_only_v1', 'fav_only_v2', 'fav_only_v3', 'fav_only_v4'}:
                    try:
                        await self._s3s5_on_settled(campaign, settlement)
                    except Exception as exc:
                        try:
                            await self.repository.record_risk_event(
                                "S3S5_PLAYBOOK_SETTLE_FAILED",
                                "ERROR",
                                str(exc),
                                campaign_id=campaign.campaign_id,
                            )
                        except Exception:
                            self.heartbeat.last_error = f"s3s5 playbook settle: {exc}"
                if self._shadow_lane_experiment_enabled():
                    winner_raw = str(settlement.get("winner") or "").upper()
                    if winner_raw in {OutcomeSide.UP.value, OutcomeSide.DOWN.value}:
                        await self._settle_shadow_lanes(campaign, OutcomeSide(winner_raw))
                try:
                    await self._settle_p3_live_lane(campaign, settlement)
                except Exception as exc:
                    LOGGER.exception("Error settling P3 live lane: %s", exc)
                try:
                    await self._settle_baseline_lane(campaign, settlement)
                except Exception as exc:
                    LOGGER.exception("Error settling Baseline lane attribution: %s", exc)
                self._clear_v3_gate_history(campaign.campaign_id)
                self._active_campaigns.pop(campaign.campaign_id, None)
                self._settlement_attempt_at_ms.pop(campaign.campaign_id, None)
                return True
            return False
        if getattr(self, "_settlement_only_recovery", False):
            return False
        if self._selected_strategy_profile not in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'} and self._defer_pre_entry_quote_collection(campaign, now_ms=self._now_ms()):
            # The staged policy intentionally starts its first book sample
            # before the entry window, not at market creation.  This keeps
            # the leader-duration gate meaningful while preserving the
            # one-minute execution reserve for a fresh entry.
            return False
        try:
            quote = None
            if self._selected_strategy_profile not in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
                if self._s3s5_profile_active():
                    quote = await self._s3s5_ws_quote_for_campaign(campaign)
                if quote is None:
                    quote = await self._quote_for_campaign(campaign)
        except PredictionRateLimitDeferred:
            # A local budget pause is a normal no-op tick.  The next loop
            # iteration retries after the limiter window/backoff.
            return False
        decision_now_ms = self._now_ms()
        if self._shadow_lane_experiment_enabled():
            await self._manage_shadow_lanes(campaign, quote, now_ms=decision_now_ms)
        if quote is not None:
            self._refresh_pnl_btc_peak(campaign, quote, self.strategy)
        # ``quote.observed_at_ms`` is the source timestamp, not the current
        # decision time.  Using it here made a quote look fresh forever and
        # allowed a rate-limit-deferred BUY to execute against an old book.
        if self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            decision = await self._c180_decide(campaign, decision_now_ms)
        elif self._s3s5_profile_active():
            decision = await self._s3s5_pair_decide(campaign, quote, decision_now_ms)
        else:
            decision = self.strategy.decide(campaign, quote, decision_now_ms)
        await self.repository.record_risk_event(
            "STRATEGY_DECISION", "INFO", decision.reason,
            campaign_id=campaign.campaign_id,
            payload={"action": decision.action.value, "mode": self.mode, "allow_new_buys": self._allow_new_buys},
        )
        reduction = decision.action in {ActionType.SELL_PROFIT_LOCK, ActionType.SELL_LOSER, ActionType.SELL_PROTECTIVE, ActionType.CANCEL, ActionType.RECONCILE, ActionType.SETTLE}
        permitted = self._allow_reductions if reduction else self._allow_new_buys
        if decision.action is ActionType.CANCEL and campaign.pending_intent_id:
            row = await self.repository.get_intent(campaign.pending_intent_id)
            if row and row.get("order_id"):
                try:
                    await self._cancel_order_once(self._intent_from_row(row), str(row["order_id"]))
                except PredictionRateLimitDeferred:
                    # Cancellation is retried on the next tick after the
                    # local emergency budget rolls forward; it is not a
                    # worker failure and must not mark the order UNKNOWN.
                    return False
        elif decision.is_trade and permitted:
            await self._handle_decision(campaign, decision)
        elif decision.is_trade:
            self._finish_entry_attempt(campaign.campaign_id, "buy_permission_disabled")

        # Sibling Lane: FAV_P3_LIVE Independent Real Order Lane
        try:
            await self._manage_p3_live_lane(campaign, quote, decision_now_ms)
        except Exception as exc:
            LOGGER.exception("Error in FAV_P3_LIVE lane for %s: %s", campaign.campaign_id, exc)

        await self.repository.save_campaign(campaign)
        return False

    def _shadow_lane_campaign(self, campaign: Campaign, lane: str) -> Campaign:
        lanes = self._shadow_lane_campaigns.setdefault(campaign.campaign_id, {})
        selected = lanes.get(lane)
        if selected is not None:
            return selected
        selected = Campaign(f"{campaign.campaign_id}::shadow::{lane}", campaign.market)
        # The lane consumes the already-collected quote stream but owns its
        # position/decision state.  Copy only continuity counters needed by
        # entry provenance; A/B intentionally do not require stable history.
        selected.last_leader = campaign.last_leader
        selected.leader_flip_count = campaign.leader_flip_count
        selected.reference_cross_count = campaign.reference_cross_count
        selected.last_quote_at_ms = campaign.last_quote_at_ms
        lanes[lane] = selected
        return selected


    async def _record_p3_lane_signal(
        self,
        *,
        campaign: Campaign,
        now_ms: int,
        reject_reason: str | None,
        status: str = "SIGNAL",
        fav_outcome: OutcomeSide | None = None,
        ask_price: float | None = None,
        gate_res: Any = None,
        quote: QuoteSnapshot | None = None,
        size_usdt: float = 1.0,
    ) -> str | None:
        """Best-effort attribution row for every FAV_P3 evaluate exit path."""
        recorder = getattr(self.repository, "record_lane_signal", None)
        if not callable(recorder):
            return None
        direction = fav_outcome.value if fav_outcome is not None else None
        market_id = None
        window_id = None
        try:
            window_id = campaign.market.market_topic_id
            if fav_outcome is not None:
                market_id = campaign.market.market_id_for(fav_outcome)
        except Exception:
            pass
        reference_price = 0.0
        spot_price = 0.0
        pre_cross_count = 0
        same_side_seconds = 0.0
        distance_bps = 0.0
        ask_at = ask_price
        bid_at = None
        if gate_res is not None:
            reference_price = float(getattr(gate_res, "reference_price", 0.0) or 0.0)
            spot_price = float(getattr(gate_res, "spot_price", 0.0) or 0.0)
            pre_cross_count = int(getattr(gate_res, "pre_cross_count", None) or getattr(gate_res, "cross_count", 0) or 0)
            same_side_seconds = float(getattr(gate_res, "same_side_seconds", 0.0) or 0.0)
            distance_bps = float(getattr(gate_res, "distance_bps", 0.0) or 0.0)
            if ask_at is None:
                ask_at = getattr(gate_res, "ask_price", None)
        if quote is not None and fav_outcome is not None:
            try:
                bid_raw = quote.up_bid if fav_outcome is OutcomeSide.UP else quote.down_bid
                bid_at = float(bid_raw or 0)
            except Exception:
                bid_at = None
            if reference_price <= 0 and getattr(quote, "reference_price", None) is not None:
                try:
                    reference_price = float(quote.reference_price)
                except Exception:
                    pass
            if spot_price <= 0 and getattr(quote, "btc_spot", None) is not None:
                try:
                    spot_price = float(quote.btc_spot)
                except Exception:
                    pass
        try:
            return await recorder(
                strategy_lane="FAV_P3_LIVE",
                campaign_id=str(campaign.campaign_id),
                market_id=market_id,
                window_id=window_id,
                intent_id=None,
                signal_ts=now_ms,
                direction=direction,
                reference_price=reference_price,
                spot_price=spot_price,
                pre_cross_count=pre_cross_count,
                same_side_seconds=same_side_seconds,
                distance_bps=distance_bps,
                ask_at_signal=float(ask_at) if ask_at is not None else None,
                bid_at_signal=bid_at,
                status=status,
                reject_reason=reject_reason,
                size_usdt=size_usdt,
            )
        except Exception as e:
            LOGGER.debug("P3 attribution signal log failed: %s", e)
            return None

    async def _manage_p3_live_lane(self, campaign: Campaign, quote: QuoteSnapshot, now_ms: int) -> None:
        """Independent switchable FAV_P3 lane (EXP-P3-01). Does not gate Baseline."""
        from src.gridbot.prediction import s3s5_pair as s3

        entry_loop, entry_profile = self._loop_id, self._selected_strategy_profile
        arm = self._fav_p3_arm()
        if arm == "off" and not self._is_fav_p3_profile():
            return

        # Only run on FAV-capable profiles (fav_p3 = dedicated P3 lane, no R3)
        curr_profile = str(getattr(self, "_selected_strategy_profile", "")).strip().lower()
        if curr_profile not in {"fav_only_v1", "fav_only_v2", "fav_only_v3", "fav_only_v4", "fav_p3", "s3s5_pair_v1"}:
            return

        # Sibling campaigns already carry ::p3 (or shadow). Never nest ::p3::p3 —
        # that duplicates attribution when a saved P3 row is reloaded into
        # _active_campaigns and manage_campaign runs again.
        raw_id = str(campaign.campaign_id or "")
        if raw_id.endswith("::p3") or "::shadow::" in raw_id:
            return
        p3_camp_id = f"{raw_id}::p3"
        p3_campaign = self._p3_active_campaigns.get(p3_camp_id)
        if p3_campaign is None:
            p3_campaign = Campaign(p3_camp_id, campaign.market)
            self._p3_active_campaigns[p3_camp_id] = p3_campaign
            if hasattr(self.repository, "save_campaign"):
                await self.repository.save_campaign(p3_campaign, loop_id=self._loop_id)

        # Ensure single entry per window
        has_buy = getattr(self.repository, "has_market_buy", None)
        if callable(has_buy) and await has_buy(p3_camp_id):
            return
        if p3_campaign.buy_count > 0 or p3_campaign.pending_intent_id or p3_campaign.position.has_any:
            return

        # 1. Check if FAV signal qualifies (every exit records attribution)
        start = int(campaign.market.start_time_ms)
        mapped = s3.quote_from_snapshot(quote, now_ms)
        if mapped is None:
            await self._record_p3_lane_signal(
                campaign=p3_campaign, now_ms=now_ms, quote=quote, reject_reason="no_quote_map",
            )
            return

        state = self._s3s5_engine.get(start) if hasattr(self, "_s3s5_engine") else None
        if not state:
            await self._record_p3_lane_signal(
                campaign=p3_campaign, now_ms=now_ms, quote=quote, reject_reason="no_engine_state",
            )
            return

        legs = state.get("legs") or {}
        fav_leg = legs.get("FAV") or {}
        # Same source as baseline FAV (_s3s5_pair_decide / s3.step):
        # side lives under pending.side or fill.side — never top-level legs.FAV.side.
        # Do not use quote.leader (book leader != ask-favorite signal).
        fav_side_val = None
        fav_ask = None
        pending = fav_leg.get("pending")
        fill = fav_leg.get("fill")
        if isinstance(pending, Mapping) and pending.get("side"):
            fav_side_val = str(pending.get("side") or "").upper() or None
            fav_ask = pending.get("ask")
        elif isinstance(fill, Mapping) and fill.get("side"):
            fav_side_val = str(fill.get("side") or "").upper() or None
            fav_ask = fill.get("ask")
        else:
            history = state.get("history") or []
            if history and s3.valid(mapped, now_ms):
                try:
                    features = s3.feature(list(history), mapped, now_ms, start)
                    fav_side_val = s3.signal(
                        "FAV",
                        features,
                        profile=self._selected_strategy_profile,
                    )
                except Exception:
                    fav_side_val = None
            if fav_side_val:
                try:
                    fav_ask = mapped[str(fav_side_val)]["ask"]
                except Exception:
                    fav_ask = None
        if fav_side_val not in ("UP", "DOWN"):
            await self._record_p3_lane_signal(
                campaign=p3_campaign, now_ms=now_ms, quote=quote, reject_reason="no_fav_side",
            )
            return

        fav_outcome = OutcomeSide(fav_side_val)
        ask_price = fav_ask or (quote.up_ask if fav_outcome is OutcomeSide.UP else quote.down_ask)
        if ask_price is None:
            await self._record_p3_lane_signal(
                campaign=p3_campaign, now_ms=now_ms, quote=quote, fav_outcome=fav_outcome,
                reject_reason="no_ask",
            )
            return

        fav_passed = s3.ask_allowed("FAV", float(ask_price), profile=self._selected_strategy_profile)

        # 2. Evaluate P3 gate
        gate_res = self._p3_gate_evaluator.evaluate(
            campaign.campaign_id,
            quote,
            fav_outcome,
            ask_price,
            now_ms,
        )

        # Normalize gate / fav reject taxonomy for attribution (gates/thresholds untouched)
        _GATE_REASON_MAP = {
            "P3_REJECT_CROSS": "gate_pre_cross",
            "P3_REJECT_SAME_SIDE": "gate_same_side",
            "P3_REJECT_DISTANCE": "gate_distance",
            "P3_REJECT_STALE_DATA": "fail_closed_stale",
        }
        fav_label = "PASS" if fav_passed else "WAIT"
        action_label = "BUY" if (fav_passed and gate_res.allowed) else "PASS"
        if fav_passed and gate_res.allowed:
            reason_label = None
        elif not fav_passed:
            reason_label = "fav_ask_not_allowed"
        else:
            raw = gate_res.reject_reason or "gate_reject"
            reason_label = _GATE_REASON_MAP.get(raw, raw)

        # 3. Formatted status log line (per specification XIII)
        log_line = (
            f"[FAV_P3] market={campaign.campaign_id} side={fav_outcome.value} fav={fav_label} "
            f"cross={gate_res.cross_count} same_side={gate_res.same_side_seconds:.1f}s "
            f"distance={gate_res.distance_bps:.1f}bps ask={float(ask_price):.2f} "
            f"ACTION={action_label}"
            + (f" size=1U" if action_label == "BUY" else f" reason={reason_label or ''}")
        )
        print(log_line, flush=True)
        LOGGER.info("%s", log_line)

        attr_id = await self._record_p3_lane_signal(
            campaign=p3_campaign,
            now_ms=now_ms,
            quote=quote,
            fav_outcome=fav_outcome,
            ask_price=float(ask_price),
            gate_res=gate_res,
            status="SIGNAL" if action_label == "PASS" else "SUBMITTED",
            reject_reason=reason_label,
        )

        if action_label != "BUY":
            return

        # 4. Real orders: fav_p3 profile or arm=live
        if not (self._is_fav_p3_profile() or self._fav_p3_live_orders_enabled()):
            LOGGER.info("[FAV_P3] arm=%s — signal accepted, no real order", arm)
            if attr_id and hasattr(self.repository, "update_lane_order"):
                try:
                    await self.repository.update_lane_order(
                        attr_id,
                        status="SHADOW_ACCEPTED",
                        reject_reason=None,
                    )
                except Exception as e:
                    LOGGER.debug("P3 shadow accept update failed: %s", e)
            return

        # 5. Live execution checks (log reject on existing SIGNAL/SUBMITTED row)
        async def _p3_exec_reject(code: str) -> None:
            if attr_id and hasattr(self.repository, "update_lane_order"):
                try:
                    await self.repository.update_lane_order(
                        attr_id, status="SIGNAL", reject_reason=code,
                    )
                except Exception as e:
                    LOGGER.debug("P3 exec reject update failed: %s", e)

        if not self.live_capability or not self.orders_enabled:
            LOGGER.warning("[FAV_P3] Live capability or orders not enabled, skipping execution")
            await _p3_exec_reject("cap_live_or_orders_disabled")
            return
        if not self._allow_new_buys or self._hard_stop_latched:
            LOGGER.warning("[FAV_P3] Buys blocked or hard stop latched")
            await _p3_exec_reject(
                "risk_hard_stop" if self._hard_stop_latched else "risk_buys_blocked"
            )
            return
        if not self.settings.wallet_address or not self.settings.wallet_id:
            LOGGER.warning("[FAV_P3] Missing wallet address/id")
            await _p3_exec_reject("cap_missing_wallet")
            return

        # Construct intent
        intent_id = f"fav-p3-{campaign.campaign_id}-{now_ms}"
        limit_px = Decimal(str(ask_price))
        token_id = campaign.market.up_token_id if fav_outcome is OutcomeSide.UP else campaign.market.down_token_id

        intent = OrderIntent(
            intent_id=intent_id,
            campaign_id=p3_camp_id,
            action=ActionType.BUY_INITIAL,
            outcome=fav_outcome,
            order_side=OrderSide.BUY,
            amount=P3_FIXED_ORDER_UNIT_USDT,
            limit_price=limit_px,
            created_at_ms=now_ms,
            ttl_ms=int(P3_UNFILLED_TIMEOUT_SECONDS * 1000),
            attempt=1,
            tier="FAV_P3_LIVE",
        )

        p3_campaign.pending_intent_id = intent.intent_id
        if hasattr(self.repository, "save_campaign_and_intent"):
            await self.repository.save_campaign_and_intent(p3_campaign, intent)
        else:
            await self.repository.create_intent(intent)

        # 5. Rate limit & Call Binance API
        execution_weight = self._endpoint_weight("get_quote") + self._endpoint_weight("place_order")
        if not self._rate_limiter.acquire(execution_weight, block=False, emergency=False):
            LOGGER.warning("[FAV_P3] Rate limit budget deferred for P3 order")
            await _p3_exec_reject("cap_rate_limit")
            return

        order_post_completed = False
        try:
            quote_resp = await self._call_api(
                "get_quote",
                wallet_address=self.settings.wallet_address,
                _trace_campaign_id=p3_camp_id,
                _trace_intent_id=intent.intent_id,
                token_id=token_id,
                side="BUY",
                amount_in=normalize_amount_in(P3_FIXED_ORDER_UNIT_USDT),
                order_type=self.settings.order_type,
                slippage_bps=self.settings.slippage_bps,
                chain_id=self.settings.chain_id,
                price_limit=str(intent.limit_price),
                funding_source=getattr(self.settings, "funding_source", "MPC"),
                _weight_pre_acquired=True,
                _shared_pre_acquired=True,
            )
            quote_id = str(quote_resp.get("quoteId") or quote_resp.get("quote_id") or "") if isinstance(quote_resp, Mapping) else ""
            if not quote_id:
                LOGGER.warning("[FAV_P3] quote response had no quoteId")
                await _p3_exec_reject("no_quote_id")
                return

            order_resp = await self._call_api(
                "place_order",
                _entry_buy=True, _entry_loop_id=entry_loop, _entry_profile=entry_profile,
                _trace_campaign_id=p3_camp_id,
                _trace_intent_id=intent.intent_id,
                wallet_address=self.settings.wallet_address,
                wallet_id=self.settings.wallet_id,
                quote_id=quote_id,
                account_type=self.settings.account_type,
                order_type=self.settings.order_type,
                time_in_force=self.settings.time_in_force,
                slippage_bps=self.settings.slippage_bps,
                price_limit=str(intent.limit_price),
                funding_source=getattr(self.settings, "funding_source", "MPC"),
                _weight_pre_acquired=True,
                _shared_pre_acquired=True,
            )
            order_post_completed = True
            order_id = str(order_resp.get("orderId") or order_resp.get("order_id") or "") if isinstance(order_resp, Mapping) else ""
            if not order_id:
                LOGGER.error("[FAV_P3] place_order response had no orderId: %s", order_resp)
                await _p3_exec_reject("no_order_id")
                return

            submitted_at = self._now_ms()
            await self._record_order_result(p3_campaign, intent, order_id, order_resp)
            await self.repository.update_intent(intent.intent_id, order_id=order_id, status="SUBMITTED")
            p3_campaign.buy_count += 1
            p3_campaign.initial_attempts += 1
            p3_campaign.initial_outcome = fav_outcome
            await self.repository.save_campaign(p3_campaign)

            if attr_id and hasattr(self.repository, "update_lane_order"):
                await self.repository.update_lane_order(
                    attr_id,
                    order_id=order_id,
                    order_submit_ts=submitted_at,
                    status="SUBMITTED",
                )

            # 6. Poll for fill with strict 5-second deadline
            LOGGER.info("[FAV_P3] REAL ORDER submitted: id=%s px=%s amt=1U, polling fill up to 5s...", order_id, limit_px)
            filled = False
            start_poll = self._now_ms()
            while self._now_ms() - start_poll <= int(P3_UNFILLED_TIMEOUT_SECONDS * 1000):
                terminal_status = await self._poll_order_terminal(p3_campaign, intent, order_id, attempts=1)
                if terminal_status == "FILLED":
                    filled = True
                    break
                await asyncio.sleep(1.0)

            now_done = self._now_ms()
            if filled:
                latency = max(0, now_done - submitted_at)
                LOGGER.info("[FAV_P3] REAL ORDER FILLED: id=%s latency=%dms", order_id, latency)
                if attr_id and hasattr(self.repository, "update_lane_order"):
                    await self.repository.update_lane_order(
                        attr_id,
                        fill_ts=now_done,
                        fill_price=float(intent.limit_price),
                        fill_shares=float(p3_campaign.position.shares(fav_outcome)),
                        fill_latency_ms=latency,
                        status="FILLED",
                    )
            else:
                LOGGER.warning("[FAV_P3] 5s timeout reached without fill for order %s. Cancelling...", order_id)
                try:
                    await self._cancel_order_once(intent, order_id)
                except Exception as e:
                    LOGGER.error("[FAV_P3] Error cancelling timed out order %s: %s", order_id, e)
                p3_campaign.pending_intent_id = None
                await self.repository.save_campaign(p3_campaign)
                if attr_id and hasattr(self.repository, "update_lane_order"):
                    await self.repository.update_lane_order(
                        attr_id,
                        status="CANCELLED",
                        reject_reason="P3_REJECT_ORDER_TIMEOUT",
                    )
        except (PredictionEntryNotSubmitted, PredictionRateLimitDeferred) as exc:
            # Admission failed before any order POST. Preserve the intent audit
            # record without stranding the campaign behind an unknown order.
            if order_post_completed:
                LOGGER.warning("[FAV_P3] Order management deferred for %s: %s", p3_camp_id, exc)
                return
            await self._reject_unsubmitted_entry(p3_campaign, intent, p3_campaign.state, str(exc))
            await _p3_exec_reject("entry_not_submitted")
        except Exception as exc:
            LOGGER.exception("[FAV_P3] Execution error for %s: %s", p3_camp_id, exc)

    async def _settle_p3_live_lane(self, campaign: Campaign, settlement: Mapping[str, Any]) -> None:
        """Settle P3 live lane independently."""
        p3_camp_id = f"{campaign.campaign_id}::p3"
        p3_campaign = self._p3_active_campaigns.get(p3_camp_id)
        winner_raw = str(settlement.get("winner") or "").upper()
        if not winner_raw:
            return

        if p3_campaign is not None and (p3_campaign.position.has_any or p3_campaign.buy_count > 0):
            target_side = p3_campaign.initial_outcome
            if target_side is not None:
                is_win = (target_side.value.upper() == winner_raw)
                shares = p3_campaign.position.shares(target_side)
                cost = p3_campaign.position.total_buy_cost
                payout = shares * Decimal("1.0") if is_win else Decimal("0")
                net_pnl = float(payout - cost)
                res_str = "WIN" if is_win else "LOSS"

                LOGGER.info("[FAV_P3] Settlement for %s: result=%s pnl=%.4f (shares=%s cost=%s)",
                            campaign.campaign_id, res_str, net_pnl, shares, cost)
                if hasattr(self.repository, "settle_lane_attribution"):
                    await self.repository.settle_lane_attribution(
                        campaign.campaign_id,
                        "FAV_P3_LIVE",
                        final_result=res_str,
                        realized_pnl=net_pnl,
                    )
        # Spec §13: always settle rejects/signals with hypothetical PnL for CONTROL comparison
        hypo = getattr(self.repository, "settle_lane_rejects_hypothetical", None)
        if callable(hypo):
            try:
                await hypo(campaign.campaign_id, "FAV_P3_LIVE", winner=winner_raw)
            except Exception as e:
                LOGGER.debug("P3 reject hypothetical settle failed: %s", e)
        if p3_campaign is not None:
            p3_campaign.state = CampaignState.DONE
            try:
                await self.repository.save_campaign(p3_campaign)
            except Exception:
                pass
        self._p3_active_campaigns.pop(p3_camp_id, None)

    async def _settle_baseline_lane(self, campaign: Campaign, settlement: Mapping[str, Any]) -> None:
        """Settle Baseline lane attribution."""
        winner_raw = str(settlement.get("winner") or "").upper()
        if not winner_raw:
            return
        net_pnl = float(settlement.get("net_pnl") or 0.0)
        has_pos = campaign.position.has_any or campaign.buy_count > 0
        if has_pos and campaign.initial_outcome:
            is_win = (campaign.initial_outcome.value.upper() == winner_raw)
            res_str = "WIN" if is_win else "LOSS"
        else:
            res_str = "PASS"

        if hasattr(self.repository, "settle_lane_attribution"):
            await self.repository.settle_lane_attribution(
                campaign.campaign_id,
                "FAV_BASELINE",
                final_result=res_str,
                realized_pnl=net_pnl,
            )

    async def _restore_shadow_lane_campaign(self, campaign: Campaign, lane: str) -> Campaign:
        """Rebuild a virtual lane position from immutable fills after restart."""

        key = (campaign.campaign_id, lane)
        lane_campaign = self._shadow_lane_campaign(campaign, lane)
        if key in self._shadow_lane_restored:
            return lane_campaign
        self._shadow_lane_restored.add(key)
        getter = getattr(self.repository, "get_shadow_campaign_identity", None)
        fills_getter = getattr(self.repository, "get_shadow_fills", None)
        if not callable(getter) or not callable(fills_getter):
            return lane_campaign
        await self._ensure_shadow_window()
        config_hash = str(self._shadow_lane_config_hashes.get(lane) or self._shadow_lane_config_hash(lane))
        window = self._shadow_lane_windows.get(lane) or self._shadow_window or {}
        start = int(window.get("window_start_ms", self._now_ms()))
        end = int(window.get("window_end_ms", start + 86_400_000))
        shadow_campaign_id = f"{campaign.campaign_id}::shadow::{lane}"
        row = await getter(
            campaign_id=shadow_campaign_id,
            config_hash=config_hash,
            window_start_ms=start,
            window_end_ms=end,
        )
        if not row:
            return lane_campaign
        shadow_id = str(row.get("shadow_campaign_id") or "")
        if shadow_id:
            self._shadow_lane_shadow_ids[key] = shadow_id
        fills = await fills_getter(shadow_id) if shadow_id else []
        for fill in fills:
            try:
                outcome = OutcomeSide(str(fill.get("outcome") or "").upper())
                order_side = OrderSide(str(fill.get("order_side") or "").upper())
                shares = as_decimal(fill.get("shares"))
                price = as_decimal(fill.get("price"))
                gross = as_decimal(fill.get("gross_amount"))
                fee = as_decimal(fill.get("simulated_fee"))
            except (TypeError, ValueError, ArithmeticError):
                continue
            if shares <= 0:
                continue
            if order_side is OrderSide.BUY:
                initial = lane_campaign.initial_outcome is None
                lane_campaign.position.add_buy(outcome, shares, gross, fee, initial=initial)
                lane_campaign.buy_count += 1
                if initial:
                    lane_campaign.initial_outcome = outcome
                    lane_campaign.initial_filled_at_ms = int(fill.get("event_time_ms") or self._now_ms())
            else:
                if shares <= lane_campaign.position.shares(outcome):
                    lane_campaign.position.add_sell(outcome, shares, gross, fee)
            lane_campaign.order_attempts += 1
        if lane_campaign.initial_outcome is not None:
            lane_campaign.state = CampaignState.INITIAL_POSITION
        return lane_campaign

    async def _freeze_v3_net_edge_candidate(
        self, campaign: Campaign, quote: QuoteSnapshot, decision: StrategyDecision,
        *, lane: str, now_ms: int,
    ) -> tuple[StrategyDecision, dict[str, Any]]:
        """Compatibility wrapper for the frozen Net Edge cost hypothesis."""

        costs = v3_net_edge_cost_gate(
            decision, fee_bps=getattr(self.settings, "shadow_fee_bps", 10),
            slippage_bps=getattr(self.settings, "shadow_slippage_bps", 0),
        ) if decision.action is ActionType.BUY_INITIAL else {}
        return await self._freeze_v3_first_candidate(
            campaign, quote, decision, lane=lane, now_ms=now_ms,
            gate_result=costs, label="V3 Net Edge",
        )

    async def _freeze_v3_momentum30_candidate(
        self, campaign: Campaign, quote: QuoteSnapshot, decision: StrategyDecision,
        *, lane: str, now_ms: int, historical_quotes: list[QuoteSnapshot],
    ) -> tuple[StrategyDecision, dict[str, Any]]:
        gate = v3_momentum30_gate(
            quote, historical_quotes, leader_side=decision.outcome,
            now_ms=now_ms, profile=lane,
        ) if decision.action is ActionType.BUY_INITIAL else {}
        return await self._freeze_v3_first_candidate(
            campaign, quote, decision, lane=lane, now_ms=now_ms,
            gate_result=gate, label=lane,
        )

    async def _freeze_v3_first_candidate(
        self,
        campaign: Campaign,
        quote: QuoteSnapshot,
        decision: StrategyDecision,
        *,
        lane: str,
        now_ms: int,
        gate_result: Mapping[str, Any],
        label: str,
    ) -> tuple[StrategyDecision, dict[str, Any]]:
        """Consume the first Base V3 BUY opportunity durably, before fill.

        Existing accept/reject records are terminal even if no fill survived
        a crash. Later quotes can never replace the recorded candidate.
        """
        if decision.action is not ActionType.BUY_INITIAL:
            return decision, {"gate_version": lane, "candidate_frozen": False}
        config_hash = str(self._shadow_lane_config_hashes.get(lane) or self._shadow_lane_config_hash(lane))
        window = self._shadow_lane_windows.get(lane) or self._shadow_window or {}
        identity = {
            "lane": lane, "config_hash": config_hash,
            "window_start_ms": int(window["window_start_ms"]),
            "window_end_ms": int(window["window_end_ms"]),
            "campaign_id": campaign.campaign_id,
            "market_topic_id": campaign.market.market_topic_id,
            "market_id": campaign.market.market_id,
        }
        key = "prediction_shadow_first_candidate:" + hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        record = {
            **identity, **dict(gate_result), "candidate_id": key,
            "decision_at_ms": int(now_ms), "quote_at_ms": quote.observed_at_ms,
            "candidate_consumed": True,
            "base_decision": asdict(decision), "quote": asdict(quote),
            "execution_model": "instant_limit_fill_with_cash_cost_reserve_not_verified_execution",
            "restart_policy": "never_retry_consumed_candidate_even_if_unfilled",
        }
        # The repository's INSERT-IF-ABSENT returns the winner of races; only
        # that creator may fill. A write failure propagates before simulation.
        created, frozen = await self.repository.create_shadow_first_candidate(key, record)
        telemetry = {
            **frozen, "candidate_frozen": True, "candidate_created": created,
            "candidate_reused": not created,
        }
        if created and frozen.get("allowed") is True:
            return replace(decision, reason=f"{label} first candidate accepted; unvalidated hypothesis"), telemetry
        return replace(
            decision, action=ActionType.HOLD, state=CampaignState.OBSERVE,
            reason=(f"{label} first candidate already consumed; no later entry"
                    if not created else f"{label} first candidate rejected: {frozen.get('reason')}"),
            trade_allowed=False,
        ), telemetry

    async def _create_shadow_cohort_record(
        self, key: str, value: Mapping[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """Create immutable cohort metadata in its own Shadow namespace."""

        if not key.startswith("prediction_shadow_cohort_"):
            raise ValueError("cohort record must be Shadow-scoped")
        inserted = await self.repository._execute(
            "INSERT INTO prediction_runtime_config(config_key,config_value_json,updated_at_ms) "
            "VALUES(?,?,?) ON CONFLICT(config_key) DO NOTHING",
            (key, json.dumps(dict(value), sort_keys=True, separators=(",", ":"), default=str), self._now_ms()),
        )
        frozen = await self.repository.get_runtime_config(key)
        if not isinstance(frozen, dict):
            raise RuntimeError("durable Shadow cohort record missing or corrupt")
        return inserted == 1, frozen

    async def _claim_momentum30_cohort(
        self, campaign: Campaign, quote: QuoteSnapshot, *, now_ms: int,
    ) -> dict[str, Any] | None:
        """Claim the shared first Base candidate before any per-arm mutation.

        An interrupted creator never hands its unfinished arms to a later
        quote.  Missing execution is censored; no fills are replayed.
        """

        treatments = V3_MOMENTUM30_PROFILES & self._shadow_lane_strategies.keys()
        if not treatments:
            return None
        lanes = sorted(treatments | ({QUALITY_HOLD_V3_PROFIT1_PROFILE} & self._shadow_lane_strategies.keys()))
        identity = {
            "version": "momentum30-shared-first-base-candidate-v1",
            "campaign_id": campaign.campaign_id,
            "market_topic_id": campaign.market.market_topic_id,
            "market_id": campaign.market.market_id,
            "lanes": lanes,
            "lane_identities": {
                lane: {
                    "config_hash": self._shadow_lane_config_hashes[lane],
                    "window_start_ms": int(self._shadow_lane_windows[lane]["window_start_ms"]),
                    "window_end_ms": int(self._shadow_lane_windows[lane]["window_end_ms"]),
                }
                for lane in lanes
            },
        }
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        key = f"prediction_shadow_cohort_first_candidate:{digest}"
        completion_key = f"prediction_shadow_cohort_completion:{digest}"
        frozen = await self.repository.get_runtime_config(key)
        created = False
        if not isinstance(frozen, Mapping):
            base_lane = QUALITY_HOLD_V3_PROFIT1_PROFILE if QUALITY_HOLD_V3_PROFIT1_PROFILE in lanes else lanes[0]
            base_campaign = await self._restore_shadow_lane_campaign(campaign, base_lane)
            base_machine = PredictionStateMachine(
                self._sized_strategy_config(QUALITY_HOLD_V3_PROFIT1_PROFILE, self._selected_order_unit_usdt)
            )
            base_decision = base_machine.decide(base_campaign, quote, now_ms)
            if base_decision.action is not ActionType.BUY_INITIAL:
                return {"lanes": lanes, "created": False, "eligible": False, "execution_censored": False}
            created, frozen = await self._create_shadow_cohort_record(key, {
                **identity, "candidate_id": key, "quote_at_ms": quote.observed_at_ms,
                "decision_at_ms": int(now_ms), "base_decision": asdict(base_decision),
                "quote": asdict(quote), "candidate_consumed": True,
                "restart_policy": "creator_call_only_unfinished_arms_execution_censored_never_retry",
            })
        complete = await self.repository.get_runtime_config(completion_key, {})
        censored = not created and not (isinstance(complete, Mapping) and complete.get("complete") is True)
        if censored:
            marked, _ = await self._create_shadow_cohort_record(
                f"prediction_shadow_cohort_execution_censored:{digest}",
                {"candidate_id": key, "lanes": lanes, "quote_at_ms": frozen.get("quote_at_ms"),
                 "detected_at_ms": int(now_ms), "execution_censored": True,
                 "reason": "shared candidate was consumed before every arm completed; later entries forbidden"},
            )
            if marked:
                await self.repository.record_risk_event(
                    "SHADOW_COHORT_EXECUTION_CENSORED", "WARNING",
                    "Interrupted shared first candidate; unfinished arms cannot use a later quote",
                    campaign_id=campaign.campaign_id,
                    payload={"candidate_id": key, "lanes": lanes, "execution_censored": True,
                             "frozen_quote_at_ms": frozen.get("quote_at_ms"), "shadow_only": True},
                )
        return {"lanes": lanes, "created": created, "eligible": True, "frozen": dict(frozen),
                "candidate_id": key, "completion_key": completion_key, "execution_censored": censored}

    async def _manage_shadow_lanes(
        self,
        campaign: Campaign,
        quote: QuoteSnapshot,
        *,
        now_ms: int | None = None,
        observer: bool = False,
    ) -> None:
        """Evaluate configured virtual lanes against the same quote snapshot."""

        await self._ensure_shadow_window()
        decision_now_ms = self._now_ms() if now_ms is None else int(now_ms)
        if ({QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE, *V3_MOMENTUM30_PROFILES} & self._shadow_lane_strategies.keys()):
            # The control and treatment must see the same source-quality
            # rejection before defining their first comparable opportunity.
            if not v3_net_edge_source_valid(quote, decision_now_ms):
                quote = replace(quote, feed_ok=False)
        cohort = await self._claim_momentum30_cohort(campaign, quote, now_ms=decision_now_ms)
        for lane, machine in self._shadow_lane_strategies.items():
            if await self._shadow_lane_at_exact_target(lane):
                continue
            lane_campaign = await self._restore_shadow_lane_campaign(campaign, lane)
            self._refresh_pnl_btc_peak(lane_campaign, quote, machine)
            decision = machine.decide(lane_campaign, quote, decision_now_ms)
            gate_telemetry = self._shadow_gate_bypass_telemetry(
                lane,
                reason="gate not applicable to this decision",
            )
            cohort_member = cohort is not None and lane in cohort["lanes"]
            cohort_blocked = cohort_member and not cohort["created"]
            if cohort_blocked and decision.action is ActionType.BUY_INITIAL:
                decision = replace(
                    decision, action=ActionType.HOLD, state=CampaignState.OBSERVE,
                    reason=("Shared first candidate consumed; execution censored and no later entry"
                            if cohort["execution_censored"] else "Shared first Base candidate unavailable or already consumed; no later entry"),
                    trade_allowed=False,
                )
            if lane == QUALITY_HOLD_V3_NET_EDGE_V1_PROFILE:
                decision, gate_telemetry = await self._freeze_v3_net_edge_candidate(
                    lane_campaign, quote, decision, lane=lane, now_ms=decision_now_ms,
                )
            if lane in V3_MOMENTUM30_PROFILES:
                historical = (
                    await self._historical_momentum30_quotes(campaign, quote, observer=observer)
                    if decision.action is ActionType.BUY_INITIAL else []
                )
                decision, gate_telemetry = await self._freeze_v3_momentum30_candidate(
                    lane_campaign, quote, decision, lane=lane, now_ms=decision_now_ms,
                    historical_quotes=historical,
                )
            if self._is_v3_gate_lane(lane):
                if lane_campaign.position.has_any or lane_campaign.initial_outcome is not None:
                    gate_telemetry = self._shadow_gate_bypass_telemetry(
                        lane,
                        reason="post-entry V3 one-shot hold bypass",
                    )
                elif decision.action is ActionType.BUY_INITIAL:
                    historical = await self._historical_v3_gate_quotes(
                        campaign,
                        quote,
                        observer=observer,
                    )
                    regime = await self._shadow_regime_for_tick()
                    gate_telemetry = quality_hold_v3_gate(
                        quote, historical, regime, quote.leader
                    )
                    if not bool(gate_telemetry.get("allowed")):
                        decision = replace(
                            decision,
                            action=ActionType.HOLD,
                            state=CampaignState.OBSERVE,
                            reason=f"V3 gate blocked: {gate_telemetry.get('reason')}",
                            trade_allowed=False,
                        )
                else:
                    gate_telemetry = self._shadow_gate_bypass_telemetry(
                        lane,
                        reason="base V3 entry gate not met",
                    )
            if cohort_member:
                gate_telemetry.update({
                    "cohort_candidate_id": cohort.get("candidate_id"),
                    "cohort_candidate_created": cohort["created"],
                    "cohort_quote_at_ms": cohort.get("frozen", {}).get("quote_at_ms"),
                    "execution_censored": cohort["execution_censored"],
                })
            await self.repository.record_risk_event(
                "SHADOW_LANE_DECISION",
                "INFO",
                decision.reason,
                campaign_id=campaign.campaign_id,
                payload={
                    "lane": lane,
                    "virtual_campaign_id": lane_campaign.campaign_id,
                    "action": decision.action.value,
                    "mode": self.mode,
                    "shadow_only": True,
                    **gate_telemetry,
                },
            )
            # Sidecar decisions are counterfactual telemetry.  Live pause,
            # loss-limit, and BUY permissions must not suppress their sample
            # or the WR estimate would be biased by the active lane's risk
            # state.  _simulate_shadow_decision never calls the API client.
            if decision.is_trade:
                await self._simulate_shadow_decision(
                    lane_campaign,
                    decision,
                    lane=lane,
                    event_time_ms=decision_now_ms,
                )
        if cohort is not None and cohort["created"]:
            await self.repository.set_runtime_config(
                cohort["completion_key"], {"complete": True, "completed_at_ms": self._now_ms()},
            )

    async def _settle_shadow_lanes(self, campaign: Campaign, winner: OutcomeSide | str) -> None:
        """Settle each virtual lane and persist lane-specific evidence."""

        if not self._shadow_lane_experiment_enabled():
            return
        resolution = winner.value if isinstance(winner, OutcomeSide) else str(winner)
        if resolution not in {"UP", "DOWN", "DRAW"}:
            raise ValueError("unconfirmed Shadow resolution")
        payout_metadata = {
            "payout_up": "0.5" if resolution == "DRAW" else ("1" if resolution == "UP" else "0"),
            "payout_down": "0.5" if resolution == "DRAW" else ("1" if resolution == "DOWN" else "0"),
        }
        await self._ensure_shadow_window()
        evidence_by_lane = await self.repository.get_runtime_config(
            "prediction_shadow_lane_evidence", {}
        )
        evidence_by_lane = dict(evidence_by_lane) if isinstance(evidence_by_lane, Mapping) else {}
        for lane in self._shadow_lane_strategies:
            if await self._shadow_lane_at_exact_target(lane):
                continue
            lane_campaign = await self._restore_shadow_lane_campaign(campaign, lane)
            fees = lane_campaign.position.fees
            pnl = (
                lane_campaign.position.realized_cash
                - lane_campaign.position.total_buy_cost
                - fees
                + lane_campaign.position.shares(OutcomeSide.UP) * Decimal(payout_metadata["payout_up"])
                + lane_campaign.position.shares(OutcomeSide.DOWN) * Decimal(payout_metadata["payout_down"])
            )
            config_hash = str(self._shadow_lane_config_hashes.get(lane) or self._shadow_lane_config_hash(lane))
            window = self._shadow_lane_windows.get(lane) or self._shadow_window or {}
            start = int(window.get("window_start_ms", self._now_ms()))
            end = int(window.get("window_end_ms", start + 86_400_000))
            key = (campaign.campaign_id, lane)
            shadow_id = self._shadow_lane_shadow_ids.get(key)
            if not shadow_id:
                shadow = await self.repository.record_shadow_campaign(
                    lane_campaign,
                    config_hash=config_hash,
                    window_start_ms=start,
                    window_end_ms=end,
                    resolved_outcome=resolution,
                    simulated_fees=fees,
                    simulated_pnl=pnl,
                    expected_fill_count=1,
                    resolved_at_ms=self._now_ms(),
                    payload={
                        "lane": lane,
                        "virtual_campaign_id": lane_campaign.campaign_id,
                        "official_resolution": resolution,
                        **payout_metadata,
                    },
                )
                shadow_id = str(shadow.get("shadow_campaign_id") or "")
                self._shadow_lane_shadow_ids[key] = shadow_id
            await self.repository.record_shadow_settlement(
                shadow_campaign_id=shadow_id,
                resolved_outcome=resolution,
                simulated_fees=fees,
                simulated_pnl=pnl,
                settled_at_ms=self._now_ms(),
                payload={
                    "lane": lane,
                    "virtual_campaign_id": lane_campaign.campaign_id,
                    "official_resolution": resolution,
                    **payout_metadata,
                    "after_fee_pnl": str(pnl),
                },
            )
            getter = getattr(self.repository, "get_shadow_promotion_evidence", None)
            if callable(getter):
                evidence_by_lane[lane] = await getter(
                    config_hash=config_hash,
                    window_start_ms=start,
                    window_end_ms=end,
                )
        await self.repository.set_runtime_config(
            "prediction_shadow_lane_evidence", evidence_by_lane
        )
        self._shadow_lane_campaigns.pop(campaign.campaign_id, None)

    async def start_shadow_observer(self) -> dict[str, Any]:
        """Start the durable read-only Shadow observer if it is not running."""

        task = self._shadow_observer_task
        if task is None or task.done():
            self._shadow_observer_task = asyncio.create_task(
                self._run_shadow_observer(),
                name="prediction-shadow-observer",
            )
        return {**self._status(), "shadow_observer_started": True}

    async def stop_shadow_observer(self) -> None:
        """Stop only the observer task during process teardown."""

        task = self._shadow_observer_task
        self._shadow_observer_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

    @staticmethod
    def _observer_campaign_id(market: MarketInfo) -> str:
        return f"shadow-observer:{market.market_topic_id}:{int(market.start_time_ms)}"

    @staticmethod
    def _observer_category_ok(detail_payload: Any, topic: Mapping[str, Any], market: MarketInfo, *, target_symbol: str = "BTCUSDT") -> bool:
        detail_data = detail_payload.get("data", detail_payload) if isinstance(detail_payload, Mapping) else {}
        if not isinstance(detail_data, Mapping):
            detail_data = {}
        topic_symbol = str(topic.get("symbol") or "").upper()
        detail_symbol = str(detail_data.get("symbol") or topic_symbol or "").upper()
        if detail_symbol and target_symbol and detail_symbol != target_symbol:
            return False
        variant_data = detail_data.get("variantData", {})
        values = {
            str(detail_data.get(key) or topic.get(key) or "").upper().replace("-", "_")
            for key in ("l1Category", "l2Category", "category", "marketCategory", "chartType")
        }
        if isinstance(variant_data, Mapping):
            values.add(str(variant_data.get("type") or "").upper().replace("-", "_"))
        values.discard("")
        combined = any(
            value in {"CRYPTO_UP_DOWN", "CRYPTO_UPDOWN"}
            or ("CRYPTO" in value and ("UP_DOWN" in value or "UPDOWN" in value))
            for value in values
        )
        return bool(
            market.reference_price is not None
            and market.up_token_id
            and market.down_token_id
            and market.up_market_id
            and market.down_market_id
            and (("CRYPTO" in values or combined) and ("UP_DOWN" in values or "UPDOWN" in values or combined))
        )

    async def _discover_shadow_observer_market(self, *, now_ms: int) -> Campaign | None:
        """Discover one current BTC 5m market without touching the Live ledger."""

        last_discovery = int(self._shadow_observer_discovery_at_ms or 0)
        cached = getattr(self, "_shadow_observer_discovery_cache", None)
        if cached is not None and now_ms - last_discovery < self._discovery_cache_ttl_ms:
            markets_payload = cached
        else:
            markets_payload = await self._call_api(
                "list_prediction_markets",
                l1_category=self.settings.market_l1_category,
                l2_category=self.settings.market_l2_category,
                limit=self.settings.discovery_limit,
            )
            self._shadow_observer_discovery_cache = markets_payload
            self._shadow_observer_discovery_at_ms = now_ms
        data = _data(markets_payload)
        topics = data.get("marketTopics", []) if isinstance(data, Mapping) else []
        if not isinstance(topics, list):
            return None
        target_symbol = str(self.settings.market_symbol or "BTCUSDT").strip().upper()
        for topic in topics:
            if not isinstance(topic, Mapping):
                continue
            topic_id = topic.get("marketTopicId") or topic.get("id")
            if topic_id is None:
                continue
            symbol = str(topic.get("symbol") or "").upper()
            title = str(topic.get("title") or topic.get("slug") or topic.get("underlying") or "").upper()
            if symbol and symbol != target_symbol:
                continue
            if not symbol and title and target_symbol not in title:
                continue
            status = str(topic.get("status") or "OPEN").upper()
            if status not in {"REGISTERED", "OPEN", "ACTIVE", "TRADING"}:
                continue
            loaded = await self._get_market_detail_with_cache(str(topic_id), now_ms=now_ms)
            if loaded is None:
                continue
            detail_payload, market = loaded
            if not self._observer_category_ok(detail_payload, topic, market, target_symbol=target_symbol):
                continue
            if market.status in {"CLOSED", "SETTLED", "RESOLVED", "EXPIRED"}:
                continue
            if market.end_time_ms - market.start_time_ms != int(self.settings.market_duration_seconds) * 1000:
                continue
            if not (market.start_time_ms <= now_ms < market.end_time_ms):
                continue
            baseline = getattr(self, "_shadow_observer_admission_baseline_ms", None)
            if baseline is not None and market.start_time_ms < baseline:
                # A service started mid-slot begins its sample at the next
                # complete market, matching the report's campaign-start scope.
                continue
            campaign = Campaign(self._observer_campaign_id(market), market)
            return campaign
        return None

    async def _restore_shadow_observer_market(self) -> Campaign | None:
        getter = getattr(self.repository, "get_active_shadow_observer_market", None)
        if not callable(getter):
            return None
        row = await getter()
        if not isinstance(row, Mapping):
            return None
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
        try:
            market = MarketInfo.from_api(payload)
        except Exception:
            market = MarketInfo(
                market_topic_id=str(row.get("market_topic_id") or ""),
                market_id=str(row.get("market_id") or ""),
                slug=str(row.get("slug") or ""),
                start_time_ms=int(row.get("start_time_ms") or 0),
                end_time_ms=int(row.get("end_time_ms") or 0),
            )
        market = replace(
            market,
            market_topic_id=market.market_topic_id or str(row.get("market_topic_id") or ""),
            market_id=market.market_id or str(row.get("market_id") or ""),
            slug=market.slug or str(row.get("slug") or ""),
            start_time_ms=market.start_time_ms or int(row.get("start_time_ms") or 0),
            end_time_ms=market.end_time_ms or int(row.get("end_time_ms") or 0),
            raw=payload,
        )
        if not market.market_topic_id or not market.reference_price:
            return None
        return Campaign(str(row.get("observer_campaign_id") or self._observer_campaign_id(market)), market)

    async def _settle_shadow_observer(self, campaign: Campaign, detail: Any) -> bool:
        winner = self._official_shadow_resolution(detail)
        if winner is None:
            updater = getattr(self.repository, "update_shadow_observer_market", None)
            if callable(updater):
                await updater(campaign.campaign_id, state="PENDING_RESOLUTION", last_seen_at_ms=self._now_ms(), last_error="official resolution pending")
            return False
        if self._shadow_lane_experiment_enabled():
            await self._settle_shadow_lanes(campaign, winner)
        updater = getattr(self.repository, "update_shadow_observer_market", None)
        if callable(updater):
            await updater(
                campaign.campaign_id,
                state="SETTLED",
                winner=winner,
                last_seen_at_ms=self._now_ms(),
                last_error=None,
            )
        return True

    @staticmethod
    def _shadow_observer_cursor_campaign(campaign: Campaign) -> dict[str, Any]:
        market = campaign.market
        return {
            "campaign_id": campaign.campaign_id,
            "market": {
                "market_topic_id": market.market_topic_id,
                "market_id": market.market_id,
                "slug": market.slug,
                "start_time_ms": market.start_time_ms,
                "end_time_ms": market.end_time_ms,
                "reference_price": str(market.reference_price) if market.reference_price is not None else None,
                "up_token_id": market.up_token_id,
                "down_token_id": market.down_token_id,
                "up_market_id": market.up_market_id,
                "down_market_id": market.down_market_id,
                "vendor": market.vendor,
                "chain_id": market.chain_id,
                "status": market.status,
                # No credentials occur in official market metadata.  The
                # canonical fields above are enough to restore a pending slot.
            },
        }

    @staticmethod
    def _shadow_observer_campaign_from_cursor(row: Mapping[str, Any]) -> Campaign:
        fields = dict(row["market"])
        if fields.get("reference_price") is not None:
            fields["reference_price"] = as_decimal(fields["reference_price"])
        return Campaign(str(row["campaign_id"]), MarketInfo(**fields))

    async def _persist_shadow_observer_cursor(self) -> None:
        active = self._shadow_observer_campaign
        await self.repository.set_runtime_config(
            "prediction_shadow_observer_cursor_v1",
            {
                "active": self._shadow_observer_cursor_campaign(active) if active is not None else None,
                "pending": dict(getattr(self, "_shadow_observer_pending", {})),
                "admitted_market_ids": list(getattr(self, "_shadow_observer_admitted_market_ids", [])),
                "admission_identity": getattr(self, "_shadow_observer_admission_identity", None),
                "updated_at_ms": self._now_ms(),
            },
        )

    async def _load_shadow_observer_cursor(self) -> None:
        if getattr(self, "_shadow_observer_cursor_loaded", False):
            return
        target = getattr(getattr(self, "settings", None), "shadow_exact_target", None)
        identity = None
        if target is not None:
            window = await self._ensure_shadow_window()
            identity = {
                "target": int(target),
                "window_start_ms": int(window["window_start_ms"]),
                "window_end_ms": int(window["window_end_ms"]),
                "config_hashes": {
                    lane: self._shadow_lane_config_hashes[lane]
                    for lane in self._shadow_lane_strategies
                },
            }
            self._shadow_observer_admission_baseline_ms = identity["window_start_ms"]
        cursor = await self.repository.get_runtime_config("prediction_shadow_observer_cursor_v1", None)
        if isinstance(cursor, Mapping) and cursor.get("admission_identity") not in (None, identity):
            raise RuntimeError("Shadow observer admission identity changed; retain the original batch cursor")
        if identity is not None and isinstance(cursor, Mapping) and cursor.get("admission_identity") is not None:
            if not isinstance(cursor.get("admitted_market_ids"), list):
                raise RuntimeError("Shadow observer admission list is missing; cannot restart its counter")
        self._shadow_observer_admission_identity = identity
        self._shadow_observer_pending = {}
        admitted = list(cursor.get("admitted_market_ids") or []) if isinstance(cursor, Mapping) else []
        if isinstance(cursor, Mapping):
            self._shadow_observer_pending = dict(cursor.get("pending") or {})
            if self._shadow_observer_campaign is None and isinstance(cursor.get("active"), Mapping):
                self._shadow_observer_campaign = self._shadow_observer_campaign_from_cursor(cursor["active"])
        elif self._shadow_observer_campaign is None:
            self._shadow_observer_campaign = await self._restore_shadow_observer_market()
        if identity is not None:
            # Older workers did not persist admission counters.  Reconstruct
            # the union of recorded markets before allowing another slot;
            # settlement counts alone omit unresolved markets and bias samples.
            fetcher = getattr(self.repository, "_fetchall", None)
            if not callable(fetcher):
                raise RuntimeError("Cannot verify existing Shadow market admissions")
            hashes = list(identity["config_hashes"].values())
            rows = await fetcher(
                "SELECT campaign_id, campaign_start_ms AS start_time_ms FROM prediction_shadow_campaigns "
                "WHERE campaign_start_ms>=? AND campaign_start_ms<? AND config_hash IN ("
                + ",".join("?" for _ in hashes) + ") "
                "UNION SELECT observer_campaign_id AS campaign_id, start_time_ms "
                "FROM prediction_shadow_observer_markets WHERE start_time_ms>=? AND start_time_ms<? "
                "ORDER BY start_time_ms, campaign_id",
                (identity["window_start_ms"], identity["window_end_ms"], *hashes,
                 identity["window_start_ms"], identity["window_end_ms"]),
            )
            recorded = [str(row["campaign_id"]).split("::shadow::", 1)[0] for row in rows]
            if isinstance(cursor, Mapping) and cursor.get("admission_identity") is not None:
                if not set(recorded).issubset(admitted):
                    raise RuntimeError("Shadow admission cursor disagrees with recorded markets; recovery required")
            admitted.extend(recorded)
        for row in self._shadow_observer_pending.values():
            admitted.append(str(row["campaign_id"]))
        if self._shadow_observer_campaign is not None:
            if identity is not None and self._shadow_observer_campaign.market.start_time_ms < identity["window_start_ms"]:
                raise RuntimeError("Existing partial Shadow market requires separate recovery before exact collection")
            admitted.append(self._shadow_observer_campaign.campaign_id)
        self._shadow_observer_admitted_market_ids = list(dict.fromkeys(admitted))
        if target is not None and len(self._shadow_observer_admitted_market_ids) > int(target):
            raise RuntimeError("Existing Shadow market admissions exceed the frozen target")
        await self._persist_shadow_observer_cursor()
        self._shadow_observer_cursor_loaded = True

    async def _poll_pending_shadow_observer(self, *, now_ms: int) -> None:
        """Retry one durable slot per minute after current-market collection.

        The oldest attempted slot goes first, so an unresolved market cannot
        starve other settlements or monopolize the shared REST weight budget.
        """

        pending = getattr(self, "_shadow_observer_pending", {})
        if not pending:
            return
        last_poll = getattr(self, "_shadow_observer_pending_poll_at_ms", None)
        if last_poll is not None and now_ms - last_poll < 60_000:
            return
        key, row = min(pending.items(), key=lambda item: int(item[1].get("last_attempt_at_ms") or 0))
        self._shadow_observer_pending_poll_at_ms = now_ms
        row["last_attempt_at_ms"] = now_ms
        campaign = self._shadow_observer_campaign_from_cursor(row)
        try:
            detail = await self._call_api("get_market_detail", campaign.market.market_topic_id)
            if await self._settle_shadow_observer(campaign, detail):
                pending.pop(key, None)
                self._clear_v3_gate_history(campaign.campaign_id)
                self._market_states.pop(campaign.campaign_id, None)
                self._latest_quotes.pop(campaign.campaign_id, None)
            else:
                row["last_error"] = "official resolution pending"
        except PredictionRateLimitDeferred:
            row["last_error"] = "resolution rate limit deferred"
        except Exception as exc:
            # Persist bounded diagnostics even on older VM repositories which
            # have no observer tables/methods, without blocking next discovery.
            row["last_error"] = type(exc).__name__
            self._shadow_observer_last_error = type(exc).__name__
        await self._persist_shadow_observer_cursor()

    async def _run_shadow_observer_once(self) -> None:
        self._begin_shadow_tick()
        now = self._now_ms()
        self._shadow_observer_last_tick_at_ms = now
        await self._load_shadow_observer_cursor()
        if await self._shadow_exact_target_complete():
            self._shadow_observer_campaign = None
            return
        # A running Live loop normally supplies the same quote stream.  Yield
        # only while it has a Live campaign for the current 5-minute slot;
        # otherwise keep the observer alive so a discovery gap cannot create
        # holes in the rolling regime window.
        active_loop = await self.repository.get_active_loop()
        if active_loop:
            get_active_campaigns = getattr(self.repository, "get_active_campaigns", None)
            if not callable(get_active_campaigns):
                return
            active_campaigns = await get_active_campaigns(now_ms=now)
            active_loop_id = str(active_loop.get("loop_id") or "")
            live_current = any(
                str(item.get("loop_id") or "") == active_loop_id
                and int(item.get("start_time_ms") or 0) <= now < int(item.get("end_time_ms") or 0)
                for item in active_campaigns
                if isinstance(item, Mapping)
            )
            if live_current:
                return
        campaign = self._shadow_observer_campaign
        if campaign is not None and now >= campaign.market.end_time_ms:
            self._shadow_observer_pending.setdefault(
                campaign.campaign_id, self._shadow_observer_cursor_campaign(campaign)
            )
            # Commit the pending identity before releasing the slot.  Its
            # positions are recovered from immutable lane fills after restart.
            self._shadow_observer_campaign = None
            await self._persist_shadow_observer_cursor()
            campaign = None
        if campaign is None:
            target = getattr(getattr(self, "settings", None), "shadow_exact_target", None)
            if target is not None and len(self._shadow_observer_admitted_market_ids) >= int(target):
                await self._poll_pending_shadow_observer(now_ms=now)
                return
            try:
                campaign = await self._discover_shadow_observer_market(now_ms=now)
            except PredictionRateLimitDeferred:
                campaign = None
            except Exception:
                await self._poll_pending_shadow_observer(now_ms=now)
                raise
            if campaign is None:
                await self._poll_pending_shadow_observer(now_ms=now)
                return
            baseline = getattr(self, "_shadow_observer_admission_baseline_ms", None)
            if baseline is not None and campaign.market.start_time_ms < baseline:
                await self._poll_pending_shadow_observer(now_ms=now)
                return
            if campaign.campaign_id in self._shadow_observer_admitted_market_ids:
                await self._poll_pending_shadow_observer(now_ms=now)
                return
            self._shadow_observer_admitted_market_ids.append(campaign.campaign_id)
            self._shadow_observer_campaign = campaign
            await self._persist_shadow_observer_cursor()
        saved = getattr(self, "_shadow_observer_saved_markets", set())
        if campaign.campaign_id not in saved:
            saver = getattr(self.repository, "save_shadow_observer_market", None)
            if callable(saver):
                # Admission is committed before the optional observer ledger,
                # avoiding an orphan row if the process exits between writes.
                await saver(campaign, payload=campaign.market.raw, last_seen_at_ms=now)
            saved.add(campaign.campaign_id)
            self._shadow_observer_saved_markets = saved
        try:
            quote = await self._quote_for_campaign(campaign, observer=True)
            if self._shadow_lane_experiment_enabled():
                raw = getattr(quote, "raw", {})
                book = raw.get("execution_book", {}) if isinstance(raw, Mapping) else {}
                sample = (
                    book.get("request_started_ms") if isinstance(book, Mapping) else None
                ) or getattr(quote, "observed_at_ms", None)
                evaluated = getattr(self, "_shadow_observer_evaluated_samples", {})
                if campaign.campaign_id not in evaluated or evaluated[campaign.campaign_id] != sample:
                    await self._manage_shadow_lanes(campaign, quote, now_ms=self._now_ms(), observer=True)
                    evaluated[campaign.campaign_id] = sample
                    self._shadow_observer_evaluated_samples = evaluated
        except PredictionRateLimitDeferred:
            pass
        finally:
            await self._poll_pending_shadow_observer(now_ms=now)

    async def _run_shadow_observer(self) -> None:
        """Keep collecting Shadow evidence while no finite loop is active."""

        while True:
            try:
                await self._run_shadow_observer_once()
                self._shadow_observer_last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - observer must never stop Live
                self._shadow_observer_last_error = str(exc)
                campaign = self._shadow_observer_campaign
                updater = getattr(self.repository, "update_shadow_observer_market", None)
                if campaign is not None and callable(updater):
                    try:
                        await updater(campaign.campaign_id, last_seen_at_ms=self._now_ms(), last_error=str(exc))
                    except Exception:
                        pass
            await asyncio.sleep(max(2.0, min(15.0, float(self.settings.poll_interval_seconds or 1.0))))

    async def reconcile_intent_without_order_id(self, campaign: Campaign, intent: OrderIntent) -> Mapping[str, Any] | None:
        """Match paged official history conservatively; ambiguity hard-stops."""
        candidates = await self._history_rows(intent)
        token = campaign.market.up_token_id if intent.outcome is OutcomeSide.UP else campaign.market.down_token_id
        def amount(row: Mapping[str, Any]) -> Decimal:
            value = as_decimal(row.get("amount") or row.get("amountIn") or row.get("filledUsdtAmount") or row.get("filledAmount") or 0)
            # amountIn is often wei while intent.amount is display USDT.
            return value / Decimal("1000000000000000000") if value >= Decimal("1000000000000000") else value
        matches = [
            row for row in candidates
            if str(row.get("tokenId") or row.get("token_id")) == str(token)
            and str(row.get("side") or row.get("orderSide") or "").upper() == intent.order_side.value
            and abs(amount(row) - intent.amount) <= Decimal("0.01")
        ]
        if len(matches) > 1:
            self._hard_stop_latched = True
            await self.repository.set_runtime_config("prediction_risk_state", {"hard_stop_latched": True, "reason": "ambiguous history match"})
            raise RuntimeError("ambiguous order history match")
        return matches[0] if matches else None

    async def _resolve_expired_c180_pre_submit(
        self, campaign: Campaign, intent: OrderIntent, row: Mapping[str, Any],
    ) -> bool:
        """Close a C180 claim that never crossed the durable submit boundary.

        The submit timestamp is written before place_order is called. A
        missing timestamp is useful evidence, but official history, active
        orders and positions must also be clean.
        """

        now = self._now_ms()
        if (self._selected_strategy_profile not in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                or now < campaign.market.end_time_ms
                or row.get("order_id") or row.get("submission_at_ms") is not None
                or row.get("ttl_deadline_ms") is not None
                or not self.settings.wallet_address):
            return False
        last_checks = getattr(self, "_c180_pre_submit_recheck_at_ms", None)
        if not isinstance(last_checks, dict):
            last_checks = {}
            self._c180_pre_submit_recheck_at_ms = last_checks
        if now - int(last_checks.get(campaign.campaign_id, 0)) < 5000:
            return False
        last_checks[campaign.campaign_id] = now

        if campaign.position.has_any or await self.repository.get_fills(campaign.campaign_id):
            return False
        if await self.repository._fetchone(
            "SELECT 1 AS found FROM prediction_orders WHERE campaign_id=? LIMIT 1",
            (campaign.campaign_id,),
        ):
            return False
        try:
            match = await self.reconcile_intent_without_order_id(campaign, intent)
            if match is not None:
                order_id = str(match.get("orderId") or match.get("order_id") or match.get("id") or "")
                if order_id:
                    await self.repository.update_intent(
                        intent.intent_id, order_id=order_id, status="SUBMITTED", unknown=False,
                    )
                    campaign.pending_unknown = False
                    await self.repository.save_campaign(campaign)
                return False
            active_orders = await self._query_active_order_rows()
            positions = await self._call_api(
                "query_positions", wallet_address=self.settings.wallet_address,
            )
        except (PredictionRateLimitDeferred, PredictionClientError) as exc:
            self.heartbeat.last_error = f"C180 pre-submit reconciliation deferred: {type(exc).__name__}"
            return False
        if active_orders or any(
            self._official_position_shares(item) > 0
            for item in self._official_position_rows(positions)
        ):
            return False

        await self.repository.update_intent(intent.intent_id, status="CANCELLED", unknown=False)
        campaign.pending_intent_id = None
        campaign.pending_unknown = False
        await self.repository.save_campaign(campaign)
        try:
            await self.repository.record_risk_event(
                "C180_PRE_SUBMIT_EXPIRED", "WARNING",
                "expired C180 intent closed after official zero-exposure reconciliation",
                campaign_id=campaign.campaign_id,
                payload={"intent_id": intent.intent_id, "no_submit_boundary": True},
            )
        except Exception:
            pass
        return True

    async def _record_discovery_deferred(self, exc: PredictionRateLimitDeferred) -> None:
        now = self._now_ms()
        if now - getattr(self, '_discovery_deferred_logged_at_ms', 0) < 60_000:
            return
        self._discovery_deferred_logged_at_ms = now
        health = exc.health if isinstance(exc.health, Mapping) else {}
        payload = {'at_ms': now, 'loop_id': self._loop_id,
                   'reason': 'discovery_request_deferred',
                   'budget_error': health.get('error'),
                   'used': health.get('used'), 'remaining': health.get('remaining'),
                   'backoff_until_ms': health.get('backoff_until_ms')}
        await self.repository.record_risk_event('DISCOVERY_REQUEST_DEFERRED', 'WARNING',
            'Market discovery deferred by request protection', payload=payload)
        await self.repository.set_runtime_config('prediction_discovery_deferred', payload)

    async def _run_market_once(self) -> bool:
        if await self._check_loop_loss_guard():
            return False
        # A loop advances one market at a time. Do not discover additional
        # markets while the current campaign is still being observed or
        # settled; otherwise a one-run can accumulate several concurrent
        # campaigns before its first market increments the durable counter.
        if self._active_campaigns:
            return False
        now = self._now_ms()
        wait_until = getattr(self, "_initial_market_wait_until_ms", None)
        if wait_until is not None:
            if now < int(wait_until):
                return False
            # The prior partial market has ended.  Force one fresh discovery
            # read at the boundary so a 60-second cache cannot make the new
            # loop miss the next market's observation window.
            self._initial_market_wait_until_ms = None
            self._discovery_cache = None
        if self._discovery_cache is not None and now - self._discovery_cache[0] < self._discovery_cache_ttl_ms:
            markets_payload = self._discovery_cache[1]
        else:
            try:
                markets_payload = await self._call_api(
                    "list_prediction_markets",
                    l1_category=self.settings.market_l1_category,
                    l2_category=self.settings.market_l2_category,
                    limit=self.settings.discovery_limit,
                )
            except PredictionRateLimitDeferred as exc:
                await self._record_discovery_deferred(exc)
                return False
            self._discovery_cache = (now, markets_payload)
        data = _data(markets_payload)
        topics = data.get("marketTopics", []) if isinstance(data, Mapping) else []
        if not isinstance(topics, list) or not topics:
            return False
        target_symbol = str(self.settings.market_symbol or "BTCUSDT").strip().upper()
        if not target_symbol:
            return False
        topic = None
        ignored_topic_ids = getattr(self, "_ignored_discovery_topic_ids", set())
        duration_min = int(self.settings.market_duration_seconds) // 60
        duration_tag = f"{duration_min}M"
        for candidate in topics:
            if not isinstance(candidate, Mapping):
                continue
            candidate_topic_id = candidate.get("marketTopicId") or candidate.get("id")
            if candidate_topic_id is not None and str(candidate_topic_id) in ignored_topic_ids:
                continue
            symbol = str(candidate.get("symbol") or "").upper()
            text = str(candidate.get("title") or candidate.get("slug") or candidate.get("underlying") or "").upper()
            # The Prediction gate is exact: a bare base symbol must not be
            # admitted when the configured market contract is BTCUSDT.
            if symbol and symbol != target_symbol:
                continue
            if not symbol and text and target_symbol not in text:
                continue
            topic_status = str(candidate.get("status") or "OPEN").upper()
            if topic_status in {"CLOSED", "SETTLED", "EXPIRED"}:
                continue
            # REGISTERED topics are intentionally sent to detail: the list
            # endpoint can publish them before side markets become OPEN.
            if topic_status not in {"REGISTERED", "OPEN", "ACTIVE", "TRADING"}:
                continue
            if re.search(r"(?:^|[\s_-])\d+[mhdMHD](?:$|[\s_-])", text) and not re.search(rf"(?:^|[\s_-]){duration_min}M(?:$|[\s_-])", text, re.IGNORECASE):
                continue
            topic = candidate
            break
        if topic is None:
            return False
        topic_id = topic.get("marketTopicId") or topic.get("id")
        if topic_id is None:
            return False
        loaded_detail = await self._get_market_detail_with_cache(str(topic_id), now_ms=now)
        if loaded_detail is None:
            return False
        detail_payload, market = loaded_detail
        # A missing start/reference price makes BTC direction unknowable.  Do
        # not admit a campaign that can only fail the feed gate for its entire
        # entry window; the short incomplete-cache TTL above will retry it.
        if market.reference_price is None:
            return False
        detail_data = detail_payload.get("data", detail_payload) if isinstance(detail_payload, Mapping) else {}
        detail_symbol = str((detail_data.get("symbol") if isinstance(detail_data, Mapping) else None) or topic.get("symbol") or "").upper()
        if detail_symbol != target_symbol:
            return False
        if self._selected_strategy_profile in ("regime_target6_7c_v1", "regime_target6_9_v1", "regime_target6_9a_v1"):
            from .loop_market import market_matches
            if not market_matches(market, target_symbol):
                return False
        variant_data = detail_data.get("variantData", {}) if isinstance(detail_data, Mapping) else {}
        category_values = {
            str((detail_data.get(key) if isinstance(detail_data, Mapping) else None) or topic.get(key) or "").upper().replace("-", "_")
            for key in ("l1Category", "l2Category", "category", "marketCategory", "chartType")
        }
        if isinstance(variant_data, Mapping):
            category_values.add(str(variant_data.get("type") or "").upper().replace("-", "_"))
        category_values.discard("")
        combined_category = any(
            value in {"CRYPTO_UP_DOWN", "CRYPTO_UPDOWN"}
            or ("CRYPTO" in value and ("UP_DOWN" in value or "UPDOWN" in value))
            for value in category_values
        )
        has_crypto = "CRYPTO" in category_values or combined_category
        has_up_down = "UP_DOWN" in category_values or "UPDOWN" in category_values or combined_category
        if not (has_crypto and has_up_down):
            return False
        detail_markets = detail_data.get("markets", []) if isinstance(detail_data, Mapping) else []
        side_statuses = {
            str(item.get("tradingStatus", item.get("status", "OPEN"))).upper()
            for item in detail_markets if isinstance(item, Mapping)
        }
        if side_statuses and side_statuses != {"OPEN"}:
            return False
        if market.status in {"CLOSED", "SETTLED", "RESOLVED", "EXPIRED"}:
            return False
        if market.end_time_ms - market.start_time_ms != int(self.settings.market_duration_seconds) * 1000:
            return False
        if not (market.start_time_ms <= now < market.end_time_ms):
            if now >= market.end_time_ms:
                ignored_topic_ids.add(str(topic_id))
                self._ignored_discovery_topic_ids = ignored_topic_ids
            return False
        first_loop_market = bool(self._loop_id) and not self._active_campaigns and int(
            getattr(self.heartbeat, "markets_completed", 0)
        ) == 0 and int(getattr(self.heartbeat, "markets_seen", 0)) == 0
        loop_origin_ms = getattr(self, "_loop_created_at_ms", None)
        if first_loop_market and loop_origin_ms is not None:
            tolerance_ms = max(0, int(self.settings.discovery_tolerance_seconds)) * 1000
            # Startup freshness only rejects the market that was already in
            # progress when this durable loop began.  A later five-minute
            # slot is new work even when Binance publishes its complete
            # detail more than a few seconds after the boundary.  Tying this
            # gate to ``markets_seen == 0`` plus discovery latency made every
            # later slot look stale after one miss, permanently starving the
            # loop at 0/N (including after a service restart).  Normal entry
            # timing, path/WHIPSAW, and risk gates still run after campaign
            # admission and remain responsible for deciding whether to buy.
            predates_loop = market.start_time_ms + tolerance_ms < int(loop_origin_ms)
            if predates_loop:
                ignored_topic_ids.add(str(topic_id))
                self._ignored_discovery_topic_ids = ignored_topic_ids
                self._initial_market_wait_until_ms = market.end_time_ms
                return False
        # Binance's binary schema uses one market node/marketId with two
        # outcome tokens.  Market IDs may therefore be shared; token IDs are
        # the distinct contract identities that must be present.
        if not str(market.up_market_id or "").strip() or not str(market.down_market_id or "").strip():
            return False
        up_token_id = str(market.up_token_id or "").strip()
        down_token_id = str(market.down_token_id or "").strip()
        if not up_token_id or not down_token_id or up_token_id == down_token_id:
            return False
        campaign_id = market.slug or market.market_topic_id or str(uuid4())
        # The active campaign was already refreshed by
        # _manage_active_campaigns() earlier in this loop tick.  Re-admitting
        # it would double API calls, risk events, and markets_seen.
        campaign = self._active_campaigns.get(campaign_id)
        if campaign is not None:
            return False
        if self._loop_id:
            cursor = await self.repository.get_loop(self._loop_id)
            if not cursor or str(cursor.get("state", "")).upper() != "RUNNING" or int(cursor.get("completed", 0)) >= int(cursor.get("target", self._target_markets)):
                self._accept_new_markets = False
                self._allow_new_orders = False
                self._allow_new_buys = False
                return False
        campaign = Campaign(campaign_id, market)
        self._active_campaigns[campaign_id] = campaign
        await self.repository.save_campaign(campaign, loop_id=self._loop_id)
        if self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            bridge = self._c180_bridge_for_worker()
            if bridge is None or not self._loop_id:
                self.heartbeat.last_error = "C180 bridge or loop unavailable"
            else:
                registered = await bridge.register_market(
                    loop_id=self._loop_id, market=campaign.market,
                    now_ms=self._now_ms(), unit_usdt=self._selected_order_unit_usdt,
                )
                if not registered.allowed:
                    self.heartbeat.last_error = "C180 market registration: " + registered.reason
        self.heartbeat.markets_seen += 1
        return await self.manage_campaign(campaign)

    async def _reject_unsubmitted_entry(self, campaign, intent, previous_state, reason):
        # The claim/counters remain durable. Known no HTTP must not latch UNKNOWN.
        await self.repository.update_intent(intent.intent_id, status="REJECTED", unknown=False,
            payload_json={"not_submitted": True, "reason": reason})
        campaign.pending_intent_id = None
        campaign.pending_unknown = False
        campaign.state = previous_state
        await self.repository.save_campaign(campaign)
        self._finish_entry_attempt(campaign.campaign_id, reason)

    async def _handle_decision(self, campaign: Campaign, decision: StrategyDecision) -> None:
        try:
            await self._handle_decision_inner(campaign, decision)
        except BaseException as exc:
            self._finish_entry_attempt(campaign.campaign_id, "execution_exception", error_type=type(exc).__name__)
            raise
        finally:
            self._finish_entry_attempt(campaign.campaign_id, "execution_return_without_post")

    async def _handle_decision_inner(self, campaign: Campaign, decision: StrategyDecision) -> None:
        entry_loop, entry_profile = self._loop_id, self._selected_strategy_profile
        from src.gridbot.prediction import r3_reversal_guard as r3guard
        c180_buy = (self._selected_strategy_profile in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                    and decision.action is ActionType.BUY_INITIAL)
        if c180_buy:
            bound_loop = await self.repository.get_loop(self._loop_id) if self._loop_id else None
            if (not bound_loop or str(bound_loop.get("strategy_profile") or "").lower() not in {"c180_favorite_hold_v1", "regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}
                    or str(bound_loop.get("mode") or "").upper() != "LIVE"
                    or str(bound_loop.get("state") or "").upper() != "RUNNING"):
                self._finish_entry_attempt(campaign.campaign_id, "loop_not_live")
                return
            ready = getattr(self, "_c180_ready", {}).get(campaign.campaign_id)
            has_buy = getattr(self.repository, "has_market_buy", None)
            if (decision.reason != "c180 durable entry" or ready is None
                    or not ready.allowed or ready.signal is None
                    or decision.amount != self._selected_order_unit_usdt
                    or campaign.position.has_any or campaign.buy_count
                    or campaign.pending_intent_id or campaign.pending_unknown
                    or not callable(has_buy) or await has_buy(campaign.campaign_id)):
                self._finish_entry_attempt(campaign.campaign_id, "frozen_entry_invalid")
                return
        if c180_buy and self._selected_strategy_profile in {"regime_target6_v1", "regime_target6_1_v1", "regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1'}:
            if (bound_loop.get("strategy_profile") != self._selected_strategy_profile
                    or (self._selected_strategy_profile not in ("regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1') and decision.amount != Decimal("1"))
                    or ready.signal.entry is None or ready.signal.entry.stake_usdt != decision.amount
                    or ready.execution is None
                    or decision.outcome.value != ready.signal.entry.side
                    or decision.limit_price != ready.execution.worst_ask_limit):
                self._finish_entry_attempt(campaign.campaign_id, "profile_or_frozen_provenance_mismatch")
                return
        # Defense at the execution boundary, independent of in-memory signals.
        # An old/stale BUY_ADD decision must not bypass the single-market rule.
        s3s5_buy = self._s3s5_profile_active() and decision.action in {
            ActionType.BUY_INITIAL, ActionType.BUY_ADD, ActionType.BUY_HEDGE,
        }
        strategy_leg = None
        if s3s5_buy:
            parts = decision.reason.split()
            strategy_leg = parts[1] if len(parts) == 3 and parts[0] == "s3s5" else None
            has_buy = getattr(self.repository, "has_market_buy", None)
            allowed_legs = (
                {"FAV", "LATE_SNIPER"}
                if self._selected_strategy_profile in {"fav_only_v1", "fav_only_v2", "fav_only_v3", "fav_only_v4"}
                else {"R3", "FAV", "LATE_SNIPER"}
            )
            if (
                decision.action is not ActionType.BUY_INITIAL
                or strategy_leg not in allowed_legs
                or decision.amount != Decimal("1")
                or campaign.position.has_any or campaign.buy_count
                or campaign.pending_intent_id or campaign.pending_unknown
                or not callable(has_buy)
                or await has_buy(campaign.campaign_id)
            ):
                return
            guard_key = r3guard.early_key(campaign.campaign_id)
            if strategy_leg == "R3" and await self.repository.get_runtime_config(guard_key, False):
                return
            from src.gridbot.prediction import s3s5_pair as s3
            if strategy_leg == "FAV" and await self.repository.get_runtime_config(s3.fav_exclusion_key(campaign.campaign_id), False):
                return
            if strategy_leg == "FAV":
                proof = await self.repository.get_runtime_config(s3.fav_approval_key(campaign.campaign_id), None)
                if decision.outcome is None or not s3.fav_approval_valid(proof, self._now_ms(), decision.outcome.value, decision.limit_price, profile=self._selected_strategy_profile):
                    return
                # CEO P3-only: when FAV_P3 arm=live, suppress Baseline FAV Live submits.
                # R3 leg unchanged; P3 lane (_manage_p3_live_lane) owns FAV-style entries.
                if self._suppress_fav_baseline_live():
                    LOGGER.info(
                        "[P3] suppress FAV_BASELINE live submit campaign=%s profile=%s arm=%s",
                        campaign.campaign_id,
                        getattr(self, "_selected_strategy_profile", None),
                        self._fav_p3_arm(),
                    )
                    return
            if strategy_leg == "R3" and self._now_ms() - campaign.market.start_time_ms < r3guard.EARLY_MS:
                await self.repository.set_runtime_config(guard_key, {"version": r3guard.VERSION, "at_ms": self._now_ms()})
                return
        r3_exit = self._s3s5_profile_active() and decision.reason == r3guard.EXIT_REASON
        fav_exit = self._s3s5_profile_active() and str(decision.reason).startswith("fav_late_exit")
        if r3_exit or fav_exit:
            if (decision.action is not ActionType.SELL_PROTECTIVE or decision.order_side is not OrderSide.SELL
                or campaign.pending_intent_id or campaign.pending_unknown
                or decision.outcome is None):
                return
            if r3_exit:
                if (not r3guard.late_window(campaign, self._now_ms())
                    or await self.repository.market_entry_tier(campaign.campaign_id) != "R3"):
                    return
            shares = campaign.position.shares(decision.outcome)
            if shares <= 0 or decision.amount != shares:
                return
            strategy_leg = "FAV_EXIT" if fav_exit else "R3_EXIT_A"
        with self._entry_stage(campaign.campaign_id, "generic_risk_snapshot"):
            snapshot = await self._risk_snapshot(campaign)
        snapshot.hard_stop_latched = self._hard_stop_latched or snapshot.hard_stop_latched
        # C180's settled-trade 20-run gate replaces the previous Loop/day PnL
        # and consecutive-loss soft guards, while preserving hard-stop latch,
        # per-market order-attempt cap, and one-BUY cap.
        assessed = (replace(snapshot, daily_net_pnl=Decimal("0"),
                            loop_net_pnl=Decimal("0"), consecutive_losses=0,
                            soft_cooldown_until_ms=None) if c180_buy else snapshot)
        risk = self.risk_engine.evaluate(assessed, now_ms=self._now_ms(), action=decision.action)
        if assessed.hard_stop_latched:
            snapshot.hard_stop_latched = True
        with self._entry_stage(campaign.campaign_id, "generic_risk_persist"):
            await self._persist_risk_state(snapshot)
        if risk.mode.value == "HARD_STOP" and (not c180_buy or assessed.hard_stop_latched):
            # A C180 per-market attempt cap blocks this market; it must not
            # become a process-wide operator/unknown hard-stop latch.
            self._hard_stop_latched = True
        if not risk.allow_trading:
            self._finish_entry_attempt(campaign.campaign_id, "risk_rejected")
            return
        is_buy = decision.order_side is OrderSide.BUY or decision.action in {
            ActionType.BUY_INITIAL,
            ActionType.BUY_ADD,
            ActionType.BUY_HEDGE,
        }
        initial_entry_priority = self._can_prioritize_initial_entry(campaign, decision)
        if not self.live_capability:
            # Shadow mode follows the same strategy/risk decision but records
            # a deterministic LIMIT fill in SQLite instead of returning at
            # the signed API boundary.
            if ((is_buy and not self._allow_new_buys) or (not is_buy and not self._allow_reductions)):
                self._finish_entry_attempt(campaign.campaign_id, "buy_permission_disabled")
                return
            if decision.is_trade:
                await self._simulate_shadow_decision(campaign, decision)
            self._finish_entry_attempt(campaign.campaign_id, "live_not_armed")
            return
        if is_buy and (not self._allow_new_buys or self._hard_stop_latched):
            self._finish_entry_attempt(campaign.campaign_id, "buy_permission_disabled")
            return
        if not is_buy and not self._allow_reductions:
            self._finish_entry_attempt(campaign.campaign_id, "reduction_permission_disabled")
            return
        if not self.settings.wallet_address or not self.settings.wallet_id:
            self._finish_entry_attempt(campaign.campaign_id, "wallet_configuration_missing")
            return
        if decision.action is ActionType.BUY_INITIAL:
            # Check the local/read-only regime monitor before constructing an
            # intent or reserving execution weight.  Repeated RED/WAIT_DATA
            # decisions must not consume the capacity needed by recovery.
            if not await self._check_regime_entry_gate(campaign):
                self._finish_entry_attempt(campaign.campaign_id, "regime_gate_rejected")
                return
        is_fav_baseline = (strategy_leg == "FAV" and decision.action is ActionType.BUY_INITIAL)
        is_sniper = (strategy_leg == "LATE_SNIPER" and decision.action is ActionType.BUY_INITIAL)
        if c180_buy:
            strategy_leg = ("REGIME_T69A" if self._selected_strategy_profile == "regime_target6_9a_v1" else "REGIME_T69" if self._selected_strategy_profile == "regime_target6_9_v1" else "REGIME_T68A" if self._selected_strategy_profile == "regime_target6_8a_v1" else "REGIME_T68" if self._selected_strategy_profile == "regime_target6_8_v1" else "REGIME_T67C" if self._selected_strategy_profile == "regime_target6_7c_v1" else "REGIME_T67D" if self._selected_strategy_profile == "regime_target6_7d_v1" else "REGIME_T67B" if self._selected_strategy_profile == "regime_target6_7b_v1" else "REGIME_T67A" if self._selected_strategy_profile == "regime_target6_7a_v1" else "REGIME_T67" if self._selected_strategy_profile == "regime_target6_7_v1" else "REGIME_T65" if self._selected_strategy_profile == "regime_target6_5_v1" else
                            "REGIME_T63B" if self._selected_strategy_profile == "regime_target6_3b_v1" else
                            "REGIME_T63A" if self._selected_strategy_profile == "regime_target6_3a_v1" else
                            "REGIME_T63" if self._selected_strategy_profile == "regime_target6_3_v1" else "REGIME_T6" if self._selected_strategy_profile == "regime_target6_v1" else
                            "REGIME_T61" if self._selected_strategy_profile == "regime_target6_1_v1" else
                            "REGIME_T62" if self._selected_strategy_profile in ("regime_target6_2_v1", 'regime_target6_3_v1', 'regime_target6_3a_v1', 'regime_target6_3b_v1', 'regime_target6_5_v1', 'regime_target6_7_v1', 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1') else "C180")
        intent_id = (
            f"fav-base-{campaign.campaign_id}-{self._now_ms()}"
            if is_fav_baseline
            else f"sniper-{campaign.campaign_id}-{self._now_ms()}"
            if is_sniper
            else str(uuid4())
        )
        intent = OrderIntent(
            intent_id=intent_id, campaign_id=campaign.campaign_id,
            action=decision.action, outcome=decision.outcome or OutcomeSide.UP,
            order_side=decision.order_side or OrderSide.BUY, amount=decision.amount,
            limit_price=decision.limit_price or Decimal("0"), created_at_ms=self._now_ms(),
            ttl_ms=decision.ttl_ms, attempt=campaign.order_attempts + 1,
            tier=strategy_leg,
        )
        # A sell must be repriced from the held token's current top bid before
        # the quote/order boundary.  The strategy quote is intentionally
        # cached for normal market-data efficiency, but using that cached bid
        # for a reduction caused the V5 loss: every retry submitted the same
        # stale 0.91 limit while the live bid had already collapsed.
        fresh_exit_book = not is_buy
        # Reserve the complete execution path before creating a durable
        # intent.  A local budget deferral happens before any HTTP call, so it
        # is known to be a no-execution event and must never become UNKNOWN.
        # The fresh book read is included in the same reservation as the
        # quote and order calls; BUY admission also leaves history headroom.
        execution_weight = self._endpoint_weight("get_quote") + self._endpoint_weight("place_order")
        if c180_buy:
            execution_weight += self._endpoint_weight("query_payment_option_balances")
        if fresh_exit_book:
            execution_weight += self._endpoint_weight("query_order_book")
        if not self._rate_limiter.acquire(
            execution_weight,
            block=False,
            emergency=not is_buy,
            headroom=40 if is_buy else 0,
        ):
            health = self._rate_limiter.health()
            self._rate_limiter.note_deferred(
                execution_weight,
                error=(
                    "exit book/get_quote/place_order deferred by local weight budget"
                    if fresh_exit_book
                    else "get_quote/place_order deferred by local weight budget"
                ),
            )
            self.heartbeat.last_error = (
                "prediction weight deferred for exit execution"
                if fresh_exit_book
                else "prediction weight deferred for get_quote"
            )
            await self.repository.record_risk_event(
                "EXECUTION_DEFERRED",
                "WARNING",
                self.heartbeat.last_error,
                campaign_id=campaign.campaign_id,
                payload={
                    "action": decision.action.value,
                    "order_side": intent.order_side.value,
                    "weight": execution_weight,
                    "health": health.as_dict(),
                    "retryable": True,
                    "read_only": True,
                    "priority": "initial_entry" if initial_entry_priority else "normal",
                },
            )
            self._finish_entry_attempt(campaign.campaign_id, "local_budget_deferred")
            return

        shared_budget = getattr(self.client, "request_budget", None)
        if shared_budget and not shared_budget.acquire(
            execution_weight, priority="normal" if is_buy else "exit", headroom=40 if is_buy else 0,
        ):
            await self.repository.record_risk_event(
                "EXECUTION_DEFERRED", "WARNING", "Shared request budget cannot fund execution and management",
                campaign_id=campaign.campaign_id,
                payload={"order_side": intent.order_side.value, "retryable": True, "read_only": True,
                         "health": shared_budget.health()},
            )
            self._finish_entry_attempt(campaign.campaign_id, "execution_guard_rejected")
            return

        if fresh_exit_book:
            token_id = (
                campaign.market.up_token_id
                if intent.outcome is OutcomeSide.UP
                else campaign.market.down_token_id
            )
            exit_request_ms = self._now_ms()
            try:
                held_book = await self._call_api(
                    "query_order_book",
                    campaign.market.market_id_for(intent.outcome),
                    token_id or "",
                    vendor=campaign.market.vendor,
                    _emergency=True,
                    _weight_pre_acquired=True,
                _shared_pre_acquired=True,
                )
            except PredictionClientError as exc:
                # Do not submit a stale reduction when the fresh held-side
                # quote cannot be read.  The position remains linked to the
                # campaign and the next management tick retries it.
                self.heartbeat.last_error = str(exc)
                await self.repository.record_risk_event(
                    "EXECUTION_REPRICE_DEFERRED",
                    "WARNING",
                    str(exc),
                    campaign_id=campaign.campaign_id,
                    payload={
                        "action": decision.action.value,
                        "order_side": intent.order_side.value,
                        "original_limit_price": str(intent.limit_price),
                        "retryable": True,
                        "read_only": True,
                    },
                )
                return
            fresh_bid, _ = _book_top(held_book)
            if r3_exit:
                now_exit = self._now_ms()
                if not r3guard.late_window(campaign, now_exit) or now_exit - exit_request_ms > r3guard.MAX_AGE_MS:
                    return
                data = _data(held_book)
                source_at = data.get("updateTimestampMs", exit_request_ms) if isinstance(data, Mapping) else exit_request_ms
                try:
                    source_at = int(source_at)
                except (ValueError, TypeError, OverflowError):
                    return
                if not 0 < source_at <= now_exit or now_exit - source_at > r3guard.MAX_AGE_MS:
                    return
                swept = r3guard.sweep_bids(held_book, intent.amount)
                if swept is None:
                    return
                gross, fresh_bid = swept
                exit_latched = await self.repository.get_runtime_config(r3guard.exit_key(campaign.campaign_id), False)
                if not exit_latched and r3guard.projected_net(campaign.position, gross) < r3guard.MIN_NET:
                    await self.repository.record_risk_event(
                        "R3_EXIT_RECHECK_HOLD", "INFO", "fresh depth does not meet 0.30 net profit",
                        campaign_id=campaign.campaign_id,
                    )
                    return
            if fresh_bid is None or fresh_bid <= 0 or not fresh_bid.is_finite():
                message = "held-side order book has no executable bid for reduction"
                self.heartbeat.last_error = message
                await self.repository.record_risk_event(
                    "EXECUTION_REPRICE_DEFERRED",
                    "WARNING",
                    message,
                    campaign_id=campaign.campaign_id,
                    payload={
                        "action": decision.action.value,
                        "order_side": intent.order_side.value,
                        "original_limit_price": str(intent.limit_price),
                        "retryable": True,
                        "read_only": True,
                    },
                )
                return
            if fresh_bid != intent.limit_price:
                await self.repository.record_risk_event(
                    "EXECUTION_REPRICE",
                    "INFO",
                    "reduce-only order repriced from the latest held-side bid",
                    campaign_id=campaign.campaign_id,
                    payload={
                        "action": decision.action.value,
                        "outcome": intent.outcome.value,
                        "original_limit_price": str(intent.limit_price),
                        "fresh_bid": str(fresh_bid),
                        "read_only": True,
                    },
                )
            intent = replace(intent, limit_price=fresh_bid)
        try:
            quote = await self._call_api(
                "get_quote", wallet_address=self.settings.wallet_address,
                _trace_campaign_id=campaign.campaign_id, _trace_intent_id=intent.intent_id,
                token_id=campaign.market.up_token_id if intent.outcome is OutcomeSide.UP else campaign.market.down_token_id,
                side=intent.order_side.value, amount_in=normalize_amount_in(intent.amount),
                order_type=self.settings.order_type, slippage_bps=self.settings.slippage_bps,
                chain_id=self.settings.chain_id, price_limit=str(intent.limit_price),
                funding_source=getattr(self.settings, "funding_source", "MPC"),
                _emergency=not is_buy,
                _weight_pre_acquired=True,
                _shared_pre_acquired=True,
            )
        except PredictionRateLimitDeferred:
            raise
        except PredictionClientError as exc:
            # get_quote is read-only and happens before the durable intent and
            # place_order boundary.  A transient Binance error therefore has
            # a known no-execution outcome and is safe to retry next tick.
            # Hard-stopping here strands an existing position and prevents
            # the post-expiry settlement path from ever running.
            self.heartbeat.last_error = str(exc)
            await self.repository.record_risk_event(
                "EXECUTION_QUOTE_DEFERRED",
                "WARNING",
                str(exc),
                campaign_id=campaign.campaign_id,
                payload={
                    "action": decision.action.value,
                    "order_side": intent.order_side.value,
                    "retryable": True,
                    "read_only": True,
                },
            )
            self._finish_entry_attempt(campaign.campaign_id, "execution_guard_rejected")
            return
        quote_id = str(quote.get("quoteId") or quote.get("quote_id") or "") if isinstance(quote, Mapping) else ""
        if not quote_id:
            message = "quote response has no quoteId"
            self.heartbeat.last_error = message
            await self.repository.record_risk_event(
                "EXECUTION_QUOTE_DEFERRED",
                "WARNING",
                message,
                campaign_id=campaign.campaign_id,
                payload={
                    "action": decision.action.value,
                    "order_side": intent.order_side.value,
                    "retryable": True,
                    "read_only": True,
                },
            )
            self._finish_entry_attempt(campaign.campaign_id, "quote_missing_id")
            return
        self.heartbeat.last_error = None

        # The quote is read-only.  Persist the intent immediately before the
        # only mutating boundary so a submitted order always has durable
        # recovery state, while a local/read-only deferral leaves no residue.
        if r3_exit:
            if not r3guard.late_window(campaign, self._now_ms()) or self._now_ms() - exit_request_ms > r3guard.MAX_AGE_MS:
                return
            # A confirmed exit intent stays armed across partial fills/restart.
            # Subsequent reductions use fresh depth, but do not re-demand profit.
            await self.repository.set_runtime_config(
                r3guard.exit_key(campaign.campaign_id),
                {"version": r3guard.VERSION, "at_ms": self._now_ms(), "reason": r3guard.EXIT_REASON},
            )
        if c180_buy:
            bridge = self._c180_bridge_for_worker()
            ready = getattr(self, "_c180_ready", {}).get(campaign.campaign_id)
            if bridge is None or ready is None or ready.book_at_ms is None:
                self._finish_entry_attempt(campaign.campaign_id, "bridge_or_frozen_book_missing")
                return
            with self._entry_stage(campaign.campaign_id, "fresh_book_wait"):
                refreshed = await self._entry_refresh_within_deadline(bridge, campaign, ready)
            if (not refreshed.allowed or refreshed.execution is None
                    or refreshed.execution.worst_ask_limit is None
                    or refreshed.execution.worst_ask_limit > intent.limit_price
                    or refreshed.signal != ready.signal):
                self._finish_entry_attempt(campaign.campaign_id, "fresh_execution_rejected", detail=refreshed.reason)
                return
            if self._now_ms() >= refreshed.execution.expires_at_ms:
                self._finish_entry_attempt(campaign.campaign_id, "execution_expired_after_book")
                return
            balance = await self._call_api(
                "query_payment_option_balances", recv_window=self.settings.recv_window,
                _trace_campaign_id=campaign.campaign_id, _trace_intent_id=intent.intent_id,
                _weight_pre_acquired=True, _shared_pre_acquired=True,
            )
            available = available_balance_display(
                balance, account_type=self._balance_account_type(self.settings))
            if available is None or available < max(intent.amount, self.settings.required_balance_usdt):
                self._finish_entry_attempt(campaign.campaign_id, "balance_insufficient")
                return
            wallet_checked_at_ms = self._now_ms()
            if wallet_checked_at_ms >= refreshed.execution.expires_at_ms:
                self._finish_entry_attempt(campaign.campaign_id, "execution_expired_after_balance")
                return
            intent = replace(intent, created_at_ms=wallet_checked_at_ms)
            atomic_regime_entry = self._selected_strategy_profile.startswith("regime_target6")
            claim_options = {}
            if atomic_regime_entry:
                claim_options = dict(
                    expires_at_ms=ready.execution.expires_at_ms,
                    entry_campaign=replace(campaign, state=decision.state,
                        initial_attempts=campaign.initial_attempts+1,
                        order_attempts=campaign.order_attempts+1,
                        pending_intent_id=intent.intent_id, pending_unknown=False),
                    trace=lambda stage, duration_ns: self._trace_event(
                        "entry_stage", campaign_id=campaign.campaign_id, stage=stage, duration_ns=duration_ns))
            with self._entry_stage(campaign.campaign_id, "claim_total"):
                claim = await bridge.ledger.reserve_c180_intent(
                    loop_id=self._loop_id, market_start_ms=campaign.market.start_time_ms,
                    campaign_id=campaign.campaign_id, intent=intent,
                    decision_at_ms=self._now_ms(),
                    wallet_reconciled_at_ms=wallet_checked_at_ms, **claim_options,
                )
            if not claim.claimed:
                await self.repository.record_risk_event(
                    "C180_ENTRY_CLAIM_REJECTED", "WARNING", claim.reason,
                    campaign_id=campaign.campaign_id,
                    payload={
                        "loop_id": self._loop_id,
                        "market_start_ms": campaign.market.start_time_ms,
                        "reason": claim.reason,
                    },
                )
                self._finish_entry_attempt(campaign.campaign_id, "claim_rejected", detail=claim.reason)
                return
        if s3s5_buy:
            if strategy_leg == "FAV":
                proof = await self.repository.get_runtime_config(s3.fav_approval_key(campaign.campaign_id), None)
                if not s3.fav_approval_valid(proof, self._now_ms(), intent.outcome.value, intent.limit_price, profile=self._selected_strategy_profile):
                    await self.repository.set_runtime_config(s3.fav_exclusion_key(campaign.campaign_id),
                        {"version": s3.FAV_VERSION, "at_ms": self._now_ms(), "reason": "fav_approval_expired_before_submit"})
                    return
            reserve = getattr(self.repository, "reserve_s3s5_intent", None)
            if not callable(reserve) or not await reserve(intent):
                return
        pre_submit_state = campaign.state
        campaign.state = decision.state
        if decision.action is ActionType.BUY_INITIAL:
            campaign.initial_attempts += 1
        elif decision.action is ActionType.BUY_ADD:
            campaign.scale_in_attempts += 1
        elif decision.action is ActionType.BUY_HEDGE:
            campaign.hedge_attempts += 1
        campaign.pending_intent_id = intent.intent_id
        campaign.pending_unknown = False
        campaign.order_attempts += 1
        if c180_buy and atomic_regime_entry:
            pass  # Full entry transition was committed with the claim/intent.
        elif s3s5_buy or c180_buy:
            # Legacy C180 and S3 keep their original persistence path.
            await self.repository.save_campaign(campaign)
        elif hasattr(self.repository, "save_campaign_and_intent"):
            await self.repository.save_campaign_and_intent(campaign, intent)
        else:
            await self.repository.create_intent(intent)
        # This column was previously never written. It is the durable submit
        # boundary; API start/ack below carry the precise monotonic span.
        if c180_buy:
            ready = getattr(self, "_c180_ready", {}).get(campaign.campaign_id)
            if ready is None or ready.execution is None or self._now_ms() >= ready.execution.expires_at_ms:
                # The claim is durable; retain it for reconciliation.  No
                # signed request is made after the frozen execution window.
                await self._reject_unsubmitted_entry(campaign, intent, pre_submit_state,
                                                      "execution_expired_after_claim")
                return
        if c180_buy:
            checked = await self._entry_refresh_within_deadline(bridge, campaign, ready,
                attempts=8 if self._selected_strategy_profile in ("regime_target6_7c_v1", "regime_target6_9_v1", "regime_target6_9a_v1") else 1)
            bound = await self.repository.get_loop(self._loop_id)
            risk_state = await self.repository.get_runtime_config("prediction_risk_state", {})
            if (not checked.allowed or checked.signal != ready.signal or checked.execution is None
                    or checked.execution.worst_ask_limit is None
                    or checked.execution.worst_ask_limit > intent.limit_price
                    or not self.live_capability or not self._allow_new_buys or self._hard_stop_latched
                    or bool(risk_state.get("hard_stop_latched"))
                    or not bound or bound.get("state") != "RUNNING"
                    or bound.get("new_entries_stopped") or bound.get("hard_stop_latched")
                    or self._now_ms() >= ready.execution.expires_at_ms):
                # Durable claim remains a one-entry barrier. This is known not sent.
                # Diagnostics run only after the unchanged admission decision.
                denial_categories = self._post_claim_admission_denials(
                    checked=checked, ready=ready, intent=intent, bound=bound, risk_state=risk_state,
                    live_capability=self.live_capability, allow_new_buys=self._allow_new_buys,
                    hard_stop_latched=self._hard_stop_latched, now_ms=self._now_ms())
                await self.repository.update_intent(intent.intent_id, status="REJECTED", unknown=False,
                    payload_json={"not_submitted": True, "reason": "post_claim_admission_rejected",
                                  "admission_reason": checked.reason,
                                  "denial_categories": list(denial_categories)})
                campaign.pending_intent_id = None
                campaign.pending_unknown = False
                campaign.state = pre_submit_state
                await self.repository.save_campaign(campaign)
                self._finish_entry_attempt(campaign.campaign_id, "post_claim_admission_rejected",
                                           book_reason=checked.reason, admission_reason=checked.reason,
                                           denial_categories=list(denial_categories))
                return
        submitted_ms = self._now_ms()
        ttl_deadline_ms = submitted_ms + max(0, int(intent.ttl_ms))
        if c180_buy:
            # The frozen shadow entry expires at the absolute C180 deadline.
            # Submission latency must not extend a real order beyond it.
            ttl_deadline_ms = min(ttl_deadline_ms, ready.execution.expires_at_ms)
        with self._entry_stage(campaign.campaign_id, "submission_marker"):
            await self.repository.update_intent(intent.intent_id, submission_at_ms=submitted_ms,
                ttl_deadline_ms=ttl_deadline_ms)
        intent = replace(intent, created_at_ms=submitted_ms)
        if c180_buy:
            ready = getattr(self, "_c180_ready", {}).get(campaign.campaign_id)
            if ready is None or ready.execution is None or self._now_ms() >= ready.execution.expires_at_ms:
                await self._reject_unsubmitted_entry(campaign, intent, pre_submit_state,
                                                      "execution_expired_after_claim")
                return
        if c180_buy:
            # Run the durable pre-HTTP admission read before the final book
            # refresh. A cold SQLite page cache made this read take ~1s after
            # the refresh, which aged a fresh book past book_max_age_ms. The
            # same read still runs again at the HTTP boundary.
            try:
                with self._entry_stage(campaign.campaign_id, "durable_admission_prefetch"):
                    await asyncio.to_thread(self._entry_durable_http_guard, entry_loop, entry_profile)
            except PredictionEntryNotSubmitted as exc:
                await self._reject_unsubmitted_entry(campaign, intent, pre_submit_state, str(exc))
                return
            final_book = await self._entry_refresh_within_deadline(bridge, campaign, ready,
                attempts=8 if self._selected_strategy_profile in ("regime_target6_7c_v1", "regime_target6_9_v1", "regime_target6_9a_v1") else 1)
            if (not final_book.allowed or final_book.signal != ready.signal
                    or final_book.execution is None or final_book.execution.worst_ask_limit is None
                    or final_book.execution.worst_ask_limit > intent.limit_price):
                await self._reject_unsubmitted_entry(campaign, intent, pre_submit_state,
                                                      "book_rejected_after_submission_marker")
                return
        try:
            order = await self._call_api(
                "place_order", _trace_campaign_id=campaign.campaign_id, _trace_intent_id=intent.intent_id, wallet_address=self.settings.wallet_address, wallet_id=self.settings.wallet_id,
                _entry_buy=is_buy, _entry_loop_id=entry_loop, _entry_profile=entry_profile,
                _entry_deadline_ms=ready.execution.expires_at_ms if c180_buy else None,
                _entry_book_at_ms=final_book.book_at_ms if c180_buy else None,
                quote_id=quote_id, account_type=self.settings.account_type, order_type=self.settings.order_type,
                time_in_force=self.settings.time_in_force, slippage_bps=self.settings.slippage_bps,
                price_limit=str(intent.limit_price), funding_source=getattr(self.settings, "funding_source", "MPC"),
                _emergency=not is_buy,
                _weight_pre_acquired=True,
                _shared_pre_acquired=True,
            )
            order_id = str(order.get("orderId") or order.get("order_id") or "") if isinstance(order, Mapping) else ""
            if not order_id:
                raise RuntimeError("place-order response has no orderId")
            await self._record_order_result(campaign, intent, order_id, order)
            await self.repository.update_intent(intent.intent_id, order_id=order_id, status="SUBMITTED")
            base_attr_id = None
            if is_fav_baseline and hasattr(self.repository, "record_lane_signal"):
                try:
                    q_snap = await self._quote_for_campaign(campaign)
                    ref_px = float(q_snap.reference_price) if q_snap and q_snap.reference_price else 0.0
                    spot_px = float(q_snap.btc_spot) if q_snap and q_snap.btc_spot else 0.0
                    ask_px = float(q_snap.up_ask if intent.outcome is OutcomeSide.UP else q_snap.down_ask) if q_snap else 0.0
                    bid_px = float(q_snap.up_bid if intent.outcome is OutcomeSide.UP else q_snap.down_bid) if q_snap else 0.0
                    dist_bps = (abs(spot_px - ref_px) / ref_px * 10000.0) if ref_px > 0 else 0.0
                    base_attr_id = await self.repository.record_lane_signal(
                        strategy_lane="FAV_BASELINE",
                        campaign_id=campaign.campaign_id,
                        market_id=campaign.market.market_id_for(intent.outcome),
                        window_id=campaign.market.market_topic_id,
                        intent_id=intent.intent_id,
                        signal_ts=submitted_ms,
                        direction=intent.outcome.value,
                        reference_price=ref_px,
                        spot_price=spot_px,
                        pre_cross_count=getattr(campaign, "reference_cross_count", 0),
                        same_side_seconds=0.0,
                        distance_bps=dist_bps,
                        ask_at_signal=ask_px,
                        bid_at_signal=bid_px,
                        status="SUBMITTED",
                        size_usdt=float(intent.amount),
                    )
                    await self.repository.update_lane_order(
                        base_attr_id,
                        order_id=order_id,
                        order_submit_ts=submitted_ms,
                        status="SUBMITTED",
                    )
                except Exception as exc:
                    LOGGER.debug("Failed recording baseline lane signal: %s", exc)

            terminal_status = await self._poll_order_terminal(campaign, intent, order_id)
            if is_fav_baseline and base_attr_id and terminal_status in ("FILLED", "CLOSED"):
                try:
                    fill_ts = self._now_ms()
                    fill_px = float(campaign.position.avg_buy_price(intent.outcome))
                    fill_sh = float(campaign.position.shares(intent.outcome))
                    await self.repository.update_lane_order(
                        base_attr_id,
                        fill_ts=fill_ts,
                        fill_price=fill_px,
                        fill_shares=fill_sh,
                        fill_latency_ms=max(0, fill_ts - submitted_ms),
                        status="FILLED",
                    )
                except Exception as exc:
                    LOGGER.debug("Failed updating baseline lane fill: %s", exc)
            if terminal_status is not None:
                # Action counters/flags were committed with the cumulative
                # snapshot; avoid a second in-memory increment here.
                await self.repository.save_campaign(campaign)
        except (PredictionRateLimitDeferred, PredictionEntryNotSubmitted) as exc:
            # Budget/admission rejected place_order before transport was called.
            # Keep the attempt, but do not invent an unknown exchange submission.
            if isinstance(exc, PredictionRateLimitDeferred) and exc.method_name != "place_order":
                raise
            campaign.pending_unknown = False
            campaign.pending_intent_id = None
            campaign.state = pre_submit_state
            await self.repository.update_intent(intent.intent_id, unknown=False, status="REJECTED",
                payload_json={"error": str(exc), "not_submitted": True, "shared_budget_deferred": isinstance(exc, PredictionRateLimitDeferred)})
            await self.repository.save_campaign(campaign)
            await self.repository.record_risk_event("EXECUTION_DEFERRED", "WARNING", str(exc),
                campaign_id=campaign.campaign_id, payload={"not_submitted": True, "intent_id": intent.intent_id})
            self._finish_entry_attempt(campaign.campaign_id, "not_submitted_before_http")
            return
        except Exception as exc:  # noqa: BLE001 - unknown execution is never retried blindly
            campaign.pending_unknown = True
            campaign.last_error = str(exc)
            await self.repository.update_intent(intent.intent_id, unknown=True, status="UNKNOWN", payload_json={"error": str(exc)})
            raise
        # Keep the intent linked to the campaign after timeout/unknown.  A
        # restart must reconcile by clientOrderId/position delta before any
        # further order is considered; clearing it here would permit a blind
        # duplicate.

    async def _simulate_shadow_decision(
        self,
        campaign: Campaign,
        decision: StrategyDecision,
        *,
        lane: str | None = None,
        event_time_ms: int | None = None,
    ) -> None:
        """Persist one counterfactual LIMIT fill and advance shadow state."""

        await self._ensure_shadow_window()
        price = decision.limit_price or Decimal("0")
        if price <= 0 or decision.amount <= 0:
            return
        side = decision.order_side or (
            OrderSide.BUY
            if decision.action in {ActionType.BUY_INITIAL, ActionType.BUY_ADD, ActionType.BUY_HEDGE}
            else OrderSide.SELL
        )
        outcome = decision.outcome or campaign.winner_candidate or OutcomeSide.UP
        # Strategy SELL decisions carry token shares already.  Only BUY
        # decisions carry a USDT notional and therefore need to be converted
        # to shares at the executable LIMIT price.  Keeping this distinction
        # at the execution boundary prevents a 0.25-share profit lock from
        # becoming 0.2604 shares at 0.96 (and, more severely, 1.04 shares at
        # a 0.24 loser price).
        if side is OrderSide.BUY:
            shares = decision.amount / price
        else:
            shares = decision.amount
            available = campaign.position.shares(outcome)
            policy = (
                self._shadow_lane_strategies[lane].config
                if lane is not None and lane in self._shadow_lane_strategies
                else getattr(self.strategy, "config", None)
            )
            if decision.action is ActionType.SELL_PROFIT_LOCK:
                # The policy is one-time and bounded by its configured fraction
                # leg.  Do not rely solely on the pure strategy: a replay,
                # restart, or duplicate decision must remain safe here.
                if campaign.profit_lock_used:
                    return
                if bool(getattr(policy, "pnl_priority_enabled", False)):
                    shares = min(shares, available)
                else:
                    original = campaign.position.initial_shares(outcome) or available
                    fraction = as_decimal(getattr(policy, "max_profit_lock_fraction", "0.25"), Decimal("0.25"))
                    shares = min(shares, original * fraction)
            elif decision.action is ActionType.SELL_LOSER:
                max_steps = int(getattr(policy, "max_loser_unwinds", 2))
                if campaign.loser_unwind_count >= max_steps:
                    return
                original = campaign.position.initial_shares(outcome) or available
                max_fraction = as_decimal(getattr(policy, "max_loser_unwind_fraction", "0.50"), Decimal("0.50"))
                step_fraction = as_decimal(getattr(policy, "loser_unwind_fraction", "0.25"), Decimal("0.25"))
                remaining_cap = original * max_fraction - campaign.loser_unwind_shares
                shares = min(shares, available * step_fraction, remaining_cap)
            shares = min(shares, available)
        if shares <= 0:
            return
        gross = decision.amount if side is OrderSide.BUY else shares * price
        fee_bps = Decimal(str(getattr(self.settings, "shadow_fee_bps", 10)))
        slippage_bps = Decimal(str(getattr(self.settings, "shadow_slippage_bps", 0)))
        total_cost_bps = fee_bps + slippage_bps
        fee = gross * total_cost_bps / Decimal("10000")
        count = self._shadow_decision_counts.get(campaign.campaign_id, 0) + 1
        self._shadow_decision_counts[campaign.campaign_id] = count
        action = decision.action.value
        if side is OrderSide.BUY:
            campaign.position.add_buy(
                outcome,
                shares,
                gross,
                fee,
                initial=decision.action is ActionType.BUY_INITIAL,
            )
            campaign.buy_count = max(
                campaign.buy_count,
                2 if decision.action in {ActionType.BUY_ADD, ActionType.BUY_HEDGE} else 1,
            )
            if decision.action is ActionType.BUY_INITIAL and campaign.initial_outcome is None:
                campaign.initial_outcome = outcome
                campaign.initial_filled_at_ms = int(event_time_ms if event_time_ms is not None else self._now_ms())
                campaign.state = CampaignState.INITIAL_POSITION
            elif decision.action is ActionType.BUY_ADD:
                campaign.state = CampaignState.INITIAL_POSITION
            elif decision.action is ActionType.BUY_HEDGE:
                campaign.hedge_used = True
                campaign.hedged_at_ms = self._now_ms()
                campaign.state = CampaignState.HEDGED
        else:
            campaign.position.add_sell(outcome, shares, gross, fee)
            if decision.action in {ActionType.SELL_PROFIT_LOCK, ActionType.SELL_PROTECTIVE}:
                campaign.profit_lock_used = True
            if decision.action is ActionType.SELL_LOSER:
                campaign.loser_unwind_count += 1
                campaign.loser_unwind_shares += shares
        campaign.state = decision.state
        campaign.order_attempts += 1
        if decision.action is ActionType.BUY_INITIAL:
            campaign.initial_attempts += 1
        elif decision.action is ActionType.BUY_ADD:
            campaign.scale_in_attempts += 1
        elif decision.action is ActionType.BUY_HEDGE:
            campaign.hedge_attempts += 1
        # Virtual lanes must never enter prediction_campaigns or the durable
        # market loop.  Their immutable state lives only in the Shadow ledger;
        # the primary Control campaign keeps the existing persistence path.
        if lane is None:
            await self.repository.save_campaign(campaign, loop_id=self._loop_id)
        config_hash = str(
            self._shadow_lane_config_hashes.get(lane or "control")
            or self._shadow_config_hash
            or self.effective_config_hash
        )
        window = self._shadow_lane_windows.get(lane or "control") or self._shadow_window or {}
        provisional = (campaign.winner_candidate or outcome).value
        shadow = await self.repository.record_shadow_campaign(
            campaign,
            config_hash=config_hash,
            window_start_ms=int(window.get("window_start_ms", self._now_ms())),
            window_end_ms=int(window.get("window_end_ms", self._now_ms() + 86_400_000)),
            resolved_outcome=provisional,
            simulated_fees="0",
            simulated_pnl="0",
            expected_fill_count=1,
            resolved_at_ms=campaign.market.end_time_ms,
            payload={
                "provisional": True,
                "action_count": count,
                "lane": lane or "control",
                "virtual_campaign_id": campaign.campaign_id if lane is not None else None,
            },
        )
        shadow_id = str(shadow.get("shadow_campaign_id") or "")
        if lane is None:
            self._shadow_campaign_ids[campaign.campaign_id] = shadow_id
        else:
            self._shadow_lane_shadow_ids[(campaign.campaign_id.split("::shadow::", 1)[0], lane)] = shadow_id
        await self.repository.record_shadow_fill(
            {
                "fill_identity": f"{campaign.campaign_id}:{action}:{count}",
                "outcome": outcome.value,
                "order_side": side.value,
                "shares": str(shares),
                "price": str(price),
                "gross_amount": str(gross),
                "simulated_fee": str(fee),
                "cost_model": {
                    "fee_bps": str(fee_bps),
                    "slippage_bps": str(slippage_bps),
                    "total_cost_bps": str(total_cost_bps),
                },
                "event_time_ms": self._now_ms(),
                "action": action,
                "position": {
                    "up_shares": str(campaign.position.up_shares),
                    "down_shares": str(campaign.position.down_shares),
                    "fees": str(campaign.position.fees),
                },
                "counters": {
                    "buy_count": campaign.buy_count,
                    "order_attempts": campaign.order_attempts,
                    "initial_attempts": campaign.initial_attempts,
                    "hedge_attempts": campaign.hedge_attempts,
                },
                "lane": lane or "control",
            },
            shadow_campaign_id=shadow_id,
        )

    async def _history_rows(self, intent: OrderIntent, *, statuses: tuple[str, ...] = ("OPENING", "CLOSED"), target_order_id: str | None = None) -> list[Mapping[str, Any]]:
        """Read official history using only documented page filters.

        The API has no orderId filter.  OPENING and CLOSED are queried as
        separate status values (never a comma-separated value) and matching
        is always done locally by the worker.
        """

        # Binance order-history dates are UTC.  A 00:00-07:59 Asia/Taipei
        # order belongs to the previous official date; querying the local
        # calendar day makes an authoritative FILLED order look absent and
        # leaves the durable intent UNKNOWN forever.
        history_date = datetime.fromtimestamp(
            max(0, intent.created_at_ms or self._now_ms()) / 1000,
            ZoneInfo("UTC"),
        ).date().isoformat()
        rows_out: list[Mapping[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        limit = 50
        def append_page(payload: Any, fallback_status: str = "") -> int:
            rows = payload.get("items", payload.get("orders", payload.get("data", []))) if isinstance(payload, Mapping) else []
            if isinstance(rows, Mapping):
                rows = [rows]
            if not isinstance(rows, list):
                return 0
            for item in rows:
                if not isinstance(item, Mapping):
                    continue
                key = (
                    str(item.get("orderId") or item.get("order_id") or item.get("id") or ""),
                    str(item.get("status") or fallback_status),
                    str(item.get("updatedAt") or item.get("eventTime") or item.get("time") or ""),
                )
                if key not in seen:
                    seen.add(key)
                    rows_out.append(item)
            return len(rows)

        async def fetch_history(*, status: str | None, offset: int) -> Any:
            filters: dict[str, Any] = {
                "wallet_address": self.settings.wallet_address,
                "startDate": history_date,
                "endDate": history_date,
                "offset": offset,
                "limit": limit,
            }
            if status is not None:
                filters["status"] = status
            # Reconciliation of a SELL is part of the de-risking path.  Give
            # it the emergency slice of the local weight budget so a normal
            # quote burst cannot leave a known reduction unconfirmed.
            return await self._call_api(
                "query_order_history",
                _emergency=intent.order_side is OrderSide.SELL,
                _management=intent.order_side is OrderSide.BUY,
                _fallback_statuses=HISTORY_STATUS_FALLBACK if status is not None else (),
                **filters,
            )

        # Prefer the documented status pages so the normal path stays small,
        # but fall back to one date-bounded query when Binance returns the
        # account-specific HTTP 500 seen for status-filtered history. An API
        # error outside that known filter failure remains fail-closed.
        fallback_all = False
        for status in statuses:
            offset = 0
            for _ in range(5):
                try:
                    payload = await fetch_history(status=status, offset=offset)
                except PredictionReadTimestampError:
                    # Timestamp exhaustion is not an unsupported status filter.
                    # Defer reconciliation without another fallback request.
                    raise
                except PredictionAPIError as exc:
                    if exc.status_code not in HISTORY_STATUS_FALLBACK:
                        raise
                    fallback_all = True
                    break
                count = append_page(payload, status)
                if target_order_id and any(str(r.get("orderId") or r.get("order_id") or r.get("id") or "") == target_order_id for r in rows_out):
                    return rows_out
                if count == 0 or count < limit:
                    break
                offset += count
            if fallback_all:
                break
        if fallback_all:
            rows_out.clear()
            seen.clear()
            offset = 0
            for _ in range(5):
                payload = await fetch_history(status=None, offset=offset)
                count = append_page(payload)
                if target_order_id and any(str(r.get("orderId") or r.get("order_id") or r.get("id") or "") == target_order_id for r in rows_out):
                    return rows_out
                if count == 0 or count < limit:
                    break
                offset += count
        return rows_out

    async def _query_history_rows(self, order_id: str, intent: OrderIntent, *, status: str | None = None) -> list[Mapping[str, Any]]:
        self._trace_event('history_poll',campaign_id=intent.campaign_id,intent_id=intent.intent_id)
        statuses = (str(status).upper(),) if status else ("OPENING", "CLOSED")
        for selected_status in statuses:
            rows = await self._history_rows(intent, statuses=(selected_status,), target_order_id=str(order_id))
            matches = [
                row for row in rows
                if str(row.get("orderId") or row.get("order_id") or row.get("id") or "") == str(order_id)
                and (status is None or str(row.get("status", "")).upper() in {str(status).upper(), "PARTIALLY_FILLED" if str(status).upper() == "OPENING" else str(status).upper()})
            ]
            if matches:
                return matches
        return []

    async def _cancel_due_before_history(self, campaign: Campaign, intent: OrderIntent, order_id: str) -> None:
        """An expired known order is cancelled before optional history reads.

        Keep the original durable cancellation claim for uncertain transport
        results. History remains responsible for the final fill/cancel state.
        """
        row = await self.repository.get_intent(intent.intent_id)
        if not row or str(row.get("status", "")).upper() in {"FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED"}:
            return
        deadline = int(row.get("ttl_deadline_ms") or
            (int(row.get("submission_at_ms") or row.get("created_at_ms") or intent.created_at_ms) + max(0, int(intent.ttl_ms))))
        if self._now_ms() < deadline or bool(row.get("cancel_requested")):
            return
        try:
            await self._cancel_order_once(intent, order_id)
        except (PredictionRateLimitDeferred, PredictionClientError) as exc:
            # A failed cancel is not a new unknown BUY. Keep its known order id
            # and reconcile without blindly resending a possibly accepted cancel.
            await self.repository.record_risk_event("ORDER_CANCEL_DEFERRED", "WARNING", str(exc),
                campaign_id=campaign.campaign_id,
                payload={"intent_id": intent.intent_id, "known_order_id": True,
                    "deadline_ms": deadline, "local_weight_backpressure": isinstance(exc, PredictionRateLimitDeferred)})

    async def _cancel_order_once(self, intent: OrderIntent, order_id: str) -> bool:
        self._trace_event('cancel_request',campaign_id=intent.campaign_id,intent_id=intent.intent_id)
        claimed = await self.repository.mark_cancel_requested(intent.intent_id, at_ms=self._now_ms())
        if not claimed:
            return False
        current = await self.repository.get_intent(intent.intent_id) or {}
        previous_attempts = int(current.get("cancel_attempt_count") or 0)
        if self.settings.wallet_address and self.settings.wallet_id and hasattr(self.client, "batch_cancel_orders"):
            await self.repository.update_intent(intent.intent_id, cancel_attempt_count=previous_attempts + 1,
                cancel_in_flight=1, cancel_last_attempt_at_ms=self._now_ms(), cancel_last_error=None)
            try:
                await self._call_api(
                    "batch_cancel_orders",
                    wallet_address=self.settings.wallet_address,
                    wallet_id=self.settings.wallet_id,
                    order_ids=[order_id],
                    _emergency=True,
                )
            except PredictionRateLimitDeferred:
                # No exchange request was made when the local budget denied
                # the call. Release the durable claim so the next tick can
                # retry the same cancellation instead of leaving an open
                # order permanently marked as already cancelled.
                await self.repository.update_intent(
                    intent.intent_id,
                    cancel_requested=False,
                    cancel_requested_at_ms=None,
                    cancel_attempt_count=previous_attempts, cancel_in_flight=0,
                    cancel_last_error="request budget deferred before transport",
                )
                raise
            except Exception as exc:
                await self.repository.update_intent(intent.intent_id, cancel_in_flight=0, cancel_last_error=str(exc))
                raise
            else:
                # This is the successful cancel response time, not terminal proof.
                await self.repository.update_intent(intent.intent_id, cancel_in_flight=0,
                    cancel_accepted_at_ms=self._now_ms(), cancel_last_error=None)
        return True

    async def _poll_order_terminal(self, campaign: Campaign, intent: OrderIntent, order_id: str, attempts: int = 3) -> str | None:
        last_status: str | None = None
        # Poll through the configured TTL boundary.  A fixed four-observation
        # loop covered a 3s TTL but silently stopped around t=3 for profiles
        # configured with a 5s TTL, marking the intent UNKNOWN too early.
        poll_count = max(1, attempts)
        ttl_ms = max(0, int(intent.ttl_ms))
        poll_interval_seconds = max(0.0, float(self.settings.poll_interval_seconds))
        if attempts > 1 and ttl_ms > 0:
            if poll_interval_seconds > 0:
                interval_ms = max(1, int(round(poll_interval_seconds * 1000)))
                ttl_poll_count = (ttl_ms + interval_ms - 1) // interval_ms + 1
                poll_count = max(poll_count, ttl_poll_count)
            else:
                # Zero-delay tests/reconciliation must remain bounded.
                poll_count += 1
        poll_count = min(poll_count, 5)
        terminal_statuses = {"CLOSED", "FILLED", "CANCELED", "CANCELLED", "EXPIRED", "FAILED"}
        for poll_index in range(poll_count):
            await self._cancel_due_before_history(campaign, intent, order_id)
            try:
                rows = await self._query_history_rows(order_id, intent)
            except PredictionRateLimitDeferred as exc:
                # The order id is already durable.  Local weight backpressure
                # is not exchange uncertainty and must not turn a submitted
                # reduction into UNKNOWN or trip the hard-stop latch.
                campaign.pending_intent_id = intent.intent_id
                campaign.pending_unknown = False
                campaign.last_error = str(exc)
                self.heartbeat.last_error = str(exc)
                await self.repository.update_intent(
                    intent.intent_id,
                    order_id=order_id,
                    unknown=False,
                    status=last_status or "SUBMITTED",
                    payload_json={
                        "error": str(exc),
                        "method": exc.method_name,
                        "order_id": str(order_id),
                        "reconciliation_deferred": True,
                        "local_weight_backpressure": True,
                    },
                )
                await self.repository.save_campaign(campaign)
                await self.repository.record_risk_event(
                    "ORDER_HISTORY_DEFERRED",
                    "WARNING",
                    str(exc),
                    campaign_id=campaign.campaign_id,
                    payload={
                        "intent_id": intent.intent_id,
                        "order_id": str(order_id),
                        "known_order_id": True,
                        "read_only": True,
                        "retryable": True,
                        "local_weight_backpressure": True,
                    },
                )
                return None
            except PredictionClientError as exc:
                # place_order already returned an authoritative order_id and
                # that identifier is persisted before polling begins.  An
                # order-history failure is therefore a read-only uncertainty,
                # not an unknown submission.  Keep this campaign fail-closed,
                # retry the same order_id on later ticks/restart, and leave the
                # rest of the finite loop alive for management and settlement.
                campaign.pending_intent_id = intent.intent_id
                campaign.pending_unknown = True
                campaign.last_error = str(exc)
                self.heartbeat.last_error = str(exc)
                await self.repository.update_intent(
                    intent.intent_id,
                    order_id=order_id,
                    unknown=True,
                    status=last_status or "UNKNOWN",
                    payload_json={
                        "error": str(exc),
                        "order_id": str(order_id),
                        "reconciliation_deferred": True,
                    },
                )
                await self.repository.save_campaign(campaign)
                await self.repository.record_risk_event(
                    "ORDER_RECONCILIATION_DEFERRED",
                    "WARNING",
                    str(exc),
                    campaign_id=campaign.campaign_id,
                    payload={
                        "intent_id": intent.intent_id,
                        "order_id": str(order_id),
                        "known_order_id": True,
                        "read_only": True,
                        "retryable": True,
                    },
                )
                return None
            row = rows[-1] if rows else None
            status = ""
            if row is not None:
                status = str(row.get("status", "")).upper()
                last_status = status or last_status
                if status in {"OPENING", "OPEN", "PARTIAL", "PARTIALLY_FILLED", "FILLED", "CLOSED", "CANCELED", "CANCELLED", "EXPIRED", "FAILED"}:
                    await self.apply_order_snapshot(campaign, row, outcome=intent.outcome, intent=intent)
                if status in terminal_statuses:
                    campaign.pending_intent_id = None
                    campaign.pending_unknown = False
                    await self.repository.update_intent(intent.intent_id, status=status, unknown=False)
                    await self.repository.save_campaign(campaign)
                    return status
            expired = self._now_ms() >= int(intent.created_at_ms) + max(0, int(intent.ttl_ms))
            partial = status in {"PARTIAL", "PARTIALLY_FILLED"}
            # Cancel at the TTL even if order-history has not indexed the
            # order yet.  We already have the authoritative order_id from the
            # submit response; waiting for an OPENING history row allowed
            # orders to fill tens of seconds after a 3s TTL.
            if partial or expired:
                try:
                    await self._cancel_order_once(intent, order_id)
                except PredictionRateLimitDeferred as exc:
                    campaign.pending_intent_id = intent.intent_id
                    campaign.pending_unknown = False
                    campaign.last_error = str(exc)
                    self.heartbeat.last_error = str(exc)
                    await self.repository.update_intent(
                        intent.intent_id,
                        order_id=order_id,
                        unknown=False,
                        status=last_status or status or "SUBMITTED",
                        payload_json={
                            "error": str(exc),
                            "method": exc.method_name,
                            "order_id": str(order_id),
                            "cancellation_deferred": True,
                            "local_weight_backpressure": True,
                        },
                    )
                    await self.repository.save_campaign(campaign)
                    await self.repository.record_risk_event(
                        "ORDER_CANCEL_DEFERRED",
                        "WARNING",
                        str(exc),
                        campaign_id=campaign.campaign_id,
                        payload={
                            "intent_id": intent.intent_id,
                            "order_id": str(order_id),
                            "known_order_id": True,
                            "read_only": True,
                            "retryable": True,
                            "local_weight_backpressure": True,
                        },
                    )
                    return None
                if partial:
                    # Keep the intent pending: the next management tick must
                    # observe CLOSED and apply only its cumulative delta.
                    return None
            if poll_index + 1 < poll_count:
                await asyncio.sleep(max(1.0 if poll_interval_seconds > 0 else 0.0, poll_interval_seconds))
        if last_status in {"OPENING", "OPEN", "PARTIAL", "PARTIALLY_FILLED"}:
            # Explicit working status is not unknown execution. Keep the barrier.
            campaign.pending_intent_id = intent.intent_id
            campaign.pending_unknown = False
            await self.repository.update_intent(intent.intent_id, unknown=False, status=last_status)
            await self.repository.save_campaign(campaign)
            return None
        campaign.pending_unknown = True
        await self.repository.update_intent(intent.intent_id, unknown=True, status=last_status or "UNKNOWN")
        await self.repository.save_campaign(campaign)
        return None

    async def _record_order_result(self, campaign: Campaign, intent: OrderIntent, order_id: str, payload: Any) -> None:
        self._trace_event('submit_result_seen',campaign_id=campaign.campaign_id,intent_id=intent.intent_id)
        result = await self.repository.apply_order_snapshot_atomic(
            campaign,
            intent,
            order_id,
            payload if isinstance(payload, Mapping) else {},
            outcome=intent.outcome,
        )
        candidate = result.get("campaign")
        if isinstance(candidate, Campaign):
            campaign.__dict__.update(candidate.__dict__)


__all__ = ["PredictionWorker", "WorkerHeartbeat", "WorkerState"]
