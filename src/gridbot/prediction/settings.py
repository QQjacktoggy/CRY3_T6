"""Configuration primitives for the isolated Prediction runtime.

The runtime deliberately does not construct a Binance client or read API
secrets.  The dedicated ``PREDICTION_BINANCE_API_KEY`` and
``PREDICTION_BINANCE_API_SECRET`` values remain an entrypoint boundary
concern: ``predict_main.py`` reads them and passes them to
:class:`~src.gridbot.prediction.client.BinancePredictionClient`.  Legacy
generic ``BINANCE_*`` values are never consulted by the Prediction service.

Only operational, non-secret settings live here.  Keeping the limits in one
small dataclass makes it difficult for a Telegram/VM adapter to accidentally
start an unbounded loop or promote a first-run process directly to live mode.
"""

from __future__ import annotations

import os
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Mapping, Any

from .strategy import DEFAULT_STRATEGY_PROFILE, SHADOW_ONLY_PROFILES, StrategyConfig, REVERSAL5_LANES, NEXT5_LANES, CONFIRM3_LANES, VALUE9_LANES


class RuntimeMode(str, Enum):
    """Requested operating mode.

    ``LIVE`` is only a request.  The runtime still requires an explicit
    promotion gate after a successful preflight and a shadow canary.
    """

    SHADOW = "shadow"
    LIVE = "live"

    @classmethod
    def parse(cls, value: str | "RuntimeMode" | None) -> "RuntimeMode":
        if isinstance(value, cls):
            return value
        normalized = str(value or cls.SHADOW.value).strip().lower()
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(f"unsupported prediction runtime mode: {value!r}") from exc


def normalize_loop_limit(value: int | None, *, default: int = 10, maximum: int = 200) -> int:
    """Return a finite loop count and reject unsafe/unbounded values."""

    selected = default if value is None else int(value)
    if selected < 1:
        raise ValueError("prediction loop limit must be at least 1")
    if selected > maximum:
        raise ValueError(f"prediction loop limit cannot exceed {maximum}")
    return selected


def _decimal(value: object, *, default: Decimal) -> Decimal:
    if value is None or value == "":
        return default
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid decimal setting: {value!r}") from exc
    if result < 0:
        raise ValueError("decimal setting cannot be negative")
    return result


@dataclass(frozen=True)
class PredictionSettings:
    """Safe defaults for the Prediction orchestrator.

    The default is shadow mode, ten iterations, and no implicit live
    promotion.  ``required_balance_usdt`` is intentionally two USDT: one
    initial leg plus one permitted hedge.  A caller can lower it for a
    read-only/shadow canary, but cannot make the loop unbounded.
    """

    mode: RuntimeMode | str = RuntimeMode.SHADOW
    market_symbol: str = "BTCUSDT"
    market_duration_seconds: int = 300
    # Binance's documented market-list category IDs are lowercase and use a
    # hyphen for the second-level category.
    market_l1_category: str | None = "crypto"
    market_l2_category: str | None = "up-down"
    market_vendor: str = "predict_fun"
    chain_id: str = "56"
    strategy_profile: str = DEFAULT_STRATEGY_PROFILE
    # Optional isolated Shadow experiment lanes.  The default stays empty so
    # existing callers retain the single-strategy Shadow behavior.
    shadow_lanes: tuple[str, ...] = ()
    # Independent FAV_P3 lane arm: off | live (phase-1). Does not affect Baseline.
    fav_p3_arm: str = "off"
    # TypeSafe Jev System One Gate Evaluator for FAV (Option A)
    jev_gate_enabled: bool = True
    jev_gate_runtime_dir: str = "/home/jack_shih/cry3/jev_shadow_lane/runtime"
    jev_gate_max_age_ms: int = 20000
    jev_gate_fail_open: bool = True
    # When set, every configured sidecar freezes after exactly this many
    # unique resolved counterfactuals.  It is independent from service
    # lifetime, so an idle observer cannot silently overrun the experiment.
    shadow_exact_target: int | None = None
    shadow_fee_bps: int = 10
    shadow_slippage_bps: int = 0

    wallet_address: str | None = None
    wallet_id: str | None = None
    account_type: str = "SPOT"
    # Binance Prediction uses ``MPC`` for the CeDeFi/prediction-wallet source
    # and ``CEX`` when drawing from the selected SPOT/FUNDING account.
    funding_source: str = "MPC"
    order_type: str = "LIMIT"
    time_in_force: str = "GTC"
    slippage_bps: int = 100
    recv_window: int | None = 5_000
    # Operator-selectable execution envelope. Only the reviewed 1/2/3 USDT
    # units are accepted, and the client enforces the same limit again at the
    # HTTP boundary so a malformed worker cannot enlarge a live order.
    order_unit_usdt: Decimal = Decimal("1")

    poll_interval_seconds: float = 1.0
    loop_limit: int = 10
    max_loop_limit: int = 200
    required_balance_usdt: Decimal = Decimal("2")
    minimum_shadow_ticks: int = 1
    live_enabled: bool = False
    # Explicit operator opt-in for live canary before the historical Shadow
    # promotion sample threshold is met. Live preflight remains mandatory.
    live_skip_shadow_promotion: bool = False
    # Binance requires SAS for the mutating Prediction trade endpoints.  The
    # API has no read-only SAS probe, so Live must carry an explicit operator
    # attestation after the wallet's SAS setting has been verified in Binance.
    # A missing attestation is fail-closed and is never inferred from a
    # successful wallet/quota read.
    require_sas: bool = True
    sas_verified: bool = False
    require_permission: bool = True
    discovery_limit: int = 50
    discovery_tolerance_seconds: int = 2
    # A canary window is an immutable provenance boundary.  When omitted,
    # the repository/worker establishes one before the first shadow sample.
    shadow_window_start_ms: int | None = None
    shadow_window_end_ms: int | None = None
    # There is no read-only SAS probe.  The live gate therefore requires the
    # separate secret-free ``sas_verified`` operator attestation; this token
    # is retained only for backwards compatibility and is never proof.
    sas_token: str | None = None
    permission_granted: bool = False
    # Injected read-only BTC spot provider; never inferred from prediction
    # token prices.
    spot_source: Any = None

    def __post_init__(self) -> None:
        mode = RuntimeMode.parse(self.mode)
        object.__setattr__(self, "mode", mode)
        # Prediction orders remain LIMIT/GTC on the SPOT payment account.
        # The only reviewed operator-selectable BUY units are 1, 2 and 3 USDT.
        object.__setattr__(self, "account_type", "SPOT")
        funding_source = str(self.funding_source or "MPC").strip().upper()
        if funding_source not in {"MPC", "CEX"}:
            raise ValueError("funding_source must be MPC or CEX")
        object.__setattr__(self, "funding_source", funding_source)
        object.__setattr__(self, "order_type", "LIMIT")
        object.__setattr__(self, "time_in_force", "GTC")
        order_unit = _decimal(self.order_unit_usdt, default=Decimal("1"))
        if order_unit not in {Decimal("1"), Decimal("2"), Decimal("3")}:
            raise ValueError("order_unit_usdt must be exactly 1, 2 or 3 USDT")
        object.__setattr__(self, "order_unit_usdt", order_unit)
        # Validate the selected policy before the process touches the API.
        selected_profile = str(self.strategy_profile or DEFAULT_STRATEGY_PROFILE).strip().lower()
        StrategyConfig.for_profile(selected_profile)
        if selected_profile in SHADOW_ONLY_PROFILES:
            raise ValueError(
                f"prediction strategy profile {selected_profile!r} is Shadow-only; configure it in shadow_lanes"
            )
        raw_lanes = self.shadow_lanes
        if isinstance(raw_lanes, str):
            raw_lanes = tuple(item.strip().lower() for item in raw_lanes.split(",") if item.strip())
        lanes = tuple(dict.fromkeys(str(item).strip().lower() for item in (raw_lanes or ()) if str(item).strip()))
        allowed_lanes = {
            *VALUE9_LANES,
            *CONFIRM3_LANES,
            *NEXT5_LANES,
            *REVERSAL5_LANES,
            'trend_continuation_v1',
            'reference_reversion_v1',
            'spot_book_lag_v1',
            'diffusion_value_v1',
            'late_distance_v1',
            "control",
            "lane_a",
            "lane_b",
            "balanced_hold",
            "quality_hold",
            "quality_hold_v2",
            "quality_hold_v3_profit1",
            "quality_hold_v3_gate_v1",
            "quality_hold_v3_loss_guard_v2",
            "quality_hold_v3_net_edge_v1",
            "quality_hold_v3_history30_control_v1",
            "quality_hold_v3_momentum30_v1",
            "quality_hold_v3_confirm30_v1",
            "quality_hold_v4_rescue",
            "quality_hold_v5_pnl",
            "quality_hold_v6_a_staged",
            "quality_hold_v6_balanced_shadow",
            "quality_hold_v6_b_45s",
            "quality_hold_v6_c_45s",
            "quality_hold_v3_sniper",
            "late_maturity_v1",
            "regime_value_v7",
            "regime_value_v8_calibrated",
            "latency_snipe",
            "pair_cost_arb",
            "complete_set_arb_v1",
            "s3s5_pair_v1",
            "fav_only_v1",
            "fav_only_v2",
            "fav_only_v3",
            "fav_only_v4",
        }
        if set(lanes)&set(REVERSAL5_LANES):
            if set(lanes)!=set(REVERSAL5_LANES) or self.mode!=RuntimeMode.SHADOW or self.live_enabled:
                raise ValueError('Reversal5 requires exactly five Shadow-only lanes and Live disabled')
            if int(self.shadow_fee_bps)!=275 or int(self.shadow_slippage_bps)!=25 or order_unit!=Decimal('2'):
                raise ValueError('Reversal5 requires frozen 275+25 bps and 2 USDT initial unit')
        if set(lanes)&set(NEXT5_LANES):
            if set(lanes)!=set(NEXT5_LANES) or self.mode!=RuntimeMode.SHADOW or self.live_enabled:
                raise ValueError('Next5 requires exactly five Shadow-only lanes')
            if int(self.shadow_fee_bps)!=275 or int(self.shadow_slippage_bps)!=25 or order_unit!=Decimal('2'):
                raise ValueError('Next5 requires frozen cost scenario and 2 USDT nominal unit')
        if set(lanes)&set(CONFIRM3_LANES):
            if set(lanes)!=set(CONFIRM3_LANES) or mode!=RuntimeMode.SHADOW or self.live_enabled:
                raise ValueError('Confirm3 requires exactly three Shadow-only research lanes')
            if order_unit!=Decimal('2') or int(self.shadow_fee_bps)!=275 or int(self.shadow_slippage_bps)!=25:
                raise ValueError('Confirm3 requires frozen 2 USDT / 300 bps scenario')
            if int(self.shadow_exact_target)!=200:
                raise ValueError('Confirm3 requires exactly 200 markets')
        if set(lanes)&set(VALUE9_LANES):
            if set(lanes)!=set(VALUE9_LANES) or mode!=RuntimeMode.SHADOW or self.live_enabled:
                raise ValueError('Vol Shadow requires exactly seven Shadow-only research lanes')
            if order_unit!=Decimal('2') or int(self.shadow_fee_bps)!=275 or int(self.shadow_slippage_bps)!=25:
                raise ValueError('Value9 requires frozen 2 USDT / 300 bps scenario')
            if int(self.shadow_exact_target)!=600:
                raise ValueError('Vol Shadow requires 600 fixed slots with a 20-slot engineering checkpoint')
        invalid_lanes = sorted(set(lanes) - allowed_lanes)
        if invalid_lanes:
            raise ValueError(f"unsupported prediction Shadow lane(s): {invalid_lanes!r}")
        object.__setattr__(self, "shadow_lanes", lanes)
        arm = str(getattr(self, "fav_p3_arm", "off") or "off").strip().lower()
        if arm in {"1", "true", "yes", "on"}:
            arm = "live"
        if arm not in {"off", "live", "shadow"}:
            raise ValueError("fav_p3_arm must be off|live|shadow")
        object.__setattr__(self, "fav_p3_arm", arm)

        research_max = 600 if set(lanes)==set(VALUE9_LANES) else 200
        if self.shadow_exact_target is not None:
            exact_target = int(self.shadow_exact_target)
            if exact_target < 1 or exact_target > research_max:
                raise ValueError("shadow_exact_target must be between 1 and 200")
            object.__setattr__(self, "shadow_exact_target", exact_target)
        if int(self.shadow_fee_bps) < 0 or int(self.shadow_slippage_bps) < 0:
            raise ValueError("Shadow fee/slippage bps cannot be negative")
        object.__setattr__(self, "shadow_fee_bps", int(self.shadow_fee_bps))
        object.__setattr__(self, "shadow_slippage_bps", int(self.shadow_slippage_bps))

        maximum = int(self.max_loop_limit)
        if maximum < 1 or maximum > research_max:
            raise ValueError("max_loop_limit must be between 1 and 200")
        object.__setattr__(self, "max_loop_limit", maximum)
        object.__setattr__(
            self,
            "loop_limit",
            normalize_loop_limit(self.loop_limit, default=10, maximum=maximum),
        )
        if int(self.market_duration_seconds) <= 0:
            raise ValueError("market_duration_seconds must be positive")
        if float(self.poll_interval_seconds) < 0:
            raise ValueError("poll_interval_seconds cannot be negative")
        if int(self.minimum_shadow_ticks) < 1:
            raise ValueError("minimum_shadow_ticks must be at least 1")
        if int(self.discovery_limit) < 1 or int(self.discovery_limit) > 50:
            raise ValueError("discovery_limit must be between 1 and 50")
        if int(self.discovery_tolerance_seconds) < 0:
            raise ValueError("discovery_tolerance_seconds cannot be negative")
        if self.shadow_window_start_ms is not None:
            object.__setattr__(self, "shadow_window_start_ms", int(self.shadow_window_start_ms))
        if self.shadow_window_end_ms is not None:
            object.__setattr__(self, "shadow_window_end_ms", int(self.shadow_window_end_ms))
        if (
            self.shadow_window_start_ms is not None
            and self.shadow_window_end_ms is not None
            and self.shadow_window_start_ms > self.shadow_window_end_ms
        ):
            raise ValueError("shadow window start must be <= end")
        if int(self.slippage_bps) < 0:
            raise ValueError("slippage_bps cannot be negative")
        if self.recv_window is not None and int(self.recv_window) <= 0:
            raise ValueError("recv_window must be positive when provided")
        object.__setattr__(
            self,
            "required_balance_usdt",
            _decimal(self.required_balance_usdt, default=Decimal("2")),
        )

    @property
    def is_live_requested(self) -> bool:
        return self.mode is RuntimeMode.LIVE

    @property
    def max_iterations(self) -> int:
        """Compatibility name for adapters that call the loop an iteration."""

        return self.max_loop_limit

    @property
    def default_iterations(self) -> int:
        return self.loop_limit

    @property
    def config_payload(self) -> dict[str, Any]:
        """Return the secret-free, canonical live configuration envelope.

        SAS tokens and injected callables are deliberately excluded.  The
        resulting payload is suitable for provenance checks without exposing
        credentials or unstable object representations.
        """

        return {
            "mode": self.mode.value,
            "market_symbol": self.market_symbol,
            "market_duration_seconds": int(self.market_duration_seconds),
            "market_l1_category": self.market_l1_category,
            "market_l2_category": self.market_l2_category,
            "market_vendor": self.market_vendor,
            "chain_id": self.chain_id,
            "strategy_profile": self.strategy_profile,
            "shadow_lanes": list(self.shadow_lanes),
            "fav_p3_arm": self.fav_p3_arm,
            "jev_gate_enabled": bool(self.jev_gate_enabled),
            "shadow_exact_target": self.shadow_exact_target,
            "shadow_fee_bps": int(self.shadow_fee_bps),
            "shadow_slippage_bps": int(self.shadow_slippage_bps),
            "wallet_address": self.wallet_address,
            "wallet_id": self.wallet_id,
            "account_type": self.account_type,
            "funding_source": self.funding_source,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "order_unit_usdt": str(self.order_unit_usdt),
            "slippage_bps": int(self.slippage_bps),
            "recv_window": self.recv_window,
            "poll_interval_seconds": float(self.poll_interval_seconds),
            "loop_limit": int(self.loop_limit),
            "max_loop_limit": int(self.max_loop_limit),
            "required_balance_usdt": str(self.required_balance_usdt),
            "minimum_shadow_ticks": int(self.minimum_shadow_ticks),
            "live_enabled": bool(self.live_enabled),
            "live_skip_shadow_promotion": bool(self.live_skip_shadow_promotion),
            "require_sas": bool(self.require_sas),
            "sas_verified": bool(self.sas_verified),
            "require_permission": bool(self.require_permission),
            "discovery_limit": int(self.discovery_limit),
            "discovery_tolerance_seconds": int(self.discovery_tolerance_seconds),
            "shadow_window_start_ms": self.shadow_window_start_ms,
            "shadow_window_end_ms": self.shadow_window_end_ms,
            "strategy": StrategyConfig.for_profile(self.strategy_profile).provenance_payload,
        }

    @property
    def config_hash(self) -> str:
        canonical = json.dumps(self.config_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "PredictionSettings":
        """Load non-secret operational values from an environment mapping.

        API credentials are intentionally not read here.  This method is safe
        to use in status/reporting code without accidentally copying secrets
        into a settings object or logs.
        """

        env = environ if environ is not None else os.environ

        def get(name: str, default: str | None = None) -> str | None:
            value = env.get(name, default)
            return value.strip() if isinstance(value, str) else value

        def int_value(name: str, default: int) -> int:
            raw = get(name)
            return default if raw in (None, "") else int(raw)

        def float_value(name: str, default: float) -> float:
            raw = get(name)
            return default if raw in (None, "") else float(raw)

        def bool_value(name: str, default: bool) -> bool:
            raw = get(name)
            if raw in (None, ""):
                return default
            return raw.lower() in {"1", "true", "yes", "on"}

        wallet_address = get("PREDICTION_WALLET_ADDRESS")
        wallet_id = get("PREDICTION_WALLET_ID")
        order_unit = _decimal(get("PREDICTION_ORDER_UNIT_USDT"), default=Decimal("1"))
        required_balance = get("PREDICTION_REQUIRED_BALANCE_USDT")
        return cls(
            mode=RuntimeMode.parse(get("PREDICTION_MODE", RuntimeMode.SHADOW.value)),
            market_symbol=get("PREDICTION_MARKET_SYMBOL", "BTCUSDT") or "BTCUSDT",
            market_duration_seconds=int_value("PREDICTION_MARKET_DURATION_SECONDS", 300),
            market_l1_category=get("PREDICTION_MARKET_L1", "crypto"),
            market_l2_category=get("PREDICTION_MARKET_L2", "up-down"),
            market_vendor=get("PREDICTION_MARKET_VENDOR", "predict_fun") or "predict_fun",
            chain_id=get("PREDICTION_CHAIN_ID", "56") or "56",
            strategy_profile=get("PREDICTION_STRATEGY_PROFILE", DEFAULT_STRATEGY_PROFILE) or DEFAULT_STRATEGY_PROFILE,
            shadow_lanes=tuple(item.strip().lower() for item in (get("PREDICTION_SHADOW_LANES", "") or "").split(",") if item.strip()),
            fav_p3_arm=(get("PREDICTION_FAV_P3_ARM", "off") or "off").strip().lower(),
            jev_gate_enabled=bool_value("PREDICTION_JEV_GATE_ENABLED", True),
            jev_gate_runtime_dir=get("PREDICTION_JEV_GATE_RUNTIME_DIR", "/home/jack_shih/cry3/jev_shadow_lane/runtime") or "/home/jack_shih/cry3/jev_shadow_lane/runtime",
            jev_gate_max_age_ms=int_value("PREDICTION_JEV_GATE_MAX_AGE_MS", 20000),
            jev_gate_fail_open=bool_value("PREDICTION_JEV_GATE_FAIL_OPEN", True),
            shadow_exact_target=(int_value("PREDICTION_SHADOW_EXACT_TARGET", 0) or None),
            shadow_fee_bps=int_value("PREDICTION_SHADOW_FEE_BPS", 10),
            shadow_slippage_bps=int_value("PREDICTION_SHADOW_SLIPPAGE_BPS", 0),
            wallet_address=wallet_address or None,
            wallet_id=wallet_id or None,
            account_type="SPOT",
            funding_source=get("PREDICTION_FUNDING_SOURCE", "MPC") or "MPC",
            order_type="LIMIT",
            time_in_force="GTC",
            slippage_bps=int_value("PREDICTION_SLIPPAGE_BPS", 100),
            recv_window=int_value("PREDICTION_RECV_WINDOW", 5_000),
            order_unit_usdt=order_unit,
            poll_interval_seconds=float_value("PREDICTION_POLL_SECONDS", 1.0),
            loop_limit=int_value("PREDICTION_LOOP_LIMIT", 10),
            max_loop_limit=int_value("PREDICTION_MAX_LOOP_LIMIT", 200),
            required_balance_usdt=_decimal(required_balance, default=order_unit * Decimal("2")),
            minimum_shadow_ticks=int_value("PREDICTION_MIN_SHADOW_TICKS", 1),
            live_enabled=bool_value("PREDICTION_LIVE_ENABLED", False),
            live_skip_shadow_promotion=bool_value("PREDICTION_LIVE_SKIP_SHADOW_PROMOTION", False),
            require_sas=bool_value("PREDICTION_REQUIRE_SAS", True),
            sas_verified=bool_value("PREDICTION_SAS_VERIFIED", False),
            require_permission=bool_value("PREDICTION_REQUIRE_PERMISSION", True),
            discovery_limit=int_value("PREDICTION_DISCOVERY_LIMIT", 50),
            discovery_tolerance_seconds=int_value("PREDICTION_DISCOVERY_TOLERANCE_SECONDS", 2),
            shadow_window_start_ms=(int_value("PREDICTION_SHADOW_WINDOW_START_MS", 0) or None),
            shadow_window_end_ms=(int_value("PREDICTION_SHADOW_WINDOW_END_MS", 0) or None),
            sas_token=get("PREDICTION_SAS_TOKEN"),
            permission_granted=bool_value("PREDICTION_PERMISSION_GRANTED", False),
        )


__all__ = ["PredictionSettings", "RuntimeMode", "normalize_loop_limit"]
