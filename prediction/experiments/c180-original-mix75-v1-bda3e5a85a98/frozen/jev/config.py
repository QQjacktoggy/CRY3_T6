from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _as_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _symbols(value: str | None) -> tuple[str, ...]:
    raw = value or "BTCUSDT,ETHUSDT"
    result = tuple(dict.fromkeys(x.strip().upper() for x in raw.split(",") if x.strip()))
    if not result:
        raise ValueError("JEV_SYMBOLS must contain at least one symbol")
    return result


@dataclass(frozen=True, slots=True)
class Settings:
    openrouter_api_key: str
    openrouter_decisions_url: str
    openrouter_model: str
    app_title: str
    app_url: str | None

    symbols: tuple[str, ...]
    futures_ws_base: str
    spot_ws_base: str
    futures_rest_base: str

    db_path: Path
    runtime_dir: Path
    reports_dir: Path
    log_level: str

    http_host: str
    http_port: int

    request_timeout_seconds: float
    min_history_seconds: float
    stale_after_seconds: float
    oi_poll_seconds: float

    interval_far_seconds: float
    interval_mid_seconds: float
    interval_near_seconds: float
    far_boundary_seconds: float
    near_boundary_seconds: float

    reconnect_min_seconds: float
    reconnect_max_seconds: float
    error_backoff_max_seconds: float
    decision_retention_days: int
    context_ttl_seconds: float
    context_refresh_min_seconds: float
    shadow_max_prediction_age_seconds: float
    shadow_min_probability: float
    shadow_min_edge: float
    shadow_max_reversal: float

    mock_jev: bool
    disable_spot: bool
    disable_oi: bool

    experiment_id: str
    target_runs_per_symbol: int

    @classmethod
    def load(cls, env_file: str | Path | None = None) -> "Settings":
        if env_file:
            env_path = Path(env_file).resolve()
            load_dotenv(env_path, override=False)
            default_root = env_path.parent
        else:
            load_dotenv(override=False)
            default_root = Path.cwd()

        env_root = os.getenv("JEV_ROOT", "")
        if env_root in {"", "."}:
            root = default_root.resolve()
        else:
            root = Path(env_root).expanduser().resolve()
        raw_db = Path(os.getenv("JEV_DB_PATH", "data/jev_shadow.sqlite3")).expanduser()
        db_path = raw_db if raw_db.is_absolute() else (root / raw_db)

        raw_runtime = Path(os.getenv("JEV_RUNTIME_DIR", "runtime")).expanduser()
        runtime_dir = raw_runtime if raw_runtime.is_absolute() else (root / raw_runtime)

        raw_reports = Path(os.getenv("JEV_REPORTS_DIR", "reports")).expanduser()
        reports_dir = raw_reports if raw_reports.is_absolute() else (root / raw_reports)

        settings = cls(
            openrouter_api_key=os.getenv("OPENROUTER_API_KEY", "").strip(),
            openrouter_decisions_url=os.getenv(
                "OPENROUTER_DECISIONS_URL", "https://openrouter.ai/api/alpha/decisions"
            ).strip(),
            openrouter_model=os.getenv("JEV_MODEL", "typesafe/jev-1.13").strip(),
            app_title=os.getenv("OPENROUTER_APP_TITLE", "CRY3 Jev Predictor Shadow Lane").strip(),
            app_url=os.getenv("OPENROUTER_APP_URL") or None,
            symbols=_symbols(os.getenv("JEV_SYMBOLS")),
            futures_ws_base=os.getenv("BINANCE_FUTURES_WS_BASE", "wss://fstream.binance.com/ws").rstrip("/"),
            spot_ws_base=os.getenv("BINANCE_SPOT_WS_BASE", "wss://stream.binance.com:9443/ws").rstrip("/"),
            futures_rest_base=os.getenv("BINANCE_FUTURES_REST_BASE", "https://fapi.binance.com").rstrip("/"),
            db_path=db_path.resolve(),
            runtime_dir=runtime_dir.resolve(),
            reports_dir=reports_dir.resolve(),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            http_host=os.getenv("JEV_HTTP_HOST", "127.0.0.1"),
            http_port=_as_int("JEV_HTTP_PORT", 8787),
            request_timeout_seconds=_as_float("JEV_REQUEST_TIMEOUT_SECONDS", 3.0),
            min_history_seconds=_as_float("JEV_MIN_HISTORY_SECONDS", 15.0),
            stale_after_seconds=_as_float("JEV_STALE_AFTER_SECONDS", 3.0),
            oi_poll_seconds=_as_float("JEV_OI_POLL_SECONDS", 5.0),
            interval_far_seconds=_as_float("JEV_INTERVAL_FAR_SECONDS", 15.0),
            interval_mid_seconds=_as_float("JEV_INTERVAL_MID_SECONDS", 5.0),
            interval_near_seconds=_as_float("JEV_INTERVAL_NEAR_SECONDS", 2.0),
            far_boundary_seconds=_as_float("JEV_FAR_BOUNDARY_SECONDS", 120.0),
            near_boundary_seconds=_as_float("JEV_NEAR_BOUNDARY_SECONDS", 30.0),
            reconnect_min_seconds=_as_float("JEV_RECONNECT_MIN_SECONDS", 1.0),
            reconnect_max_seconds=_as_float("JEV_RECONNECT_MAX_SECONDS", 30.0),
            error_backoff_max_seconds=_as_float("JEV_ERROR_BACKOFF_MAX_SECONDS", 30.0),
            decision_retention_days=_as_int("JEV_PREDICTION_RETENTION_DAYS", _as_int("JEV_DECISION_RETENTION_DAYS", 30)),
            context_ttl_seconds=_as_float("JEV_CONTEXT_TTL_SECONDS", 10.0),
            context_refresh_min_seconds=_as_float("JEV_CONTEXT_REFRESH_MIN_SECONDS", 0.75),
            shadow_max_prediction_age_seconds=_as_float(
                "JEV_SHADOW_MAX_PREDICTION_AGE_SECONDS",
                _as_float("JEV_SHADOW_MAX_DECISION_AGE_SECONDS", 5.0),
            ),
            shadow_min_probability=_as_float("JEV_SHADOW_MIN_PROBABILITY", 0.55),
            shadow_min_edge=_as_float("JEV_SHADOW_MIN_EDGE", 0.05),
            shadow_max_reversal=_as_float("JEV_SHADOW_MAX_REVERSAL", 0.70),
            mock_jev=_as_bool(os.getenv("JEV_MOCK"), False),
            disable_spot=_as_bool(os.getenv("JEV_DISABLE_SPOT"), False),
            disable_oi=_as_bool(os.getenv("JEV_DISABLE_OI"), False),
            experiment_id=os.getenv("JEV_EXPERIMENT_ID", "jev_btc_eth_200_v1").strip(),
            target_runs_per_symbol=_as_int("JEV_TARGET_RUNS_PER_SYMBOL", 200),
        )
        settings.validate()
        settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        settings.reports_dir.mkdir(parents=True, exist_ok=True)
        return settings

    def validate(self) -> None:
        if not self.mock_jev and not self.openrouter_api_key:
            raise ValueError("OPENROUTER_API_KEY is required unless JEV_MOCK=1")
        if self.request_timeout_seconds <= 0:
            raise ValueError("JEV_REQUEST_TIMEOUT_SECONDS must be positive")
        if not (0 < self.interval_near_seconds <= self.interval_mid_seconds <= self.interval_far_seconds):
            raise ValueError("Expected near interval <= mid interval <= far interval")
        if not (0 < self.near_boundary_seconds < self.far_boundary_seconds < 300):
            raise ValueError("Expected 0 < near boundary < far boundary < 300")
        if self.stale_after_seconds <= 0:
            raise ValueError("JEV_STALE_AFTER_SECONDS must be positive")
        if not (1 <= self.http_port <= 65535):
            raise ValueError("JEV_HTTP_PORT is invalid")
        for name, value in (
            ("JEV_SHADOW_MIN_PROBABILITY", self.shadow_min_probability),
            ("JEV_SHADOW_MIN_EDGE", self.shadow_min_edge),
            ("JEV_SHADOW_MAX_REVERSAL", self.shadow_max_reversal),
        ):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.context_ttl_seconds <= 0 or self.context_refresh_min_seconds <= 0:
            raise ValueError("Context timing settings must be positive")
        if self.target_runs_per_symbol <= 0:
            raise ValueError("JEV_TARGET_RUNS_PER_SYMBOL must be positive")
        if not self.experiment_id:
            raise ValueError("JEV_EXPERIMENT_ID cannot be empty")

    def cadence_for(self, seconds_to_close: float) -> float:
        if seconds_to_close <= self.near_boundary_seconds:
            return self.interval_near_seconds
        if seconds_to_close <= self.far_boundary_seconds:
            return self.interval_mid_seconds
        return self.interval_far_seconds

    def latest_path(self, symbol: str) -> Path:
        return self.runtime_dir / f"latest_{symbol.upper()}.json"

    def all_latest_paths(self) -> Iterable[Path]:
        return (self.latest_path(symbol) for symbol in self.symbols)
