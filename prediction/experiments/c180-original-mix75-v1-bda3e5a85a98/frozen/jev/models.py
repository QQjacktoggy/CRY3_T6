from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True, frozen=True)
class Trade:
    ts: float
    price: float
    qty: float
    taker_buy: bool


@dataclass(slots=True)
class Quote:
    ts: float = 0.0
    bid: float = 0.0
    bid_qty: float = 0.0
    ask: float = 0.0
    ask_qty: float = 0.0


@dataclass(slots=True)
class MarkState:
    ts: float = 0.0
    mark_price: float = 0.0
    index_price: float = 0.0
    funding_rate: float = 0.0
    next_funding_time: float = 0.0


@dataclass(slots=True)
class KlineState:
    ts: float = 0.0
    start_time: float = 0.0
    close_time: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    quote_volume: float = 0.0
    taker_buy_volume: float = 0.0
    trade_count: int = 0
    closed: bool = False


@dataclass(slots=True)
class SourceHealth:
    connected: bool = False
    last_message_at: float = 0.0
    reconnects: int = 0
    last_error: str | None = None


@dataclass(slots=True)
class JevPrediction:
    symbol: str
    observed_at: float
    requested_at: float
    completed_at: float
    latency_ms: int
    model: str
    provider: str | None
    response_id: str | None

    p_up: float
    p_down: float
    continuation_prob: float | None
    reversal_prob: float | None
    regime: str | None
    regime_confidence: float | None

    input_tokens: int | None
    output_tokens: int | None
    cost: float | None
    state: dict[str, Any]
    raw_response: dict[str, Any]
    db_id: int | None = None
    experiment_id: str | None = None
    market_id: str | None = None

    def age_ms(self, now: float) -> int:
        return max(0, int((now - self.completed_at) * 1000))

    @property
    def direction(self) -> str:
        return "UP" if self.p_up >= self.p_down else "DOWN"

    @property
    def direction_confidence(self) -> float:
        return max(self.p_up, self.p_down)

    def to_public_dict(self, *, now: float | None = None, include_state: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "db_id": self.db_id,
            "experiment_id": self.experiment_id,
            "market_id": self.market_id,
            "symbol": self.symbol,
            "observed_at": self.observed_at,
            "requested_at": self.requested_at,
            "completed_at": self.completed_at,
            "latency_ms": self.latency_ms,
            "model": self.model,
            "provider": self.provider,
            "response_id": self.response_id,
            "direction": self.direction,
            "direction_confidence": self.direction_confidence,
            "p_up": self.p_up,
            "p_down": self.p_down,
            "continuation_prob": self.continuation_prob,
            "reversal_prob": self.reversal_prob,
            "regime": self.regime,
            "regime_confidence": self.regime_confidence,
            "usage": {
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cost": self.cost,
            },
        }
        if now is not None:
            result["age_ms"] = self.age_ms(now)
        if include_state:
            result["state"] = self.state
        return result


@dataclass(slots=True)
class PredictionError:
    symbol: str
    requested_at: float
    completed_at: float
    latency_ms: int
    model: str
    error: str
    state: dict[str, Any]
    response_body: str | None = None
    experiment_id: str | None = None
    market_id: str | None = None


@dataclass(slots=True)
class StrategyContext:
    symbol: str
    received_at: float
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "received_at": self.received_at, "payload": self.payload}


@dataclass(slots=True)
class ExperimentRecord:
    experiment_id: str
    created_at: float
    started_at: float | None
    completed_at: float | None
    status: str
    target_runs_per_symbol: int
    model: str
    sampling_far_sec: float
    sampling_mid_sec: float
    sampling_near_sec: float
    btc_valid_runs: int = 0
    eth_valid_runs: int = 0
    config_json: str | None = None


@dataclass(slots=True)
class ExperimentMarketRecord:
    id: int | None
    experiment_id: str
    symbol: str
    market_id: str
    start_ts: float
    end_ts: float
    reference_price: float | None
    settlement_price: float | None
    actual_direction: str | None
    prediction_count: int = 0
    status: str = "OPEN"
    settled_at: float | None = None
    details_json: str | None = None
