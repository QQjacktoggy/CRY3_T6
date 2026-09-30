"""Small, fail-closed control plane for the Prediction canary.

This module is intentionally narrower than the eventual market worker.  It
owns the pieces that must be safe before an order executor is introduced:
finite loop control, an explicit shadow-to-live evidence gate, and the
Telegram-facing manager protocol.  Network clients, market ticks, and order
placement are injected callbacks and are deliberately out of scope here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
import inspect
from typing import Any, Callable, Mapping, Protocol, Sequence


def _decimal(value: Any, *, percent: bool = False) -> Decimal:
    raw = str(value).strip()
    if percent and raw.endswith("%"):
        raw = raw[:-1].strip()
    try:
        result = Decimal(raw)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid promotion evidence value: {value!r}") from exc
    if percent and result > 1:
        result /= Decimal("100")
    return result


@dataclass(frozen=True)
class PromotionEvidence:
    """Measured shadow evidence required before live promotion."""

    shadow_samples: int = 0
    coverage: Decimal | float | str = Decimal("0")
    settlement_rate: Decimal | float | str = Decimal("0")
    after_fee_pnl: Decimal | float | str = Decimal("0")
    fill_rate: Decimal | float | str = Decimal("0")
    invariant_violations: int | Sequence[Any] = 0

    @property
    def coverage_ratio(self) -> Decimal:
        return _decimal(self.coverage, percent=True)

    @property
    def settlement_ratio(self) -> Decimal:
        return _decimal(self.settlement_rate, percent=True)

    @property
    def fill_ratio(self) -> Decimal:
        return _decimal(self.fill_rate, percent=True)

    @property
    def after_fee(self) -> Decimal:
        return _decimal(self.after_fee_pnl)

    @property
    def no_invariant_violations(self) -> bool:
        if isinstance(self.invariant_violations, Sequence) and not isinstance(self.invariant_violations, (str, bytes)):
            return len(self.invariant_violations) == 0
        return int(self.invariant_violations) == 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "PromotionEvidence":
        """Accept the common report spellings used by review tooling."""

        return cls(
            shadow_samples=int(payload.get("shadow_samples", payload.get("samples", 0))),
            coverage=payload.get("coverage", payload.get("coverage_ratio", 0)),
            settlement_rate=payload.get(
                "settlement_rate", payload.get("settlement_coverage", payload.get("settlement", 0))
            ),
            after_fee_pnl=payload.get("after_fee_pnl", payload.get("after_fee", payload.get("net_after_fee", 0))),
            fill_rate=payload.get("fill_rate", payload.get("fill_ratio", 0)),
            invariant_violations=payload.get("invariant_violations", payload.get("invariants", 0)),
        )


@dataclass(frozen=True)
class PromotionDecision:
    eligible: bool
    reasons: tuple[str, ...] = ()
    evidence: PromotionEvidence | None = None

    def __bool__(self) -> bool:
        return self.eligible

    @property
    def passed(self) -> bool:
        return self.eligible


class PromotionGate:
    """Review gate: every threshold must pass; no bypass/force flag exists."""

    def __init__(
        self,
        *,
        min_shadow_samples: int = 100,
        min_coverage: Decimal | float | str = Decimal("0.99"),
        min_settlement_rate: Decimal | float | str = Decimal("1"),
        min_after_fee_pnl: Decimal | float | str = Decimal("0"),
        min_fill_rate: Decimal | float | str = Decimal("0.80"),
    ) -> None:
        self.min_shadow_samples = int(min_shadow_samples)
        self.min_coverage = _decimal(min_coverage, percent=True)
        self.min_settlement_rate = _decimal(min_settlement_rate, percent=True)
        self.min_after_fee_pnl = _decimal(min_after_fee_pnl)
        self.min_fill_rate = _decimal(min_fill_rate, percent=True)

    def evaluate(self, evidence: PromotionEvidence | Mapping[str, Any]) -> PromotionDecision:
        item = evidence if isinstance(evidence, PromotionEvidence) else PromotionEvidence.from_mapping(evidence)
        reasons: list[str] = []
        if item.shadow_samples < self.min_shadow_samples:
            reasons.append(f"shadow samples {item.shadow_samples} < {self.min_shadow_samples}")
        if item.coverage_ratio < self.min_coverage:
            reasons.append(f"coverage {item.coverage_ratio} < {self.min_coverage}")
        if item.settlement_ratio < self.min_settlement_rate:
            reasons.append(f"settlement rate {item.settlement_ratio} < {self.min_settlement_rate}")
        if item.after_fee <= self.min_after_fee_pnl:
            reasons.append(f"after-fee PnL {item.after_fee} is not positive")
        if item.fill_ratio < self.min_fill_rate:
            reasons.append(f"fill rate {item.fill_ratio} < {self.min_fill_rate}")
        if not item.no_invariant_violations:
            reasons.append("invariant violations are present")
        return PromotionDecision(not reasons, tuple(reasons), item)

    check = evaluate

    def can_promote(self, evidence: PromotionEvidence | Mapping[str, Any]) -> bool:
        return self.evaluate(evidence).eligible


class LoopPhase(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    HARD_STOP = "hard_stop"


@dataclass
class LoopState:
    """Finite loop counter with explicit terminal and pause transitions."""

    default_iterations: int = 10
    max_iterations: int = 50
    phase: LoopPhase = LoopPhase.IDLE
    target: int = 0
    completed: int = 0

    def __post_init__(self) -> None:
        self.max_iterations = int(self.max_iterations)
        self.default_iterations = int(self.default_iterations)
        if not 1 <= self.max_iterations <= 50:
            raise ValueError("max_iterations must be between 1 and 50")
        if not 1 <= self.default_iterations <= self.max_iterations:
            raise ValueError("default_iterations must be within the loop bounds")

    @staticmethod
    def _validate(value: int, maximum: int) -> int:
        selected = int(value)
        if not 1 <= selected <= maximum:
            raise ValueError(f"loop iterations must be between 1 and {maximum}")
        return selected

    def start(self, iterations: int | None = None) -> int:
        if self.phase is LoopPhase.HARD_STOP:
            raise RuntimeError("hard stop is latched")
        self.target = self._validate(self.default_iterations if iterations is None else iterations, self.max_iterations)
        self.completed = 0
        self.phase = LoopPhase.RUNNING
        return self.target

    def complete_one(self) -> int:
        if self.phase is not LoopPhase.RUNNING:
            return self.completed
        self.completed = min(self.target, self.completed + 1)
        if self.completed >= self.target:
            self.phase = LoopPhase.COMPLETED
        return self.completed

    advance = complete_one
    complete = complete_one

    def stop(self) -> None:
        if self.phase is not LoopPhase.HARD_STOP:
            self.phase = LoopPhase.STOPPED

    def pause(self) -> None:
        if self.phase is LoopPhase.RUNNING:
            self.phase = LoopPhase.PAUSED

    def resume(self) -> bool:
        if self.phase is LoopPhase.PAUSED:
            self.phase = LoopPhase.RUNNING
            return True
        return self.phase is LoopPhase.RUNNING

    def hard_stop(self) -> None:
        self.phase = LoopPhase.HARD_STOP

    @property
    def running(self) -> bool:
        return self.phase is LoopPhase.RUNNING

    @property
    def paused(self) -> bool:
        return self.phase is LoopPhase.PAUSED

    @property
    def done(self) -> bool:
        return self.phase in {LoopPhase.STOPPED, LoopPhase.COMPLETED, LoopPhase.HARD_STOP}

    @property
    def remaining(self) -> int:
        return max(0, self.target - self.completed)


class ManagerCallbacks(Protocol):
    """Optional side-effect boundary used by Telegram/VM adapters."""

    def risk(self, manager: "PredictionRuntimeManager") -> Any: ...

    def reconcile(self, manager: "PredictionRuntimeManager") -> Any: ...

    def shadow_request(self, manager: "PredictionRuntimeManager") -> Any: ...

    def shadow_confirm(self, manager: "PredictionRuntimeManager", decision: PromotionDecision) -> Any: ...


@dataclass
class PredictionRuntimeManager:
    """Telegram protocol manager with no market or order side effects."""

    callbacks: Any = None
    promotion_gate: PromotionGate = field(default_factory=PromotionGate)
    loop: LoopState = field(default_factory=LoopState)
    mode: str = "shadow"
    hard_stop_latched: bool = False
    shadow_requested: bool = False
    promotion_decision: PromotionDecision | None = None
    last_risk: Any = None
    last_reconcile: Any = None
    last_error: str | None = None

    def _callback(self, name: str, *args: Any) -> Any:
        callback = None
        if isinstance(self.callbacks, Mapping):
            callback = self.callbacks.get(name)
        elif self.callbacks is not None:
            callback = getattr(self.callbacks, name, None)
        if callback is None:
            return None
        if not callable(callback):
            return callback
        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            return callback(*args)
        parameters = list(signature.parameters.values())
        if not parameters:
            return callback()
        if any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in parameters):
            return callback(*args)
        return callback(*args[: len([p for p in parameters if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)])])

    def status(self) -> dict[str, Any]:
        return {
            "state": self.loop.phase.value,
            "mode": self.mode,
            "running": self.loop.running,
            "paused": self.loop.paused,
            "loop_limit": self.loop.target,
            "completed": self.loop.completed,
            "remaining": self.loop.remaining,
            "hard_stop_latched": self.hard_stop_latched,
            "shadow_requested": self.shadow_requested,
            "promotion_eligible": bool(self.promotion_decision and self.promotion_decision.eligible),
            "last_error": self.last_error,
        }

    get_status = status

    def start(self, iterations: int = 10) -> dict[str, Any]:
        if self.hard_stop_latched:
            self.last_error = "hard stop is latched; start denied"
            return self.status()
        try:
            self._callback("start", self, int(iterations))
            self.loop.start(iterations)
            self.last_error = None
        except Exception as exc:  # noqa: BLE001 - control plane fails closed
            self.last_error = str(exc)
            self.hard_stop("start callback failed")
        return self.status()

    def stop(self) -> dict[str, Any]:
        try:
            self._callback("stop", self)
        finally:
            self.loop.stop()
        return self.status()

    def pause(self) -> dict[str, Any]:
        if not self.hard_stop_latched:
            self._callback("pause", self)
            self.loop.pause()
        return self.status()

    def resume(self) -> dict[str, Any]:
        if self.hard_stop_latched:
            self.last_error = "hard stop is latched; resume denied"
            return self.status()
        try:
            self._callback("resume", self)
            self.loop.resume()
        except Exception as exc:  # noqa: BLE001
            self.last_error = str(exc)
            self.hard_stop("resume callback failed")
        return self.status()

    def complete_one(self) -> dict[str, Any]:
        self.loop.complete_one()
        return self.status()

    advance = complete_one

    def hard_stop(self, reason: str = "manual hard stop") -> dict[str, Any]:
        self.hard_stop_latched = True
        self.last_error = reason
        self.loop.hard_stop()
        try:
            self._callback("hard_stop", self, reason)
        except Exception:
            pass
        return self.status()

    def risk(self) -> Any:
        try:
            self.last_risk = self._callback("risk", self)
            return self.last_risk
        except Exception as exc:  # noqa: BLE001
            self.hard_stop(f"risk callback failed: {exc}")
            return {"ok": False, "error": str(exc), "hard_stop_latched": True}

    def reconcile(self) -> Any:
        try:
            self.last_reconcile = self._callback("reconcile", self)
            return self.last_reconcile
        except Exception as exc:  # noqa: BLE001
            self.hard_stop(f"reconcile callback failed: {exc}")
            return {"ok": False, "error": str(exc), "hard_stop_latched": True}

    def shadow_request(self) -> dict[str, Any]:
        self.shadow_requested = True
        try:
            self._callback("shadow_request", self)
        except Exception as exc:  # noqa: BLE001
            self.last_error = str(exc)
        return self.status()

    def shadow_confirm(self, evidence: PromotionEvidence | Mapping[str, Any]) -> dict[str, Any]:
        decision = self.promotion_gate.evaluate(evidence)
        self.promotion_decision = decision
        if not decision.eligible:
            self.last_error = "; ".join(decision.reasons)
            return self.status()
        try:
            callback_result = self._callback("shadow_confirm", self, decision)
        except Exception as exc:  # noqa: BLE001
            self.last_error = str(exc)
            return self.status()
        if callback_result is False:
            self.last_error = "shadow confirmation callback denied promotion"
            return self.status()
        if not self.hard_stop_latched:
            self.mode = "live"
            self.last_error = None
        return self.status()

    # Telegram command protocol aliases.  They are intentionally plain
    # methods so an adapter can bind them without importing a Telegram SDK.
    predict_status = status
    predict_start = start
    predict_stop = stop
    predict_pause = pause
    predict_resume = resume
    predict_risk = risk
    predict_reconcile = reconcile
    predict_shadow = shadow_request
    predict_shadow_confirm = shadow_confirm


__all__ = [
    "LoopPhase",
    "LoopState",
    "ManagerCallbacks",
    "PredictionRuntimeManager",
    "PromotionDecision",
    "PromotionEvidence",
    "PromotionGate",
]
