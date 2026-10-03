"""Fail-closed control plane for the isolated Prediction worker.

The worker owns market execution.  This adapter owns the authority boundary:
live capability is established from official Binance responses, and a live
switch is impossible without a fresh preflight followed by an independently
validated shadow promotion record.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal, InvalidOperation
import inspect
import os
from pathlib import Path
import subprocess
import time
from typing import Any, Callable, Mapping, Sequence

from .worker import PredictionWorker
from .client import available_balance_display, payment_option_balance_summary
from .runtime import PromotionEvidence, PromotionGate


PROMOTION_EVIDENCE_MAX_AGE_MS = 86_400_000
PROMOTION_EVIDENCE_FUTURE_SKEW_MS = 5 * 60_000
MIN_UNIQUE_SHADOW_COUNTERFACTUALS = 100


def _as_decimal(value: Any, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} is not numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{name} is not finite")
    return result


def _ratio(value: Any, *, name: str) -> Decimal:
    raw = str(value).strip()
    if raw.endswith("%"):
        raw = raw[:-1].strip()
        result = _as_decimal(raw, name=name) / Decimal("100")
    else:
        result = _as_decimal(raw, name=name)
        if result > 1:
            result /= Decimal("100")
    return result


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    return None


def _count_value(value: Any, *, name: str) -> int:
    """Normalize an integer metric or an explicit violation list."""

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return len(value)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} is invalid") from exc


def _official_wallets(payload: Any) -> list[Mapping[str, Any]] | None:
    """Read only Binance's documented ``wallets[]`` collection.

    ``items`` and an arbitrary ``data`` list are intentionally not accepted:
    they are different response contracts and must not silently become wallet
    authority.
    """

    data: Any = payload
    if isinstance(payload, Mapping) and "data" in payload:
        data = payload["data"]
    wallets: Any = None
    if isinstance(data, Mapping):
        wallets = data.get("wallets")
    if wallets is None and isinstance(payload, Mapping):
        wallets = payload.get("wallets")
    if not isinstance(wallets, list):
        return None
    return [item for item in wallets if isinstance(item, Mapping)]


def _wallet_address(item: Mapping[str, Any]) -> str:
    value = item.get("walletAddress") if "walletAddress" in item else item.get("address")
    return str(value or "").strip()


def _wallet_id(item: Mapping[str, Any]) -> str:
    value = item.get("walletId") if "walletId" in item else item.get("id")
    return str(value or "").strip()


def _capability_ok(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, Mapping):
        explicit = _first(value, "passed", "eligible", "authorized", "allowed", "ok", "enabled")
        if explicit is None:
            return False
        return bool(explicit)
    return bool(value)


def _authenticated_capability_ok(value: Any) -> bool:
    """Accept only explicit auth/permission evidence, never wallet reads."""

    if isinstance(value, bool):
        return value
    if not isinstance(value, Mapping):
        return False
    explicit = _first(value, "authenticated", "authorized", "valid", "passed", "allowed", "ok")
    if isinstance(explicit, str):
        return explicit.strip().lower() in {"1", "true", "yes", "authorized", "authenticated", "valid"}
    return explicit is True


def _call_shape(checker: Callable[..., Any], *, kind: str, settings: Any) -> Any:
    """Call a real client capability without hiding a checker TypeError."""

    if kind == "SAS":
        candidates = (
            {"token": getattr(settings, "sas_token", None)},
            {"sas_token": getattr(settings, "sas_token", None)},
            {},
        )
    else:
        candidates = (
            {
                "wallet_address": getattr(settings, "wallet_address", None),
                "wallet_id": getattr(settings, "wallet_id", None),
            },
            {"walletAddress": getattr(settings, "wallet_address", None), "walletId": getattr(settings, "wallet_id", None)},
            {},
        )
    try:
        signature = inspect.signature(checker)
    except (TypeError, ValueError):
        return checker(**candidates[0])
    for kwargs in candidates:
        try:
            signature.bind(**kwargs)
        except TypeError:
            continue
        return checker(**kwargs)
    raise TypeError(f"official {kind} capability signature is unsupported")


def _call_authoritative_preflight(checker: Callable[..., Any], *, settings: Any) -> Any:
    """Bind the client-owned read-only preflight without assuming a shape."""

    candidates = (
        {
            "wallet_address": getattr(settings, "wallet_address", None),
            "wallet_id": getattr(settings, "wallet_id", None),
            "required_balance_usdt": getattr(settings, "required_balance_usdt", "0"),
            "account_type": getattr(settings, "account_type", "SPOT"),
            "funding_source": getattr(settings, "funding_source", None),
            "recv_window": getattr(settings, "recv_window", None),
        },
        {
            "wallet_address": getattr(settings, "wallet_address", None),
            "wallet_id": getattr(settings, "wallet_id", None),
            "required_balance_usdt": getattr(settings, "required_balance_usdt", "0"),
            "recv_window": getattr(settings, "recv_window", None),
        },
        {},
    )
    try:
        signature = inspect.signature(checker)
    except (TypeError, ValueError):
        return checker(**candidates[0])
    for kwargs in candidates:
        try:
            signature.bind(**kwargs)
        except TypeError:
            continue
        return checker(**kwargs)
    raise TypeError("authoritative client preflight signature is unsupported")


class PredictionController:
    """Stable async control protocol over a worker with safe thread bridging."""

    def __init__(self, worker: PredictionWorker) -> None:
        self.worker = worker
        self._preflight_result: dict[str, Any] = {
            "checked": False,
            "passed": False,
            "mode": "SHADOW",
            "reasons": ["not checked"],
        }

    @property
    def hard_stop_latched(self) -> bool:
        return bool(getattr(self.worker, "hard_stop_latched", False))

    @property
    def worker_available(self) -> bool:
        return bool(getattr(self.worker, "worker_available", False))

    @property
    def orders_enabled(self) -> bool:
        return bool(getattr(self.worker, "orders_enabled", False))

    @property
    def fail_closed(self) -> bool:
        return bool(getattr(self.worker, "fail_closed", False))

    async def _invoke(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        method = getattr(self.worker, method_name)
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        return await asyncio.to_thread(method, *args, **kwargs)

    async def _persist(self, key: str, value: Any) -> bool:
        setter = getattr(self.worker.repository, "set_runtime_config", None)
        if not callable(setter):
            return False
        try:
            result = setter(key, value)
            if inspect.isawaitable(result):
                await result
            return True
        except Exception:
            return False

    async def status(self) -> Any:
        result = await self._invoke("status")
        if isinstance(result, dict):
            result = {**result, "preflight": dict(self._preflight_result)}
        return result

    async def _prepare_live_start(self) -> dict[str, Any] | None:
        """Validate Live without promoting it as a side effect of Start.

        The explicit Telegram ``/predict_live`` confirmation is the only
        authority transition.  Loop and One Run may use an already armed
        Live worker, or run in requested Shadow mode, but they cannot turn a
        failed/未 armed Live request into Live implicitly.
        """

        # Apply an idle, queued lane before checking Live authority.  Changing
        # the strategy changes the effective config hash and deliberately
        # disarms Live, so an authorization for the old lane cannot leak into
        # the next loop.
        activate_pending = getattr(self.worker, "_activate_pending_strategy_if_idle", None)
        if callable(activate_pending):
            activated = activate_pending()
            if inspect.isawaitable(activated):
                await activated
        activate_pending_amount = getattr(self.worker, "_activate_pending_order_unit_if_idle", None)
        if callable(activate_pending_amount):
            activated_amount = activate_pending_amount()
            if inspect.isawaitable(activated_amount):
                await activated_amount

        live_requested = bool(getattr(self.worker.settings, "is_live_requested", False))
        if not live_requested:
            return None
        preflight = await self.preflight(require_live=True)
        if not preflight.get("passed"):
            return {
                "action_denied": True,
                "reason": "live preflight failed",
                "preflight": preflight,
            }
        if not bool(getattr(self.worker, "live_capability", False)):
            return {
                "action_denied": True,
                "reason": "live is not armed; confirm /predict_live first",
                "preflight": preflight,
            }
        return None

    async def wallet_balances(self) -> dict[str, Any]:
        """Read all official payment-option balances for the status screen."""

        client = getattr(self.worker, "client", None)
        method = getattr(client, "query_payment_option_balances", None)
        if not callable(method):
            return {"wallet_balances": [], "wallet_balances_error": "balance API unavailable"}
        try:
            payload = await asyncio.to_thread(method)
            return {
                "wallet_balances": payment_option_balance_summary(payload),
                "balance_account_type": "CeDeFi" if str(getattr(self.worker.settings, "funding_source", "MPC")).upper() == "MPC" else str(getattr(self.worker.settings, "account_type", "SPOT")),
            }
        except Exception as exc:  # noqa: BLE001 - status must remain readable
            return {"wallet_balances": [], "wallet_balances_error": type(exc).__name__}

    async def select_market(self, symbol: str) -> Any:
        return await self._invoke("select_market", symbol)

    async def select_strategy(self, profile: str) -> Any:
        """Select a strategy only while no market loop is active."""

        return await self._invoke("select_strategy", profile)

    async def set_fav_p3_arm(self, arm: str) -> Any:
        """Arm/disarm independent FAV_P3 Live lane (does not change Baseline profile)."""

        return await self._invoke("set_fav_p3_arm", arm)

    async def select_order_unit(self, value: Decimal | str | int) -> Any:
        """Select the reviewed 1/2-USDT unit at a safe loop boundary."""

        return await self._invoke("select_order_unit", value)

    async def start_loop(self, count: int = 10) -> Any:
        blocked = await self._prepare_live_start()
        if blocked is not None:
            return {**(await self.status()), **blocked}
        return await self._invoke("start_loop", count)

    async def resume_existing_loop(self, loop_id: str, target: int) -> Any:
        """Explicit operator recovery; never create or extend a loop."""
        blocked = await self._prepare_live_start()
        if blocked is not None:
            return blocked
        return await self._invoke('start_loop', int(target), expected_loop_id=str(loop_id))

    async def one_run(self) -> Any:
        """Start one guarded market cycle through the worker."""

        blocked = await self._prepare_live_start()
        if blocked is not None:
            return {**(await self.status()), **blocked}
        return await self._invoke("one_run")

    async def loop_pnl(self) -> Any:
        """Read exact loop PnL totals without changing runtime state."""

        getter = getattr(self.worker.repository, "get_loop_pnl_summary", None)
        if callable(getter):
            result = getter(limit=20)
            if inspect.isawaitable(result):
                result = await result
        else:
            result = {
                "total_loop_pnl": "0",
                "current_loop_pnl": "0",
                "active_loop_id": None,
                "loop_count": 0,
                "loops": [],
            }
        if isinstance(result, dict):
            pending: Mapping[str, Any] = {}
            pending_getter = getattr(self.worker, "pending_pnl_status", None)
            if callable(pending_getter):
                candidate = pending_getter(result.get("current_loop_pnl", "0"))
                if inspect.isawaitable(candidate):
                    candidate = await candidate
                if isinstance(candidate, Mapping):
                    pending = candidate
            return {
                **result,
                **pending,
                "mode": getattr(self.worker, "mode", "SHADOW"),
            }
        return result

    async def stop_loop(self) -> Any:
        return await self._invoke("stop_loop")

    async def cancel_loop(self, reason: str = "telegram operator cancelled loop") -> Any:
        return await self._invoke("cancel_loop", reason)

    async def pause(self) -> Any:
        return await self._invoke("pause")

    async def resume(self) -> Any:
        return await self._invoke("resume")

    async def reset_hard_stop_once(self, reason: str = "telegram operator hard-stop reset") -> Any:
        """Run the worker's guarded repeatable reset; never mutate risk here."""

        return await self._invoke("reset_hard_stop_once", reason)

    async def reset_hard_stop(self, reason: str = "telegram operator hard-stop reset") -> Any:
        """Named alias for the guarded repeatable hard-stop reset."""

        return await self._invoke("reset_hard_stop", reason)

    async def risk(self) -> Any:
        return await self._invoke("risk")

    async def reconcile(self) -> Any:
        result = await self._invoke("reconcile")
        if isinstance(result, dict):
            await self._persist("prediction_unresolved_orders", result.get("orders", 0))
        return result

    async def set_shadow_mode(self, enabled: bool) -> Any:
        """Change mode, requiring live preflight and promotion on ``False``."""

        if bool(enabled):
            return await self._invoke("set_shadow_mode", True)
        preflight = await self.preflight(require_live=True)
        if not preflight.get("passed"):
            await self._invoke("set_shadow_mode", True)
            return {"action_denied": True, "reason": "authoritative live preflight failed", "preflight": preflight}
        if getattr(self.worker.settings, "live_skip_shadow_promotion", False):
            promotion = {
                "passed": True,
                "eligible": True,
                "reasons": ("Shadow promotion skipped by explicit live configuration",),
                "evidence": {},
            }
            await self._persist("prediction_promotion_gate", promotion)
        else:
            promotion = await self.promotion_gate(_preflight_result=preflight)
            if not promotion.get("passed"):
                await self._invoke("set_shadow_mode", True)
                return {"action_denied": True, "reason": "promotion gate failed", "promotion": promotion, "preflight": preflight}
        # Pass the exact signed envelope through to the worker as well as
        # persisting it.  The worker remains the final authority and
        # re-validates freshness/config/wallet identity before promotion.
        result = await self._invoke("set_shadow_mode", False, preflight_evidence=preflight)
        if isinstance(result, dict):
            return {**result, "preflight": preflight, "promotion": promotion}
        return result

    async def _promotion_evidence_scope(self) -> dict[str, Any]:
        """Select immutable evidence for the configured strategy lane."""

        settings = self.worker.settings
        profile = str(
            getattr(self.worker, "_selected_strategy_profile", None)
            or getattr(settings, "strategy_profile", "")
            or ""
        ).strip().lower()
        raw_lanes = getattr(settings, "shadow_lanes", ())
        if isinstance(raw_lanes, str):
            raw_lanes = raw_lanes.split(",")
        lanes = {
            str(item).strip().lower()
            for item in (raw_lanes or ())
            if str(item).strip()
        }
        if profile not in lanes:
            return {}

        config_hashes = getattr(self.worker, "_shadow_lane_config_hashes", {})
        windows = getattr(self.worker, "_shadow_lane_windows", {})
        config_hash = str(config_hashes.get(profile) or "") if isinstance(config_hashes, Mapping) else ""
        window = windows.get(profile) if isinstance(windows, Mapping) else None
        if not config_hash or not isinstance(window, Mapping):
            runtime_getter = getattr(self.worker.repository, "get_runtime_config", None)
            if callable(runtime_getter):
                runtime_value = runtime_getter("prediction_shadow_lanes", {})
                if inspect.isawaitable(runtime_value):
                    runtime_value = await runtime_value
                if isinstance(runtime_value, Mapping):
                    lane_value = runtime_value.get(profile)
                    if isinstance(lane_value, Mapping):
                        config_hash = str(lane_value.get("config_hash") or config_hash)
                        window = lane_value
        start = window.get("window_start_ms", window.get("start_ms")) if isinstance(window, Mapping) else None
        end = window.get("window_end_ms", window.get("end_ms")) if isinstance(window, Mapping) else None
        if config_hash and start is not None and end is not None:
            return {
                "config_hash": config_hash,
                "window_start_ms": int(start),
                "window_end_ms": int(end),
            }
        return {}

    async def promotion_gate(self, *, _preflight_result: dict[str, Any] | None = None) -> dict[str, Any]:
        """Evaluate immutable repository evidence and preserve its provenance."""

        settings = self.worker.settings
        preflight: dict[str, Any] | None = _preflight_result
        if preflight is None and getattr(settings, "is_live_requested", False):
            preflight = await self.preflight(require_live=True)

        # The production Prediction unit is explicitly Live-only and has no
        # Shadow collectors/lanes.  Its signed, exact-wallet preflight is the
        # authorization gate; asking that unit for historical Shadow evidence
        # makes Telegram's Live confirmation impossible after every Lane
        # change even though ``set_shadow_mode(False)`` correctly bypasses the
        # same evidence check.  Keep both entry points consistent and still
        # fail closed unless the fresh Live preflight passed.
        if getattr(settings, "live_skip_shadow_promotion", False):
            reasons: list[str] = []
            if not getattr(settings, "is_live_requested", False):
                reasons.append("Live-only promotion bypass requires requested Live mode")
            if preflight is None:
                reasons.append("Live-only promotion bypass requires a fresh preflight")
            elif not preflight.get("passed"):
                reasons.extend(f"live preflight: {reason}" for reason in preflight.get("reasons", ()))
                if not preflight.get("reasons"):
                    reasons.append("live preflight failed")
            result = {
                "passed": not reasons,
                "eligible": not reasons,
                "reasons": (
                    tuple(dict.fromkeys(reasons))
                    if reasons
                    else ("Shadow promotion skipped by explicit live configuration",)
                ),
                "evidence": {},
            }
            if preflight is not None:
                result["preflight"] = preflight
            await self._persist("prediction_promotion_gate", result)
            return result

        getter = getattr(self.worker.repository, "get_shadow_promotion_evidence", None)
        strict_shadow_repo = callable(getattr(self.worker.repository, "get_shadow_invariant_violations", None))
        reasons: list[str] = []
        raw: dict[str, Any] = {}
        evidence_scope = await self._promotion_evidence_scope()
        if not callable(getter):
            reasons.append("repository promotion evidence interface is unavailable")
        else:
            try:
                if evidence_scope:
                    try:
                        inspect.signature(getter).bind(**evidence_scope)
                    except (TypeError, ValueError):
                        value = getter()
                    else:
                        value = getter(**evidence_scope)
                else:
                    value = getter()
                if inspect.isawaitable(value):
                    value = await value
                if not isinstance(value, Mapping):
                    reasons.append("repository promotion evidence is not a mapping")
                else:
                    raw = dict(value)
            except Exception as exc:  # noqa: BLE001
                reasons.append(f"repository promotion evidence unavailable: {exc}")

        now_ms = int(time.time() * 1000)
        evidence_time = _first(
            raw,
            "generated_at_ms",
            "evidence_generated_at_ms",
            "evidence_generated_at",
            "shadow_evidence_generated_at_ms",
            "evidence_time_ms",
        )
        if evidence_time is None:
            reasons.append("repository promotion evidence timestamp is missing")
        else:
            try:
                evidence_time = int(evidence_time)
                if evidence_time > now_ms + PROMOTION_EVIDENCE_FUTURE_SKEW_MS:
                    reasons.append("promotion evidence timestamp is in the future")
                elif now_ms - evidence_time > PROMOTION_EVIDENCE_MAX_AGE_MS:
                    reasons.append("promotion evidence is stale")
            except (TypeError, ValueError):
                reasons.append("promotion evidence timestamp is invalid")

        if strict_shadow_repo:
            if str(raw.get("mode", "")).upper() != "SHADOW":
                reasons.append("repository promotion evidence mode is not SHADOW")
            evidence_start = _first(raw, "window_start_ms", "shadow_window_start_ms")
            evidence_end = _first(raw, "window_end_ms", "shadow_window_end_ms")
            if evidence_start is None or evidence_end is None:
                reasons.append("repository promotion evidence shadow window is missing")
            else:
                try:
                    if int(evidence_start) > int(evidence_end):
                        reasons.append("promotion evidence shadow window is reversed")
                    if evidence_time is not None and int(evidence_end) > int(evidence_time):
                        reasons.append("promotion evidence generated before shadow window ended")
                except (TypeError, ValueError):
                    reasons.append("promotion evidence shadow window is invalid")
            db_path = _first(raw, "database_path", "db_path")
            repo_db = getattr(self.worker.repository, "db_path", None)
            if not db_path or not repo_db or Path(str(db_path)).resolve() != Path(str(repo_db)).resolve():
                reasons.append("repository promotion evidence database path does not match repository")
            if not raw.get("evidence_identity"):
                reasons.append("repository promotion evidence identity is missing")

        commit = _first(raw, "repository_commit", "git_commit", "commit")
        current_commit = self._repository_commit()
        if not commit:
            reasons.append("repository promotion evidence commit is missing")
        if current_commit == "unknown":
            reasons.append("repository commit identity unavailable")
        elif commit and str(commit) != current_commit:
            reasons.append("promotion evidence repository commit does not match current checkout")

        config_hash = _first(raw, "config_hash", "configuration_hash", "config_sha256")
        if not config_hash:
            reasons.append("repository promotion evidence config hash is missing")
        expected_hash = evidence_scope.get("config_hash") or getattr(self.worker, "effective_config_hash", None) or getattr(settings, "config_hash", None)
        if expected_hash and config_hash and str(config_hash) != str(expected_hash):
            reasons.append("promotion evidence config hash does not match current settings")

        unique_value = _first(
            raw,
            "unique_shadow_counterfactual_resolved",
            "unique_shadow_counterfactuals",
            "unique_counterfactual_resolved",
            "shadow_counterfactual_resolved_unique",
            "counterfactual_resolved_unique",
            "shadow_counterfactual_count",
            "counterfactuals_resolved",
            "unique_shadow_counterfactual_resolved_count",
            "resolved_shadow_counterfactuals",
            "counterfactual_resolved",
            "shadow_counterfactual_resolved",
            "resolved_counterfactual_count",
            "counterfactual_resolved_unique_count",
            "unique_settled_markets",
        )
        if unique_value is None:
            reasons.append("repository evidence is missing unique resolved shadow counterfactual count")
        else:
            try:
                unique_count = _count_value(unique_value, name="unique counterfactual count")
                if unique_count < MIN_UNIQUE_SHADOW_COUNTERFACTUALS:
                    reasons.append(f"unique resolved shadow counterfactuals {unique_count} < {MIN_UNIQUE_SHADOW_COUNTERFACTUALS}")
            except (TypeError, ValueError):
                reasons.append("unique resolved shadow counterfactual count is invalid")

        unresolved_value = _first(raw, "unresolved_orders", "unresolved_order_count", "unresolved_orders_count", "db_unresolved_orders")
        unresolved_loader = getattr(self.worker.repository, "load_unresolved_intents", None)
        if callable(unresolved_loader):
            try:
                unresolved = unresolved_loader()
                if inspect.isawaitable(unresolved):
                    unresolved = await unresolved
                unresolved_value = len(unresolved) if isinstance(unresolved, Sequence) else None
            except Exception as exc:  # noqa: BLE001
                reasons.append(f"unresolved order ledger unavailable: {exc}")
        if unresolved_value is None:
            reasons.append("repository evidence is missing unresolved order count")
        else:
            try:
                unresolved_count = _count_value(unresolved_value, name="unresolved order count")
                if unresolved_count != 0:
                    reasons.append(f"unresolved orders {unresolved_count} != 0")
            except (TypeError, ValueError):
                reasons.append("unresolved order count is invalid")

        invariant_value = _first(raw, "real_invariant_violations", "real_invariants", "invariant_violations", "invariants")
        if strict_shadow_repo:
            try:
                invariant_loader = getattr(self.worker.repository, "get_shadow_invariant_violations")
                invariant_result = invariant_loader(
                    config_hash=str(config_hash or ""),
                    window_start_ms=int(_first(raw, "window_start_ms", "shadow_window_start_ms")),
                    window_end_ms=int(_first(raw, "window_end_ms", "shadow_window_end_ms")),
                )
                if inspect.isawaitable(invariant_result):
                    invariant_result = await invariant_result
                if isinstance(invariant_result, Mapping):
                    invariant_value = invariant_result.get("real_invariant_violations", invariant_result.get("invariant_violations"))
                    unresolved_value = int(invariant_result.get("unresolved_intents", 0)) + int(invariant_result.get("unresolved_orders", 0))
            except Exception as exc:  # noqa: BLE001 - evidence cannot be trusted if SQL proof is unavailable
                reasons.append(f"real shadow invariant SQL is unavailable: {exc}")
        if invariant_value is None:
            reasons.append("repository evidence is missing real invariant violation count")
        else:
            try:
                invariant_count = _count_value(invariant_value, name="real invariant violation count")
                if invariant_count != 0:
                    reasons.append(f"real invariant violations {invariant_count} != 0")
            except (TypeError, ValueError):
                reasons.append("real invariant violation count is invalid")

        eval_mapping = dict(raw)
        if "shadow_samples" not in eval_mapping and unique_value is not None:
            eval_mapping["shadow_samples"] = unique_value
        try:
            decision = PromotionGate().evaluate(PromotionEvidence.from_mapping(eval_mapping))
            reasons.extend(decision.reasons)
            coverage = _ratio(_first(raw, "coverage", "coverage_ratio", "coverage_percent", 0), name="coverage")
            settlement = _ratio(_first(raw, "settlement_rate", "settlement_coverage", "settlement_percent", "settlement", 0), name="settlement rate")
            if coverage != Decimal("1"):
                reasons.append("coverage is not exactly 100%")
            if settlement != Decimal("1"):
                reasons.append("settlement coverage is not exactly 100%")
        except (TypeError, ValueError, ArithmeticError) as exc:
            reasons.append(f"invalid promotion evidence: {exc}")

        if preflight is not None and not preflight.get("passed"):
            reasons.extend(f"live preflight: {reason}" for reason in preflight.get("reasons", ()))

        if raw and not await self._persist("promotion_evidence", raw):
            reasons.append("repository cannot persist promotion evidence")
        result = {
            "passed": not reasons,
            "eligible": not reasons,
            "reasons": tuple(dict.fromkeys(str(item) for item in reasons)),
            "evidence": raw,
        }
        if preflight is not None:
            result["preflight"] = preflight
        await self._persist("prediction_promotion_gate", result)
        return result

    def _repository_commit(self) -> str:
        db_path = getattr(self.worker.repository, "db_path", None)
        cwd = Path(os.getcwd())
        source_root = Path(__file__).resolve().parents[3]
        if (source_root / ".git").exists():
            cwd = source_root
        if db_path:
            candidate = Path(str(db_path)).resolve()
            if not (source_root / ".git").exists():
                for parent in (candidate.parent, *candidate.parents):
                    if parent == parent.anchor:
                        continue
                    if (parent / ".git").exists():
                        cwd = parent
                        break
        try:
            return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(cwd), text=True, timeout=3).strip()
        except Exception:
            return "unknown"

    async def health(self) -> Any:
        result = await self._invoke("health")
        if isinstance(result, dict):
            getter = getattr(self.worker.repository, "get_runtime_config", None)
            unresolved = 0
            if callable(getter):
                unresolved = getter("prediction_unresolved_orders", 0)
                if inspect.isawaitable(unresolved):
                    unresolved = await unresolved
            result = {
                **result,
                "market": bool(result.get("feed")),
                "spot": bool(result.get("feed")),
                "preflight": bool(self._preflight_result.get("passed")),
                "unresolved_orders": unresolved,
            }
        return result

    async def preflight(self, *, require_live: bool | None = None) -> dict[str, Any]:
        """Verify live capability using only official client responses."""

        restore_amount = getattr(self.worker, "restore_order_unit", None)
        if callable(restore_amount):
            restored_amount = restore_amount()
            if inspect.isawaitable(restored_amount):
                await restored_amount
        restore = getattr(self.worker, "restore_selected_strategy", None)
        if callable(restore):
            restored = restore()
            if inspect.isawaitable(restored):
                await restored
        for method_name in (
            "_activate_pending_strategy_if_idle",
            "_activate_pending_order_unit_if_idle",
        ):
            activate = getattr(self.worker, method_name, None)
            if callable(activate):
                activated = activate()
                if inspect.isawaitable(activated):
                    await activated
        restore_market = getattr(self.worker, "restore_loop_market", None)
        if callable(restore_market):
            await restore_market()
        settings = self.worker.settings
        config_hash = getattr(self.worker, "effective_config_hash", None)
        if callable(config_hash):
            config_hash = config_hash()
        config_hash = config_hash or getattr(settings, "config_hash", None)
        live_required = bool(getattr(settings, "is_live_requested", False)) if require_live is None else bool(require_live)
        if not live_required:
            self._preflight_result = {
                "checked": True,
                "passed": True,
                "mode": "SHADOW",
                "requested_live": False,
                "reasons": [],
                "checked_at_ms": int(time.time() * 1000),
                "config_hash": config_hash,
                "wallet_address": getattr(settings, "wallet_address", None),
                "wallet_id": getattr(settings, "wallet_id", None),
                "account_type": getattr(settings, "account_type", None),
                "funding_source": str(getattr(settings, "funding_source", "MPC") or "MPC").upper(),
                "balance_account_type": "CeDeFi" if str(getattr(settings, "funding_source", "MPC") or "MPC").upper() == "MPC" else str(getattr(settings, "account_type", "SPOT") or "SPOT").upper(),
                "sas_verified": bool(getattr(settings, "sas_verified", False)),
            }
            await self._persist("prediction_preflight", self._preflight_result)
            return dict(self._preflight_result)

        reasons: list[str] = []
        details: dict[str, Any] = {}
        if getattr(settings, "live_enabled", True) is not True:
            reasons.append("live trading is not explicitly enabled")
        if str(getattr(settings, "account_type", "")).upper() != "SPOT":
            reasons.append("live account type must be SPOT")
        if str(getattr(settings, "order_type", "")).upper() != "LIMIT":
            reasons.append("live order type must be LIMIT")
        if str(getattr(settings, "time_in_force", "")).upper() != "GTC":
            reasons.append("live time in force must be GTC")
        try:
            if _as_decimal(getattr(settings, "order_unit_usdt", "1"), name="order unit") not in {
                Decimal("1"),
                Decimal("2"),
                Decimal("3"),
            }:
                reasons.append("live order unit must be exactly 1, 2 or 3 USDT")
        except ValueError as exc:
            reasons.append(str(exc))

        wallet_address = str(getattr(settings, "wallet_address", "") or "").strip()
        wallet_id = str(getattr(settings, "wallet_id", "") or "").strip()
        if not wallet_address or not wallet_id:
            reasons.append("wallet address and wallet id are required")
        require_sas = bool(getattr(settings, "require_sas", True))
        require_permission = bool(getattr(settings, "require_permission", False))
        if not bool(getattr(settings, "sas_verified", False)):
            reasons.append("SAS trade authorization has not been verified for this wallet")
        client = getattr(self.worker, "client", None)
        if client is None:
            reasons.append("prediction client is unavailable")

        # All static prerequisites are evaluated before any authenticated
        # network call.  A partially configured process cannot turn a useful
        # wallet response into an accidental live capability.
        if reasons:
            self._preflight_result = {
                "checked": True,
                "passed": False,
                "mode": "SHADOW",
                "requested_live": True,
                "reasons": list(dict.fromkeys(reasons)),
                "checked_at_ms": int(time.time() * 1000),
                "config_hash": config_hash,
                "wallet_address": wallet_address,
                "wallet_id": wallet_id,
                "account_type": getattr(settings, "account_type", None),
                "funding_source": str(getattr(settings, "funding_source", "MPC") or "MPC").upper(),
                "balance_account_type": "CeDeFi" if str(getattr(settings, "funding_source", "MPC") or "MPC").upper() == "MPC" else str(getattr(settings, "account_type", "SPOT") or "SPOT").upper(),
                "sas_verified": bool(getattr(settings, "sas_verified", False)),
                "wallet_match": bool(details.get("wallet_match") is True),
                **details,
            }
            if not await self._persist("prediction_preflight", self._preflight_result):
                self._preflight_result["reasons"].append("repository cannot persist live preflight evidence")
            return dict(self._preflight_result)

        # The client owns the authoritative signed PREDICTION_TRADE read-only
        # capability bundle when available.  It proves wallet/quota/SPOT
        # permission; no SAS endpoint is invented.
        # Lightweight test/client adapters retain the explicit response
        # parsing below.
        authoritative_method = self._client_method(client, "authoritative_preflight", "preflight_authoritative")
        manual_capability = {"wallet": False, "quota": False, "balance": False}
        if authoritative_method is not None:
            try:
                authoritative = await asyncio.to_thread(
                    _call_authoritative_preflight, authoritative_method, settings=settings
                )
                if not isinstance(authoritative, Mapping):
                    reasons.append("authoritative client preflight did not return a mapping")
                else:
                    details.update({key: value for key, value in authoritative.items() if key not in {"reasons", "passed"}})
                    details.setdefault("account_type", getattr(settings, "account_type", "SPOT"))
                    details.setdefault("funding_source", str(getattr(settings, "funding_source", "MPC") or "MPC").upper())
                    details.setdefault(
                        "balance_account_type",
                        "CeDeFi"
                        if str(getattr(settings, "funding_source", "MPC") or "MPC").upper() == "MPC"
                        else str(getattr(settings, "account_type", "SPOT") or "SPOT").upper(),
                    )
                    if not authoritative.get("passed"):
                        reasons.extend(str(reason) for reason in authoritative.get("reasons", ()))
            except Exception as exc:  # noqa: BLE001
                reasons.append(f"authoritative read-only preflight failed: {exc}")
        else:
            wallet_method = self._client_method(client, "list_prediction_wallets", "list_wallets")
            if wallet_method is None:
                reasons.append("official wallet list capability is unavailable")
            else:
                try:
                    payload = await asyncio.to_thread(wallet_method)
                    wallets = _official_wallets(payload)
                    if wallets is None:
                        reasons.append("official wallet response does not contain wallets[]")
                    elif not any(_wallet_address(item).lower() == wallet_address.lower() and _wallet_id(item) == wallet_id for item in wallets):
                        reasons.append("configured wallet address/id is not an exact official wallet match")
                    else:
                        details["wallet_match"] = True
                        manual_capability["wallet"] = True
                except Exception as exc:  # noqa: BLE001
                    reasons.append(f"wallet capability check failed: {exc}")

            quota_method = self._client_method(client, "get_quota_status", "quota_status")
            if quota_method is None:
                reasons.append("official quota capability is unavailable")
            else:
                try:
                    quota = await asyncio.to_thread(quota_method)
                    data = quota.get("data", quota) if isinstance(quota, Mapping) else None
                    remaining = data.get("remainingDailyLimit") if isinstance(data, Mapping) else None
                    if remaining is None or _as_decimal(remaining, name="remainingDailyLimit") <= 0:
                        reasons.append("remainingDailyLimit is missing or exhausted")
                    else:
                        details["remaining_daily_limit"] = str(remaining)
                        manual_capability["quota"] = True
                except Exception as exc:  # noqa: BLE001
                    reasons.append(f"quota capability check failed: {exc}")

            balance_method = self._client_method(client, "query_payment_option_balances", "payment_option_balances")
            if balance_method is None:
                reasons.append("official payment option balance capability is unavailable")
            else:
                try:
                    balances = await asyncio.to_thread(balance_method)
                    funding_source = str(getattr(settings, "funding_source", "MPC") or "MPC").upper()
                    balance_account_type = "CeDeFi" if funding_source == "MPC" else str(getattr(settings, "account_type", "SPOT") or "SPOT").upper()
                    details["funding_source"] = funding_source
                    details["balance_account_type"] = balance_account_type
                    value = available_balance_display(balances, account_type=balance_account_type)
                    required = _as_decimal(getattr(settings, "required_balance_usdt", "0"), name="required balance")
                    if value is None:
                        reasons.append(f"enabled {balance_account_type} availableBalanceDisplay is missing; no account fallback is allowed")
                    elif value < required:
                        reasons.append(f"enabled {balance_account_type} availableBalanceDisplay {value} < required {required}")
                    else:
                        details["available_balance_display"] = str(value)
                        manual_capability["balance"] = True
                except Exception as exc:  # noqa: BLE001
                    reasons.append(f"account balance capability check failed: {exc}")

        if authoritative_method is None:
            permission_verified = all(manual_capability.values())
            details["permission_verified"] = permission_verified
            details["permission_capability"] = {
                "verified": permission_verified,
                "security_type": "PREDICTION_TRADE",
                "signed": True,
                "endpoints": [
                    "/sapi/v1/w3w/wallet/prediction/wallet/list",
                    "/sapi/v1/w3w/wallet/prediction/quota/limit/status",
                    "/sapi/v1/w3w/wallet/prediction/balance/payment-options",
                ],
            }
        else:
            permission_verified = bool(details.get("permission_verified"))
        if require_permission and not permission_verified:
            reasons.append("signed PREDICTION_TRADE permission capability evidence is unavailable or failed")

        # Persist a self-contained, restart-verifiable envelope.  The client
        # officially proves signed capability; ``authenticated`` is an
        # explicit normalized marker so the worker does not infer authority
        # from a settings flag or a truthy string after restart.
        capability = details.get("permission_capability")
        if isinstance(capability, Mapping):
            capability = dict(capability)
            if "authenticated" not in capability:
                capability["authenticated"] = details.get("authenticated", details.get("signed") is True)
            details["permission_capability"] = capability
        if "authenticated" not in details:
            details["authenticated"] = details.get("signed") is True

        self._preflight_result = {
            "checked": True,
            "passed": not reasons,
            "mode": "LIVE" if not reasons else "SHADOW",
            "requested_live": True,
            "reasons": list(dict.fromkeys(reasons)),
            "checked_at_ms": int(time.time() * 1000),
            "config_hash": config_hash,
            "wallet_address": wallet_address,
            "wallet_id": wallet_id,
            "account_type": getattr(settings, "account_type", None),
            "sas_verified": bool(getattr(settings, "sas_verified", False)),
            "wallet_match": bool(details.get("wallet_match") is True),
            **details,
        }
        if not await self._persist("prediction_preflight", self._preflight_result):
            self._preflight_result["passed"] = False
            self._preflight_result["mode"] = "SHADOW"
            self._preflight_result["reasons"].append("repository cannot persist live preflight evidence")
        return dict(self._preflight_result)

    @staticmethod
    def _client_method(client: Any, *names: str) -> Callable[..., Any] | None:
        for name in names:
            method = getattr(client, name, None)
            if callable(method):
                return method
        return None

    async def close(self) -> Any:
        # Process shutdown is not an operator stop. Persisting OPERATOR_STOP
        # here made every systemd restart silently terminate a finite loop and
        # prevented startup recovery from resuming its durable cursor. The
        # explicit Telegram stop command still calls ``stop_loop``; close only
        # cancels this process's in-memory task and leaves durable authority
        # unchanged for restart recovery.
        task = getattr(self.worker, "_task", None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                # Shutdown must not resurrect a failed market task or leak an
                # exception through Telegram/systemd cleanup.
                pass
        stop_observer = getattr(self.worker, "stop_shadow_observer", None)
        if callable(stop_observer):
            result = stop_observer()
            if inspect.isawaitable(result):
                await result
        return None


__all__ = ["PredictionController"]
