"""Durable C180 20-run gate adapter; no order or exchange API.

The caller invokes ``evaluate`` immediately before every C180 buy.  A single
writer owns the runtime_config key.  The injected ledger provider must query
only this loop's C180 LIVE trades and reconcile unknown order state first.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

from .c180_batch_gate import GateDecision, SettledTrade, block_bounds, evaluate_batch_gate


SLOT_MS = 300_000
STATE_KEY = "c180_batch_gate_runtime_v1"


@dataclass(frozen=True)
class LiveSettlement:
    settlement_id: str
    market_start_ms: int
    net_pnl_usdt: Decimal
    known_at_ms: int
    unit_usdt: Decimal


@dataclass(frozen=True)
class LoopLedgerSnapshot:
    """Atomic, reconciled view supplied by the live repository adapter.

    ``verified_market_starts`` are official C180 market identities persisted
    before entry admission.  ``confirmed_empty_market_starts`` additionally
    attest that skipped windows contain no C180 buy/fill/unknown intent.
    Every confirmed settlement event appears exactly once.
    """

    loop_id: str
    complete: bool
    verified_market_starts: tuple[int, ...]
    confirmed_empty_market_starts: tuple[int, ...]
    settlements: tuple[LiveSettlement, ...]
    unresolved_market_starts: tuple[int, ...]


@dataclass(frozen=True)
class RuntimeGateResult:
    allow_entry: bool
    reason: str
    loop_id: str
    market_start_ms: int
    run_ordinal: int | None
    gate: GateDecision | None
    missed_runs: tuple[int, ...] = ()


LedgerProvider = Callable[[str, int], Awaitable[LoopLedgerSnapshot | None]]
ExposureChecker = Callable[[], Awaitable[bool]]


def recovery_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Only terminal, fully priced paper rows can qualify a whole window."""
    settled = [row for row in rows if row.get("status") == "FILLED"]
    equity = peak = mdd = Decimal("0")
    wins = losses = 0
    for row in rows:
        if row.get("status") != "FILLED":
            continue
        pnl = Decimal(str(row["net_pnl_usdt"]))
        if not pnl.is_finite():
            raise ValueError("nonfinite paper PnL")
        equity += pnl
        peak = max(peak, equity)
        mdd = max(mdd, peak - equity)
        wins += pnl > 0
        losses += pnl < 0
    complete = len(rows) == 10 and all(row.get("status") in {"FILLED", "SKIP", "NO_FILL"} for row in rows)
    return {"observed": len(rows), "settled": len(settled), "wins": wins,
            "losses": losses, "pnl_usdt": str(equity), "mdd_usdt": str(mdd),
            "complete": complete,
            "qualified": complete and len(settled) >= 5 and wins >= 3
            and wins * 5 >= (wins + losses) * 3
            and equity >= Decimal("1") and mdd <= Decimal("1")}


def early_recovery_metrics(rows: list[Mapping[str, Any]], *, expected_count: int) -> dict[str, Any]:
    """A complete positive five-market prefix may qualify before the window ends."""
    if not 0 <= expected_count <= 10:
        raise ValueError("invalid early recovery horizon")
    base = recovery_metrics(rows)
    pnl = Decimal(base["pnl_usdt"])
    complete = (expected_count >= 5 and len(rows) == expected_count
                and all(row.get("status") in {"FILLED", "SKIP", "NO_FILL"} for row in rows))
    return {**base, "complete": complete,
            "qualified": complete and int(base["settled"]) >= 1 and pnl > 0}


def _unit(value: Decimal | str | int) -> Decimal:
    unit = Decimal(str(value))
    if unit not in (Decimal("1"), Decimal("2"), Decimal("3")):
        raise ValueError("C180 unit must be 1, 2, or 3 USDT")
    return unit


def _ordinal(start_ms: int, anchor_ms: int) -> int:
    delta = int(start_ms) - int(anchor_ms)
    if delta < 0 or delta % SLOT_MS:
        raise ValueError("market start is not on the armed 5-minute grid")
    return delta // SLOT_MS + 1


class C180GateRuntime:
    def __init__(self, repository: Any, ledger_provider: LedgerProvider, *,
                 state_key: str = STATE_KEY, signal_db: str | Path | None = None,
                 exposure_checker: ExposureChecker | None = None) -> None:
        self.repository = repository
        self.ledger_provider = ledger_provider
        self.state_key = state_key
        self.signal_db = Path(signal_db) if signal_db else None
        self.exposure_checker = exposure_checker
        self._lock = asyncio.Lock()

    async def arm(
        self,
        *,
        loop_id: str,
        first_market_start_ms: int,
        unit_usdt: Decimal | str | int,
        replace_completed_loop: bool = False,
    ) -> Mapping[str, Any]:
        """Explicitly establish the immutable ordinal anchor before run one.

        Replacing a prior loop requires caller verification of its terminal
        state and a reconciled old ledger with no unresolved C180 exposure.
        """

        loop = str(loop_id).strip()
        start = int(first_market_start_ms)
        unit = _unit(unit_usdt)
        if not loop or start <= 0 or start % SLOT_MS:
            raise ValueError("loop id or first market start invalid")
        async with self._lock:
            old = await self.repository.get_runtime_config(self.state_key, None)
            if isinstance(old, Mapping):
                if str(old.get("loop_id")) == loop:
                    if int(old.get("first_market_start_ms", -1)) != start:
                        raise ValueError("existing loop anchor differs")
                    if str(old.get("unit_usdt")) != str(unit):
                        raise ValueError("existing loop unit differs")
                    return dict(old)
                if not replace_completed_loop:
                    raise ValueError("previous C180 loop state exists")
                previous_loop = await self.repository.get_loop(str(old.get("loop_id")))
                if not isinstance(previous_loop, Mapping) or str(previous_loop.get("state", "")).upper() not in {
                    "DONE", "STOPPED", "CANCELLED"
                }:
                    raise ValueError("previous C180 loop is still active or has unknown state")
                previous = await self.ledger_provider(str(old.get("loop_id")), start)
                if previous is None or not previous.complete or previous.unresolved_market_starts:
                    raise ValueError("previous loop exposure is not reconciled")
            current = await self.ledger_provider(loop, start)
            if current is None or current.loop_id != loop or not current.complete:
                raise ValueError("new C180 loop ledger unavailable")
            if current.settlements or current.unresolved_market_starts:
                raise ValueError("new C180 loop already has live exposure")
            state: dict[str, Any] = {
                "version": 2,
                "policy_version": "1.1",
                "loop_id": loop,
                "first_market_start_ms": start,
                "last_market_start_ms": start - SLOT_MS,
                "unit_usdt": str(unit),
                "latches": {},
                "loop_loss_latched": False,
                "recovery_hold_latched": False,
                "recovery": {},
            }
            await self.repository.set_runtime_config(self.state_key, state)
            return state

    async def evaluate(self, *, loop_id: str, market_start_ms: int, decision_at_ms: int) -> RuntimeGateResult:
        """Fail closed on absent state, ledger uncertainty, or time discontinuity."""

        loop = str(loop_id).strip()
        try:
            start = int(market_start_ms)
            at = int(decision_at_ms)
        except (TypeError, ValueError, OverflowError):
            return RuntimeGateResult(False, "invalid_market_time", loop, 0, None, None)

        def hold(reason: str, ordinal: int | None = None, gate: GateDecision | None = None,
                 missed: tuple[int, ...] = ()) -> RuntimeGateResult:
            return RuntimeGateResult(False, reason, loop, start, ordinal, gate, missed)

        async with self._lock:
            try:
                state = await self.repository.get_runtime_config(self.state_key, None)
            except Exception:
                return hold("gate_state_unavailable")
            if not isinstance(state, Mapping) or state.get("version") not in (1, 2):
                return hold("gate_state_missing")
            if state.get("loop_id") != loop:
                return hold("wrong_loop")
            try:
                anchor = int(state["first_market_start_ms"])
                last = int(state["last_market_start_ms"])
                run = _ordinal(start, anchor)
                last_run = _ordinal(last, anchor) if last >= anchor else 0
                if start % SLOT_MS or run < last_run:
                    return hold("market_time_discontinuous", run)
                if at < start + 120_000 or at >= start + SLOT_MS:
                    return hold("decision_outside_c180_market", run)
                missing_starts = tuple(range(last + SLOT_MS, start, SLOT_MS))
                snapshot = await self.ledger_provider(loop, at)
                if snapshot is None or snapshot.loop_id != loop or not snapshot.complete:
                    return hold("ledger_snapshot_missing", run)
                verified = set(snapshot.verified_market_starts)
                if start not in verified:
                    return hold("market_snapshot_missing", run)
                if not set(missing_starts).issubset(verified) or not set(missing_starts).issubset(set(snapshot.confirmed_empty_market_starts)):
                    return hold("market_time_discontinuous", run)
                missed_runs = tuple(_ordinal(item, anchor) for item in missing_starts)

                block, _block_start, _block_end = block_bounds(run)
                # `/predict_amount` already defers an active-loop change to
                # the next Loop.  All 20-run blocks in this Loop use one unit.
                frozen = _unit(state["unit_usdt"])
                latches = dict(state.get("latches") or {})

                settlements: list[SettledTrade] = []
                seen_settlement_ids: set[str] = set()
                for row in snapshot.settlements:
                    if not row.settlement_id or row.settlement_id in seen_settlement_ids:
                        return hold("duplicate_or_missing_settlement_id", run)
                    seen_settlement_ids.add(row.settlement_id)
                    row_run = _ordinal(row.market_start_ms, anchor)
                    if row_run > run:
                        return hold("future_settlement_in_ledger", run)
                    if row_run == run:
                        return hold("current_market_already_settled", run)
                    if row.market_start_ms in missing_starts:
                        return hold("missing_slot_has_settlement", run)
                    if _unit(row.unit_usdt) != frozen:
                        return hold("mixed_unit_batch", run)
                    settlements.append(
                        SettledTrade(row_run, Decimal(str(row.net_pnl_usdt)), int(row.known_at_ms), _unit(row.unit_usdt))
                    )
                unresolved = tuple(_ordinal(item, anchor) for item in snapshot.unresolved_market_starts)
                if any(item in missing_starts for item in snapshot.unresolved_market_starts):
                    return hold("missing_slot_has_unresolved_intent", run)
                if any(item > run for item in unresolved):
                    return hold("future_unresolved_intent", run)
                if run in unresolved:
                    return hold("current_market_unresolved_intent", run)
                gate = evaluate_batch_gate(
                    run_ordinal=run,
                    decision_at_ms=at,
                    frozen_unit_usdt=frozen,
                    settlements=settlements,
                    ledger_complete=True,
                    unresolved_filled_runs=unresolved,
                    persisted_trigger_run=latches.get(str(block)),
                    policy_version=str(state.get("policy_version") or "1.0"),
                    persisted_loop_loss_latched=bool(state.get("loop_loss_latched")),
                    persisted_recovery_hold_latched=bool(state.get("recovery_hold_latched")),
                )
                if gate.trigger_run is not None:
                    latches[str(block)] = gate.trigger_run
                recovery = dict(state.get("recovery") or {})
                # A resumed process cannot infer executable paper fills for old
                # slots. Begin at a future complete market after sidecar warmup.
                if gate.recovery_hold_latched and not recovery:
                    recovery = {"state": "SHADOW", "start_run": run + 2,
                                "halt_run": max((int(v) for v in latches.values()), default=run),
                                "attempts": 0, "recoveries": 0, "metrics": {}}
                elif gate.trigger_run is not None and recovery.get("state") in {"LIVE", "PROBATION"}:
                    recovery.update(state="SHADOW", start_run=run + 1,
                                    halt_run=gate.trigger_run, metrics={})
                if recovery.get("state") == "SHADOW" and self.signal_db is not None:
                    from .c180_signal_runtime import read_c180_recovery_outcomes
                    window_start = int(recovery["start_run"])
                    window_end = window_start + 9
                    starts = [anchor + (r - 1) * SLOT_MS for r in range(window_start, min(run, window_end + 1))]
                    rows_by_start = read_c180_recovery_outcomes(self.signal_db, starts)
                    rows = [rows_by_start[s] for s in starts if s in rows_by_start]
                    metrics = recovery_metrics(rows)
                    metrics.update(window_start_run=window_start, window_end_run=window_end)
                    recovery["metrics"] = metrics
                    early = early_recovery_metrics(rows, expected_count=len(starts))
                    recovery["early_metrics"] = early
                    if run > window_end + 10:
                        # A later non-overlapping window is already complete;
                        # never promote an older result that settled late.
                        recovery.update(start_run=window_end + 1,
                                        attempts=int(recovery.get("attempts", 0)) + 1)
                    elif run > window_end and metrics["complete"]:
                        halt_block_end = block_bounds(int(recovery["halt_run"]))[2]
                        if (metrics["qualified"] and run > halt_block_end
                                and not gate.loop_loss_latched
                                and not snapshot.unresolved_market_starts
                                and int(recovery.get("recoveries", 0)) < 2):
                            recovery.update(state="QUALIFIED", resume_at_run=run + 1)
                        else:
                            recovery.update(start_run=window_end + 1,
                                            attempts=int(recovery.get("attempts", 0)) + 1)
                    elif window_start + 5 <= run <= window_end and early["qualified"]:
                        halt_block_end = block_bounds(int(recovery["halt_run"]))[2]
                        if (run > halt_block_end and not gate.loop_loss_latched
                                and not snapshot.unresolved_market_starts
                                and int(recovery.get("recoveries", 0)) < 2):
                            recovery.update(state="QUALIFIED", resume_at_run=run + 1,
                                            qualification="early_five_market")
                if recovery.get("state") == "QUALIFIED" and run >= int(recovery["resume_at_run"]):
                    loop_row = await self.repository.get_loop(loop)
                    safe = (isinstance(loop_row, Mapping)
                            and str(loop_row.get("state")) == "RUNNING"
                            and not loop_row.get("new_entries_stopped")
                            and not loop_row.get("hard_stop_latched")
                            and not gate.loop_loss_latched
                            and not snapshot.unresolved_market_starts
                            and self.exposure_checker is not None
                            and await self.exposure_checker())
                    if safe:
                        recovery.update(state="PROBATION", probation_start_run=run,
                                        recoveries=int(recovery.get("recoveries", 0)) + 1)
                        gate = evaluate_batch_gate(
                            run_ordinal=run, decision_at_ms=at,
                            frozen_unit_usdt=frozen, settlements=settlements,
                            ledger_complete=True, unresolved_filled_runs=unresolved,
                            persisted_trigger_run=latches.get(str(block)),
                            policy_version=str(state.get("policy_version") or "1.0"),
                            persisted_loop_loss_latched=gate.loop_loss_latched,
                            persisted_recovery_hold_latched=False,
                        )
                    else:
                        recovery["state"] = "SHADOW"
                        recovery["start_run"] = run + 1
                if recovery.get("state") == "PROBATION" and gate.recovery_hold_latched:
                    recovery.update(state="SHADOW", start_run=run + 1,
                                    halt_run=run, metrics={})
                if recovery.get("state") == "PROBATION" and gate.allow_entry:
                    first = int(recovery["probation_start_run"])
                    paperless = [x for x in settlements if first <= x.run_ordinal < run]
                    equity = peak = mdd = Decimal("0")
                    for trade in sorted(paperless, key=lambda x: (x.known_at_ms, x.run_ordinal)):
                        equity += trade.net_pnl_usdt
                        peak = max(peak, equity)
                        mdd = max(mdd, peak - equity)
                    recovery["probation"] = {"settled": len(paperless),
                                             "pnl_usdt": str(equity), "mdd_usdt": str(mdd)}
                    if mdd >= Decimal("1.5") or (len(paperless) >= 5 and equity < 0) or run > first + 9 and len(paperless) < 5:
                        recovery.update(state="SHADOW", start_run=run + 1,
                                        halt_run=run, metrics={})
                        gate = evaluate_batch_gate(
                            run_ordinal=run, decision_at_ms=at,
                            frozen_unit_usdt=frozen, settlements=settlements,
                            ledger_complete=True, unresolved_filled_runs=unresolved,
                            persisted_trigger_run=latches.get(str(block)),
                            policy_version=str(state.get("policy_version") or "1.0"),
                            persisted_loop_loss_latched=gate.loop_loss_latched,
                            persisted_recovery_hold_latched=True,
                        )
                    elif len(paperless) >= 5 and equity >= 0:
                        recovery["state"] = "LIVE"
                updated = dict(state)
                updated.update(
                    last_market_start_ms=start,
                    latches=latches,
                    loop_loss_latched=gate.loop_loss_latched,
                    recovery_hold_latched=gate.recovery_hold_latched,
                    recovery=recovery,
                )
                if updated != dict(state):
                    await self.repository.set_runtime_config(self.state_key, updated)
                if recovery.get("state") == "QUALIFIED":
                    return hold("recovery_qualified_next_market", run, gate, missed_runs)
                return RuntimeGateResult(gate.allow_entry, gate.reason, loop, start, run, gate, missed_runs)
            except Exception:
                return hold("gate_evaluation_unavailable")
