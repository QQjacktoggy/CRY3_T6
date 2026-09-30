"""Pure, decision-time-aware C180 20-run entry gate.

The caller owns the durable run ordinal, batch unit, settlement ledger, and
order reconciliation.  This module never places orders or changes persistence.
An ordinal counts every scheduled market, including skips and halted markets.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable


BATCH_SIZE = 20
BASE_MDD_USDT = Decimal("4.49")
V11_MDD_USDT = Decimal("3.50")
V11_LOOP_LOSS_USDT = Decimal("-6.00")
ALLOWED_UNITS = frozenset({Decimal("1"), Decimal("2"), Decimal("3")})


@dataclass(frozen=True)
class SettledTrade:
    """One confirmed live PNL event, available to the worker at ``known_at_ms``.

    ``unit_usdt`` is the configured entry unit for that run, including partial
    fills.  It makes PNL comparable if a legacy batch contains mixed sizes.
    ``net_pnl_usdt`` must include actual execution fees but exclude model cost.
    The caller supplies each confirmed settlement event exactly once.  Several
    events for one run are allowed.
    """

    run_ordinal: int
    net_pnl_usdt: Decimal
    known_at_ms: int
    unit_usdt: Decimal


@dataclass(frozen=True)
class GateDecision:
    allow_entry: bool
    reason: str
    block_index: int
    block_start_run: int
    block_end_run: int
    next_block_start_run: int
    frozen_unit_usdt: Decimal
    queued_unit_usdt: Decimal | None
    threshold_usdt: Decimal
    pnl_usdt: Decimal
    peak_usdt: Decimal
    drawdown_usdt: Decimal
    max_drawdown_usdt: Decimal
    trigger_run: int | None
    unresolved_runs: tuple[int, ...]
    mixed_unit_runs: tuple[int, ...]
    loop_pnl_usdt: Decimal = Decimal("0")
    loop_loss_latched: bool = False
    recovery_hold_latched: bool = False


def _unit(value: Decimal | str | int) -> Decimal:
    unit = Decimal(str(value))
    if unit not in ALLOWED_UNITS:
        raise ValueError("C180 entry unit must be exactly 1, 2, or 3 USDT")
    return unit


def block_bounds(run_ordinal: int) -> tuple[int, int, int]:
    """Return one-based (block index, first run, final run)."""

    run = int(run_ordinal)
    if run < 1 or run != run_ordinal:
        raise ValueError("run_ordinal must be a positive integer")
    index = (run - 1) // BATCH_SIZE + 1
    start = (index - 1) * BATCH_SIZE + 1
    return index, start, start + BATCH_SIZE - 1


def evaluate_batch_gate(
    *,
    run_ordinal: int,
    decision_at_ms: int,
    frozen_unit_usdt: Decimal | str | int,
    settlements: Iterable[SettledTrade],
    ledger_complete: bool,
    unresolved_filled_runs: Iterable[int] = (),
    persisted_trigger_run: int | None = None,
    requested_unit_usdt: Decimal | str | int | None = None,
    policy_version: str = "1.0",
    persisted_loop_loss_latched: bool = False,
    persisted_recovery_hold_latched: bool = False,
) -> GateDecision:
    """Decide whether a new C180 entry is allowed for this scheduled market.

    The caller sets ``ledger_complete`` only after a successful, reconciled
    ledger read.  The caller freezes ``frozen_unit_usdt`` at the first run of
    each block and persists it.  A changed ``requested_unit_usdt`` is returned
    as a queue for the next block; it does not resize this block's orders or
    threshold.

    Only confirmed PNL known by ``decision_at_ms`` may affect the gate.  An
    earlier filled run whose PNL is unresolved holds new entries, including
    after a block boundary, while existing exits/reconciliation continue.
    The first historical threshold crossing remains latched even if a later
    settlement recovers the current drawdown.
    """

    index, start, end = block_bounds(run_ordinal)
    now = int(decision_at_ms)
    if now < 0 or now != decision_at_ms:
        raise ValueError("decision_at_ms must be a nonnegative integer")
    unit = _unit(frozen_unit_usdt)
    requested = _unit(requested_unit_usdt) if requested_unit_usdt is not None else unit
    queued = requested if requested != unit else None
    if policy_version not in ("1.0", "1.1"):
        raise ValueError("unsupported C180 risk policy")
    threshold = V11_MDD_USDT if policy_version == "1.1" else BASE_MDD_USDT * unit

    events: list[SettledTrade] = []
    loop_events: list[SettledTrade] = []
    pending = {int(run) for run in unresolved_filled_runs if 0 < int(run) < run_ordinal}
    mixed_units: set[int] = set()
    for trade in settlements:
        trade_run = int(trade.run_ordinal)
        if trade_run < 1 or trade_run != trade.run_ordinal:
            raise ValueError("settlement run_ordinal must be a positive integer")
        if trade_run >= run_ordinal:
            continue  # This run and future runs cannot inform this decision.
        trade_unit = _unit(trade.unit_usdt)
        if int(trade.known_at_ms) > now:
            pending.add(trade_run)
            continue
        pnl = Decimal(str(trade.net_pnl_usdt))
        if not pnl.is_finite():
            raise ValueError("settlement PNL must be finite")
        loop_events.append(SettledTrade(trade_run, pnl, int(trade.known_at_ms), trade_unit))
        if start <= trade_run <= end:
            if trade_unit != unit:
                mixed_units.add(trade_run)
            # Convert each actual fill to this block's frozen stake.  With a
            # correctly frozen batch this equals the actual live USDT PNL.
            events.append(
                SettledTrade(trade_run, pnl / trade_unit * unit, int(trade.known_at_ms), unit)
            )

    events.sort(key=lambda trade: (trade.known_at_ms, trade.run_ordinal))
    current = Decimal("0")
    peak = Decimal("0")
    max_dd = Decimal("0")
    trigger_run = None
    offset = 0
    while offset < len(events):
        known_at = events[offset].known_at_ms
        same_time: list[SettledTrade] = []
        while offset < len(events) and events[offset].known_at_ms == known_at:
            same_time.append(events[offset])
            offset += 1
        # A single observation timestamp has no observable ordering inside
        # it; sum all its fills before computing the equity high-water mark.
        current += sum((event.net_pnl_usdt for event in same_time), Decimal("0"))
        peak = max(peak, current)
        drawdown = peak - current
        max_dd = max(max_dd, drawdown)
        if trigger_run is None and (drawdown >= threshold if policy_version == "1.1" else drawdown > threshold):
            trigger_run = max(event.run_ordinal for event in same_time)

    loop_events.sort(key=lambda trade: (trade.known_at_ms, trade.run_ordinal))
    loop_pnl = Decimal("0")
    loop_loss_latched = bool(persisted_loop_loss_latched)
    offset = 0
    while offset < len(loop_events):
        known_at = loop_events[offset].known_at_ms
        same_time = []
        while offset < len(loop_events) and loop_events[offset].known_at_ms == known_at:
            same_time.append(loop_events[offset])
            offset += 1
        loop_pnl += sum((event.net_pnl_usdt for event in same_time), Decimal("0"))
        if policy_version == "1.1" and loop_pnl <= V11_LOOP_LOSS_USDT:
            loop_loss_latched = True

    if persisted_trigger_run is not None:
        saved = int(persisted_trigger_run)
        if start <= saved <= end:
            if saved >= run_ordinal:
                raise ValueError("persisted trigger cannot come from this or a future run")
            trigger_run = saved if trigger_run is None else min(saved, trigger_run)

    recovery_hold_latched = bool(persisted_recovery_hold_latched)
    if policy_version == "1.1" and trigger_run is not None:
        recovery_hold_latched = True

    unresolved = tuple(sorted(pending))
    if not ledger_complete:
        reason = "ledger_unavailable"
    elif unresolved:
        reason = "unresolved_prior_fill"
    elif mixed_units:
        reason = "mixed_unit_batch"
    elif policy_version == "1.1" and loop_loss_latched:
        reason = "loop_loss_halted"
    elif trigger_run is not None:
        reason = "batch_mdd_halted"
    elif policy_version == "1.1" and recovery_hold_latched:
        reason = "recovery_shadow_pending"
    else:
        reason = "entry_allowed"
    return GateDecision(
        allow_entry=reason == "entry_allowed",
        reason=reason,
        block_index=index,
        block_start_run=start,
        block_end_run=end,
        next_block_start_run=end + 1,
        frozen_unit_usdt=unit,
        queued_unit_usdt=queued,
        threshold_usdt=threshold,
        pnl_usdt=current,
        peak_usdt=peak,
        drawdown_usdt=peak - current,
        max_drawdown_usdt=max_dd,
        trigger_run=trigger_run,
        unresolved_runs=unresolved,
        mixed_unit_runs=tuple(sorted(mixed_units)),
        loop_pnl_usdt=loop_pnl,
        loop_loss_latched=loop_loss_latched,
        recovery_hold_latched=recovery_hold_latched,
    )
