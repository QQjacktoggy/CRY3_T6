"""Fail-closed C180 worker adapter for the frozen signal and live gate.

The producer is a separate, read-only process.  This module never places an
order.  A caller must perform a fresh wallet check, then atomically claim the
BUY through C180LiveLedger immediately before its existing signed API path.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .c180_favorite import (
    C180ExecutionInput, C180ExecutionRecheck, PriceLevel,
    recheck_c180_execution,
)
from .c180_gate_runtime import C180GateRuntime, STATE_KEY
from .c180_live_ledger import C180LiveLedger
from .c180_signal_runtime import read_c180_book, read_c180_signal
from .c180_signal_service import C180Signal


PROFILE = "c180_favorite_hold_v1"


@dataclass(frozen=True)
class C180Ready:
    allowed: bool
    reason: str
    signal: C180Signal | None = None
    execution: C180ExecutionRecheck | None = None
    book_at_ms: int | None = None
    run_ordinal: int | None = None


def _integer(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (ValueError, TypeError, InvalidOperation):
        return None


def _levels(value: Any) -> tuple[PriceLevel, ...] | None:
    if not isinstance(value, (list, tuple)) or not value:
        return None
    levels: list[PriceLevel] = []
    for item in value:
        if isinstance(item, Mapping):
            price = _decimal(item.get("price"))
            shares = _decimal(item.get("shares"))
        elif isinstance(item, (tuple, list)) and len(item) == 2:
            price, shares = _decimal(item[0]), _decimal(item[1])
        else:
            return None
        if price is None or shares is None:
            return None
        levels.append(PriceLevel(price, shares))
    return tuple(levels)


class C180WorkerBridge:
    def __init__(self, repository: Any, signal_db: str | Path,
                 exposure_checker: Any = None) -> None:
        self.repository = repository
        self.signal_db = Path(signal_db)
        self.ledger = C180LiveLedger(repository)
        self.gate = C180GateRuntime(repository, self.ledger.snapshot,
                                    signal_db=self.signal_db,
                                    exposure_checker=exposure_checker)

    async def register_market(self, *, loop_id: str, market: Any, now_ms: int,
                              unit_usdt: Decimal) -> C180Ready:
        """Bind the official slot at admission, before its 12-second BUY window."""

        start = _integer(getattr(market, "start_time_ms", None))
        topic = str(getattr(market, "market_topic_id", "") or "")
        up_market_id = str(getattr(market, "up_market_id", "") or "")
        if not loop_id or start is None or start <= 0 or not topic or not up_market_id:
            return C180Ready(False, "official_market_identity_missing")
        if unit_usdt not in (Decimal("1"), Decimal("2"), Decimal("3")):
            return C180Ready(False, "unsupported_unit")
        try:
            state = await self.repository.get_runtime_config(STATE_KEY, None)
            if not isinstance(state, Mapping) or state.get("loop_id") != loop_id:
                # Existing completed-loop state is replaced only after the gate
                # proves that the former loop is terminal and reconciled.
                await self.ledger.seed_schedule(loop_id=loop_id, first_market_start_ms=start)
                await self.gate.arm(loop_id=loop_id, first_market_start_ms=start,
                                    unit_usdt=unit_usdt, replace_completed_loop=True)
            await self.ledger.verify_market(
                loop_id=loop_id, market_start_ms=start,
                market_topic_id=topic, market_id=up_market_id,
                verified_at_ms=now_ms,
            )
        except Exception:
            return C180Ready(False, "schedule_or_market_registration_unavailable")
        return C180Ready(True, "official_market_registered")

    async def prepare_market(self, *, loop_id: str, market: Any, now_ms: int,
                             unit_usdt: Decimal) -> C180Ready:
        """Evaluate the durable 20-run gate immediately before a possible BUY."""

        registered = await self.register_market(
            loop_id=loop_id, market=market, now_ms=now_ms,
            unit_usdt=unit_usdt,
        )
        if not registered.allowed:
            return registered
        start = int(market.start_time_ms)
        try:
            gate = await self.gate.evaluate(
                loop_id=loop_id, market_start_ms=start, decision_at_ms=now_ms,
            )
        except Exception:
            return C180Ready(False, "gate_evaluation_unavailable")
        if not gate.allow_entry:
            return C180Ready(False, gate.reason, run_ordinal=gate.run_ordinal)
        return C180Ready(True, "gate_pass", run_ordinal=gate.run_ordinal)

    def check_signal(self, *, market: Any, unit_usdt: Decimal, at_ms: int,
                     last_seen_book_at_ms: int) -> C180Ready:
        """Check the immutable T+120 decision on a fresh T+124..136 book."""

        start = _integer(getattr(market, "start_time_ms", None))
        if start is None:
            return C180Ready(False, "market_start_missing")
        signal = read_c180_signal(self.signal_db, start)
        if signal is None or not signal.is_entry or signal.entry is None:
            return C180Ready(False, "c180_entry_signal_missing")
        if (signal.market_start_ms != start
                or signal.market_topic != getattr(market, "market_topic_id", None)
                or signal.market_id != getattr(market, "up_market_id", None)
                or signal.entry.stake_usdt != unit_usdt):
            return C180Ready(False, "c180_signal_identity_or_unit_mismatch")
        snapshot = read_c180_book(self.signal_db, start)
        if not isinstance(snapshot, Mapping):
            return C180Ready(False, "c180_execution_book_missing")
        if (snapshot.get("market_start_ms") != start
                or snapshot.get("market_topic") != signal.market_topic
                or snapshot.get("market_id") != signal.market_id):
            return C180Ready(False, "c180_execution_book_identity_mismatch")
        quote = snapshot.get("quote")
        selected = quote.get(signal.entry.side) if isinstance(quote, Mapping) else None
        levels = _levels(selected.get("ask_levels")) if isinstance(selected, Mapping) else None
        observed_book_at = _integer(snapshot.get("book_at_ms"))
        try:
            result = recheck_c180_execution(C180ExecutionInput(
                entry_decision=signal.entry,
                market_start_ms=start,
                at_ms=at_ms,
                frozen_market_topic=signal.market_topic,
                current_market_topic=str(getattr(market, "market_topic_id", "") or ""),
                frozen_market_id=signal.market_id,
                current_market_id=str(getattr(market, "up_market_id", "") or ""),
                book_market_id=str(snapshot.get("market_id") or ""),
                frozen_fee_bps=signal.frozen_fee_bps,
                current_fee_bps=_decimal(snapshot.get("fee_bps")),
                original_p_up=signal.original_p_up,
                book_at_ms=observed_book_at,
                book_received_at_ms=_integer(snapshot.get("received_at")),
                book_received_at_ms_secondary=_integer(snapshot.get("received_at_ms")),
                last_seen_book_at_ms=last_seen_book_at_ms,
                selected_ask_levels=levels,
                full_depth=snapshot.get("full_depth") is True,
            ))
        except (ValueError, TypeError, ArithmeticError):
            return C180Ready(False, "c180_execution_book_invalid")
        return C180Ready(result.permitted, result.reason, signal, result,
                         observed_book_at)
