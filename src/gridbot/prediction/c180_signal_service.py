"""Compose frozen C180 evidence, the Original JEV call, and pure entry policy.

The service never places an order. A signal is visible to a trading worker only
after its provenance has been durably recorded by the injected callback.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping

import aiohttp

from .c180_evidence_collector import FrozenEvidence
from .c180_favorite import (
    C180EntryDecision,
    C180EntryInput,
    MODEL_ID,
    PriceLevel,
    decide_c180_favorite_entry,
)
from .c180_jev_client import infer_original_jev_p_up


@dataclass(frozen=True)
class C180Signal:
    market_start_ms: int
    market_topic: str | None
    market_id: str | None
    cutoff_ms: int
    completed_at_ms: int
    status: str
    entry: C180EntryDecision | None
    original_p_up: Decimal | None
    original_input_sha256: str | None
    model_cost_usdt: Decimal | None
    frozen_fee_bps: Decimal | None

    @property
    def is_entry(self) -> bool:
        return self.entry is not None and self.entry.action in ("UP", "DOWN")


class C180SignalService:
    """One immutable signal per scheduled market; no late packet rebuilding."""

    def __init__(
        self,
        *,
        frozen_logic: Any,
        symbol: str = "BTCUSDT",
        api_key: str,
        unit_provider: Callable[[], Decimal],
        persist_result: Callable[[C180Signal], Awaitable[None]],
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        from .loop_market import symbol as valid_symbol
        self.symbol = valid_symbol(symbol)
        self._logic = frozen_logic
        self._api_key = api_key
        self._unit_provider = unit_provider
        self._persist = persist_result
        self._clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._session: aiohttp.ClientSession | None = None
        self._tasks: dict[int, asyncio.Task[None]] = {}
        self._signals: dict[int, C180Signal] = {}
        self._seen_starts: set[int] = set()
        self._closed = False

    async def start(self) -> None:
        if self._closed or self._session is not None:
            raise RuntimeError("C180 signal service already started or closed")
        self._session = aiohttp.ClientSession()

    async def close(self) -> None:
        self._closed = True
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._api_key = ""

    def on_frozen(self, evidence: FrozenEvidence) -> None:
        """Synchronous cutoff callback for ``C180EvidenceCollector``."""
        start = evidence.market_start_ms
        if self._closed or self._session is None or start in self._seen_starts:
            return
        # Results from old markets cannot authorize a new order. Bound the
        # service's memory during a long-running VM process.
        floor = start - 12 * 300_000
        self._seen_starts = {value for value in self._seen_starts if value >= floor}
        self._signals = {value: signal for value, signal in self._signals.items() if value >= floor}
        self._seen_starts.add(start)
        # /predict_amount may queue a different unit for the next Loop while
        # this Loop is RUNNING. Freeze the current durable unit with the T+120
        # evidence, before the JEV request can cross a Loop boundary.
        try:
            frozen_unit = Decimal(str(self._unit_provider()))
            if frozen_unit not in (Decimal("1"), Decimal("2"), Decimal("3")):
                frozen_unit = None
        except Exception:
            frozen_unit = None
        task = asyncio.create_task(
            self._process(evidence, frozen_unit), name=f"c180-jev-{start}"
        )
        self._tasks[start] = task
        task.add_done_callback(lambda _task, key=start: self._tasks.pop(key, None))

    def signal(self, market_start_ms: int) -> C180Signal | None:
        """Read only a successfully persisted result; absent means HOLD."""
        return self._signals.get(int(market_start_ms))

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        try:
            result = Decimal(str(value))
            return result if result.is_finite() else None
        except (ValueError, TypeError, ArithmeticError):
            return None

    @classmethod
    def _levels(cls, raw: Any) -> tuple[PriceLevel, ...]:
        if not isinstance(raw, list):
            return ()
        levels: list[PriceLevel] = []
        for item in raw:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                return ()
            price, shares = cls._decimal(item[0]), cls._decimal(item[1])
            if price is None or shares is None:
                return ()
            levels.append(PriceLevel(price, shares))
        return tuple(levels)

    async def _process(self, evidence: FrozenEvidence,
                       frozen_unit: Decimal | None) -> None:
        packet = evidence.packet
        market = packet.get("market") if isinstance(packet, Mapping) else None
        state = packet.get("state") if isinstance(packet, Mapping) else None
        quote = state.get("quote") if isinstance(state, Mapping) else None
        raw_book = evidence.raw_book
        topic = str(market.get("topic")) if isinstance(market, Mapping) and market.get("topic") else None
        market_id = str(market.get("market_id")) if isinstance(market, Mapping) and market.get("market_id") else None
        fee = self._decimal(market.get("fee_bps")) if isinstance(market, Mapping) else None

        async def finish(status: str, *, entry: C180EntryDecision | None = None,
                         p_up: Decimal | None = None, input_hash: str | None = None,
                         cost: Decimal | None = None) -> None:
            signal = C180Signal(evidence.market_start_ms, topic, market_id,
                                evidence.cutoff_ms, self._clock(), status,
                                entry, p_up, input_hash, cost, fee)
            # If persistence fails, this result is never exposed for a BUY.
            try:
                await self._persist(signal)
            except Exception as exc:
                # A write may have committed before its caller reported an
                # error. Never issue a second, conflicting record or expose
                # an in-memory BUY after that ambiguity.
                raise _PersistenceUnavailable from exc
            self._signals[evidence.market_start_ms] = signal

        try:
            # The frozen paper candidate passed spot/futures stale warnings to
            # Original JEV as data_warnings; it did not reject those packets.
            # Keep that signal rule while treating disconnected live sources
            # and evidence-persistence failures as hard admission faults.
            hard_reasons = tuple(reason for reason in evidence.reasons
                                 if reason not in {"spot_stale", "futures_stale"})
            if packet is None or hard_reasons or not isinstance(market, Mapping) or not isinstance(quote, Mapping):
                await finish("evidence_unusable:" + ",".join(hard_reasons))
                return
            if frozen_unit is None:
                await finish("order_unit_unavailable_at_cutoff")
                return
            session = self._session
            if session is None:
                return
            frozen_state = self._logic.model_state(packet, "Original")
            if self.symbol != "BTCUSDT":
                frozen_state["contract"]["symbol"] = self.symbol
            model = await infer_original_jev_p_up(
                session, api_key=self._api_key, frozen_state=frozen_state,
                now_ms=self._clock, **({"symbol": self.symbol} if self.symbol != "BTCUSDT" else {}),
            )
            if model.status != "ok" or model.p_up is None:
                await finish("model_" + model.status, input_hash=model.input_sha256,
                             cost=model.provider_cost_usdt)
                return
            up = quote.get("UP") if isinstance(quote.get("UP"), Mapping) else {}
            down = quote.get("DOWN") if isinstance(quote.get("DOWN"), Mapping) else {}
            book = raw_book if isinstance(raw_book, Mapping) else {}
            decision = decide_c180_favorite_entry(C180EntryInput(
                market_start_ms=evidence.market_start_ms,
                cutoff_ms=evidence.cutoff_ms,
                decision_at_ms=self._clock(),
                market_id=market_id,
                book_market_id=str(book.get("market_id")) if book.get("market_id") else None,
                market_identified_at_ms=int(market["identified_at"]) if market.get("identified_at") is not None else None,
                reference_price=self._decimal(market.get("reference")),
                fee_bps=fee,
                original_model_id=MODEL_ID,
                original_p_up=model.p_up,
                model_completed_at_ms=model.completed_at_ms,
                book_at_ms=int(quote["book_at_ms"]) if quote.get("book_at_ms") is not None else None,
                book_received_at_ms=int(quote["received_at"]) if quote.get("received_at") is not None else None,
                book_received_at_ms_secondary=int(book["received_at_ms"]) if book.get("received_at_ms") is not None else None,
                up_ask_levels=self._levels(up.get("ask_levels")),
                down_ask_levels=self._levels(down.get("ask_levels")),
                stake_usdt=frozen_unit,
            ))
            await finish("entry_" + decision.reason, entry=decision,
                         p_up=model.p_up, input_hash=model.input_sha256,
                         cost=model.provider_cost_usdt)
        except asyncio.CancelledError:
            raise
        except _PersistenceUnavailable:
            return
        except Exception as exc:
            # A malformed packet or a persistence failure cannot authorize a BUY.
            try:
                await finish("internal_error:" + type(exc).__name__)
            except _PersistenceUnavailable:
                pass


class _PersistenceUnavailable(RuntimeError):
    """Durable signal write was uncertain; this market must remain HOLD."""
