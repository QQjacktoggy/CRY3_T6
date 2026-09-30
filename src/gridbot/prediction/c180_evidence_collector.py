"""Live evidence adapter for the immutable C180 ``logic.Tape``.

The caller injects the frozen Tape and the frozen, full-depth book feed factory.
No experiment source or manifest is changed here.  This adapter only collects
public aggTrades and prediction book evidence; it has no order API.
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping


SLOT_MS = 300_000
C180_OFFSET_MS = 120_000
MAX_FREEZE_LAG_MS = 500
TRADE_URLS = {
    "spot": "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
    "futures": "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
}


@dataclass(frozen=True)
class FrozenEvidence:
    market_start_ms: int
    cutoff_ms: int
    captured_at_ms: int
    packet: Mapping[str, Any] | None
    raw_book: Mapping[str, Any] | None
    reasons: tuple[str, ...]

    @property
    def usable(self) -> bool:
        return self.packet is not None and not self.reasons


class C180EvidenceCollector:
    """Keep frozen Tape warm and capture one causal packet at start + 120 s.

    ``feed_factory(on_book_event)`` must create a frozen Reversal5Feeds wrapper
    whose callback emits the full-depth event shape used by frozen ``live.Feeds``:
    ``{'received_at': ms, 'kind': 'prediction_book', 'body':
    {'market_id': id, 'book_at_ms': ms, 'received_at_ms': ms,
    'bids_levels': [...], 'asks_levels': [...]}}``.

    ``switch_market`` receives frozen ``live.metadata`` output, with topic,
    market_id, start/end, reference, yes, fee_bps, and identified_at.  The
    caller owns catalog selection and verifies that metadata is official.
    """

    def __init__(
        self,
        *,
        tape: Any,
        feed_factory: Callable[[Callable[[Mapping[str, Any]], None]], Any],
        clock_ms: Callable[[], int] | None = None,
        on_raw_event: Callable[[Mapping[str, Any]], None] | None = None,
        on_frozen: Callable[[FrozenEvidence], None] | None = None,
    ) -> None:
        self.tape = tape
        self._feed_factory = feed_factory
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on_raw_event = on_raw_event
        # A caller may use this synchronous hook to schedule its async JEV
        # task immediately; no later worker tick is needed to notice cutoff.
        self._on_frozen = on_frozen
        self.feeds: Any | None = None
        self._session: Any | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._timer: asyncio.Task[Any] | None = None
        self._closed = False
        self._active: dict[str, Any] | None = None
        self._market_identity: dict[int, tuple[str, str, float, str]] = {}
        self._frozen: dict[int, FrozenEvidence] = {}
        self.trade_connected = {"spot": False, "futures": False}
        self.trade_last_received_ms: dict[str, int | None] = {"spot": None, "futures": None}
        self.trade_errors: dict[str, int] = {"spot": 0, "futures": 0}
        self.book_rejected = 0
        self._evidence_error: str | None = None

    async def start(self) -> None:
        if self._closed or self.feeds is not None:
            raise RuntimeError("collector already started or closed")
        import aiohttp

        self.feeds = self._feed_factory(self.ingest_book)
        self._session = aiohttp.ClientSession()
        for source, url in TRADE_URLS.items():
            task = asyncio.create_task(self._trade_loop(source, url), name=f"c180-{source}-aggtrade")
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._timer is not None:
            self._timer.cancel()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._timer is not None:
            await asyncio.gather(self._timer, return_exceptions=True)
        if self.feeds is not None:
            await self.feeds.close()
        if self._session is not None:
            await self._session.close()
        self._active = None

    async def switch_market(self, market: Mapping[str, Any]) -> None:
        """Revoke the old book owner, then select this market's full-depth book."""

        if self.feeds is None or self._closed:
            raise RuntimeError("collector not running")
        required = ("topic", "market_id", "start", "end", "reference", "yes", "fee_bps", "identified_at")
        if any(market.get(key) is None for key in required):
            raise ValueError("incomplete C180 market metadata")
        start = int(market["start"])
        end = int(market["end"])
        identified_at = int(market["identified_at"])
        if end - start != SLOT_MS or market["yes"] not in ("UP", "DOWN"):
            raise ValueError("wrong C180 market window or orientation")
        if float(market["reference"]) <= 0 or not 0 < identified_at <= start + C180_OFFSET_MS:
            raise ValueError("reference or identity unavailable by C180 cutoff")
        identity = (str(market["topic"]), str(market["market_id"]), float(market["reference"]), str(market["yes"]))
        old_identity = self._market_identity.get(start)
        if old_identity is not None and old_identity != identity:
            raise ValueError("market identity changed after selection")
        if self._active is not None and start < int(self._active["start"]):
            raise ValueError("market switch moved backward")

        self._capture_due(self._clock())
        if self._timer is not None:
            self._timer.cancel()
        self._active = None  # Reject late callbacks from the retiring book.
        await self.feeds.select(identity[1])
        self._market_identity[start] = identity
        self._active = dict(market)
        self._timer = asyncio.create_task(self._freeze_timer(start), name=f"c180-freeze-{start}")

    def ingest_book(self, event: Mapping[str, Any]) -> None:
        """Accept a validated full-depth callback from the frozen book feed."""

        if self._closed:
            return
        at = int(event.get("received_at") or self._clock())
        self._capture_due(at)
        active = self._active
        body = event.get("body")
        if (
            active is None
            or event.get("kind") != "prediction_book"
            or not isinstance(body, Mapping)
            or str(body.get("market_id")) != str(active["market_id"])
            or not at < int(active["end"])
        ):
            self.book_rejected += 1
            return
        # Frozen Tape and normalize_book enforce book exchange/receipt age,
        # full-depth validity, orientation, and crossed-book checks at cutoff.
        try:
            self._ingest(event)
        except Exception as exc:
            self._evidence_error = "book_ingest:" + type(exc).__name__
            self.book_rejected += 1

    def ingest_trade(self, source: str, body: Mapping[str, Any], *, received_at_ms: int | None = None) -> None:
        """Feed one public aggTrade using local receipt time, never event time."""

        if self._closed:
            return
        if source not in TRADE_URLS:
            raise ValueError("unknown C180 trade source")
        at = self._clock() if received_at_ms is None else int(received_at_ms)
        self._capture_due(at)
        event = {"received_at": at, "kind": f"binance_{source}_aggTrade", "body": dict(body)}
        self._ingest(event)
        self.trade_last_received_ms[source] = at

    def frozen_packet(self, market_start_ms: int) -> FrozenEvidence | None:
        """Return a previously captured packet; never reconstruct it later."""

        evidence = self._frozen.get(int(market_start_ms))
        return copy.deepcopy(evidence) if evidence is not None else None

    def health(self) -> dict[str, Any]:
        now = self._clock()
        active = self._active
        mid = str(active["market_id"]) if active else None
        book = self.tape.books.get(mid, {}) if mid else {}
        return {
            "active_start_ms": int(active["start"]) if active else None,
            "active_market_id": mid,
            "trade_connected": dict(self.trade_connected),
            "trade_last_received_ms": dict(self.trade_last_received_ms),
            "trade_errors": dict(self.trade_errors),
            "book_rejected": self.book_rejected,
            "evidence_error": self._evidence_error,
            "book_age_ms": now - int(book["book_at_ms"]) if book.get("book_at_ms") else None,
            "book_feed": dict(getattr(self.feeds, "health", {}) or {}),
        }

    def _ingest(self, event: Mapping[str, Any]) -> None:
        self.tape.ingest(event)
        if self._on_raw_event is not None:
            try:
                self._on_raw_event(event)
            except Exception as exc:
                self._evidence_error = "raw_persist:" + type(exc).__name__

    def _capture_due(self, at_ms: int) -> None:
        market = self._active
        if market is None:
            return
        start = int(market["start"])
        cutoff = start + C180_OFFSET_MS
        if start in self._frozen or at_ms < cutoff:
            return
        reasons: list[str] = []
        packet: Mapping[str, Any] | None = None
        raw_book: Mapping[str, Any] | None = None
        if at_ms > cutoff + MAX_FREEZE_LAG_MS:
            reasons.append("missed_cutoff")
        else:
            try:
                raw_book = copy.deepcopy(self.tape.books.get(str(market["market_id"])))
                packet = copy.deepcopy(self.tape.packet(market, cutoff))
                reasons.extend(str(reason) for reason in packet.get("reasons", ()))
            except Exception as exc:
                reasons.append("packet_error:" + type(exc).__name__)
        for source in TRADE_URLS:
            if not self.trade_connected[source]:
                reasons.append(source + "_disconnected")
        if self._evidence_error:
            reasons.append(self._evidence_error)
        feed_health = getattr(self.feeds, "health", {}) or {}
        if not feed_health.get("book_connected"):
            reasons.append("book_disconnected")
        evidence = FrozenEvidence(start, cutoff, at_ms, packet, raw_book, tuple(dict.fromkeys(reasons)))
        self._frozen[start] = evidence
        if self._on_frozen is not None:
            try:
                self._on_frozen(copy.deepcopy(evidence))
            except Exception as exc:
                self._evidence_error = "freeze_callback:" + type(exc).__name__
                self._frozen[start] = FrozenEvidence(
                    start, cutoff, at_ms, packet, raw_book,
                    tuple(dict.fromkeys((*reasons, self._evidence_error))),
                )

    async def _freeze_timer(self, start: int) -> None:
        cutoff = start + C180_OFFSET_MS
        try:
            await asyncio.sleep(max(0, (cutoff - self._clock()) / 1000))
            self._capture_due(self._clock())
        except asyncio.CancelledError:
            raise

    async def _trade_loop(self, source: str, url: str) -> None:
        import aiohttp

        while not self._closed:
            try:
                async with self._session.ws_connect(
                    url,
                    heartbeat=20,
                    timeout=aiohttp.ClientWSTimeout(ws_receive=15, ws_close=5),
                ) as ws:
                    self.trade_connected[source] = True
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            try:
                                body = json.loads(message.data)
                                self.ingest_trade(source, body)
                            except (TypeError, ValueError, KeyError):
                                self.trade_errors[source] += 1
                        elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception:
                self.trade_errors[source] += 1
            finally:
                self.trade_connected[source] = False
            if not self._closed:
                await asyncio.sleep(2)
