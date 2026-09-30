from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import asdict
from typing import Any

import aiohttp

from .config import Settings
from .models import KlineState, MarkState, Quote, SourceHealth, Trade

logger = logging.getLogger(__name__)


class SymbolMarketState:
    def __init__(self, symbol: str, retention_seconds: float = 600.0) -> None:
        self.symbol = symbol.upper()
        self.retention_seconds = retention_seconds
        self.futures_trades: deque[Trade] = deque()
        self.spot_trades: deque[Trade] = deque()
        self.oi_history: deque[tuple[float, float]] = deque()
        self.futures_quote = Quote()
        self.spot_quote = Quote()
        self.mark = MarkState()
        self.futures_kline = KlineState()
        self.spot_kline = KlineState()
        self.last_futures_trade_at = 0.0
        self.last_spot_trade_at = 0.0

    def _prune(self, now: float) -> None:
        cutoff = now - self.retention_seconds
        while self.futures_trades and self.futures_trades[0].ts < cutoff:
            self.futures_trades.popleft()
        while self.spot_trades and self.spot_trades[0].ts < cutoff:
            self.spot_trades.popleft()
        while self.oi_history and self.oi_history[0][0] < cutoff:
            self.oi_history.popleft()

    def add_trade(self, source: str, trade: Trade) -> None:
        target = self.futures_trades if source == "futures" else self.spot_trades
        target.append(trade)
        if source == "futures":
            self.last_futures_trade_at = trade.ts
        else:
            self.last_spot_trade_at = trade.ts
        self._prune(trade.ts)

    def update_quote(self, source: str, quote: Quote) -> None:
        if source == "futures":
            self.futures_quote = quote
        else:
            self.spot_quote = quote

    def update_mark(self, mark: MarkState) -> None:
        self.mark = mark

    def update_kline(self, source: str, kline: KlineState) -> None:
        if source == "futures":
            self.futures_kline = kline
        else:
            self.spot_kline = kline

    def add_open_interest(self, ts: float, value: float) -> None:
        self.oi_history.append((ts, value))
        self._prune(ts)

    def snapshot(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "futures_trades": list(self.futures_trades),
            "spot_trades": list(self.spot_trades),
            "oi_history": list(self.oi_history),
            "futures_quote": asdict(self.futures_quote),
            "spot_quote": asdict(self.spot_quote),
            "mark": asdict(self.mark),
            "futures_kline": asdict(self.futures_kline),
            "spot_kline": asdict(self.spot_kline),
            "last_futures_trade_at": self.last_futures_trade_at,
            "last_spot_trade_at": self.last_spot_trade_at,
        }


class MarketDataHub:
    """Public Binance collector using one raw websocket per feed.

    v0.1.3 uses Binance's post-2026 USD-M websocket routing.
    Each (source, symbol, feed) has an independent raw stream. Futures
    bookTicker is routed through /public; aggTrade/kline/markPrice through
    /market. Legacy BINANCE_FUTURES_WS_BASE values ending in /ws or /stream
    are normalized automatically so existing .env files keep working.
    """

    FUTURES_FEEDS = {
        "aggTrade": lambda s: f"{s}@aggTrade",
        "bookTicker": lambda s: f"{s}@bookTicker",
        "kline": lambda s: f"{s}@kline_5m",
        "markPrice": lambda s: f"{s}@markPrice@1s",
    }
    SPOT_FEEDS = {
        "aggTrade": lambda s: f"{s}@aggTrade",
        "bookTicker": lambda s: f"{s}@bookTicker",
        "kline": lambda s: f"{s}@kline_5m",
    }
    FEED_NAMES = ("aggTrade", "bookTicker", "kline", "markPrice")

    def __init__(self, settings: Settings, session: aiohttp.ClientSession) -> None:
        self.settings = settings
        self.session = session
        self.states = {symbol: SymbolMarketState(symbol) for symbol in settings.symbols}
        self.health = {"open_interest": SourceHealth()}
        self.stream_health: dict[tuple[str, str, str], SourceHealth] = {}
        self.feed_counts: dict[str, dict[str, dict[str, int]]] = {
            symbol: {
                "futures": {name: 0 for name in self.FEED_NAMES},
                "spot": {name: 0 for name in self.FEED_NAMES},
            }
            for symbol in settings.symbols
        }
        self.last_feed_at: dict[str, dict[str, dict[str, float]]] = {
            symbol: {
                "futures": {name: 0.0 for name in self.FEED_NAMES},
                "spot": {name: 0.0 for name in self.FEED_NAMES},
            }
            for symbol in settings.symbols
        }
        self._ready_logged: set[tuple[str, str, str]] = set()
        self._tasks: list[asyncio.Task[Any]] = []
        self._stopping = asyncio.Event()

    def state(self, symbol: str) -> SymbolMarketState:
        key = symbol.upper()
        if key not in self.states:
            raise KeyError(f"Unsupported symbol: {symbol}")
        return self.states[key]

    async def start(self) -> None:
        self._stopping.clear()
        for symbol in self.settings.symbols:
            lower = symbol.lower()
            for feed, factory in self.FUTURES_FEEDS.items():
                stream = factory(lower)
                self._spawn_stream("futures", symbol, feed, stream)
            if not self.settings.disable_spot:
                for feed, factory in self.SPOT_FEEDS.items():
                    stream = factory(lower)
                    self._spawn_stream("spot", symbol, feed, stream)
        if not self.settings.disable_oi:
            self._tasks.append(asyncio.create_task(self._oi_loop(), name="binance-open-interest"))

    def _spawn_stream(self, source: str, symbol: str, feed: str, stream: str) -> None:
        key = (source, symbol, feed)
        self.stream_health[key] = SourceHealth()
        name = f"binance-{source}-{symbol}-{feed}"
        self._tasks.append(asyncio.create_task(self._raw_ws_loop(source, symbol, feed, stream), name=name))

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def _base_ws(self, source: str) -> str:
        base = self.settings.futures_ws_base if source == "futures" else self.settings.spot_ws_base
        base = base.rstrip("/")
        if "?streams=" in base:
            base = base.split("?streams=", 1)[0]
        if source == "futures":
            # Binance USD-M migrated on 2026-04-23 from the legacy
            # /ws and /stream roots to category-specific /public, /market
            # and /private roots. Strip either old or new suffixes so an
            # existing .env keeps working without manual migration.
            suffixes = (
                "/public/ws", "/public/stream",
                "/market/ws", "/market/stream",
                "/private/ws", "/private/stream",
                "/public", "/market", "/private",
                "/ws", "/stream",
            )
            changed = True
            while changed:
                changed = False
                for suffix in suffixes:
                    if base.endswith(suffix):
                        base = base[: -len(suffix)].rstrip("/")
                        changed = True
                        break
            return base

        if base.endswith("/stream"):
            base = base[: -len("/stream")] + "/ws"
        elif not base.endswith("/ws"):
            base += "/ws"
        return base

    def _raw_url(self, source: str, feed: str, stream: str) -> str:
        if source == "futures":
            root = self._base_ws("futures")
            category = "public" if feed == "bookTicker" else "market"
            return f"{root}/{category}/ws/{stream}"
        return f"{self._base_ws(source)}/{stream}"

    async def _raw_ws_loop(self, source: str, symbol: str, feed: str, stream: str) -> None:
        health = self.stream_health[(source, symbol, feed)]
        url = self._raw_url(source, feed, stream)
        backoff = self.settings.reconnect_min_seconds
        while not self._stopping.is_set():
            try:
                logger.info("connecting Binance %s raw stream symbol=%s feed=%s", source, symbol, feed)
                timeout = aiohttp.ClientWSTimeout(ws_receive=90.0, ws_close=10.0)
                async with self.session.ws_connect(
                    url,
                    heartbeat=20.0,
                    autoping=True,
                    timeout=timeout,
                    max_msg_size=2 * 1024 * 1024,
                ) as ws:
                    health.connected = True
                    health.last_error = None
                    backoff = self.settings.reconnect_min_seconds
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            health.last_message_at = time.time()
                            self._handle_feed_payload(source, symbol, feed, message.data)
                        elif message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED}:
                            break
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            raise ws.exception() or RuntimeError(f"{source}/{symbol}/{feed} websocket error")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health.last_error = f"{type(exc).__name__}: {exc}"
                health.reconnects += 1
                logger.warning(
                    "Binance stream disconnected source=%s symbol=%s feed=%s error=%s",
                    source,
                    symbol,
                    feed,
                    health.last_error,
                )
            finally:
                health.connected = False

            if self._stopping.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, self.settings.reconnect_max_seconds)

    def _mark_feed(self, symbol: str, source: str, feed: str, now: float) -> None:
        self.feed_counts[symbol][source][feed] += 1
        self.last_feed_at[symbol][source][feed] = now
        key = (symbol, source, feed)
        if key not in self._ready_logged:
            self._ready_logged.add(key)
            logger.info("WS READY symbol=%s source=%s feed=%s", symbol, source, feed)

    def _handle_feed_payload(self, source: str, expected_symbol: str, feed: str, raw: str) -> None:
        try:
            envelope = json.loads(raw)
            if not isinstance(envelope, dict):
                return
            data = envelope.get("data", envelope)
            if not isinstance(data, dict):
                return
            symbol = str(data.get("s") or expected_symbol).upper()
            if symbol != expected_symbol or symbol not in self.states:
                return
            state = self.states[symbol]
            now = time.time()

            if feed == "aggTrade":
                ts = float(data.get("T") or data.get("E") or now * 1000) / 1000.0
                state.add_trade(
                    source,
                    Trade(
                        ts=ts,
                        price=float(data["p"]),
                        qty=float(data["q"]),
                        taker_buy=not bool(data.get("m", False)),
                    ),
                )
            elif feed == "bookTicker":
                ts = float(data.get("E") or data.get("T") or now * 1000) / 1000.0
                state.update_quote(
                    source,
                    Quote(
                        ts=ts,
                        bid=float(data["b"]),
                        bid_qty=float(data["B"]),
                        ask=float(data["a"]),
                        ask_qty=float(data["A"]),
                    ),
                )
            elif feed == "markPrice":
                state.update_mark(
                    MarkState(
                        ts=float(data.get("E") or now * 1000) / 1000.0,
                        mark_price=float(data.get("p") or 0.0),
                        index_price=float(data.get("i") or 0.0),
                        funding_rate=float(data.get("r") or 0.0),
                        next_funding_time=float(data.get("T") or 0.0) / 1000.0,
                    )
                )
            elif feed == "kline":
                k = data.get("k") or {}
                state.update_kline(
                    source,
                    KlineState(
                        ts=float(data.get("E") or now * 1000) / 1000.0,
                        start_time=float(k.get("t") or 0.0) / 1000.0,
                        close_time=float(k.get("T") or 0.0) / 1000.0,
                        open=float(k.get("o") or 0.0),
                        high=float(k.get("h") or 0.0),
                        low=float(k.get("l") or 0.0),
                        close=float(k.get("c") or 0.0),
                        volume=float(k.get("v") or 0.0),
                        quote_volume=float(k.get("q") or 0.0),
                        taker_buy_volume=float(k.get("V") or 0.0),
                        trade_count=int(k.get("n") or 0),
                        closed=bool(k.get("x", False)),
                    ),
                )
            else:
                return
            self._mark_feed(symbol, source, feed, now)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.debug("ignored malformed source=%s symbol=%s feed=%s message: %s", source, expected_symbol, feed, exc)

    async def _oi_loop(self) -> None:
        health = self.health["open_interest"]
        backoff = self.settings.reconnect_min_seconds
        health.connected = True
        while not self._stopping.is_set():
            started = time.monotonic()
            any_success = False
            for symbol in self.settings.symbols:
                try:
                    url = f"{self.settings.futures_rest_base}/fapi/v1/openInterest"
                    timeout = aiohttp.ClientTimeout(total=min(5.0, self.settings.request_timeout_seconds + 2.0))
                    async with self.session.get(url, params={"symbol": symbol}, timeout=timeout) as response:
                        body = await response.text()
                        if response.status != 200:
                            raise RuntimeError(f"HTTP {response.status}: {body[:200]}")
                        payload = json.loads(body)
                        self.states[symbol].add_open_interest(time.time(), float(payload["openInterest"]))
                        any_success = True
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    health.last_error = f"{type(exc).__name__}: {exc}"
                    logger.debug("open-interest poll failed for %s: %s", symbol, health.last_error)

            if any_success:
                health.connected = True
                health.last_message_at = time.time()
                health.last_error = None
                backoff = self.settings.reconnect_min_seconds
            else:
                health.connected = False
                health.reconnects += 1
                backoff = min(backoff * 2.0, self.settings.reconnect_max_seconds)

            elapsed = time.monotonic() - started
            sleep_for = max(0.1, (self.settings.oi_poll_seconds if any_success else backoff) - elapsed)
            await asyncio.sleep(sleep_for)

    def symbol_feed_health(self, symbol: str) -> dict[str, Any]:
        now = time.time()
        symbol = symbol.upper()
        result: dict[str, Any] = {}
        for source in ("futures", "spot"):
            source_result: dict[str, Any] = {}
            for feed in self.FEED_NAMES:
                count = self.feed_counts[symbol][source][feed]
                last = self.last_feed_at[symbol][source][feed]
                health = self.stream_health.get((source, symbol, feed))
                source_result[feed] = {
                    "count": count,
                    "last_at": last or None,
                    "age_ms": int((now - last) * 1000) if last else None,
                    "connected": health.connected if health else False,
                    "reconnects": health.reconnects if health else 0,
                    "last_error": health.last_error if health else None,
                }
            result[source] = source_result
        state = self.states[symbol]
        result["rolling"] = {
            "futures_trades": len(state.futures_trades),
            "spot_trades": len(state.spot_trades),
            "open_interest_points": len(state.oi_history),
        }
        return result

    def health_dict(self) -> dict[str, Any]:
        now = time.time()
        source_summary: dict[str, Any] = {}
        for source in ("futures", "spot"):
            relevant = [
                h for (src, _symbol, _feed), h in self.stream_health.items() if src == source
            ]
            source_summary[f"{source}_ws"] = {
                "connected": bool(relevant) and all(h.connected for h in relevant),
                "streams_connected": sum(1 for h in relevant if h.connected),
                "streams_total": len(relevant),
                "last_message_at": max((h.last_message_at for h in relevant), default=0.0) or None,
                "reconnects": sum(h.reconnects for h in relevant),
                "last_errors": [h.last_error for h in relevant if h.last_error][-5:],
            }
            last = source_summary[f"{source}_ws"]["last_message_at"]
            source_summary[f"{source}_ws"]["age_ms"] = int((now - last) * 1000) if last else None
        oi = self.health["open_interest"]
        source_summary["open_interest"] = {
            "connected": oi.connected,
            "last_message_at": oi.last_message_at or None,
            "age_ms": int((now - oi.last_message_at) * 1000) if oi.last_message_at else None,
            "reconnects": oi.reconnects,
            "last_error": oi.last_error,
        }
        source_summary["symbols"] = {symbol: self.symbol_feed_health(symbol) for symbol in self.settings.symbols}
        return source_summary
