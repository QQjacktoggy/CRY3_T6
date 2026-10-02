"""Standalone, read-only C180 signal producer with an immutable SQLite outbox.

Run this in its own process so the frozen experiment's top-level ``logic`` and
``live`` imports cannot collide with the existing trading worker. It discovers
official Binance BTC five-minute markets, builds the frozen Original packet,
requests JEV once at T+120 seconds, and stores a signal for the worker to read.
It has no route for placing, cancelling, or redeeming orders.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from .c180_evidence_collector import C180EvidenceCollector
from .evidence_retention import EvidenceBudget, bounded_payload, require_free_space
from .c180_favorite import (
    C180EntryDecision, C180ExecutionInput, PriceLevel, recheck_c180_execution,
)
from .c180_signal_service import C180Signal, C180SignalService


LOGGER = logging.getLogger(__name__)
SLOT_MS = 300_000
PREOPEN_WARMUP_MS = 60_000


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _signal_json(value: C180Signal) -> str:
    return json.dumps(asdict(value), default=str, sort_keys=True,
                      separators=(",", ":"), ensure_ascii=False)


def _from_signal_json(raw: str) -> C180Signal:
    value = json.loads(raw)
    entry = value.get("entry")
    if entry is not None:
        entry = C180EntryDecision(
            action=entry["action"], reason=entry["reason"], side=entry["side"],
            stake_usdt=Decimal(entry["stake_usdt"]),
            expected_shares=(Decimal(entry["expected_shares"])
                             if entry["expected_shares"] is not None else None),
            cost_after_ev_usdt=(Decimal(entry["cost_after_ev_usdt"])
                                  if entry["cost_after_ev_usdt"] is not None else None),
        )
    return C180Signal(
        market_start_ms=int(value["market_start_ms"]),
        market_topic=value["market_topic"], market_id=value["market_id"],
        cutoff_ms=int(value["cutoff_ms"]), completed_at_ms=int(value["completed_at_ms"]),
        status=value["status"], entry=entry,
        original_p_up=(Decimal(value["original_p_up"])
                       if value["original_p_up"] is not None else None),
        original_input_sha256=value["original_input_sha256"],
        model_cost_usdt=(Decimal(value["model_cost_usdt"])
                         if value["model_cost_usdt"] is not None else None),
        frozen_fee_bps=(Decimal(value["frozen_fee_bps"])
                        if value["frozen_fee_bps"] is not None else None),
    )


class C180SignalStore:
    """Single-writer, immutable signal ledger; safe for concurrent worker reads."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        require_free_space(self.path)
        self.db = sqlite3.connect(self.path, timeout=10)
        self.budget = EvidenceBudget(self.db, self.path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.budget.prepare()
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS c180_signals ("
            "market_start_ms INTEGER PRIMARY KEY, market_topic TEXT, market_id TEXT, "
            "cutoff_ms INTEGER NOT NULL, completed_at_ms INTEGER NOT NULL, "
            "status TEXT NOT NULL, signal_json TEXT NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS c180_books ("
            "market_start_ms INTEGER PRIMARY KEY, market_topic TEXT NOT NULL, "
            "market_id TEXT NOT NULL, book_at_ms INTEGER NOT NULL, "
            "snapshot_json TEXT NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS c180_book_events ("
            "market_start_ms INTEGER NOT NULL, book_at_ms INTEGER NOT NULL, "
            "captured_at_ms INTEGER NOT NULL, snapshot_json TEXT NOT NULL, "
            "PRIMARY KEY(market_start_ms,book_at_ms))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS c180_recovery_outcomes ("
            "market_start_ms INTEGER PRIMARY KEY, status TEXT NOT NULL, "
            "detail_json TEXT NOT NULL, updated_at_ms INTEGER NOT NULL)"
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS c180_events_retention "
                        "ON c180_book_events(captured_at_ms)")
        self.db.commit()

    async def persist(self, value: C180Signal) -> None:
        """Commit before exposing a signal; conflicting rewrites fail closed."""

        raw = bounded_payload(_signal_json(value))
        self.budget.prepare()
        with self.db:
            self.db.execute(
                "INSERT INTO c180_signals VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(market_start_ms) DO NOTHING",
                (value.market_start_ms, value.market_topic, value.market_id,
                 value.cutoff_ms, value.completed_at_ms, value.status, raw),
            )
            stored = self.db.execute(
                "SELECT signal_json FROM c180_signals WHERE market_start_ms=?",
                (value.market_start_ms,),
            ).fetchone()
            if stored is None or stored[0] != raw:
                raise RuntimeError("conflicting immutable C180 signal")

    def get(self, market_start_ms: int) -> C180Signal | None:
        row = self.db.execute(
            "SELECT signal_json FROM c180_signals WHERE market_start_ms=?",
            (int(market_start_ms),),
        ).fetchone()
        return _from_signal_json(row[0]) if row else None

    def persist_book(self, snapshot: Mapping[str, Any]) -> None:
        """Expire quotes at 24h or row cap; preserve immutable signal/outcome audit."""

        start = int(snapshot["market_start_ms"])
        topic = str(snapshot["market_topic"])
        market_id = str(snapshot["market_id"])
        book_at = int(snapshot["book_at_ms"])
        raw = json.dumps(snapshot, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False)
        bounded_payload(raw)
        self.budget.prepare()
        with self.db:
            prior = self.db.execute(
                "SELECT market_topic,market_id,book_at_ms FROM c180_books "
                "WHERE market_start_ms=?", (start,),
            ).fetchone()
            if prior and (prior[0] != topic or prior[1] != market_id):
                raise RuntimeError("C180 execution book identity changed")
            if prior and book_at <= prior[2]:
                return
            self.db.execute(
                "INSERT INTO c180_books VALUES(?,?,?,?,?) "
                "ON CONFLICT(market_start_ms) DO UPDATE SET "
                "book_at_ms=excluded.book_at_ms,snapshot_json=excluded.snapshot_json",
                (start, topic, market_id, book_at, raw),
            )
            self.db.execute(
                "INSERT OR IGNORE INTO c180_book_events VALUES(?,?,?,?)",
                (start, book_at, int(snapshot["captured_at_ms"]), raw),
            )
            # Recovery examines at most 20 five-minute markets. Keep 24h of
            # raw books, independently of the durable signals and outcomes.
            # Runtime captures at most 121 quotes in each 12-second entry
            # window per five-minute market: 40k rows exceeds 24h of traffic.
            at = int(snapshot["captured_at_ms"])
            self.budget.prune('c180_book_events', 'captured_at_ms', at,
                              age_ms=86400000, rows=40000)
            self.budget.prune('c180_books', 'market_start_ms', at,
                              age_ms=86400000, rows=300)

    def book_events(self, start: int) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT snapshot_json FROM c180_book_events WHERE market_start_ms=? ORDER BY book_at_ms",
            (start,),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def recovery_outcome(self, start: int) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT detail_json FROM c180_recovery_outcomes WHERE market_start_ms=?", (start,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def persist_recovery_outcome(self, value: Mapping[str, Any]) -> None:
        raw = bounded_payload(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
        self.budget.prepare()
        with self.db:
            self.db.execute(
                "INSERT INTO c180_recovery_outcomes VALUES(?,?,?,?) "
                "ON CONFLICT(market_start_ms) DO UPDATE SET "
                "status=excluded.status,detail_json=excluded.detail_json,updated_at_ms=excluded.updated_at_ms",
                (int(value["market_start_ms"]), str(value["status"]), raw, _now_ms()),
            )

    def close(self) -> None:
        self.db.close()


def read_c180_signal(path: str | Path, market_start_ms: int) -> C180Signal | None:
    """Worker-side read of a durable signal; absent or unreadable means HOLD."""

    try:
        uri = Path(path).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
            row = db.execute(
                "SELECT signal_json FROM c180_signals WHERE market_start_ms=?",
                (int(market_start_ms),),
            ).fetchone()
        return _from_signal_json(row[0]) if row else None
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        return None


def read_c180_book(path: str | Path, market_start_ms: int) -> dict[str, Any] | None:
    """Worker-side read of the latest execution book; caller checks its age."""

    try:
        uri = Path(path).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
            row = db.execute(
                "SELECT snapshot_json FROM c180_books WHERE market_start_ms=?",
                (int(market_start_ms),),
            ).fetchone()
        value = json.loads(row[0]) if row else None
        return value if isinstance(value, dict) else None
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        return None


def read_c180_recovery_outcomes(path: str | Path, starts: list[int]) -> dict[int, dict[str, Any]]:
    """The trading gate reads only committed paper evidence; errors mean HOLD."""
    if not starts:
        return {}
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=2)) as db:
        marks = ",".join("?" for _ in starts)
        rows = db.execute(
            f"SELECT market_start_ms,detail_json FROM c180_recovery_outcomes WHERE market_start_ms IN ({marks})",
            starts,
        ).fetchall()
    return {int(start): json.loads(raw) for start, raw in rows}


def _official_winner(detail: Mapping[str, Any]) -> str | None:
    """Require one exact winner flag in the official binary outcome list."""
    winners = []
    for market in detail.get("markets", []):
        if not isinstance(market, Mapping):
            continue
        for outcome in market.get("outcomes", []):
            if isinstance(outcome, Mapping) and (outcome.get("winner") is True or outcome.get("isWinner") is True):
                side = str(outcome.get("name") or outcome.get("title") or "").upper()
                if side not in ("UP", "DOWN"):
                    return None
                winners.append(side)
    return winners[0] if len(winners) == 1 else None


def simulate_recovery_market(signal: C180Signal | None, books: list[Mapping[str, Any]],
                             *, market_start_ms: int, unit_usdt: Decimal) -> dict[str, Any]:
    """Replay the V1.1 executable quote path without assuming an exchange fill."""
    result: dict[str, Any] = {"market_start_ms": market_start_ms, "status": "UNKNOWN"}
    if signal is None or signal.market_start_ms != market_start_ms or signal.entry is None:
        result["reason"] = "signal_missing"
        return result
    if signal.entry.stake_usdt != unit_usdt:
        result["reason"] = "unit_mismatch"
        return result
    if signal.entry.action == "SKIP":
        result.update(status="SKIP", reason=signal.entry.reason)
        return result
    if not signal.is_entry or signal.entry.side is None:
        result["reason"] = "signal_unknown"
        return result
    ready, expiry = market_start_ms + 124_000, market_start_ms + 136_000
    stamps: list[int] = []
    seen = market_start_ms + 120_000
    for book in books:
        at = int(book.get("captured_at_ms") or 0)
        if not ready <= at <= expiry:
            continue
        if (book.get("market_topic") != signal.market_topic
                or book.get("market_id") != signal.market_id):
            result["reason"] = "book_identity_mismatch"
            return result
        stamps.append(at)
        quote = book.get("quote")
        selected = quote.get(signal.entry.side) if isinstance(quote, Mapping) else None
        raw_levels = selected.get("ask_levels") if isinstance(selected, Mapping) else None
        try:
            levels = tuple(PriceLevel(Decimal(str(row[0])), Decimal(str(row[1])))
                           for row in raw_levels) if raw_levels else None
            book_at = int(book["book_at_ms"])
            checked = recheck_c180_execution(C180ExecutionInput(
                entry_decision=signal.entry, market_start_ms=market_start_ms, at_ms=at,
                frozen_market_topic=signal.market_topic, current_market_topic=signal.market_topic,
                frozen_market_id=signal.market_id, current_market_id=signal.market_id,
                book_market_id=str(book["market_id"]), frozen_fee_bps=signal.frozen_fee_bps,
                current_fee_bps=Decimal(str(book["fee_bps"])), original_p_up=signal.original_p_up,
                book_at_ms=book_at, book_received_at_ms=int(book["received_at"]),
                book_received_at_ms_secondary=int(book["received_at_ms"]),
                last_seen_book_at_ms=seen, selected_ask_levels=levels,
                full_depth=book.get("full_depth") is True,
            ))
        except (TypeError, ValueError, KeyError, ArithmeticError):
            result["reason"] = "book_invalid"
            return result
        seen = max(seen, book_at)
        if checked.permitted and checked.expected_cash_usdt and checked.expected_shares:
            result.update(status="FILLED_PENDING", side=signal.entry.side,
                          cash_usdt=str(checked.expected_cash_usdt),
                          shares=str(checked.expected_shares),
                          fee_bps=str(signal.frozen_fee_bps),
                          filled_at_ms=at, limit_price=str(checked.worst_ask_limit))
            return result
    points = [ready, *stamps, expiry]
    if max(b - a for a, b in zip(points, points[1:])) <= 2000:
        result.update(status="NO_FILL", reason="no_executable_quote_with_complete_coverage")
    else:
        result["reason"] = "book_coverage_gap"
    return result


def read_selected_order_unit(prediction_db: str | Path) -> Decimal:
    """Read the worker's durable active unit, never its pending next-loop unit.

    The current schema has no order-unit field on ``prediction_loops``. The
    worker keeps ``prediction_selected_order_unit`` frozen during a RUNNING
    Loop and writes /predict_amount changes to ``prediction_pending_order_unit``.
    It activates pending only after the Loop ends. Missing or invalid selected
    state fails closed; the trading worker must also reject stake mismatches.
    """

    uri = Path(prediction_db).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
        row = db.execute(
            "SELECT config_value_json FROM prediction_runtime_config "
            "WHERE config_key='prediction_selected_order_unit'"
        ).fetchone()
    if row is None:
        raise RuntimeError("durable active Prediction order unit missing")
    data = json.loads(row[0])
    if not isinstance(data, Mapping):
        raise RuntimeError("durable active Prediction order unit invalid")
    try:
        selected = Decimal(str(data.get("order_unit_usdt")))
    except ArithmeticError:
        raise RuntimeError("durable active Prediction order unit invalid") from None
    if selected not in (Decimal("1"), Decimal("2"), Decimal("3")):
        raise RuntimeError("durable active Prediction order unit unsupported")
    return selected


def load_frozen_source(source_dir: str | Path) -> tuple[Any, Any]:
    """Load the verified immutable experiment in this standalone process."""

    source = Path(source_dir).resolve()
    manifest_path = source / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError("C180 frozen source manifest missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest.get("files")
    if manifest.get("version") != "c180-original-mix75-v1" or not isinstance(files, dict):
        raise RuntimeError("wrong frozen C180 source version")
    actual = {str(path.relative_to(source)).replace("\\", "/")
              for path in source.rglob("*.py")}
    if actual != set(files):
        raise RuntimeError("frozen C180 source inventory mismatch")
    for name, expected_sha in files.items():
        path = (source / name).resolve()
        if not path.is_relative_to(source) or not path.is_file():
            raise RuntimeError("frozen C180 source path invalid")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha:
            raise RuntimeError("frozen C180 source SHA256 mismatch")
    for name in ("logic", "live", "store", "original_logic", "candidate_engine"):
        if name in sys.modules:
            raise RuntimeError("frozen module name already loaded: " + name)
    sys.path.insert(0, str(source))
    logic = importlib.import_module("logic")
    live = importlib.import_module("live")
    live.verify_source()  # Recheck against the package's own verification.
    return logic, live


def _items(payload: Any) -> list[Mapping[str, Any]]:
    data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
    if isinstance(data, Mapping):
        data = data.get("marketTopics", [])
    return [item for item in data if isinstance(item, Mapping)] if isinstance(data, list) else []


class C180SignalRuntime:
    def __init__(
        self, *, logic: Any, live: Any, prediction_key: str,
        prediction_secret: str, jev_key: str, db_path: str | Path,
        prediction_db: str | Path,
        feature_db: str | Path | None = None,
    ) -> None:
        self.logic = logic
        self.live = live
        self.prediction_db = Path(prediction_db)
        self.feature_db = (Path(feature_db) if feature_db is not None else
                           self.prediction_db.parent / 'regime-target6/features.sqlite3')
        self.store = C180SignalStore(db_path)
        self.client = live.ReadOnlyClient(
            prediction_key, prediction_secret,
            transport=live.ReadOnlyTransport(), timeout=5,
        )
        self.signals = C180SignalService(
            frozen_logic=logic, api_key=jev_key,
            unit_provider=lambda: read_selected_order_unit(self.prediction_db),
            persist_result=self.store.persist,
        )
        self.evidence = C180EvidenceCollector(
            tape=logic.Tape(),
            feed_factory=lambda callback: live.Feeds(
                prediction_key, prediction_secret, callback=callback,
            ),
            on_raw_event=self._on_raw_event,
            on_frozen=self._on_frozen,
        )
        self._stop = asyncio.Event()
        self._last_list_ms = 0
        self._timeline: dict[int, str] = {}
        self._current_start: int | None = None
        self._current_identity: tuple[str, str, float, str] | None = None
        self._last_detail_ms = 0
        self._current_market: dict[str, Any] | None = None
        self._last_book_persist_ms = 0
        self._last_recovery_scan_ms = 0
        self._last_t65_shadow_scan_ms = 0
        self._started_ms = _now_ms()
        self._t67_store = None
        self._t67_selected = False
        self._t67_profile_checked_ms = 0
        self._t67_book_persist_ms = 0
        self._t67_last_error_ms = 0

    def _t67_active(self, now):
        if now-self._t67_profile_checked_ms >= 2000:
            from .regime_t67_evidence import selected_profile
            from .regime_t67_policy import PROFILE
            self._t67_profile_checked_ms = now
            self._t67_selected = None
            self._t67_selected_profile = None
            try:
                self._t67_selected_profile = selected_profile(self.prediction_db)
                self._t67_selected = self._t67_selected_profile in (PROFILE, 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1')
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError) as exc:
                if now-self._t67_last_error_ms >= 10000:
                    LOGGER.warning('Strategy selection unavailable: %s', type(exc).__name__)
                    self._t67_last_error_ms = now
        return self._t67_selected

    def _on_frozen(self, evidence):
        # T6.7 consumes public evidence, not a paid Original/JEV decision.
        try:
            active = self._t67_active(_now_ms())
            if active is None:
                return
            if active is not False and getattr(self, '_t67_selected_profile', None) not in ('regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1'):
                return
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
            return
        self.signals.on_frozen(evidence)

    def _t67_raw_event(self, event):
        from .regime_t67_evidence import EvidenceStore, evidence_path
        at = int(event['received_at'])
        if not self._t67_active(at):
            return
        if self._t67_store is None:
            self._t67_store = EvidenceStore(evidence_path(self.store.path))
        kind = event.get('kind')
        if kind in ('binance_spot_aggTrade', 'binance_futures_aggTrade'):
            source = 'spot' if kind == 'binance_spot_aggTrade' else 'futures'
            # Reconnect counters alone reset on process restart. Persist a
            # session epoch too, so old opening anchors cannot authorize a
            # model after the feed lost its continuity.
            generation = self._started_ms*1000000+self.evidence.trade_errors[source]
            self._t67_store.spot(event, generation)
            return
        market = self._current_market
        if kind != 'prediction_book' or market is None or at-self._t67_book_persist_ms < 100:
            return
        if not int(market['start']) <= at < int(market['end']):
            return
        raw = self.evidence.tape.books.get(str(market['market_id']))
        if not isinstance(raw, Mapping):
            return
        quote = self.logic.normalize_book(raw, market, at)
        if quote is None:
            return
        # Full source depth is validated before retaining enough cash depth
        # for every supported (1/2/3U) order, rather than an unbounded raw book.
        selected = {}
        for side in ('UP', 'DOWN'):
            levels, cash = [], Decimal(0)
            for price, quantity in quote[side]['ask_levels']:
                levels.append([str(price), str(quantity)])
                cash += Decimal(str(price))*Decimal(str(quantity))
                if cash >= Decimal('3.000001'):
                    break
            selected[side] = {'ask_levels': levels, 'ask': quote[side].get('ask'), 'bid': quote[side].get('bid')}
        self._t67_store.book(dict(
            market_start_ms=int(market['start']), market_topic=str(market['topic']), market_id=str(market['market_id']),
            fee_bps=market['fee_bps'], reference=str(market['reference']),
            reference_received_ms=int(market['identified_at']), captured_at_ms=at,
            book_at_ms=quote['book_at_ms'], received_at=quote['received_at'], received_at_ms=raw['received_at_ms'],
            full_depth=True, retained_cash_depth='3.000001', quote=selected))
        self._t67_book_persist_ms = at

    def _on_raw_event(self, event: Mapping[str, Any]) -> None:
        """Persist a fresh full-depth book only during the live entry window."""

        try:
            self._t67_raw_event(event)
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
            now = _now_ms()
            if now-self._t67_last_error_ms >= 10000:
                LOGGER.warning('T6.7 evidence unavailable: %s', type(exc).__name__)
                self._t67_last_error_ms = now

        if event.get("kind") != "prediction_book" or self._current_market is None:
            return
        market = self._current_market
        at = int(event["received_at"])
        start = int(market["start"])
        if not start + 124_000 <= at <= start + 136_000:
            return
        if at - self._last_book_persist_ms < 100:
            return
        raw = self.evidence.tape.books.get(str(market["market_id"]))
        if not isinstance(raw, Mapping):
            return
        quote = self.logic.normalize_book(raw, market, at)
        if quote is None:
            return
        snapshot = {
            "market_start_ms": start,
            "market_topic": str(market["topic"]),
            "market_id": str(market["market_id"]),
            "fee_bps": market["fee_bps"],
            "captured_at_ms": at,
            "book_at_ms": quote["book_at_ms"],
            "received_at": quote["received_at"],
            "received_at_ms": raw.get("received_at_ms"),
            "full_depth": True,
            "quote": quote,
        }
        self.store.persist_book(snapshot)
        self._last_book_persist_ms = at

    async def start(self) -> None:
        selected = read_selected_order_unit(self.prediction_db)
        LOGGER.info("C180 active order unit from durable worker state: %s USDT", selected)
        await self.signals.start()
        await self.evidence.start()
        self._started_ms = _now_ms()

    async def close(self) -> None:
        self._stop.set()
        await self.evidence.close()
        await self.signals.close()
        self.store.close()
        if self._t67_store is not None:
            self._t67_store.close()

    def stop(self) -> None:
        self._stop.set()

    async def _list_markets(self) -> list[Mapping[str, Any]]:
        payload = await asyncio.to_thread(
            self.client.list_prediction_markets,
            l1_category="crypto", l2_category="up-down", limit=100,
        )
        return _items(payload)

    async def _detail(self, topic: str) -> Mapping[str, Any]:
        payload = await asyncio.to_thread(self.client.get_market_detail, topic)
        data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
        if not isinstance(data, Mapping):
            raise ValueError("market detail is not a mapping")
        return data

    async def _accept(self, raw: Mapping[str, Any], now_ms: int) -> bool:
        if raw.get("symbol") != "BTCUSDT":
            return False
        market = self.live.MarketInfo.from_api(raw)
        if (market.end_time_ms - market.start_time_ms != SLOT_MS
                or not self.live.reversal5_orientation(market)
                or not market.start_time_ms <= now_ms < market.end_time_ms):
            return False
        if market.reference_price is None or market.reference_price <= 0:
            return False
        # The frozen Tape includes pre-open trades in its 60-second lookback.
        # A restarted sidecar must not build a seemingly valid packet from an
        # incomplete first market history.
        if self._started_ms > market.start_time_ms - PREOPEN_WARMUP_MS:
            return False
        meta = self.live.metadata(raw, now_ms)
        if meta["fee_bps"] is None:
            return False
        identity = (meta["topic"], meta["market_id"], meta["reference"], meta["yes"])
        if self._current_start == market.start_time_ms:
            if self._current_identity != identity:
                self.stop()
                raise RuntimeError("C180 official market identity changed")
            return True
        await self.evidence.switch_market(meta)
        self._current_start = market.start_time_ms
        self._current_identity = identity
        self._current_market = meta
        self._last_book_persist_ms = 0
        LOGGER.info("C180 official market selected: start_ms=%s topic=%s",
                    market.start_time_ms, meta["topic"])
        return True

    async def discover_once(self) -> None:
        now_ms = _now_ms()
        slot = now_ms // SLOT_MS * SLOT_MS
        if now_ms - self._last_list_ms >= 20_000:
            for raw in await self._list_markets():
                if raw.get("symbol") != "BTCUSDT":
                    continue
                for item in raw.get("timeline", ()):
                    if (isinstance(item, Mapping)
                            and type(item.get("startDate")) is int
                            and item.get("marketTopicId") is not None):
                        self._timeline[item["startDate"]] = str(item["marketTopicId"])
                try:
                    market = self.live.MarketInfo.from_api(raw)
                    if market.start_time_ms == slot and market.end_time_ms - slot == SLOT_MS:
                        self._timeline[slot] = str(market.market_topic_id)
                except (ValueError, TypeError, KeyError):
                    continue
            self._last_list_ms = _now_ms()
        topic = self._timeline.get(slot)
        if topic and (self._current_start != slot
                      or now_ms - self._last_detail_ms >= 20_000):
            detail = await self._detail(topic)
            await self._accept(detail, _now_ms())
            self._last_detail_ms = _now_ms()

    async def recovery_scan_once(self) -> None:
        """Settle at most one paper market per pass from post-halt evidence."""
        now = _now_ms()
        if self._t67_active(now) is not False:
            return
        if now - self._last_recovery_scan_ms < 20_000:
            return
        self._last_recovery_scan_ms = now
        uri = self.prediction_db.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as db:
            row = db.execute(
                "SELECT config_value_json FROM prediction_runtime_config "
                "WHERE config_key='c180_batch_gate_runtime_v1'"
            ).fetchone()
            if not row:
                return
            gate = json.loads(row[0])
            recovery = gate.get("recovery") or {}
            if not gate.get("recovery_hold_latched") or not recovery.get("start_run"):
                return
            loop = db.execute("SELECT state FROM prediction_loops WHERE loop_id=?",
                              (gate.get("loop_id"),)).fetchone()
            if not loop or loop[0] != "RUNNING":
                return
        anchor = int(gate["first_market_start_ms"])
        start_run = int(recovery["start_run"])
        latest_complete_run = (now - anchor) // SLOT_MS
        for run in range(start_run, min(latest_complete_run, start_run + 19) + 1):
            start = anchor + (run - 1) * SLOT_MS
            prior = self.store.recovery_outcome(start)
            if prior and prior["status"] not in ("FILLED_PENDING",):
                continue
            value = prior or simulate_recovery_market(
                self.store.get(start), self.store.book_events(start),
                market_start_ms=start, unit_usdt=Decimal(str(gate["unit_usdt"])),
            )
            if value["status"] == "FILLED_PENDING":
                signal = self.store.get(start)
                if signal is None or not signal.market_topic:
                    value = {"market_start_ms": start, "status": "UNKNOWN", "reason": "topic_missing"}
                else:
                    detail = await self._detail(signal.market_topic)
                    winner = _official_winner(detail)
                    if winner is not None:
                        cash = Decimal(value["cash_usdt"])
                        shares = Decimal(value["shares"])
                        fee = Decimal(value["fee_bps"]) / Decimal("10000")
                        pnl = (shares if winner == value["side"] else Decimal("0")) - cash * (1 + fee)
                        value.update(status="FILLED", winner=winner, net_pnl_usdt=str(pnl))
            self.store.persist_recovery_outcome(value)
            return

    async def t65_shadow_scan_once(self) -> None:
        """Resolve shadow-only markets without consuming the entry window."""
        now = _now_ms()
        if (119500 <= now % SLOT_MS <= 137500
                or now-self._last_t65_shadow_scan_ms < 20000
                or not self.feature_db.is_file()):
            return
        self._last_t65_shadow_scan_ms = now
        from .regime_feature_service import connect
        from .regime_t67a_shadow import resolve_once as resolve_t67a
        with closing(connect(self.feature_db)) as db:
            # Pending paper outcomes belong to their stored profile/loop.
            # Changing the admission profile must not strand that backlog.
            await resolve_t67a(db, now, self._detail)
            from .regime_t67b_shadow import resolve_once as resolve_t67b
            await resolve_t67b(db, now, self._detail)
            from .regime_t67c_shadow import resolve_once as resolve_t67c
            await resolve_t67c(db, now, self._detail)
            if self._t67_active(now) is False:
                from .regime_t65_shadow import resolve_outcome_once
                from .regime_t66_observer import resolve_once
                await resolve_outcome_once(db, now, self._detail)
                await resolve_once(db, now, self._detail)

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stop.is_set():
                try:
                    await self.discover_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.warning("C180 market discovery failed: %s", type(exc).__name__)
                try:
                    await self.recovery_scan_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.warning("C180 recovery evidence scan failed: %s", type(exc).__name__)
                try:
                    await self.t65_shadow_scan_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.warning('T6.5 shadow outcome scan failed: %s', type(exc).__name__)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=0.5)
                except TimeoutError:
                    pass
        finally:
            await self.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only C180 Original JEV signal producer")
    parser.add_argument("--frozen-source", type=Path, required=True)
    parser.add_argument("--signal-db", type=Path, required=True)
    parser.add_argument('--feature-db', type=Path)
    parser.add_argument("--shared-weight-db", type=Path, required=True)
    parser.add_argument("--prediction-db", type=Path, required=True,
                        help="Worker's live Prediction SQLite DB for the active order unit")
    parser.add_argument("--prediction-key-env", default="PREDICTION_BINANCE_API_KEY")
    parser.add_argument("--prediction-secret-env", default="PREDICTION_BINANCE_API_SECRET")
    parser.add_argument("--jev-key-env", default="OPENROUTER_API_KEY")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    prediction_key = os.environ.get(args.prediction_key_env, "")
    prediction_secret = os.environ.get(args.prediction_secret_env, "")
    jev_key = os.environ.get(args.jev_key_env, "")
    if not all((prediction_key, prediction_secret, jev_key)):
        raise SystemExit("C180 runtime credentials missing from environment")
    os.environ["PREDICTION_SHARED_WEIGHT_DB"] = str(args.shared_weight_db.resolve())
    logic, live = load_frozen_source(args.frozen_source)
    runtime = C180SignalRuntime(
        logic=logic, live=live, prediction_key=prediction_key,
        prediction_secret=prediction_secret, jev_key=jev_key,
        db_path=args.signal_db, prediction_db=args.prediction_db, feature_db=args.feature_db,
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, runtime.stop)
        except NotImplementedError:
            pass
    try:
        loop.run_until_complete(runtime.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
