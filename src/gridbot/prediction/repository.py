"""Independent SQLite persistence for the Prediction trading domain.

This repository intentionally does not depend on ``src.gridbot.storage``.
That separation lets the Prediction service recover its own active campaigns
without opening, migrating, or mutating the legacy Futures database.
"""

from __future__ import annotations

import json
import asyncio
import functools
import inspect
import sqlite3
import time
import copy
import hashlib
import os
import subprocess
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import aiosqlite

from .models import (
    ActionType,
    Campaign,
    CampaignState,
    Fill,
    MarketInfo,
    OrderIntent,
    OrderSide,
    OutcomeSide,
    Position,
    QuoteSnapshot,
)


# Shared by the ordinary save and the atomic Regime entry claim.
CAMPAIGN_UPSERT_SQL = """INSERT INTO prediction_campaigns
               (campaign_id, loop_id, market_topic_id, market_id, slug,
                start_time_ms, end_time_ms, state, initial_outcome, hedge_used,
                profit_lock_used, loser_unwind_count, loser_unwind_shares,
                buy_count, order_attempts, initial_attempts, hedge_attempts,
                pending_intent_id, pending_unknown, hedged_at_ms, last_error,
                payload_json, created_at_ms, updated_at_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(campaign_id) DO UPDATE SET
                 loop_id=COALESCE(excluded.loop_id, prediction_campaigns.loop_id), market_topic_id=excluded.market_topic_id,
                 market_id=excluded.market_id, slug=excluded.slug,
                 start_time_ms=excluded.start_time_ms, end_time_ms=excluded.end_time_ms,
                 state=excluded.state, initial_outcome=excluded.initial_outcome,
                 hedge_used=excluded.hedge_used, profit_lock_used=excluded.profit_lock_used,
                 loser_unwind_count=excluded.loser_unwind_count,
                 loser_unwind_shares=excluded.loser_unwind_shares,
                 buy_count=excluded.buy_count, order_attempts=excluded.order_attempts,
                 initial_attempts=excluded.initial_attempts, hedge_attempts=excluded.hedge_attempts,
                 pending_intent_id=excluded.pending_intent_id,
                 pending_unknown=excluded.pending_unknown, hedged_at_ms=excluded.hedged_at_ms,
                 last_error=excluded.last_error, payload_json=excluded.payload_json,
                 updated_at_ms=excluded.updated_at_ms"""


MIGRATIONS_DIR = Path(__file__).parent / "migrations"
ACTIVE_STATES = {
    CampaignState.BOOTSTRAP.value,
    CampaignState.RECOVER.value,
    CampaignState.OBSERVE.value,
    CampaignState.INITIAL_PENDING.value,
    CampaignState.INITIAL_POSITION.value,
    CampaignState.PROFIT_LOCK.value,
    CampaignState.HEDGE_PENDING.value,
    CampaignState.HEDGED.value,
    CampaignState.WAIT_CONFIRM.value,
    CampaignState.UNWIND_LOSER.value,
    CampaignState.FINAL_HOLD.value,
    CampaignState.SETTLEMENT.value,
    CampaignState.PAUSED.value,
    CampaignState.SOFT_COOLDOWN.value,
    CampaignState.HARD_STOP.value,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (OutcomeSide, OrderSide, ActionType, CampaignState)):
        return value.value
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_load(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _d(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _int(value: Any, default: int = 0) -> int:
    return default if value is None or value == "" else int(value)


def _bool(value: Any) -> bool:
    return bool(int(value)) if isinstance(value, (int, str)) and str(value).isdigit() else bool(value)


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return _jsonable(value)
    raise TypeError("repository payload must be a mapping or supported dataclass")


class _RepositoryGate:
    """Task-reentrant gate; shared by execution and observation connections."""

    def __init__(self):
        self.lock = asyncio.Lock()
        self.owner = None


def _serialize_repository(cls):
    # aiosqlite serializes individual statements, not an entire async method.
    # Protect the complete transaction, including reads and commit/rollback.
    def wrap(method):
        @functools.wraps(method)
        async def guarded(self, *args, **kwargs):
            gate = self._operation_gate
            task = asyncio.current_task()
            if gate.owner is task:
                return await method(self, *args, **kwargs)
            async with gate.lock:
                gate.owner = task
                try:
                    return await method(self, *args, **kwargs)
                except BaseException:
                    # Cancellation must release SQLite's write transaction
                    # before another operation is admitted on this connection.
                    conn = self._conn
                    if conn is not None and conn.in_transaction:
                        await conn.rollback()
                    raise
                finally:
                    gate.owner = None
        return guarded
    for name, method in list(vars(cls).items()):
        if inspect.iscoroutinefunction(method):
            setattr(cls, name, wrap(method))
    return cls


@_serialize_repository
class PredictionRepository:
    """Async SQLite repository with idempotent writes and restart recovery."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._operation_gate = _RepositoryGate()
        self._conn: aiosqlite.Connection | None = None
        # Test/process-boundary hook.  It is intentionally opt-in and has no
        # effect in production; stateful tests use it to prove a failed DB
        # transaction cannot leave a half-recorded fill behind.
        self.failure_injection: str | None = None

    def _maybe_fail(self, stage: str) -> None:
        if self.failure_injection and self.failure_injection == stage:
            raise RuntimeError(f"injected repository failure at {stage}")

    async def initialize(self) -> None:
        if self._conn is not None:
            return
        if self.db_path.parent != Path(""):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None: caller owns BEGIN/COMMIT. Default aiosqlite
        # autobegins on DML, so a later explicit BEGIN raises
        # "cannot start a transaction within a transaction" and the worker
        # falsely hard-stops.
        self._conn = await aiosqlite.connect(str(self.db_path), timeout=30, isolation_level=None)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA busy_timeout=30000")
        await self._run_migrations()

    async def _run_migrations(self) -> None:
        conn = self._require_conn()
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS prediction_migrations "
            "(filename TEXT PRIMARY KEY, applied_at_ms INTEGER NOT NULL)"
        )
        rows = await conn.execute_fetchall("SELECT filename FROM prediction_migrations")
        applied = {str(row[0]) for row in rows}
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if path.name in applied:
                continue
            await conn.executescript(path.read_text(encoding="utf-8"))
            await conn.execute(
                "INSERT INTO prediction_migrations(filename, applied_at_ms) VALUES (?, ?)",
                (path.name, _now_ms()),
            )
            await conn.commit()

        # Older workers could increment a loop cursor after the terminal
        # market was already recorded, leaving completed > target.  Repair
        # that monotonic cursor on startup without changing any immutable
        # campaign, fill, settlement, or PnL rows.
        await conn.execute(
            "UPDATE prediction_loops SET completed=target, updated_at_ms=? WHERE completed > target",
            (_now_ms(),),
        )
        await conn.commit()

    def _require_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("PredictionRepository is not initialized")
        return self._conn

    @staticmethod
    async def _tx_fetchone(conn: aiosqlite.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Fetch one row on an explicit transaction (aiosqlite portable)."""

        cursor = await conn.execute(sql, params)
        row = await cursor.fetchone()
        await cursor.close()
        return row


    @staticmethod
    async def _begin(conn: aiosqlite.Connection, mode: str = "IMMEDIATE") -> None:
        """Reserve the writer before reading; never commit another operation."""
        if conn.in_transaction:
            raise RuntimeError("unexpected open Prediction transaction")
        # Every caller performs writes. Reserve the writer BEFORE its reads:
        # a deferred WAL read snapshot cannot upgrade after an observer commits
        # (SQLITE_BUSY_SNAPSHOT), even with a long busy_timeout.
        sql = "BEGIN IMMEDIATE" if mode.upper() == "IMMEDIATE" else "BEGIN"
        for attempt in range(3):
            try:
                await conn.execute(sql)
                return
            except sqlite3.OperationalError as exc:
                code = getattr(exc, "sqlite_errorcode", None)
                busy = (code is not None and (code & 0xff) in
                        (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))
                if not busy or attempt == 2 or conn.in_transaction:
                    raise
                # Only retry acquisition, before any transaction body or API
                # call. Never replay fills, submissions, or unknown orders.
                await asyncio.sleep(0.2 * (attempt + 1))

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    init = initialize

    async def start_loop(
        self,
        loop_id: str,
        target: int,
        *,
        mode: str | None = None,
        strategy_profile: str | None = None,
        market_symbol: str | None = None,
        market_unit: str | None = None,
        lane_mask: str = "",
        consume_pending_lane_mask: bool = False,
    ) -> dict[str, Any]:
        if market_symbol is not None:
            return await self.start_bound_loop(loop_id, target, mode=mode, strategy_profile=strategy_profile,
                                               market_symbol=market_symbol, market_unit=market_unit,
                                               lane_mask=lane_mask,
                                               consume_pending_lane_mask=consume_pending_lane_mask)
        if lane_mask:
            raise ValueError("lane mask requires a bound loop market")
        binding = await self.get_loop_market_binding(loop_id)
        if binding:
            return await self.start_bound_loop(loop_id, target, mode=mode, strategy_profile=strategy_profile,
                market_symbol=binding["symbol"], market_unit=binding["unit"],
                lane_mask=binding.get("lane_mask") or "")
        conn = self._require_conn()
        await self._begin(conn)
        bound_active = await self._tx_fetchone(conn, "SELECT 1 FROM prediction_loops l JOIN prediction_loop_market_bindings b ON l.loop_id=b.loop_id WHERE l.state='RUNNING' LIMIT 1")
        if bound_active:
            raise ValueError("another bound loop is running")
        now = _now_ms()
        normalized_mode = str(mode or "SHADOW").strip().upper() or "SHADOW"
        normalized_profile = str(strategy_profile or "").strip().lower()
        await self._execute(
            """INSERT INTO prediction_loops
               (loop_id,target,completed,state,mode,strategy_profile,created_at_ms,updated_at_ms)
               VALUES(?,?,0,'RUNNING',?,?,?,?)
               ON CONFLICT(loop_id) DO UPDATE SET
                 target=MAX(prediction_loops.target, excluded.target),
                 state=CASE WHEN prediction_loops.state='DONE'
                            THEN prediction_loops.state ELSE 'RUNNING' END,
                 mode=excluded.mode,
                 strategy_profile=CASE
                   WHEN prediction_loops.strategy_profile=''
                   THEN excluded.strategy_profile
                   ELSE prediction_loops.strategy_profile
                 END,
                 updated_at_ms=excluded.updated_at_ms""",
            (loop_id, int(target), normalized_mode, normalized_profile, now, now),
        )
        return await self._fetchone("SELECT * FROM prediction_loops WHERE loop_id=?", (loop_id,)) or {}

    async def get_loop_market_binding(self, loop_id):
        # lane_mask lives in its own immutable table (migration 029); '' = no row.
        if not await self._fetchone("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prediction_loop_lane_masks'"):
            row = await self._fetchone("SELECT * FROM prediction_loop_market_bindings WHERE loop_id=?", (loop_id,))
            return {**row, "lane_mask": ""} if row else None
        return await self._fetchone("""SELECT b.*, COALESCE(m.lane_mask,'') AS lane_mask
            FROM prediction_loop_market_bindings b
            LEFT JOIN prediction_loop_lane_masks m ON m.loop_id=b.loop_id
            WHERE b.loop_id=?""", (loop_id,))

    async def loop_market_local_clear(self):
        # Pre-migration DONE snapshots from an older closed loop are historical.
        # Current/post-migration exposure remains blocking. The worker MUST also
        # require official zero orders/positions before selecting/starting Live.
        if await self.get_active_campaign_metadata() or await self.load_unresolved_intents():
            return False
        orders = await self._fetchone("SELECT 1 FROM prediction_orders WHERE status NOT IN ('FILLED','CLOSED','CANCELED','CANCELLED','EXPIRED','FAILED','REJECTED') LIMIT 1")
        pending = await self._fetchone("SELECT 1 FROM prediction_campaigns WHERE pending_unknown=1 OR pending_intent_id IS NOT NULL LIMIT 1")
        positions = await self._fetchone("""SELECT 1 FROM prediction_position_snapshots p
            WHERE p.snapshot_id=(SELECT MAX(q.snapshot_id) FROM prediction_position_snapshots q WHERE q.campaign_id=p.campaign_id)
            AND (CAST(p.up_shares AS REAL)>0 OR CAST(p.down_shares AS REAL)>0)
            AND NOT EXISTS(SELECT 1 FROM prediction_settlements s WHERE s.campaign_id=p.campaign_id AND s.status='SETTLED')
            AND NOT EXISTS(
                SELECT 1 FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id
                WHERE c.campaign_id=p.campaign_id AND c.state='DONE'
                AND l.state IN ('DONE','CANCELLED','STOPPED') AND c.end_time_ms>0
                AND c.end_time_ms<(SELECT applied_at_ms FROM prediction_migrations WHERE filename='026_loop_market.sql')
                AND c.end_time_ms<(SELECT created_at_ms FROM prediction_loops ORDER BY created_at_ms DESC,rowid DESC LIMIT 1)
                AND c.loop_id<>(SELECT loop_id FROM prediction_loops ORDER BY created_at_ms DESC,rowid DESC LIMIT 1)
            ) LIMIT 1""")
        return not (orders or pending or positions)

    async def start_bound_loop(self, loop_id, target, *, mode, strategy_profile, market_symbol, market_unit,
                               lane_mask="", consume_pending_lane_mask=False):
        from .loop_market import PROFILES, symbol, execution_fingerprint, binding_fingerprint
        from .regime_t69a_lane_mask import to_text, normalize as normalize_lane_mask
        asset = symbol(market_symbol)
        if strategy_profile not in PROFILES or str(market_unit) not in ('1','2','3') or int(target) < 1:
            raise ValueError("invalid bound loop configuration")
        # Canonical stored text; raises for unknown tokens or a non-T6.9a mask.
        mask = to_text(lane_mask)
        fingerprint = execution_fingerprint(asset, strategy_profile, mask)
        conn = self._require_conn()
        await self._begin(conn)
        active = await self._tx_fetchone(conn, "SELECT loop_id FROM prediction_loops WHERE state='RUNNING' AND loop_id!=? LIMIT 1", (loop_id,))
        if active:
            raise ValueError("another loop is already running")
        old = await self._tx_fetchone(conn, "SELECT * FROM prediction_loops WHERE loop_id=?", (loop_id,))
        binding = await self.get_loop_market_binding(loop_id)
        if old:
            if old['state'] != 'RUNNING' or old['strategy_profile'] != strategy_profile or old['mode'] != str(mode).upper():
                raise ValueError("bound loop state/profile/mode changed")
            if binding:
                if (binding['symbol'] != asset or binding['unit'] != str(market_unit)
                        or binding['target'] != int(target) or (binding.get('lane_mask') or '') != mask
                        or binding['execution_fingerprint'] != fingerprint
                        or binding['execution_fingerprint'] != binding_fingerprint(binding)):
                    raise ValueError("bound loop identity immutable")
            elif asset != 'BTCUSDT' or mask:
                raise ValueError("legacy loop has no asset binding")
            await conn.commit()
            return dict(old)
        if not await self.loop_market_local_clear():
            raise ValueError("unresolved local exposure")
        if consume_pending_lane_mask:
            # Compare-and-clear in the creating transaction: the queued mask is
            # bound to exactly this loop, or the start fails and stays queued.
            row = await self._tx_fetchone(conn, "SELECT config_value_json FROM prediction_runtime_config WHERE config_key='prediction_pending_lane_mask'")
            queued = _json_load(row["config_value_json"], {}) if row else {}
            try:
                if not isinstance(queued, Mapping):
                    raise ValueError("lane mask invalid")
                queued_text = to_text(normalize_lane_mask(queued.get("mask") or ()))
            except ValueError:
                queued_text = None
            if queued_text != mask:
                await conn.rollback()
                raise ValueError("queued lane mask changed; start the loop again")
        now = _now_ms()
        await conn.execute("""INSERT INTO prediction_loops
            (loop_id,target,completed,state,mode,strategy_profile,created_at_ms,updated_at_ms)
            VALUES(?,?,0,'RUNNING',?,?,?,?)""", (loop_id,int(target),str(mode).upper(),strategy_profile,now,now))
        await conn.execute("INSERT INTO prediction_loop_market_bindings VALUES(?,?,?,?,?,?,?)",
            (loop_id,asset,strategy_profile,fingerprint,str(market_unit),int(target),now))
        if mask:
            await conn.execute("INSERT INTO prediction_loop_lane_masks(loop_id,lane_mask) VALUES(?,?)", (loop_id,mask))
        if consume_pending_lane_mask:
            await conn.execute("""INSERT INTO prediction_runtime_config(config_key, config_value_json, updated_at_ms)
               VALUES ('prediction_pending_lane_mask', ?, ?)
               ON CONFLICT(config_key) DO UPDATE SET
                 config_value_json=excluded.config_value_json, updated_at_ms=excluded.updated_at_ms""",
                (_json_dumps({"consumed_by": loop_id}), now))
        await conn.commit()
        return await self._fetchone("SELECT * FROM prediction_loops WHERE loop_id=?", (loop_id,))

    async def get_active_loop(self) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM prediction_loops WHERE state='RUNNING' ORDER BY updated_at_ms DESC LIMIT 1")

    async def get_latest_loop(self) -> dict[str, Any] | None:
        """Return the most recently created loop, regardless of state."""

        return await self._fetchone(
            "SELECT * FROM prediction_loops ORDER BY created_at_ms DESC, rowid DESC LIMIT 1"
        )

    async def get_campaign_execution_mode(self, campaign_id: str) -> str | None:
        """Durable execution identity; the current process mode is not provenance."""
        row = await self._fetchone(
            """SELECT l.mode,
                      EXISTS(SELECT 1 FROM prediction_order_intents WHERE campaign_id=c.campaign_id)
                      OR EXISTS(SELECT 1 FROM prediction_orders WHERE campaign_id=c.campaign_id)
                      OR EXISTS(SELECT 1 FROM prediction_fills WHERE campaign_id=c.campaign_id)
                      OR EXISTS(SELECT 1 FROM prediction_settlements WHERE campaign_id=c.campaign_id)
                      AS live_execution,
                      EXISTS(SELECT 1 FROM prediction_shadow_campaigns WHERE campaign_id=c.campaign_id)
                      AS shadow_execution
               FROM prediction_campaigns c
               LEFT JOIN prediction_loops l ON l.loop_id=c.loop_id
               WHERE c.campaign_id=?""", (str(campaign_id),),
        )
        if row is None:
            return None
        # Execution evidence also protects legacy loops whose migration mode
        # defaulted to SHADOW. Paper execution lives in separate shadow tables.
        if row['live_execution'] or str(row['mode'] or '').upper() == 'LIVE':
            return 'LIVE'
        if str(row['mode'] or '').upper() == 'SHADOW' or row['shadow_execution']:
            return 'SHADOW'
        return None

    async def get_active_campaign_metadata(self) -> list[dict[str, Any]]:
        """Return the durable ownership cursor for active campaigns.

        A campaign without a matching RUNNING loop is recoverable state, but
        it must not be silently mixed into a new One Run/Loop.  The worker
        uses this small metadata query before admitting new work.
        """

        return await self._fetchall(
            """SELECT c.campaign_id,c.loop_id,c.state,
                      CASE WHEN l.loop_id IS NULL THEN 0 ELSE 1 END AS loop_exists,
                      l.state AS loop_state
               FROM prediction_campaigns c
               LEFT JOIN prediction_loops l ON l.loop_id=c.loop_id
               WHERE c.state IN ("""
            + ",".join("?" for _ in ACTIVE_STATES)
            + ") AND c.campaign_id NOT LIKE '%::p3' AND c.campaign_id NOT LIKE '%::shadow::%' ORDER BY c.start_time_ms ASC",
            tuple(sorted(ACTIVE_STATES)),
        )

    async def cancel_loop(self, loop_id: str, *, reason: str = "OPERATOR_CANCEL") -> dict[str, Any] | None:
        """Close the cursor after the worker verifies orders and positions."""
        await self._execute(
            """UPDATE prediction_loops SET state='CANCELLED',new_entries_stopped=1,
               terminal_reason=?,updated_at_ms=? WHERE loop_id=? AND state='RUNNING'""",
            (str(reason or "OPERATOR_CANCEL"), _now_ms(), str(loop_id)),
        )
        return await self.get_loop(loop_id)

    async def stop_loop(self, loop_id: str, *, state: str = "STOPPED") -> dict[str, Any] | None:
        """Persist a non-running loop state without touching settlements."""

        safe_state = str(state or "STOPPED").upper()
        await self._execute(
            "UPDATE prediction_loops SET state=?, updated_at_ms=? WHERE loop_id=?",
            (safe_state, _now_ms(), str(loop_id)),
        )
        return await self.get_loop(loop_id)

    async def request_operator_stop(self, loop_id: str) -> dict[str, Any]:
        """Persist the reset/stop admission pause across a service restart."""
        await self._execute(
            "UPDATE prediction_loops SET new_entries_stopped=1, "
            "terminal_reason=CASE WHEN terminal_reason IS NULL OR terminal_reason='' "
            "THEN 'OPERATOR_STOP' ELSE terminal_reason END, updated_at_ms=? "
            "WHERE loop_id=? AND state='RUNNING'",
            (_now_ms(), str(loop_id)),
        )
        return await self.get_loop(loop_id) or {}

    async def resume_operator_stopped_loop(self, loop_id: str, *, allow_adaptive: bool = False) -> dict[str, Any]:
        """Clear only an operator pause after worker reconciliation/guards."""
        allowed = ('OPERATOR_STOP', 'ADAPTIVE_JUMP_STOP') if allow_adaptive else ('OPERATOR_STOP',)
        conn = self._require_conn()
        await self._begin(conn)
        try:
            row = await self._tx_fetchone(conn, "SELECT * FROM prediction_loops WHERE loop_id=?", (str(loop_id),))
            row = dict(row) if row else {}
            reason = str(row.get('terminal_reason') or '')
            if (not row or row.get('state') != 'RUNNING' or row.get('hard_stop_latched')
                    or (reason and reason not in allowed)):
                await conn.rollback()
                return {'resumed': False, 'reason': 'loop is not operator-resumable'}
            await conn.execute(
                "UPDATE prediction_loops SET new_entries_stopped=0, terminal_reason=NULL, updated_at_ms=? WHERE loop_id=?",
                (_now_ms(), str(loop_id)),
            )
            updated = await self._tx_fetchone(conn, "SELECT * FROM prediction_loops WHERE loop_id=?", (str(loop_id),))
            await conn.commit()
            return {'resumed': True, 'loop_id': str(loop_id), 'row': dict(updated)}
        except Exception:
            await conn.rollback()
            raise

    async def get_loop(self, loop_id: str | None) -> dict[str, Any] | None:
        if not loop_id:
            return None
        return await self._fetchone("SELECT * FROM prediction_loops WHERE loop_id=?", (str(loop_id),))

    async def reconcile_shadow_loop_completion(self, loop_id: str) -> dict[str, Any]:
        """Rebuild a loop cursor from immutable terminal rows.

        SHADOW settlements deliberately do not enter the live risk ledger.
        Counting their immutable campaign identities here keeps loop progress
        restart-safe without contaminating daily/live PnL or loss guards.
        """

        conn = self._require_conn()
        now = _now_ms()
        await self._begin(conn)
        try:
            row = await self._tx_fetchone(
                conn,
                """SELECT COUNT(*) FROM (
                       SELECT s.campaign_id
                       FROM prediction_shadow_settlements s
                       JOIN prediction_campaigns c ON c.campaign_id=s.campaign_id
                       WHERE c.loop_id=? AND s.status='SETTLED'
                       UNION
                       SELECT s.campaign_id
                       FROM prediction_settlements s
                       WHERE s.loop_id=? AND s.status='SETTLED'
                   ) AS terminal_campaigns""",
                (str(loop_id), str(loop_id)),
            )
            terminal_count = int(row[0] if row else 0)
            loop_row = await self._tx_fetchone(
                conn, "SELECT target FROM prediction_loops WHERE loop_id=?", (str(loop_id),)
            )
            target = int(loop_row[0] if loop_row and loop_row[0] is not None else terminal_count)
            completed = min(terminal_count, max(0, target))
            await conn.execute(
                """UPDATE prediction_loops
                   SET completed=?,
                       state=CASE WHEN ?>=target THEN 'DONE' ELSE state END,
                       updated_at_ms=?
                   WHERE loop_id=?""",
                (completed, completed, now, str(loop_id)),
            )
            result = await self._tx_fetchone(
                conn, "SELECT * FROM prediction_loops WHERE loop_id=?", (str(loop_id),)
            )
            await conn.commit()
            return dict(result) if result else {}
        except Exception:
            await conn.rollback()
            raise

    async def complete_loop_market(self, loop_id: str, campaign_id: str, *, net_pnl: Decimal = Decimal("0"), status: str = "SETTLED") -> bool:
        if str(status).upper() != "SETTLED":
            return False
        if str(campaign_id or "").endswith("::p3") or "::shadow::" in str(campaign_id or ""):
            return False
        conn = self._require_conn()
        now = _now_ms()
        await self._begin(conn)
        try:
            prior = await self._tx_fetchone(conn,
                "SELECT campaign_id FROM prediction_risk_ledger WHERE campaign_id=?",
                (campaign_id,),
            )
            changed = prior is None
            if changed:
                day = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
                await conn.execute(
                    """INSERT INTO prediction_risk_ledger
                       (ledger_id,loop_id,campaign_id,day,net_pnl,created_at_ms)
                       VALUES(?,?,?,?,?,?)""",
                    (f"loop:{loop_id}:{campaign_id}", loop_id, campaign_id, day, str(net_pnl), now),
                )
                await conn.execute(
                        "UPDATE prediction_loops SET completed=MIN(target, completed+1), updated_at_ms=? WHERE loop_id=? AND state='RUNNING' AND completed < target",
                    (now, loop_id),
                )
                # SQLite cannot add Decimal values; the exact text aggregate
                # is written below after reading all settled ledger rows.
            loop_row = await self._tx_fetchone(conn,
                "SELECT target,completed FROM prediction_loops WHERE loop_id=?",
                (loop_id,),
            )
            await conn.execute(
                "UPDATE prediction_loops SET state='DONE',updated_at_ms=? WHERE loop_id=? AND completed>=target",
                (now, loop_id),
            )
            await self._refresh_risk_aggregate_tx(conn, now_ms=now, loop_id=loop_id)
            await conn.commit()
            return changed
        except Exception:
            await conn.rollback()
            raise

    async def _refresh_risk_aggregate_tx(
        self,
        conn: aiosqlite.Connection,
        *,
        now_ms: int,
        loop_id: str | None = None,
    ) -> dict[str, Any]:
        """Recompute risk aggregates while the caller owns one transaction.

        Recomputing from terminal rows is intentionally a little more work
        than incrementing a runtime counter.  It makes duplicate settlement
        delivery and process restart harmless, and gives the risk state one
        auditable source of truth in SQLite.
        """

        zone = ZoneInfo("Asia/Taipei")
        local_now = datetime.fromtimestamp(now_ms / 1000, zone)
        day = local_now.date()
        day_start = int(datetime.combine(day, dt_time.min, tzinfo=zone).timestamp() * 1000)
        day_end = int((datetime.combine(day, dt_time.min, tzinfo=zone) + timedelta(days=1)).timestamp() * 1000)
        rows = await conn.execute_fetchall(
            "SELECT net_pnl, loop_id, settled_at_ms FROM prediction_settlements WHERE status='SETTLED'"
        )
        daily = sum(
            (_d(row[0]) for row in rows if day_start <= int(row[2]) < day_end),
            Decimal("0"),
        )
        selected_loop = None
        if loop_id is not None:
            selected_loop = sum((_d(row[0]) for row in rows if str(row[1] or "") == str(loop_id)), Decimal("0"))
            await conn.execute(
                "UPDATE prediction_loops SET net_pnl=?,updated_at_ms=? WHERE loop_id=?",
                (str(selected_loop), now_ms, loop_id),
            )
        ordered = sorted(rows, key=lambda row: int(row[2]), reverse=True)
        consecutive = 0
        for row in ordered:
            value = _d(row[0])
            if value < 0:
                consecutive += 1
            else:
                break
        prior_row = await self._tx_fetchone(conn,
            "SELECT config_value_json FROM prediction_runtime_config WHERE config_key='prediction_risk_state'"
        )
        prior = _json_load(prior_row[0], {}) if prior_row else {}
        if not isinstance(prior, Mapping):
            prior = {}
        prior_day = str(prior.get("day") or day.isoformat())
        latched = bool(prior.get("hard_stop_latched")) if prior_day == day.isoformat() else False
        profile_row = await self._tx_fetchone(
            conn,
            "SELECT config_value_json FROM prediction_runtime_config WHERE config_key='prediction_selected_strategy'",
        )
        profile_payload = _json_load(profile_row[0], {}) if profile_row else {}
        profile = ""
        if isinstance(profile_payload, Mapping):
            profile = str(profile_payload.get("profile") or "")
        elif isinstance(profile_payload, str):
            profile = profile_payload
        from src.gridbot.prediction.s3s5_pair import hard_stop_latch_thresholds

        daily_limit, consecutive_limit = hard_stop_latch_thresholds(profile)
        # C180 and Regime LIVE loops have their own durable entry risk gates.
        # Preserve an existing operator/unknown latch, but do not create a
        # legacy daily/consecutive latch for these bound LIVE loops. Regime
        # risk normalizes every settled trade by its claimed 1/2/3U stake.
        durable_risk_loop = None
        if loop_id is not None:
            durable_risk_loop = await self._tx_fetchone(
                conn,
                "SELECT strategy_profile,mode FROM prediction_loops WHERE loop_id=?",
                (str(loop_id),),
            )
        from .regime_live_ledger import RISK_PROFILES

        if not (durable_risk_loop and str(durable_risk_loop[0]).lower() in
                ("c180_favorite_hold_v1", *RISK_PROFILES)
                and str(durable_risk_loop[1]).upper() == "LIVE"):
            latched = latched or daily <= daily_limit or consecutive >= consecutive_limit
        state = {
            "day": day.isoformat(),
            "daily_net_pnl": str(daily),
            "loop_net_pnl": str(selected_loop if selected_loop is not None else _d(prior.get("loop_net_pnl"))),
            "consecutive_losses": consecutive,
            "hard_stop_latched": latched,
        }
        await conn.execute(
            """INSERT INTO prediction_runtime_config(config_key,config_value_json,updated_at_ms)
               VALUES('prediction_risk_state',?,?)
               ON CONFLICT(config_key) DO UPDATE SET
                 config_value_json=excluded.config_value_json,
                 updated_at_ms=excluded.updated_at_ms""",
            (_json_dumps(state), now_ms),
        )
        return state

    async def __aenter__(self) -> "PredictionRepository":
        await self.initialize()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def _fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        cursor = await self._require_conn().execute(sql, params)
        row = await cursor.fetchone()
        await cursor.close()
        return dict(row) if row else None

    async def _fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cursor = await self._require_conn().execute(sql, params)
        rows = await cursor.fetchall()
        await cursor.close()
        return [dict(row) for row in rows]

    async def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        conn = self._require_conn()
        cursor = None
        try:
            cursor = await conn.execute(sql, params)
            await conn.commit()
            return cursor.rowcount
        except Exception:
            await conn.rollback()
            raise
        finally:
            if cursor is not None:
                await cursor.close()

    # -- Campaigns -----------------------------------------------------

    @staticmethod
    def _campaign_values(campaign: Campaign | Mapping[str, Any], now_ms: int | None = None) -> tuple[Any, ...]:
        now = now_ms or _now_ms()
        if isinstance(campaign, Campaign):
            market = campaign.market
            position = campaign.position
            payload = _json_dumps(campaign)
            return (
                campaign.campaign_id,
                None,
                market.market_topic_id,
                market.market_id,
                market.slug,
                market.start_time_ms,
                market.end_time_ms,
                campaign.state.value,
                campaign.initial_outcome.value if campaign.initial_outcome else None,
                int(campaign.hedge_used),
                int(campaign.profit_lock_used),
                campaign.loser_unwind_count,
                str(campaign.loser_unwind_shares),
                campaign.buy_count,
                campaign.order_attempts,
                campaign.initial_attempts,
                campaign.hedge_attempts,
                campaign.pending_intent_id,
                int(campaign.pending_unknown),
                campaign.hedged_at_ms,
                campaign.last_error,
                payload,
                now,
                now,
            )
        data = _jsonable(campaign)
        market = data.get("market") if isinstance(data.get("market"), Mapping) else data
        position = data.get("position") if isinstance(data.get("position"), Mapping) else {}
        return (
            str(data["campaign_id"]),
            data.get("loop_id"),
            str(market.get("market_topic_id") or market.get("marketTopicId") or ""),
            str(market.get("market_id") or market.get("marketId") or ""),
            str(market.get("slug") or ""),
            _int(market.get("start_time_ms") or market.get("startTime")),
            _int(market.get("end_time_ms") or market.get("endTime")),
            str(data.get("state", CampaignState.OBSERVE.value)),
            data.get("initial_outcome"),
            int(bool(data.get("hedge_used"))),
            int(bool(data.get("profit_lock_used"))),
            _int(data.get("loser_unwind_count")),
            str(data.get("loser_unwind_shares", "0")),
            _int(data.get("buy_count")),
            _int(data.get("order_attempts")),
            _int(data.get("initial_attempts")),
            _int(data.get("hedge_attempts")),
            data.get("pending_intent_id"),
            int(bool(data.get("pending_unknown"))),
            data.get("hedged_at_ms"),
            data.get("last_error"),
            _json_dumps(data),
            _int(data.get("created_at_ms"), now),
            now,
        )

    async def save_campaign(self, campaign: Campaign | Mapping[str, Any], *, loop_id: str | None = None) -> None:
        values = list(self._campaign_values(campaign))
        if loop_id is not None:
            values[1] = loop_id
        await self._execute(
            CAMPAIGN_UPSERT_SQL,
            tuple(values),
        )

    upsert_campaign = save_campaign

    @staticmethod
    def _decode_campaign_row(row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = _json_load(result.pop("payload_json", None), {})
        for key in ("hedge_used", "profit_lock_used", "pending_unknown"):
            if key in result:
                result[key] = bool(result[key])
        return result

    async def get_campaign(self, campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone("SELECT * FROM prediction_campaigns WHERE campaign_id = ?", (campaign_id,))
        return self._decode_campaign_row(row) if row else None

    async def get_active_campaigns(self, *, now_ms: int | None = None) -> list[dict[str, Any]]:
        states = tuple(sorted(ACTIVE_STATES))
        placeholders = ",".join("?" for _ in states)
        rows = await self._fetchall(
            f"SELECT * FROM prediction_campaigns WHERE state IN ({placeholders}) AND campaign_id NOT LIKE '%::p3' AND campaign_id NOT LIKE '%::shadow::%' ORDER BY start_time_ms ASC",
            states,
        )
        return [self._decode_campaign_row(row) for row in rows]

    list_active_campaigns = get_active_campaigns

    @staticmethod
    def _campaign_from_payload(payload: Mapping[str, Any], row: Mapping[str, Any] | None = None) -> Campaign:
        market_data = payload.get("market") if isinstance(payload.get("market"), Mapping) else {}
        position_data = payload.get("position") if isinstance(payload.get("position"), Mapping) else {}
        row = row or {}
        market = MarketInfo(
            market_topic_id=str(market_data.get("market_topic_id") or row.get("market_topic_id") or ""),
            market_id=str(market_data.get("market_id") or row.get("market_id") or ""),
            slug=str(market_data.get("slug") or row.get("slug") or ""),
            start_time_ms=_int(market_data.get("start_time_ms"), _int(row.get("start_time_ms"))),
            end_time_ms=_int(market_data.get("end_time_ms"), _int(row.get("end_time_ms"))),
            reference_price=(_d(market_data["reference_price"]) if market_data.get("reference_price") is not None else None),
            up_token_id=market_data.get("up_token_id"),
            down_token_id=market_data.get("down_token_id"),
            vendor=str(market_data.get("vendor", "predict_fun")),
            chain_id=str(market_data.get("chain_id", "56")),
            raw=market_data.get("raw", {}),
            up_market_id=market_data.get("up_market_id") or market_data.get("upMarketId"),
            down_market_id=market_data.get("down_market_id") or market_data.get("downMarketId"),
            status=str(market_data.get("status", "OPEN")).upper(),
        )
        position = Position(
            up_shares=_d(position_data.get("up_shares")),
            down_shares=_d(position_data.get("down_shares")),
            up_cost=_d(position_data.get("up_cost")),
            down_cost=_d(position_data.get("down_cost")),
            realized_cash=_d(position_data.get("realized_cash")),
            fees=_d(position_data.get("fees")),
            up_initial_shares=_d(position_data.get("up_initial_shares")),
            down_initial_shares=_d(position_data.get("down_initial_shares")),
        )
        return Campaign(
            campaign_id=str(payload.get("campaign_id") or row.get("campaign_id") or ""),
            market=market,
            state=CampaignState(str(payload.get("state") or row.get("state") or CampaignState.OBSERVE.value)),
            position=position,
            initial_outcome=(OutcomeSide(str(payload["initial_outcome"])) if payload.get("initial_outcome") else None),
            hedge_used=bool(payload.get("hedge_used")),
            profit_lock_used=bool(payload.get("profit_lock_used")),
            loser_unwind_count=_int(payload.get("loser_unwind_count")),
            loser_unwind_shares=_d(payload.get("loser_unwind_shares")),
            buy_count=_int(payload.get("buy_count")),
            order_attempts=_int(payload.get("order_attempts")),
            initial_attempts=_int(payload.get("initial_attempts")),
            hedge_attempts=_int(payload.get("hedge_attempts")),
            pending_intent_id=payload.get("pending_intent_id"),
            pending_unknown=bool(payload.get("pending_unknown")),
            last_leader=(OutcomeSide(str(payload["last_leader"])) if payload.get("last_leader") else None),
            leader_flip_count=_int(payload.get("leader_flip_count")),
            hedged_at_ms=payload.get("hedged_at_ms"),
            last_error=payload.get("last_error"),
            initial_filled_at_ms=payload.get("initial_filled_at_ms"),
            fee_rate_bps=(_d(payload["fee_rate_bps"]) if payload.get("fee_rate_bps") is not None else None),
            fee_per_share=(_d(payload["fee_per_share"]) if payload.get("fee_per_share") is not None else None),
            prior_spot=(_d(payload["prior_spot"]) if payload.get("prior_spot") is not None else None),
            last_spot=(_d(payload["last_spot"]) if payload.get("last_spot") is not None else None),
            last_spot_at_ms=payload.get("last_spot_at_ms"),
            prior_leader=(OutcomeSide(str(payload["prior_leader"])) if payload.get("prior_leader") else None),
            leader_since_ms=_int(payload.get("leader_since_ms")),
            leader_quotes=_int(payload.get("leader_quotes")),
            reference_cross_count=_int(payload.get("reference_cross_count")),
            last_quote_at_ms=_int(payload.get("last_quote_at_ms")),
        )

    async def load_campaign(self, campaign_id: str) -> Campaign | None:
        row = await self._fetchone("SELECT * FROM prediction_campaigns WHERE campaign_id = ?", (campaign_id,))
        if not row:
            return None
        return self._campaign_from_payload(_json_load(row.get("payload_json"), {}), row)

    async def load_active_campaigns(self) -> list[Campaign]:
        rows = await self._fetchall(
            "SELECT * FROM prediction_campaigns WHERE state IN (" + ",".join("?" for _ in ACTIVE_STATES) + ") ORDER BY start_time_ms ASC",
            tuple(sorted(ACTIVE_STATES)),
        )
        return [self._campaign_from_payload(_json_load(row.get("payload_json"), {}), row) for row in rows]

    # -- Quotes --------------------------------------------------------

    @staticmethod
    def _quote_values(campaign_id: str, quote: QuoteSnapshot | Mapping[str, Any]) -> tuple[Any, ...]:
        data = _jsonable(quote)
        if isinstance(quote, QuoteSnapshot):
            get = lambda key, default=None: getattr(quote, key, default)
        else:
            get = lambda key, default=None: data.get(key, default)
        return (
            campaign_id,
            _int(get("observed_at_ms")),
            str(get("up_bid")) if get("up_bid") is not None else None,
            str(get("up_ask")) if get("up_ask") is not None else None,
            str(get("down_bid")) if get("down_bid") is not None else None,
            str(get("down_ask")) if get("down_ask") is not None else None,
            str(get("leader")) if get("leader") is not None else None,
            str(get("btc_spot")) if get("btc_spot") is not None else None,
            str(get("reference_price")) if get("reference_price") is not None else None,
            int(bool(get("feed_ok", False))),
            int(bool(get("flip_confirmed", False))),
            int(bool(get("btc_crossed_reference", False))),
            int(bool(get("stable_final", False))),
            _int(get("leader_duration_ms")),
            int(bool(get("reference_recross", False))),
            (_int(get("spot_observed_at_ms")) if get("spot_observed_at_ms") is not None else None),
            _json_dumps(quote),
        )

    async def save_quote(self, campaign_id: str, quote: QuoteSnapshot | Mapping[str, Any]) -> int:
        persist_start = _now_ms()
        value = _jsonable(quote)
        raw = dict(value.get('raw') or {})
        raw['persistence'] = {'started_at_ms': persist_start, 'version': 'observability-v1'}
        value['raw'] = raw
        values = self._quote_values(campaign_id, value)
        await self._execute(
            """INSERT INTO prediction_quotes
               (campaign_id, observed_at_ms, up_bid, up_ask, down_bid, down_ask,
                leader, btc_spot, reference_price, feed_ok, flip_confirmed,
                btc_crossed_reference, stable_final, leader_duration_ms,
                reference_recross, spot_observed_at_ms, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(campaign_id, observed_at_ms) DO UPDATE SET
                up_bid=excluded.up_bid, up_ask=excluded.up_ask,
                down_bid=excluded.down_bid, down_ask=excluded.down_ask,
                leader=excluded.leader, btc_spot=excluded.btc_spot,
                reference_price=excluded.reference_price, feed_ok=excluded.feed_ok,
                 flip_confirmed=excluded.flip_confirmed,
                 btc_crossed_reference=excluded.btc_crossed_reference,
                 leader_duration_ms=excluded.leader_duration_ms,
                 reference_recross=excluded.reference_recross,
                 spot_observed_at_ms=excluded.spot_observed_at_ms,
                 stable_final=excluded.stable_final, payload_json=excluded.payload_json
                WHERE COALESCE(json_extract(excluded.payload_json,'$.raw.observer_only'),0)=0""",
            values,
        )
        row = await self._fetchone(
            "SELECT quote_id FROM prediction_quotes WHERE campaign_id = ? AND observed_at_ms = ?",
            (campaign_id, values[1]),
        )
        if not row:
            raise RuntimeError("quote write did not produce a row")
        return int(row["quote_id"])

    insert_quote = save_quote

    async def get_latest_quote(self, campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM prediction_quotes WHERE campaign_id=? ORDER BY observed_at_ms DESC LIMIT 1",
            (campaign_id,),
        )
        if row:
            row["payload"] = _json_load(row.get("payload_json"), {})
        return row

    async def get_quotes(self, campaign_id: str, *, since_ms: int | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        if since_ms is None:
            return await self._fetchall(
                "SELECT * FROM prediction_quotes WHERE campaign_id = ? ORDER BY observed_at_ms ASC LIMIT ?",
                (campaign_id, max(1, int(limit))),
            )
        return await self._fetchall(
            "SELECT * FROM prediction_quotes WHERE campaign_id = ? AND observed_at_ms >= ? ORDER BY observed_at_ms ASC LIMIT ?",
            (campaign_id, int(since_ms), max(1, int(limit))),
        )

    # -- Campaign-scoped market continuity -----------------------------

    async def get_market_state(self, campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM prediction_market_state WHERE campaign_id=?",
            (campaign_id,),
        )
        if row:
            payload = _json_load(row.get("payload_json"), {})
            if isinstance(payload, Mapping):
                row["payload"] = payload
        return row

    async def save_market_state(self, campaign_id: str, state: Mapping[str, Any]) -> None:
        data = _jsonable(state)
        now = _now_ms()
        await self._execute(
            """INSERT INTO prediction_market_state
               (campaign_id,prior_spot,last_spot,last_spot_at_ms,prior_leader,
                last_leader,leader_since_ms,leader_quotes,cross_count,
                last_quote_at_ms,payload_json,updated_at_ms)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(campaign_id) DO UPDATE SET
                 prior_spot=excluded.prior_spot,last_spot=excluded.last_spot,
                 last_spot_at_ms=excluded.last_spot_at_ms,
                 prior_leader=excluded.prior_leader,last_leader=excluded.last_leader,
                 leader_since_ms=excluded.leader_since_ms,
                 leader_quotes=excluded.leader_quotes,cross_count=excluded.cross_count,
                 last_quote_at_ms=excluded.last_quote_at_ms,
                 payload_json=excluded.payload_json,updated_at_ms=excluded.updated_at_ms""",
            (
                campaign_id,
                str(data.get("prior_spot")) if data.get("prior_spot") is not None else None,
                str(data.get("last_spot")) if data.get("last_spot") is not None else None,
                data.get("last_spot_at_ms"),
                data.get("prior_leader"),
                data.get("last_leader"),
                _int(data.get("leader_since_ms")),
                _int(data.get("leader_quotes")),
                _int(data.get("cross_count")),
                _int(data.get("last_quote_at_ms")),
                _json_dumps(data),
                now,
            ),
        )

    # -- Adaptive router and rescue outbox ------------------------------

    async def get_adaptive_state(self, campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT route_json FROM prediction_adaptive_state WHERE campaign_id=?",
            (campaign_id,),
        )
        payload = _json_load(row.get("route_json") if row else None, None)
        return dict(payload) if isinstance(payload, Mapping) else None

    async def save_adaptive_state(self, campaign_id: str, state: Mapping[str, Any], *, updated_at_ms: int | None = None) -> None:
        await self._execute(
            """INSERT INTO prediction_adaptive_state(campaign_id,route_json,updated_at_ms)
               VALUES(?,?,?)
               ON CONFLICT(campaign_id) DO UPDATE SET
                 route_json=excluded.route_json,updated_at_ms=excluded.updated_at_ms""",
            (campaign_id, _json_dumps(state), int(updated_at_ms if updated_at_ms is not None else _now_ms())),
        )

    @staticmethod
    def _rescue_values(plan: Any, *, updated_at_ms: int) -> tuple[Any, ...]:
        return (
            str(plan.campaign_id), str(plan.client_order_id), int(plan.attempt), str(plan.side),
            str(plan.shares), str(plan.limit_price), str(plan.expected_vwap), int(plan.created_at_ms),
            int(plan.ttl_ms), str(plan.state), plan.order_id, str(plan.filled_shares),
            str(plan.filled_amount), (str(plan.remaining_shares) if plan.remaining_shares is not None else None),
            plan.error, int(plan.version), int(plan.book_timestamp), int(plan.quote_expiry_ms),
            str(plan.depth_summary), int(bool(plan.dispatch_claimed)), plan.intent_id,
            _json_dumps(sorted(str(item) for item in plan.seen_trade_ids)), int(updated_at_ms),
        )

    async def load_rescue_plan(self, campaign_id: str, client_order_id: str | None = None) -> Any | None:
        from .adaptive import RescuePlan, RescueState

        if client_order_id:
            row = await self._fetchone(
                "SELECT * FROM prediction_rescue_plans WHERE campaign_id=? AND client_order_id=?",
                (campaign_id, client_order_id),
            )
        else:
            row = await self._fetchone(
                "SELECT * FROM prediction_rescue_plans WHERE campaign_id=? ORDER BY attempt DESC LIMIT 1",
                (campaign_id,),
            )
        if not row:
            return None
        try:
            state = RescueState(str(row["state"]))
            side = OutcomeSide(str(row["side"]))
        except (KeyError, ValueError) as exc:
            raise RuntimeError(f"invalid durable rescue plan for {campaign_id}") from exc
        seen = _json_load(row.get("seen_trade_ids_json"), [])
        return RescuePlan(
            campaign_id=str(row["campaign_id"]), attempt=int(row["attempt"]),
            client_order_id=str(row["client_order_id"]), side=side,
            shares=_d(row["shares"]), limit_price=_d(row["limit_price"]),
            expected_vwap=_d(row["expected_vwap"]), created_at_ms=int(row["created_at_ms"]),
            ttl_ms=int(row["ttl_ms"]), state=state, order_id=row.get("order_id"),
            filled_shares=_d(row.get("filled_shares")), filled_amount=_d(row.get("filled_amount")),
            remaining_shares=(_d(row["remaining_shares"]) if row.get("remaining_shares") is not None else None),
            error=row.get("error"), version=int(row.get("version") or 1),
            book_timestamp=int(row.get("book_timestamp") or 0),
            quote_expiry_ms=int(row.get("quote_expiry_ms") or 0),
            depth_summary=str(row.get("depth_summary") or ""),
            seen_trade_ids={str(item) for item in seen} if isinstance(seen, list) else set(),
            dispatch_claimed=bool(row.get("dispatch_claimed")), intent_id=row.get("intent_id"),
        )

    async def save_rescue_plan(self, plan: Any, *, updated_at_ms: int | None = None) -> None:
        """Persist a rescue plan with an optimistic version CAS.

        The caller owns the in-memory plan and advances its version only after
        this transaction commits.  A stale writer rolls back completely.
        """
        conn = self._require_conn()
        now = int(updated_at_ms if updated_at_ms is not None else _now_ms())
        await self._begin(conn, "IMMEDIATE")
        try:
            existing = await self._tx_fetchone(
                conn,
                "SELECT version FROM prediction_rescue_plans WHERE campaign_id=? AND client_order_id=?",
                (plan.campaign_id, plan.client_order_id),
            )
            values = self._rescue_values(plan, updated_at_ms=now)
            if existing is None:
                await conn.execute(
                    """INSERT INTO prediction_rescue_plans
                       (campaign_id,client_order_id,attempt,side,shares,limit_price,expected_vwap,
                        created_at_ms,ttl_ms,state,order_id,filled_shares,filled_amount,remaining_shares,
                        error,version,book_timestamp,quote_expiry_ms,depth_summary,dispatch_claimed,
                        intent_id,seen_trade_ids_json,updated_at_ms)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
            else:
                expected_version = int(plan.version)
                if int(existing[0]) != expected_version:
                    raise RuntimeError(
                        f"rescue plan CAS conflict for {plan.campaign_id}/{plan.client_order_id}: "
                        f"expected {expected_version}, found {existing[0]}"
                    )
                next_version = expected_version + 1
                update_values = list(values[2:20] + (values[20], values[21], values[22]))
                update_values[13] = next_version  # version is index 15 in the full row
                assignments = "attempt=?,side=?,shares=?,limit_price=?,expected_vwap=?,created_at_ms=?,ttl_ms=?,state=?,order_id=?,filled_shares=?,filled_amount=?,remaining_shares=?,error=?,version=?,book_timestamp=?,quote_expiry_ms=?,depth_summary=?,dispatch_claimed=?,intent_id=?,seen_trade_ids_json=?,updated_at_ms=?"
                cur = await conn.execute(
                    f"UPDATE prediction_rescue_plans SET {assignments} WHERE campaign_id=? AND client_order_id=? AND version=?",
                    tuple(update_values) + (plan.campaign_id, plan.client_order_id, expected_version),
                )
                if cur.rowcount != 1:
                    await cur.close()
                    raise RuntimeError("rescue plan CAS update lost race")
                await cur.close()
                plan.version = next_version
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def get_intent_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
        return await self._fetchone(
            "SELECT * FROM prediction_order_intents WHERE client_order_id=?",
            (client_order_id,),
        )

    async def claim_rescue_plan(self, campaign_id: str, client_order_id: str, *, now_ms: int | None = None) -> dict[str, Any] | None:
        """Atomically claim one planned rescue and create its stable intent.

        A retry returns the existing intent.  No network call is made here;
        this is the durable outbox boundary immediately before dispatch.
        """
        conn = self._require_conn()
        now = int(now_ms if now_ms is not None else _now_ms())
        await self._begin(conn, "IMMEDIATE")
        try:
            row = await self._tx_fetchone(
                conn,
                "SELECT * FROM prediction_rescue_plans WHERE campaign_id=? AND client_order_id=?",
                (campaign_id, client_order_id),
            )
            if row is None:
                await conn.rollback()
                return None
            if bool(row["dispatch_claimed"]):
                result = await self._tx_fetchone(
                    conn,
                    "SELECT * FROM prediction_order_intents WHERE client_order_id=?",
                    (client_order_id,),
                )
                await conn.commit()
                return dict(result) if result else None
            if str(row["state"]) != "RESCUE_PLANNED":
                await conn.commit()
                return None
            expiry = int(row["quote_expiry_ms"] or 0)
            if expiry and now > expiry:
                await conn.commit()
                return None
            intent_id = str(row["intent_id"] or f"rescue-intent:{campaign_id}:{row['attempt']}")
            cur = await conn.execute(
                """UPDATE prediction_rescue_plans
                   SET dispatch_claimed=1,intent_id=?,version=version+1,updated_at_ms=?
                   WHERE campaign_id=? AND client_order_id=? AND state='RESCUE_PLANNED'
                     AND dispatch_claimed=0""",
                (intent_id, now, campaign_id, client_order_id),
            )
            if cur.rowcount != 1:
                await cur.close()
                await conn.rollback()
                return None
            await cur.close()
            await conn.execute(
                """INSERT INTO prediction_order_intents
                   (intent_id,campaign_id,action,outcome,order_side,amount,limit_price,created_at_ms,
                    ttl_ms,attempt,order_id,status,unknown,client_order_id,tier,payload_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT DO NOTHING""",
                (intent_id, campaign_id, "SELL_PROTECTIVE", row["side"], "SELL", row["shares"],
                 row["limit_price"], row["created_at_ms"], row["ttl_ms"], row["attempt"], None,
                 "PENDING", 0, client_order_id, None, _json_dumps({"source": "adaptive_rescue_outbox", "client_order_id": client_order_id})),
            )
            await conn.execute(
                "UPDATE prediction_campaigns SET pending_intent_id=?,pending_unknown=0,updated_at_ms=? WHERE campaign_id=?",
                (intent_id, now, campaign_id),
            )
            result = await self._tx_fetchone(
                conn, "SELECT * FROM prediction_order_intents WHERE client_order_id=?", (client_order_id,)
            )
            await conn.commit()
            return dict(result) if result else None
        except Exception:
            await conn.rollback()
            raise

    # -- Intents, orders, fills ---------------------------------------

    @staticmethod
    def _intent_values(intent: OrderIntent | Mapping[str, Any]) -> tuple[Any, ...]:
        data = _jsonable(intent)
        get = (lambda key, default=None: getattr(intent, key, default)) if isinstance(intent, OrderIntent) else (lambda key, default=None: data.get(key, default))
        action = get("action")
        outcome = get("outcome")
        order_side = get("order_side")
        return (
            str(get("intent_id")),
            str(get("campaign_id")),
            str(action),
            str(outcome),
            str(order_side),
            str(get("amount", "0")),
            str(get("limit_price", "0")),
            _int(get("created_at_ms")),
            _int(get("ttl_ms")),
            _int(get("attempt", 1), 1),
            get("order_id"),
            str(get("status", "PENDING")),
            int(bool(get("unknown", False))),
            get("client_order_id"),
            get("tier"),
            _json_dumps(intent),
        )

    async def create_intent(self, intent: OrderIntent | Mapping[str, Any]) -> dict[str, Any]:
        values = self._intent_values(intent)
        await self._execute(
            """INSERT INTO prediction_order_intents
               (intent_id, campaign_id, action, outcome, order_side, amount,
                 limit_price, created_at_ms, ttl_ms, attempt, order_id, status,
                 unknown, client_order_id, tier, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT DO NOTHING""",
            values,
        )
        row = await self._fetchone("SELECT * FROM prediction_order_intents WHERE intent_id = ?", (values[0],))
        if row is None:
            # A retry can carry a new process-generated id but the same
            # campaign/action/attempt.  The composite key is the idempotency
            # boundary that prevents a duplicate order intent.
            row = await self._fetchone(
                "SELECT * FROM prediction_order_intents WHERE campaign_id = ? AND action = ? AND attempt = ?",
                (values[1], values[2], values[9]),
            )
        if not row:
            raise RuntimeError("intent write did not produce a row")
        return row

    async def save_campaign_and_intent(self, campaign: Campaign | Mapping[str, Any], intent: OrderIntent | Mapping[str, Any], *, loop_id: str | None = None) -> None:
        """Atomic pre-network persistence boundary for campaign + intent."""
        conn = self._require_conn()
        campaign_values = list(self._campaign_values(campaign))
        if loop_id is not None:
            campaign_values[1] = loop_id
        intent_values = self._intent_values(intent)
        await conn.execute("INSERT INTO prediction_campaigns (campaign_id, loop_id, market_topic_id, market_id, slug, start_time_ms, end_time_ms, state, initial_outcome, hedge_used, profit_lock_used, loser_unwind_count, loser_unwind_shares, buy_count, order_attempts, initial_attempts, hedge_attempts, pending_intent_id, pending_unknown, hedged_at_ms, last_error, payload_json, created_at_ms, updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(campaign_id) DO UPDATE SET loop_id=COALESCE(excluded.loop_id,prediction_campaigns.loop_id), pending_intent_id=excluded.pending_intent_id, pending_unknown=excluded.pending_unknown, order_attempts=excluded.order_attempts, payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms", tuple(campaign_values))
        try:
            await conn.execute("INSERT INTO prediction_order_intents (intent_id, campaign_id, action, outcome, order_side, amount, limit_price, created_at_ms, ttl_ms, attempt, order_id, status, unknown, client_order_id, tier, payload_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(intent_id) DO NOTHING", intent_values)
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def has_market_buy(self, campaign_id: str) -> bool:
        """Any previous BUY attempt consumes this profile's market slot.

        Cancelled/unknown/partially filled orders retain ownership. A completed
        zero-fill cancellation does not silently authorize a different strategy.
        """
        row = await self._fetchone(
            "SELECT 1 FROM prediction_order_intents WHERE campaign_id=? AND order_side='BUY' "
            "UNION ALL SELECT 1 FROM prediction_fills WHERE campaign_id=? AND order_side='BUY' LIMIT 1",
            (campaign_id, campaign_id),
        )
        return row is not None

    async def market_entry_tier(self, campaign_id: str) -> str | None:
        """Attribute a held market from durable BUY ownership, including restart."""
        rows = await self._fetchall(
            "SELECT DISTINCT tier FROM prediction_order_intents WHERE campaign_id=? AND order_side='BUY'",
            (campaign_id,),
        )
        if len(rows) != 1:
            return None
        return rows[0].get("tier")

    async def reserve_s3s5_intent(self, intent: OrderIntent) -> bool:
        """Atomically consume the one market slot BEFORE any order submission.

        One INSERT/SELECT is the SQLite concurrency boundary, even for two
        independent connections. A crash after this commit leaves a recoverable
        PENDING intent. Retrying the same id also returns False (never resend).
        """
        if intent.action is not ActionType.BUY_INITIAL or intent.order_side is not OrderSide.BUY:
            raise ValueError("s3s5 allows only one BUY_INITIAL")
        if intent.tier not in {"R3", "FAV", "LATE_SNIPER"} or intent.amount not in {Decimal("1"), Decimal("2"), Decimal("3")}:
            raise ValueError("s3s5 requires an attributed 1/2/3U entry")
        from src.gridbot.prediction import s3s5_pair as s3
        if intent.tier != "LATE_SNIPER" and await self.get_runtime_config(s3.fav_exclusion_key(intent.campaign_id), False):
            raise ValueError("s3s5 market excluded by FAV guard")
        if intent.tier == "FAV":
            proof = await self.get_runtime_config(s3.fav_approval_key(intent.campaign_id), None)
            # Bind the approval to the campaign's durable loop identity, not
            # the validator's legacy default or the proof's self-declared lane.
            owner = await self._fetchone(
                "SELECT l.strategy_profile FROM prediction_campaigns c "
                "JOIN prediction_loops l ON l.loop_id=c.loop_id WHERE c.campaign_id=?",
                (intent.campaign_id,),
            )
            profile = str((owner or {}).get("strategy_profile") or s3.PROFILE).strip().lower()
            if not s3.fav_approval_valid(proof, intent.created_at_ms, intent.outcome.value, intent.limit_price, profile=profile):
                raise ValueError("FAV requires a fresh distance approval")
        conn = self._require_conn()
        cursor = await conn.execute(
            """INSERT INTO prediction_order_intents
               (intent_id,campaign_id,action,outcome,order_side,amount,limit_price,
                created_at_ms,ttl_ms,attempt,order_id,status,unknown,client_order_id,tier,payload_json)
               SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
               WHERE NOT EXISTS (SELECT 1 FROM prediction_order_intents WHERE campaign_id=? AND order_side='BUY')
                 AND NOT EXISTS (SELECT 1 FROM prediction_fills WHERE campaign_id=? AND order_side='BUY')
               ON CONFLICT DO NOTHING""",
            self._intent_values(intent) + (intent.campaign_id, intent.campaign_id),
        )
        claimed = cursor.rowcount == 1
        await cursor.close()
        await conn.commit()
        return claimed

    async def load_unresolved_intents(self) -> list[dict[str, Any]]:
        return await self._fetchall("SELECT * FROM prediction_order_intents WHERE status NOT IN ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED','REJECTED') OR unknown = 1 ORDER BY created_at_ms")

    async def get_order_cumulative(self, order_id: str) -> dict[str, Any] | None:
        return await self.get_order(order_id)

    insert_intent = create_intent
    upsert_intent = create_intent


    async def reconcile_s3s5_book(self, payload: Mapping[str, Any], *, campaign_id: str | None = None) -> dict[str, Any]:
        """Build idempotent actual/paper rows without changing settlement or hard-stop state.

        Existing paper scores are retained separately. Missing engine history is
        never reconstructed as a zero. SQL stays scoped to this strategy/epoch.
        """
        from . import s3s5_pair as s3
        result = copy.deepcopy(dict(payload))
        rows = {int(x['start_ms']): dict(x) for x in result.get('markets', [])}
        params = [s3.PROFILE, s3.EPOCH_MS]
        where = ''
        if campaign_id is not None:
            where = ' AND c.campaign_id=?'
            params.append(campaign_id)
        campaigns = await self._fetchall(
            "SELECT c.campaign_id,c.start_time_ms,c.end_time_ms,c.state,c.pending_intent_id,c.pending_unknown,c.buy_count "
            "FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id "
            "WHERE l.strategy_profile=? AND c.start_time_ms>=?" + where, tuple(params))
        for c in campaigns:
            start = int(c['start_time_ms'])
            intents = await self._fetchall('SELECT status,unknown FROM prediction_order_intents WHERE campaign_id=?',(c['campaign_id'],))
            terminal = {'FILLED','CLOSED','CANCELED','CANCELLED','EXPIRED','FAILED','REJECTED'}
            pending = bool(c['pending_intent_id'] or c['pending_unknown'] or any(x['unknown'] or str(x['status']).upper() not in terminal for x in intents))
            fills = await self._fetchone("SELECT COUNT(*) n FROM prediction_fills WHERE campaign_id=? AND order_side='BUY' AND CAST(shares AS REAL)>0",(c['campaign_id'],))
            taken = bool(fills and fills['n']) or bool(c['buy_count'])
            settlement = await self.get_settlement(c['campaign_id'])
            ended = c['state'] in {'DONE','CANCELLED'} and _now_ms() >= int(c['end_time_ms'])
            live = None
            winner = str((settlement or {}).get('winner') or '').upper()
            settlement_status = str((settlement or {}).get('status') or '').upper()
            resolved_pending = settlement_status == 'CLOSED_PENDING_REDEEM' and winner in {'UP','DOWN','DRAW'}
            if not pending and ended:
                if settlement and (settlement_status == 'SETTLED' or resolved_pending) and settlement.get('net_pnl') is not None:
                    live = str(settlement['net_pnl'])
                elif not taken:
                    live = '0'
            # Economic settlement PnL is separate from claim/withdrawal status.
            # A known resolved payout awaiting redemption is not an open order.
            old = rows.get(start, {})
            old.update(start_ms=start,batch=s3.batch_of(start),accounting_v2=True,
                outcome=winner if winner in {'UP','DOWN','DRAW'} else old.get('outcome'),
                censored=False,taken=taken,live_pnl=live,
                window_status='ended' if ended else 'monitoring',
                execution_status='pending' if pending else ('filled' if taken else 'no_fill'),
                live_pnl_status=('resolved_pending_redemption' if resolved_pending else 'settlement_record') if live is not None and taken else ('confirmed_no_trade' if live is not None else 'unknown'),
                redemption_status='pending' if resolved_pending else 'not_pending_or_unknown')
            old.setdefault('pnl',None)
            rows[start]=old
        result['markets']=[rows[k] for k in sorted(rows)]
        result['accounting_version']=2
        return result

    async def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM prediction_order_intents WHERE intent_id = ?", (intent_id,))

    async def update_intent(self, intent_id: str, **updates: Any) -> None:
        allowed = {"order_id", "status", "unknown", "payload_json", "cancel_requested", "cancel_requested_at_ms", "client_order_id", "tier", "submission_at_ms", "ttl_deadline_ms", "cancel_attempt_count", "cancel_in_flight", "cancel_last_attempt_at_ms", "cancel_accepted_at_ms", "cancel_last_error"}
        fields = [(key, value) for key, value in updates.items() if key in allowed]
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key, _ in fields)
        params = tuple(
            int(bool(value)) if key in {"unknown", "cancel_requested"} else (_json_dumps(value) if key == "payload_json" and not isinstance(value, str) else value)
            for key, value in fields
        ) + (intent_id,)
        await self._execute(f"UPDATE prediction_order_intents SET {assignments} WHERE intent_id = ?", params)

    async def mark_cancel_requested(self, intent_id: str, *, at_ms: int | None = None) -> bool:
        """Durably claim the one permitted TTL cancellation attempt."""

        conn = self._require_conn()
        cursor = await conn.execute(
            """UPDATE prediction_order_intents
               SET cancel_requested=1,cancel_requested_at_ms=?
               WHERE intent_id=? AND COALESCE(cancel_requested,0)=0""",
            (int(at_ms if at_ms is not None else _now_ms()), intent_id),
        )
        changed = cursor.rowcount > 0
        await conn.commit()
        await cursor.close()
        return changed

    async def cancel_requested(self, intent_id: str) -> bool:
        row = await self._fetchone(
            "SELECT cancel_requested FROM prediction_order_intents WHERE intent_id=?",
            (intent_id,),
        )
        return bool(row and row.get("cancel_requested"))

    async def save_order(self, order: Mapping[str, Any]) -> None:
        data = _jsonable(order)
        order_id = str(data.get("order_id") or data.get("orderId") or data.get("id") or "")
        if not order_id:
            raise ValueError("order_id is required")
        await self._execute(
            """INSERT INTO prediction_orders
               (order_id, intent_id, campaign_id, token_id, outcome, order_side,
                 status, requested_amount, limit_price, filled_shares, avg_price,
                 submitted_at_ms, updated_at_ms, cumulative_gross, cumulative_fee,
                 payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(order_id) DO UPDATE SET
                 intent_id=excluded.intent_id, campaign_id=excluded.campaign_id,
                 token_id=excluded.token_id, outcome=excluded.outcome,
                 order_side=excluded.order_side, status=excluded.status,
                  requested_amount=excluded.requested_amount, limit_price=excluded.limit_price,
                  filled_shares=excluded.filled_shares, avg_price=excluded.avg_price,
                  submitted_at_ms=excluded.submitted_at_ms, updated_at_ms=excluded.updated_at_ms,
                  cumulative_gross=excluded.cumulative_gross,
                  cumulative_fee=excluded.cumulative_fee,
                  payload_json=excluded.payload_json""",
            (
                order_id,
                data.get("intent_id"),
                str(data.get("campaign_id") or ""),
                data.get("token_id") or data.get("tokenId"),
                data.get("outcome"),
                data.get("order_side") or data.get("side"),
                str(data.get("status", "UNKNOWN")),
                str(data.get("requested_amount", data.get("amount", "0"))),
                str(data.get("limit_price", "0")),
                str(data.get("filled_shares", data.get("executedQty", "0"))),
                 (str(data["avg_price"]) if data.get("avg_price") is not None else None),
                 data.get("submitted_at_ms"),
                 _int(data.get("updated_at_ms"), _now_ms()),
                 str(data.get("cumulative_gross", data.get("filled_usdt_amount", data.get("filledUsdtAmount", "0")))),
                 str(data.get("cumulative_fee", data.get("fee", "0"))),
                 _json_dumps(order),
             ),
        )

    async def apply_order_snapshot_atomic(
        self,
        campaign: Campaign,
        intent: OrderIntent | Mapping[str, Any],
        order_id: str,
        payload: Mapping[str, Any],
        *,
        outcome: OutcomeSide | None = None,
        _transaction_owned: bool = False,
    ) -> dict[str, Any]:
        """Apply one cumulative order snapshot in one SQLite transaction.

        The exchange reports cumulative quantities.  The durable order row is
        therefore the cursor: a restart computes the same share/gross/fee
        delta before inserting a fill.  Order, fill, position snapshot,
        campaign counters, and terminal intent state all commit or all roll
        back together.
        """

        snapshot_seen_ms = _now_ms()
        snapshot_seen_mono_ns = time.monotonic_ns()
        if not order_id:
            raise ValueError("order_id is required")
        raw = payload.get("data", payload) if isinstance(payload, Mapping) else {}
        data = raw if isinstance(raw, Mapping) else {}
        intent_data = _jsonable(intent)
        strategy_leg = intent_data.get("tier")
        if strategy_leg in {"R3", "FAV"}:
            payload = dict(payload, strategy_leg=strategy_leg)
        selected_outcome = outcome or OutcomeSide(str(intent_data.get("outcome", "UP")).upper())
        order_side = OrderSide(str(data.get("side") or data.get("orderSide") or intent_data.get("order_side", "BUY")).upper())
        status = str(data.get("status") or "UNKNOWN").upper()
        def first_value(*keys: str) -> Any:
            for key in keys:
                if key in data and data[key] is not None and data[key] != "":
                    return data[key]
            return None

        total_shares = _d(first_value("filledShareQty", "filledShares", "executedQty"))
        total_gross = _d(first_value("filledUsdtAmount", "filledAmount", "grossAmount"))
        total_fee = _d(first_value("marketProviderFee", "providerFee")) + _d(data.get("networkFee")) + _d(first_value("fee", "feeAmount"))
        avg_price = _d(first_value("avgPrice", "price") or intent_data.get("limit_price"))
        if any(not value.is_finite() or value < Decimal("0") for value in (total_shares, total_gross, total_fee, avg_price)):
            raise ValueError("order snapshot contains a negative or non-finite cumulative value")
        if total_gross <= 0 and total_shares > 0 and avg_price > 0:
            total_gross = total_shares * avg_price
        token_id = str(data.get("tokenId") or data.get("token_id") or (campaign.market.up_token_id if selected_outcome is OutcomeSide.UP else campaign.market.down_token_id) or "")
        terminal = status in {"FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED"}
        conn = self._require_conn()
        now = _now_ms()
        if _transaction_owned:
            if not conn.in_transaction:
                raise RuntimeError("repair must own the transaction")
        else:
            await self._begin(conn)
        try:
            prior = await self._tx_fetchone(conn,
                "SELECT filled_shares,cumulative_gross,cumulative_fee,payload_json FROM prediction_orders WHERE order_id=?",
                (order_id,),
            )
            prior_shares = _d(prior[0]) if prior else Decimal("0")
            prior_gross = _d(prior[1]) if prior and prior[1] is not None else Decimal("0")
            prior_fee = _d(prior[2]) if prior and prior[2] is not None else Decimal("0")
            if prior and prior[3]:
                prior_payload = _json_load(prior[3], {})
                if isinstance(prior_payload, Mapping):
                    nested = prior_payload.get("payload") if isinstance(prior_payload.get("payload"), Mapping) else prior_payload
                    prior_shares = max(prior_shares, _d(nested.get("filledShareQty") or nested.get("filledShares") or nested.get("executedQty")))
                    prior_gross = max(prior_gross, _d(nested.get("filledUsdtAmount") or nested.get("filledAmount") or nested.get("grossAmount")))
                    prior_fee = max(
                        prior_fee,
                        _d(nested.get("marketProviderFee") or nested.get("providerFee"))
                        + _d(nested.get("networkFee"))
                        + _d(nested.get("fee") or nested.get("feeAmount")),
                    )
            # Exchange cursors are cumulative.  A stale/reordered response is
            # an observational no-op, never a position reversal.  SELL plans
            # carry the exact share cap; BUY caps are optional because legacy
            # intents store USDT notional rather than planned share quantity.
            planned_shares = _d(intent_data.get("planned_shares")) if intent_data.get("planned_shares") is not None else None
            if order_side is OrderSide.SELL:
                planned_shares = _d(intent_data.get("amount"))
            if planned_shares is not None and (not planned_shares.is_finite() or planned_shares < 0 or total_shares > planned_shares):
                raise ValueError("cumulative filled shares exceed the planned order quantity")
            total_shares = max(prior_shares, total_shares)
            total_gross = max(prior_gross, total_gross)
            total_fee = max(prior_fee, total_fee)
            delta_shares = total_shares - prior_shares
            delta_gross = total_gross - prior_gross
            delta_fee = total_fee - prior_fee
            if delta_shares > 0 and delta_gross <= 0:
                delta_gross = delta_shares * (avg_price or Decimal("0"))
                total_gross = prior_gross + delta_gross

            previous_timing = (_json_load(prior[3], {}) if prior and prior[3] else {}).get('local_timing', {})
            first_seen = previous_timing.get('first_positive_snapshot_at_ms')
            if first_seen is None and prior_shares <= 0 and total_shares > 0:
                first_seen = snapshot_seen_ms
            payload = dict(payload, local_timing={
                'version': 'observability-v1',
                'first_positive_snapshot_at_ms': first_seen,
                'snapshot_seen_at_ms': snapshot_seen_ms,
                'snapshot_seen_monotonic_ns': snapshot_seen_mono_ns,
                'transaction_started_at_ms': now,
            })
            submission = await self._tx_fetchone(conn,'SELECT submission_at_ms FROM prediction_order_intents WHERE intent_id=?',(intent_data.get('intent_id'),))
            order_values = (
                str(order_id),
                intent_data.get("intent_id"),
                campaign.campaign_id,
                token_id,
                selected_outcome.value,
                order_side.value,
                status,
                str(intent_data.get("amount", "0")),
                str(intent_data.get("limit_price", "0")),
                str(total_shares),
                str(avg_price) if avg_price > 0 else None,
                submission[0] if submission else None,
                now,
                str(total_gross),
                str(total_fee),
                _json_dumps(payload),
            )
            await conn.execute(
                """INSERT INTO prediction_orders
                   (order_id,intent_id,campaign_id,token_id,outcome,order_side,
                    status,requested_amount,limit_price,filled_shares,avg_price,
                    submitted_at_ms,updated_at_ms,cumulative_gross,cumulative_fee,
                    payload_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(order_id) DO UPDATE SET
                    intent_id=excluded.intent_id,campaign_id=excluded.campaign_id,
                    token_id=excluded.token_id,outcome=excluded.outcome,
                    order_side=excluded.order_side,status=excluded.status,
                    requested_amount=excluded.requested_amount,
                    limit_price=excluded.limit_price,filled_shares=excluded.filled_shares,
                    avg_price=excluded.avg_price,submitted_at_ms=excluded.submitted_at_ms,
                    updated_at_ms=excluded.updated_at_ms,
                    cumulative_gross=excluded.cumulative_gross,
                    cumulative_fee=excluded.cumulative_fee,payload_json=excluded.payload_json""",
                order_values,
            )
            self._maybe_fail("after_order")

            candidate = copy.deepcopy(campaign)
            trade_id = first_value("tradeId", "trade_id")
            fill_id = f"trade:{trade_id}" if trade_id is not None else f"{order_id}:cum:{total_shares}:{total_gross}:{total_fee}"
            inserted = False
            if delta_shares > 0:
                price = delta_gross / delta_shares if delta_shares else avg_price
                fill_values = (
                     fill_id,
                     str(trade_id) if trade_id is not None else None,
                    str(order_id),
                    candidate.campaign_id,
                    token_id,
                    selected_outcome.value,
                    order_side.value,
                    str(delta_shares),
                    str(price),
                    str(delta_gross),
                    str(delta_fee),
                    now,
                    _json_dumps({"order": payload, "strategy_leg": strategy_leg, "cumulative": {"shares": str(total_shares), "gross": str(total_gross), "fee": str(total_fee)}}),
                )
                cur = await conn.execute(
                    """INSERT INTO prediction_fills
                       (fill_id,trade_id,order_id,campaign_id,token_id,outcome,
                        order_side,shares,price,gross_amount,fee,event_time_ms,payload_json)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                    fill_values,
                )
                inserted = cur.rowcount > 0
                await cur.close()
                if inserted:
                    if order_side is OrderSide.BUY:
                        candidate.position.add_buy(
                            selected_outcome,
                            delta_shares,
                            delta_gross,
                            delta_fee,
                            initial=str(intent_data.get("action", "")) == ActionType.BUY_INITIAL.value,
                        )
                    else:
                        candidate.position.add_sell(selected_outcome, delta_shares, delta_gross, delta_fee)
                self._maybe_fail("after_fill")

            action = str(intent_data.get("action", ""))
            if inserted and order_side is OrderSide.BUY:
                if action == ActionType.BUY_INITIAL.value and candidate.initial_outcome is None:
                    candidate.initial_outcome = selected_outcome
                    candidate.state = CampaignState.INITIAL_POSITION
                # Count distinct orders, not fill fragments or action names.
                # Replayed cumulative snapshots must not increment this count.
                count_row = await self._tx_fetchone(conn,
                    "SELECT COUNT(DISTINCT order_id) FROM prediction_fills "
                    "WHERE campaign_id=? AND order_side='BUY' AND CAST(shares AS REAL)>0",
                    (candidate.campaign_id,),
                )
                candidate.buy_count = int(count_row[0])
                if action == ActionType.BUY_HEDGE.value:
                    candidate.hedge_used = True
                    candidate.hedged_at_ms = now
                    candidate.state = CampaignState.HEDGED
            if inserted and delta_shares > 0 and action in {
                ActionType.SELL_PROFIT_LOCK.value,
                ActionType.SELL_PROTECTIVE.value,
            }:
                # Any positive partial reduction consumes the one-shot slot.
                # Waiting for FILLED/CLOSED would allow another sell decision
                # after a PARTIALLY_FILLED order was cancelled or restarted.
                candidate.profit_lock_used = True
            if terminal:
                # A terminal status is not proof of a fill.  Counters and
                # reduction shares advance only for a newly inserted,
                # positive cumulative delta; replayed CLOSED/zero-fill
                # snapshots are observational no-ops.
                if inserted and delta_shares > 0 and action == ActionType.SELL_LOSER.value and status in {"FILLED", "CLOSED"}:
                    candidate.loser_unwind_count += 1
                    candidate.loser_unwind_shares += delta_shares
                candidate.pending_intent_id = None
                candidate.pending_unknown = False
            elif delta_shares > 0:
                # A non-terminal cumulative fill is known, not a submission
                # uncertainty.  The intent remains pending until the next
                # terminal/reconcile observation.
                candidate.pending_unknown = False

            snapshot = candidate.position
            captured = now
            await conn.execute(
                """INSERT INTO prediction_position_snapshots
                   (campaign_id,captured_at_ms,up_shares,down_shares,up_cost,
                    down_cost,realized_cash,fees,payload_json)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(campaign_id,captured_at_ms) DO UPDATE SET
                    up_shares=excluded.up_shares,down_shares=excluded.down_shares,
                    up_cost=excluded.up_cost,down_cost=excluded.down_cost,
                    realized_cash=excluded.realized_cash,fees=excluded.fees,
                    payload_json=excluded.payload_json""",
                (candidate.campaign_id, captured, str(snapshot.up_shares), str(snapshot.down_shares), str(snapshot.up_cost), str(snapshot.down_cost), str(snapshot.realized_cash), str(snapshot.fees), _json_dumps(snapshot)),
            )
            self._maybe_fail("after_position")

            existing_campaign = await self._tx_fetchone(conn,
                "SELECT loop_id FROM prediction_campaigns WHERE campaign_id=?",
                (candidate.campaign_id,),
            )
            campaign_values = list(self._campaign_values(candidate, now_ms=now))
            campaign_values[1] = existing_campaign[0] if existing_campaign else None
            await conn.execute(
                """INSERT INTO prediction_campaigns
                   (campaign_id,loop_id,market_topic_id,market_id,slug,start_time_ms,
                    end_time_ms,state,initial_outcome,hedge_used,profit_lock_used,
                    loser_unwind_count,loser_unwind_shares,buy_count,order_attempts,
                    initial_attempts,hedge_attempts,pending_intent_id,pending_unknown,
                    hedged_at_ms,last_error,payload_json,created_at_ms,updated_at_ms)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(campaign_id) DO UPDATE SET
                    loop_id=COALESCE(excluded.loop_id,prediction_campaigns.loop_id),
                    market_topic_id=excluded.market_topic_id,market_id=excluded.market_id,
                    slug=excluded.slug,start_time_ms=excluded.start_time_ms,
                    end_time_ms=excluded.end_time_ms,state=excluded.state,
                    initial_outcome=excluded.initial_outcome,hedge_used=excluded.hedge_used,
                    profit_lock_used=excluded.profit_lock_used,
                    loser_unwind_count=excluded.loser_unwind_count,
                    loser_unwind_shares=excluded.loser_unwind_shares,
                    buy_count=excluded.buy_count,order_attempts=excluded.order_attempts,
                    initial_attempts=excluded.initial_attempts,hedge_attempts=excluded.hedge_attempts,
                    pending_intent_id=excluded.pending_intent_id,
                    pending_unknown=excluded.pending_unknown,hedged_at_ms=excluded.hedged_at_ms,
                    last_error=excluded.last_error,payload_json=excluded.payload_json,
                    updated_at_ms=excluded.updated_at_ms""",
                tuple(campaign_values),
            )
            self._maybe_fail("after_campaign")
            if intent_data.get("intent_id"):
                intent_status = status if terminal else ("PARTIALLY_FILLED" if delta_shares > 0 else str(intent_data.get("status", "SUBMITTED")))
                await conn.execute(
                    "UPDATE prediction_order_intents SET order_id=COALESCE(order_id,?),status=?,unknown=? WHERE intent_id=?",
                    (str(order_id), intent_status, int(not terminal and delta_shares > 0), str(intent_data["intent_id"])),
                )
            self._maybe_fail("after_terminal")
            if not _transaction_owned:
                await conn.commit()
            return {
                "order_id": str(order_id),
                "status": status,
                "delta_shares": str(delta_shares if inserted else Decimal("0")),
                "delta_gross": str(delta_gross if inserted else Decimal("0")),
                "delta_fee": str(delta_fee if inserted else Decimal("0")),
                "inserted": inserted,
                "terminal": terminal,
                "campaign": candidate,
            }
        except Exception:
            if not _transaction_owned:
                await conn.rollback()
            raise

    upsert_order = save_order

    async def submitted_cancelled_orders(self, campaign_id: str) -> list[dict[str, Any]]:
        """Known submitted orders need a final history read before NO_FILL."""
        return await self._fetchall(
            "SELECT o.order_id,o.payload_json AS order_payload_json,i.* FROM prediction_orders o "
            "JOIN prediction_order_intents i ON i.intent_id=o.intent_id "
            "WHERE o.campaign_id=? AND o.submitted_at_ms IS NOT NULL "
            "AND UPPER(o.status) IN ('CANCELLED','CANCELED','EXPIRED','FAILED')",
            (campaign_id,))

    async def repair_cancelled_fill(self, evidence: Mapping[str, Any]) -> dict[str, Any]:
        """Explicit operator repair; no signing, new orders, or progress increment."""
        from .late_fill_repair import repair_transaction
        return await repair_transaction(self, evidence)

    async def get_order(self, order_id: str) -> dict[str, Any] | None:
        return await self._fetchone("SELECT * FROM prediction_orders WHERE order_id = ?", (order_id,))

    @staticmethod
    def _fill_values(fill: Fill | Mapping[str, Any], *, campaign_id: str | None = None, fill_id: str | None = None) -> tuple[Any, ...]:
        data = _jsonable(fill)
        get = (lambda key, default=None: getattr(fill, key, default)) if isinstance(fill, Fill) else (lambda key, default=None: data.get(key, default))
        order_id = str(get("order_id") or "")
        trade_id = get("trade_id")
        key = fill_id or (f"trade:{trade_id}" if trade_id else f"{order_id}:{get('event_time_ms', 0)}:{get('shares', '0')}:{get('price', '0')}:{get('order_side', get('side', ''))}")
        return (
            key,
            str(trade_id) if trade_id else None,
            order_id,
            str(campaign_id or get("campaign_id") or ""),
            str(get("token_id") or ""),
            str(get("outcome") or ""),
            str(get("side") or get("order_side") or ""),
            str(get("shares", "0")),
            str(get("price", "0")),
            str(get("gross_amount", get("amount", "0"))),
            str(get("fee", "0")),
            _int(get("event_time_ms")),
            _json_dumps(fill),
        )

    async def record_fill(self, fill: Fill | Mapping[str, Any], *, campaign_id: str | None = None, fill_id: str | None = None) -> bool:
        data = _jsonable(fill)
        getter = (lambda key, default=None: getattr(fill, key, default)) if isinstance(fill, Fill) else (lambda key, default=None: data.get(key, default))
        for field_name in ("shares", "price", "gross_amount", "fee"):
            value = _d(getter(field_name, "0"))
            if not value.is_finite() or value < 0 or (field_name == "shares" and value <= 0):
                raise ValueError(f"fill {field_name} must be finite and non-negative")
        values = self._fill_values(fill, campaign_id=campaign_id, fill_id=fill_id)
        count = await self._execute(
            """INSERT INTO prediction_fills
               (fill_id, trade_id, order_id, campaign_id, token_id, outcome,
                order_side, shares, price, gross_amount, fee, event_time_ms,
                payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING""",
            values,
        )
        return count > 0

    insert_fill = record_fill
    save_fill = record_fill

    async def get_fills(self, campaign_id: str, *, limit: int = 1000) -> list[dict[str, Any]]:
        return await self._fetchall(
            "SELECT * FROM prediction_fills WHERE campaign_id = ? ORDER BY event_time_ms ASC LIMIT ?",
            (campaign_id, max(1, int(limit))),
        )

    # -- Position snapshots and settlement ----------------------------

    async def save_position_snapshot(self, campaign_id: str, snapshot: Position | Mapping[str, Any], *, captured_at_ms: int | None = None) -> None:
        data = _jsonable(snapshot)
        get = (lambda key, default=None: getattr(snapshot, key, default)) if isinstance(snapshot, Position) else (lambda key, default=None: data.get(key, default))
        captured = int(captured_at_ms if captured_at_ms is not None else data.get("captured_at_ms", _now_ms()))
        await self._execute(
            """INSERT INTO prediction_position_snapshots
               (campaign_id, captured_at_ms, up_shares, down_shares, up_cost,
                down_cost, realized_cash, fees, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(campaign_id, captured_at_ms) DO UPDATE SET
                up_shares=excluded.up_shares, down_shares=excluded.down_shares,
                up_cost=excluded.up_cost, down_cost=excluded.down_cost,
                realized_cash=excluded.realized_cash, fees=excluded.fees,
                payload_json=excluded.payload_json""",
            (
                campaign_id,
                captured,
                str(get("up_shares", "0")),
                str(get("down_shares", "0")),
                str(get("up_cost", "0")),
                str(get("down_cost", "0")),
                str(get("realized_cash", "0")),
                str(get("fees", "0")),
                _json_dumps(snapshot),
            ),
        )

    record_position_snapshot = save_position_snapshot

    async def get_latest_position_snapshot(self, campaign_id: str) -> dict[str, Any] | None:
        return await self._fetchone(
            "SELECT * FROM prediction_position_snapshots WHERE campaign_id = ? ORDER BY captured_at_ms DESC LIMIT 1",
            (campaign_id,),
        )

    async def record_settlement(self, settlement: Mapping[str, Any]) -> bool:
        data = _jsonable(settlement)
        campaign_id = str(data.get("campaign_id") or "")
        if not campaign_id:
            raise ValueError("campaign_id is required")
        settled_at = _int(data.get("settled_at_ms"), _now_ms())
        settlement_id = str(data.get("settlement_id") or f"{campaign_id}:{settled_at}")
        existing = await self._fetchone(
            "SELECT status, gross_pnl, realized_pnl, net_pnl, fees, payload_json FROM prediction_settlements WHERE settlement_id = ?",
            (settlement_id,),
        )
        next_status = str(data.get("status", "SETTLED"))
        next_gross = str(data.get("gross_pnl", "0"))
        next_realized = str(data.get("realized_pnl", data.get("net_pnl", "0")))
        next_net = str(data.get("net_pnl", "0"))
        next_fees = str(data.get("fees", "0"))
        next_payload = _json_dumps(settlement)
        if existing is not None:
            unchanged = (
                str(existing.get("status")) == next_status
                and str(existing.get("gross_pnl")) == next_gross
                and str(existing.get("realized_pnl")) == next_realized
                and str(existing.get("net_pnl")) == next_net
                and str(existing.get("fees")) == next_fees
                and str(existing.get("payload_json")) == next_payload
            )
            if unchanged:
                return False
        count = await self._execute(
            """INSERT INTO prediction_settlements
               (settlement_id, campaign_id, loop_id, settled_at_ms, winner,
                status, gross_pnl, realized_pnl, net_pnl, fees, tx_hash,
                batch_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(settlement_id) DO UPDATE SET
                 campaign_id=excluded.campaign_id, loop_id=excluded.loop_id,
                 settled_at_ms=excluded.settled_at_ms, winner=excluded.winner,
                 status=excluded.status, gross_pnl=excluded.gross_pnl,
                 realized_pnl=excluded.realized_pnl, net_pnl=excluded.net_pnl,
                  fees=excluded.fees, tx_hash=excluded.tx_hash,
                  batch_id=excluded.batch_id, payload_json=excluded.payload_json""",
            (
                settlement_id,
                campaign_id,
                data.get("loop_id"),
                settled_at,
                data.get("winner"),
                next_status,
                next_gross,
                next_realized,
                next_net,
                next_fees,
                data.get("tx_hash") or data.get("txHash"),
                data.get("batch_id") or data.get("batchId"),
                next_payload,
            ),
        )
        return count > 0

    insert_settlement = record_settlement
    save_settlement = record_settlement

    async def finalize_settlement(
        self,
        campaign: Campaign,
        settlement: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Atomically close a campaign and account its terminal settlement.

        A settlement is considered risk-bearing only on the first transition
        to ``SETTLED``.  The same transaction updates the campaign, loop
        counter, per-loop ledger, and day/loop/consecutive-loss risk state.
        """

        data = _jsonable(settlement)
        campaign_id = str(data.get("campaign_id") or campaign.campaign_id)
        settlement_id = str(data.get("settlement_id") or campaign_id)
        status = str(data.get("status", "SETTLED")).upper()
        settled_at = _int(data.get("settled_at_ms"), _now_ms())
        now = _now_ms()
        conn = self._require_conn()
        await self._begin(conn)
        try:
            existing = await self._tx_fetchone(conn,
                "SELECT settlement_id,status,loop_id,settled_at_ms FROM prediction_settlements WHERE settlement_id=?",
                (settlement_id,),
            )
            # A client may retry with a new timestamp but the same campaign;
            # the campaign-level row is the idempotency boundary.
            if existing is None:
                existing = await self._tx_fetchone(conn,
                    "SELECT settlement_id,status,loop_id,settled_at_ms FROM prediction_settlements WHERE campaign_id=? ORDER BY settled_at_ms DESC LIMIT 1",
                    (campaign_id,),
                )
                if existing is not None:
                    settlement_id = str(existing[0])
            previous_status = str(existing[1]).upper() if existing else ""
            next_status = "SETTLED" if status in {"SETTLED", "CONFIRMED", "CLAIMED", "REDEEMED"} else status
            loop_id = data.get("loop_id")
            campaign_row = await self._tx_fetchone(conn,
                "SELECT loop_id FROM prediction_campaigns WHERE campaign_id=?",
                (campaign_id,),
            )
            if loop_id is None and campaign_row:
                loop_id = campaign_row[0]
            if loop_id is None and existing:
                loop_id = existing[2]
            payload_json = _json_dumps({**dict(data), "settlement_id": settlement_id, "campaign_id": campaign_id, "status": next_status, "loop_id": loop_id})
            values = (
                settlement_id,
                campaign_id,
                loop_id,
                settled_at,
                data.get("winner"),
                next_status,
                str(data.get("gross_pnl", "0")),
                str(data.get("realized_pnl", data.get("net_pnl", "0"))),
                str(data.get("net_pnl", "0")),
                str(data.get("fees", "0")),
                data.get("tx_hash") or data.get("txHash"),
                data.get("batch_id") or data.get("batchId"),
                payload_json,
            )
            await conn.execute(
                """INSERT INTO prediction_settlements
                   (settlement_id,campaign_id,loop_id,settled_at_ms,winner,status,
                    gross_pnl,realized_pnl,net_pnl,fees,tx_hash,batch_id,payload_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(settlement_id) DO UPDATE SET
                    campaign_id=excluded.campaign_id,loop_id=excluded.loop_id,
                    settled_at_ms=excluded.settled_at_ms,winner=excluded.winner,
                    status=CASE WHEN prediction_settlements.status='SETTLED'
                                THEN prediction_settlements.status ELSE excluded.status END,
                    gross_pnl=excluded.gross_pnl,realized_pnl=excluded.realized_pnl,
                    net_pnl=excluded.net_pnl,fees=excluded.fees,tx_hash=excluded.tx_hash,
                    batch_id=excluded.batch_id,payload_json=excluded.payload_json""",
                values,
            )
            self._maybe_fail("after_settlement")
            transitioned = next_status == "SETTLED" and previous_status != "SETTLED"
            risk_state: Mapping[str, Any] = {}
            if next_status == "SETTLED":
                # Use the caller's latest campaign payload, but retain the
                # durable loop id when ordinary saves omitted it.
                candidate = copy.deepcopy(campaign)
                candidate.state = CampaignState.DONE
                campaign_values = list(self._campaign_values(candidate, now_ms=now))
                campaign_values[1] = loop_id
                await conn.execute(
                    """INSERT INTO prediction_campaigns
                       (campaign_id,loop_id,market_topic_id,market_id,slug,start_time_ms,
                        end_time_ms,state,initial_outcome,hedge_used,profit_lock_used,
                        loser_unwind_count,loser_unwind_shares,buy_count,order_attempts,
                        initial_attempts,hedge_attempts,pending_intent_id,pending_unknown,
                        hedged_at_ms,last_error,payload_json,created_at_ms,updated_at_ms)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(campaign_id) DO UPDATE SET
                        loop_id=COALESCE(excluded.loop_id,prediction_campaigns.loop_id),
                        state=excluded.state,initial_outcome=excluded.initial_outcome,
                        hedge_used=excluded.hedge_used,profit_lock_used=excluded.profit_lock_used,
                        loser_unwind_count=excluded.loser_unwind_count,
                        loser_unwind_shares=excluded.loser_unwind_shares,
                        buy_count=excluded.buy_count,order_attempts=excluded.order_attempts,
                        initial_attempts=excluded.initial_attempts,hedge_attempts=excluded.hedge_attempts,
                        pending_intent_id=excluded.pending_intent_id,
                        pending_unknown=excluded.pending_unknown,hedged_at_ms=excluded.hedged_at_ms,
                        last_error=excluded.last_error,payload_json=excluded.payload_json,
                        updated_at_ms=excluded.updated_at_ms""",
                    tuple(campaign_values),
                )
                self._maybe_fail("after_done_campaign")
                is_sub_lane = str(campaign_id or "").endswith("::p3") or "::shadow::" in str(campaign_id or "")
                if loop_id is not None and not is_sub_lane:
                    ledger = await self._tx_fetchone(conn,
                        "SELECT campaign_id FROM prediction_risk_ledger WHERE campaign_id=?",
                        (campaign_id,),
                    )
                    if ledger is None:
                        day = datetime.fromtimestamp(settled_at / 1000, ZoneInfo("Asia/Taipei")).date().isoformat()
                        await conn.execute(
                            """INSERT INTO prediction_risk_ledger
                               (ledger_id,loop_id,campaign_id,day,net_pnl,created_at_ms)
                               VALUES(?,?,?,?,?,?)""",
                            (f"loop:{loop_id}:{campaign_id}", loop_id, campaign_id, day, str(data.get("net_pnl", "0")), now),
                        )
                        await conn.execute(
                            "UPDATE prediction_loops SET completed=MIN(target,completed+1),updated_at_ms=? WHERE loop_id=? AND state='RUNNING' AND completed < target",
                            (now, loop_id),
                        )
                        await conn.execute(
                            "UPDATE prediction_loops SET state='DONE',updated_at_ms=? WHERE loop_id=? AND completed>=target",
                            (now, loop_id),
                        )
                risk_state = await self._refresh_risk_aggregate_tx(conn, now_ms=now, loop_id=str(loop_id) if loop_id is not None else None)
            await conn.commit()
            return {
                "settlement_id": settlement_id,
                "campaign_id": campaign_id,
                "status": next_status,
                "loop_id": loop_id,
                "risk_transition": transitioned,
                "hard_stop_latched": bool(risk_state.get("hard_stop_latched", False)),
            }
        except Exception:
            await conn.rollback()
            raise

    async def get_settlement(self, campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM prediction_settlements WHERE campaign_id = ? ORDER BY settled_at_ms DESC LIMIT 1",
            (campaign_id,),
        )
        if not row:
            return None
        payload = _json_load(row.get("payload_json"), {})
        if isinstance(payload, Mapping):
            row["payload"] = payload
            for key, value in payload.items():
                row.setdefault(str(key), value)
        return row

    # -- Risk and runtime configuration -------------------------------

    async def save_risk_snapshot(self, payload: Mapping[str, Any]) -> None:
        """Merge a snapshot with current control/reset state under the write gate.

        A concurrently committed operator HS must not be replaced by an older
        admission snapshot. Explicit operator reset uses its separate path.
        """
        conn = self._require_conn()
        await self._begin(conn)
        try:
            row = await self._tx_fetchone(conn,
                "SELECT config_value_json FROM prediction_runtime_config WHERE config_key='prediction_risk_state'")
            old = _json_load(row[0], {}) if row else {}
            if not isinstance(old, Mapping):
                raise ValueError("invalid persisted risk state")
            merged = dict(payload)
            for key in ("hard_stop_reset_day", "hard_stop_reset_at_ms", "hard_stop_reset_count",
                        "hard_stop_reset_baseline_pnl", "raw_daily_net_pnl"):
                if key in old:
                    merged[key] = old[key]
            if old.get("day") == merged.get("day") and old.get("hard_stop_latched"):
                merged["hard_stop_latched"] = True
            await conn.execute(
                """INSERT INTO prediction_runtime_config(config_key,config_value_json,updated_at_ms)
                   VALUES('prediction_risk_state',?,?) ON CONFLICT(config_key) DO UPDATE SET
                   config_value_json=excluded.config_value_json,updated_at_ms=excluded.updated_at_ms""",
                (_json_dumps(merged), _now_ms()))
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise

    async def record_execution_timings(self, events: Sequence[tuple[str, str | None, Mapping[str, Any]]]) -> None:
        """Bounded telemetry only: one commit; trading evidence uses its own path."""
        if len(events) > 32:
            raise ValueError("execution timing batch exceeds 32")
        if not events:
            return
        conn = self._require_conn()
        await self._begin(conn)
        try:
            await conn.executemany(
                """INSERT INTO prediction_risk_events
                   (campaign_id,event_time_ms,event_type,severity,message,payload_json)
                   VALUES(?,?,'EXECUTION_TIMING','INFO',?,?)""",
                [(cid, _now_ms(), event, _json_dumps(fields)) for event, cid, fields in events])
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise

    async def record_risk_event(
        self,
        event_type: str | Mapping[str, Any],
        severity: str = "INFO",
        message: str = "",
        *,
        campaign_id: str | None = None,
        event_time_ms: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> int:
        if isinstance(event_type, Mapping):
            data = dict(event_type)
            event_type, severity, message = str(data.get("event_type", "UNKNOWN")), str(data.get("severity", "INFO")), str(data.get("message", ""))
            campaign_id = data.get("campaign_id", campaign_id)
            event_time_ms = data.get("event_time_ms", event_time_ms)
            payload = data.get("payload", payload)
        cursor = await self._require_conn().execute(
            """INSERT INTO prediction_risk_events
               (campaign_id, event_time_ms, event_type, severity, message, payload_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (campaign_id, int(event_time_ms or _now_ms()), str(event_type), str(severity), str(message), _json_dumps(payload or {})),
        )
        await self._require_conn().commit()
        event_id = int(cursor.lastrowid)
        await cursor.close()
        return event_id

    save_risk_event = record_risk_event

    async def get_risk_events(self, *, campaign_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if campaign_id is None:
            return await self._fetchall("SELECT * FROM prediction_risk_events ORDER BY event_time_ms DESC LIMIT ?", (max(1, int(limit)),))
        return await self._fetchall(
            "SELECT * FROM prediction_risk_events WHERE campaign_id = ? ORDER BY event_time_ms DESC LIMIT ?",
            (campaign_id, max(1, int(limit))),
        )

    async def set_runtime_config(self, key: str, value: Any) -> None:
        await self._execute(
            """INSERT INTO prediction_runtime_config(config_key, config_value_json, updated_at_ms)
               VALUES (?, ?, ?)
               ON CONFLICT(config_key) DO UPDATE SET
                 config_value_json=excluded.config_value_json, updated_at_ms=excluded.updated_at_ms""",
            (str(key), _json_dumps(value), _now_ms()),
        )

    set_config = set_runtime_config

    async def get_runtime_config(self, key: str, default: Any = None) -> Any:
        row = await self._fetchone("SELECT config_value_json FROM prediction_runtime_config WHERE config_key = ?", (str(key),))
        return _json_load(row["config_value_json"], default) if row else default

    get_config = get_runtime_config

    async def get_all_runtime_config(self) -> dict[str, Any]:
        rows = await self._fetchall("SELECT config_key, config_value_json FROM prediction_runtime_config")
        return {str(row["config_key"]): _json_load(row["config_value_json"]) for row in rows}

    # -- Immutable shadow canary ledger -------------------------------

    @staticmethod
    def _shadow_outcome(value: Any) -> str:
        outcome = str(value or "").strip().upper()
        if outcome not in {"UP", "DOWN", "DRAW"}:
            raise ValueError("shadow resolved outcome must be UP, DOWN or DRAW")
        return outcome

    @staticmethod
    def _shadow_provenance(
        *,
        config_hash: Any,
        window_start_ms: Any,
        window_end_ms: Any,
    ) -> tuple[str, int, int]:
        config = str(config_hash or "").strip()
        if not config:
            raise ValueError("shadow config_hash is required")
        try:
            start = int(window_start_ms)
            end = int(window_end_ms)
        except (TypeError, ValueError) as exc:
            raise ValueError("shadow window bounds must be integer milliseconds") from exc
        if start < 0 or end < 0 or start > end:
            raise ValueError("shadow window must satisfy start <= end")
        return config, start, end

    def _shadow_database_path(self) -> str:
        return str(self.db_path.resolve())

    def _git_commit(self) -> str:
        # Isolated VM deployments intentionally do not include .git metadata.
        # The deployment pipeline supplies this immutable, secret-free build
        # identity so shadow evidence remains append-only and restart-safe.
        configured = str(os.environ.get("PREDICTION_REPOSITORY_COMMIT") or "").strip()
        if configured and configured != "unknown":
            return configured
        cwd = Path.cwd()
        source_root = Path(__file__).resolve().parents[3]
        if (source_root / ".git").exists():
            cwd = source_root
        candidate = self.db_path.resolve()
        if not (source_root / ".git").exists():
            for parent in (candidate.parent, *candidate.parents):
                if parent == parent.anchor:
                    continue
                if (parent / ".git").exists():
                    cwd = parent
                    break
        try:
            return subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=str(cwd), text=True, timeout=3
            ).strip()
        except Exception:
            return "unknown"

    @staticmethod
    def _shadow_campaign_fields(campaign: Any) -> dict[str, Any]:
        """Extract stable campaign identity without storing a mutable object."""

        data = _jsonable(campaign) if not isinstance(campaign, Mapping) else dict(campaign)
        market = getattr(campaign, "market", None)
        if market is None and isinstance(data.get("market"), Mapping):
            market = data.get("market")
        if market is not None and not isinstance(market, Mapping):
            market = _jsonable(market)
        market = market if isinstance(market, Mapping) else {}
        return {
            "campaign_id": str(getattr(campaign, "campaign_id", None) or data.get("campaign_id") or ""),
            "market_topic_id": str(
                getattr(market, "market_topic_id", None) if not isinstance(market, Mapping) else market.get("market_topic_id")
                or (market.get("marketTopicId") if isinstance(market, Mapping) else None)
                or data.get("market_topic_id")
                or data.get("marketTopicId")
                or ""
            ),
            "market_id": str(
                getattr(market, "market_id", None) if not isinstance(market, Mapping) else market.get("market_id")
                or (market.get("marketId") if isinstance(market, Mapping) else None)
                or data.get("market_id")
                or data.get("marketId")
                or ""
            ),
            "slug": str(
                getattr(market, "slug", None) if not isinstance(market, Mapping) else market.get("slug")
                or data.get("slug")
                or ""
            ),
            "campaign_start_ms": int(
                getattr(market, "start_time_ms", None) if not isinstance(market, Mapping) else market.get("start_time_ms")
                or (market.get("startTimeMs") if isinstance(market, Mapping) else None)
                or data.get("start_time_ms")
                or data.get("startTimeMs")
                or 0
            ),
            "campaign_end_ms": int(
                getattr(market, "end_time_ms", None) if not isinstance(market, Mapping) else market.get("end_time_ms")
                or (market.get("endTimeMs") if isinstance(market, Mapping) else None)
                or data.get("end_time_ms")
                or data.get("endTimeMs")
                or 0
            ),
            "payload": data,
        }

    @staticmethod
    def _shadow_fill_fields(fill: Any, *, default_campaign_id: str) -> dict[str, Any]:
        data = _jsonable(fill) if not isinstance(fill, Mapping) else dict(fill)
        side = str(data.get("order_side") or data.get("side") or "BUY").upper()
        outcome = str(data.get("outcome") or "").upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("shadow fill order_side must be BUY or SELL")
        if outcome not in {"UP", "DOWN"}:
            raise ValueError("shadow fill outcome must be UP or DOWN")
        shares = _d(data.get("shares") or data.get("filled_shares") or data.get("quantity"))
        price = _d(data.get("price") or data.get("avg_price"))
        gross = _d(data.get("gross_amount") or data.get("amount") or data.get("filled_usdt_amount"))
        fee = _d(data.get("simulated_fee", data.get("fee", "0")))
        event_time = _int(data.get("event_time_ms") or data.get("filled_at_ms"), _now_ms())
        if shares <= 0 or price < 0 or gross < 0 or fee < 0:
            raise ValueError("shadow fill quantities and simulated fees must be non-negative; shares must be positive")
        identity = str(
            data.get("fill_identity")
            or data.get("shadow_fill_id")
            or data.get("fill_id")
            or data.get("trade_id")
            or f"{default_campaign_id}:{event_time}:{outcome}:{side}:{shares}:{price}"
        )
        return {
            "fill_identity": identity,
            "outcome": outcome,
            "order_side": side,
            "shares": str(shares),
            "price": str(price),
            "gross_amount": str(gross),
            "simulated_fee": str(fee),
            "event_time_ms": event_time,
            "payload": data,
        }

    async def ensure_shadow_window(
        self,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
        database_path: str | None = None,
    ) -> dict[str, Any]:
        """Create (but never rewrite) one named SHADOW canary window."""

        config, start, end = self._shadow_provenance(
            config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        path = str(Path(database_path).resolve()) if database_path else self._shadow_database_path()
        identity = hashlib.sha256(f"SHADOW|{config}|{start}|{end}|{path}".encode()).hexdigest()
        await self._execute(
            """INSERT INTO prediction_shadow_windows
               (window_id,mode,config_hash,window_start_ms,window_end_ms,database_path,created_at_ms)
               VALUES(?, 'SHADOW', ?, ?, ?, ?, ?)
               ON CONFLICT(window_id) DO NOTHING""",
            (identity, config, start, end, path, _now_ms()),
        )
        row = await self._fetchone("SELECT * FROM prediction_shadow_windows WHERE window_id=?", (identity,))
        if not row:
            raise RuntimeError("shadow window was not persisted")
        return row

    async def get_shadow_window(
        self,
        *,
        config_hash: str | None = None,
        window_start_ms: int | None = None,
        window_end_ms: int | None = None,
    ) -> dict[str, Any] | None:
        clauses = ["mode='SHADOW'"]
        params: list[Any] = []
        if config_hash is not None:
            clauses.append("config_hash=?")
            params.append(str(config_hash))
        if window_start_ms is not None:
            clauses.append("window_start_ms=?")
            params.append(int(window_start_ms))
        if window_end_ms is not None:
            clauses.append("window_end_ms=?")
            params.append(int(window_end_ms))
        return await self._fetchone(
            "SELECT * FROM prediction_shadow_windows WHERE " + " AND ".join(clauses) +
            " ORDER BY created_at_ms DESC LIMIT 1",
            tuple(params),
        )

    async def _insert_shadow_campaign_conn(
        self,
        conn: aiosqlite.Connection,
        *,
        campaign: Any,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
        resolved_outcome: str,
        simulated_fees: Any,
        simulated_pnl: Any,
        expected_fill_count: int,
        simulated_fill_count: int,
        resolved_at_ms: int,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        fields = self._shadow_campaign_fields(campaign)
        if not fields["campaign_id"]:
            raise ValueError("shadow campaign_id is required")
        if fields["campaign_start_ms"] > fields["campaign_end_ms"]:
            raise ValueError("shadow campaign campaign start must be <= end")
        shadow_id = hashlib.sha256(
            f"SHADOW|{config_hash}|{window_start_ms}|{window_end_ms}|{fields['campaign_id']}".encode()
        ).hexdigest()
        values = (
            shadow_id,
            fields["campaign_id"],
            config_hash,
            window_start_ms,
            window_end_ms,
            fields["market_topic_id"],
            fields["market_id"],
            fields["slug"],
            fields["campaign_start_ms"],
            fields["campaign_end_ms"],
            self._shadow_outcome(resolved_outcome),
            int(resolved_at_ms),
            str(_d(simulated_fees)),
            str(_d(simulated_pnl)),
            max(0, int(expected_fill_count)),
            max(0, int(simulated_fill_count)),
            _now_ms(),
            _json_dumps(payload if payload is not None else fields["payload"]),
            str((payload or {}).get('lane') or ''),
            _json_dumps((payload or {}).get('strategy_identity') or {}),
            _json_dumps((payload or {}).get('execution_identity') or {}),
            str((payload or {}).get('collection_release_fingerprint') or ''),
        )
        await conn.execute(
            """INSERT INTO prediction_shadow_campaigns
               (shadow_campaign_id,campaign_id,mode,config_hash,window_start_ms,window_end_ms,
                market_topic_id,market_id,slug,campaign_start_ms,campaign_end_ms,resolved_outcome,
                resolved_at_ms,simulated_fees,simulated_pnl,expected_fill_count,simulated_fill_count,
                created_at_ms,payload_json,lane,strategy_identity_json,execution_identity_json,collection_release_fingerprint)
               VALUES(?,?, 'SHADOW', ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(shadow_campaign_id) DO NOTHING""",
            values,
        )
        row = await self._tx_fetchone(conn, "SELECT * FROM prediction_shadow_campaigns WHERE shadow_campaign_id=?", (shadow_id,))
        if row is None:
            raise RuntimeError("shadow campaign identity was not persisted")
        return dict(row)

    async def _insert_shadow_fill_conn(
        self,
        conn: aiosqlite.Connection,
        *,
        shadow_campaign: Mapping[str, Any],
        fill: Any,
    ) -> bool:
        fields = self._shadow_fill_fields(fill, default_campaign_id=str(shadow_campaign["campaign_id"]))
        fill_id = hashlib.sha256(
            f"{shadow_campaign['shadow_campaign_id']}|{fields['fill_identity']}".encode()
        ).hexdigest()
        await conn.execute(
            """INSERT INTO prediction_shadow_fills
               (shadow_fill_id,shadow_campaign_id,campaign_id,mode,config_hash,window_start_ms,window_end_ms,
                fill_identity,outcome,order_side,shares,price,gross_amount,simulated_fee,event_time_ms,payload_json)
               VALUES(?,?,?,'SHADOW',?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(shadow_fill_id) DO NOTHING""",
            (
                fill_id,
                shadow_campaign["shadow_campaign_id"],
                shadow_campaign["campaign_id"],
                shadow_campaign["config_hash"],
                shadow_campaign["window_start_ms"],
                shadow_campaign["window_end_ms"],
                fields["fill_identity"],
                fields["outcome"],
                fields["order_side"],
                fields["shares"],
                fields["price"],
                fields["gross_amount"],
                fields["simulated_fee"],
                fields["event_time_ms"],
                _json_dumps(fields["payload"]),
            ),
        )
        return True

    async def _insert_shadow_settlement_conn(
        self,
        conn: aiosqlite.Connection,
        *,
        shadow_campaign: Mapping[str, Any],
        resolved_outcome: str,
        simulated_fees: Any,
        simulated_pnl: Any,
        settled_at_ms: int,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        outcome = self._shadow_outcome(resolved_outcome)
        settlement_id = hashlib.sha256(
            f"{shadow_campaign['shadow_campaign_id']}|SETTLED".encode()
        ).hexdigest()
        await conn.execute(
            """INSERT INTO prediction_shadow_settlements
               (shadow_settlement_id,shadow_campaign_id,campaign_id,mode,config_hash,window_start_ms,window_end_ms,
                resolved_outcome,status,simulated_fees,simulated_pnl,settled_at_ms,payload_json)
               VALUES(?,?,?,'SHADOW',?,?,?,?, 'SETTLED',?,?,?,?)
               ON CONFLICT(shadow_settlement_id) DO NOTHING""",
            (
                settlement_id,
                shadow_campaign["shadow_campaign_id"],
                shadow_campaign["campaign_id"],
                shadow_campaign["config_hash"],
                shadow_campaign["window_start_ms"],
                shadow_campaign["window_end_ms"],
                outcome,
                str(_d(simulated_fees)),
                str(_d(simulated_pnl)),
                int(settled_at_ms),
                _json_dumps(payload or {}),
            ),
        )
        row = await self._tx_fetchone(conn, "SELECT * FROM prediction_shadow_settlements WHERE shadow_settlement_id=?", (settlement_id,))
        if row is None:
            raise RuntimeError("shadow settlement identity was not persisted")
        return dict(row)

    async def record_shadow_counterfactual(
        self,
        campaign: Any,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
        resolved_outcome: str,
        simulated_fees: Any = "0",
        simulated_pnl: Any = "0",
        fills: Sequence[Mapping[str, Any]] | None = None,
        expected_fill_count: int | None = None,
        resolved_at_ms: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically append one immutable resolved SHADOW counterfactual.

        This is intentionally wallet-independent.  The worker may call it
        after a shadow market resolves even when no live wallet/API account
        exists.  Retries use deterministic identities and cannot rewrite an
        existing campaign, fill, settlement, or provenance row.
        """

        config, start, end = self._shadow_provenance(
            config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        fill_items = list(fills or ())
        expected = len(fill_items) if expected_fill_count is None else max(0, int(expected_fill_count))
        settled_at = int(resolved_at_ms if resolved_at_ms is not None else _now_ms())
        conn = self._require_conn()
        await self._begin(conn)
        try:
            path = self._shadow_database_path()
            window_id = hashlib.sha256(f"SHADOW|{config}|{start}|{end}|{path}".encode()).hexdigest()
            await conn.execute(
                """INSERT INTO prediction_shadow_windows
                   (window_id,mode,config_hash,window_start_ms,window_end_ms,database_path,created_at_ms)
                   VALUES(?, 'SHADOW', ?, ?, ?, ?, ?)
                   ON CONFLICT(window_id) DO NOTHING""",
                (window_id, config, start, end, path, _now_ms()),
            )
            shadow_campaign = await self._insert_shadow_campaign_conn(
                conn,
                campaign=campaign,
                config_hash=config,
                window_start_ms=start,
                window_end_ms=end,
                resolved_outcome=resolved_outcome,
                simulated_fees=simulated_fees,
                simulated_pnl=simulated_pnl,
                expected_fill_count=expected,
                simulated_fill_count=len(fill_items),
                resolved_at_ms=settled_at,
                payload=payload,
            )
            for fill in fill_items:
                await self._insert_shadow_fill_conn(conn, shadow_campaign=shadow_campaign, fill=fill)
            shadow_settlement = await self._insert_shadow_settlement_conn(
                conn,
                shadow_campaign=shadow_campaign,
                resolved_outcome=resolved_outcome,
                simulated_fees=simulated_fees,
                simulated_pnl=simulated_pnl,
                settled_at_ms=settled_at,
                payload=payload,
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        return {
            "mode": "SHADOW",
            "shadow_campaign_id": shadow_campaign["shadow_campaign_id"],
            "shadow_settlement_id": shadow_settlement["shadow_settlement_id"],
            "campaign_id": shadow_campaign["campaign_id"],
            "config_hash": config,
            "window_start_ms": start,
            "window_end_ms": end,
            "resolved_outcome": self._shadow_outcome(resolved_outcome),
            "simulated_fees": str(_d(simulated_fees)),
            "simulated_pnl": str(_d(simulated_pnl)),
            "simulated_fill_count": len(fill_items),
        }

    async def record_shadow_campaign(
        self,
        campaign: Any,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
        resolved_outcome: str,
        simulated_fees: Any = "0",
        simulated_pnl: Any = "0",
        expected_fill_count: int = 0,
        resolved_at_ms: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append only the immutable campaign identity (fills may follow)."""

        config, start, end = self._shadow_provenance(
            config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        conn = self._require_conn()
        await self._begin(conn)
        try:
            shadow = await self._insert_shadow_campaign_conn(
                conn,
                campaign=campaign,
                config_hash=config,
                window_start_ms=start,
                window_end_ms=end,
                resolved_outcome=resolved_outcome,
                simulated_fees=simulated_fees,
                simulated_pnl=simulated_pnl,
                expected_fill_count=expected_fill_count,
                simulated_fill_count=0,
                resolved_at_ms=int(resolved_at_ms if resolved_at_ms is not None else _now_ms()),
                payload=payload,
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        return dict(shadow)

    async def record_shadow_fill(
        self,
        fill: Mapping[str, Any],
        *,
        shadow_campaign_id: str | None = None,
        campaign_id: str | None = None,
        config_hash: str | None = None,
        window_start_ms: int | None = None,
        window_end_ms: int | None = None,
    ) -> bool:
        """Append one immutable simulated fill to a shadow campaign."""

        if shadow_campaign_id:
            row = await self._fetchone(
                "SELECT * FROM prediction_shadow_campaigns WHERE shadow_campaign_id=? AND mode='SHADOW'",
                (str(shadow_campaign_id),),
            )
        else:
            if not campaign_id or not config_hash or window_start_ms is None or window_end_ms is None:
                raise ValueError("shadow fill campaign identity is incomplete")
            config, start, end = self._shadow_provenance(
                config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
            )
            row = await self._fetchone(
                """SELECT * FROM prediction_shadow_campaigns
                   WHERE campaign_id=? AND config_hash=? AND window_start_ms=? AND window_end_ms=? AND mode='SHADOW'""",
                (str(campaign_id), config, start, end),
            )
        if not row:
            raise ValueError("shadow campaign identity does not exist")
        conn = self._require_conn()
        await self._begin(conn)
        try:
            await self._insert_shadow_fill_conn(conn, shadow_campaign=row, fill=fill)
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        return True

    async def get_shadow_campaign_identity(
        self,
        *,
        campaign_id: str,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
    ) -> dict[str, Any] | None:
        """Load one immutable Shadow campaign identity for restart recovery."""

        config, start, end = self._shadow_provenance(
            config_hash=config_hash,
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
        )
        return await self._fetchone(
            """SELECT * FROM prediction_shadow_campaigns
               WHERE campaign_id=? AND config_hash=? AND window_start_ms=?
                 AND window_end_ms=? AND mode='SHADOW'""",
            (str(campaign_id), config, start, end),
        )

    async def get_shadow_campaign_for_campaign(self, campaign_id: str) -> dict[str, Any] | None:
        """Load the newest Shadow identity for restart-safe settlement.

        A worker restart clears its in-memory campaign-to-shadow-id cache.  A
        campaign may already have an immutable Shadow row in that case; the
        worker must reuse it instead of creating a second counterfactual with
        a new config/window hash.
        """

        return await self._fetchone(
            """SELECT sc.*
               FROM prediction_shadow_campaigns sc
               WHERE sc.campaign_id=? AND sc.mode='SHADOW'
               ORDER BY sc.created_at_ms DESC, sc.shadow_campaign_id DESC
               LIMIT 1""",
            (str(campaign_id),),
        )

    async def get_shadow_fills(self, shadow_campaign_id: str) -> list[dict[str, Any]]:
        """Return immutable fills in event order for a Shadow campaign."""

        return await self._fetchall(
            """SELECT * FROM prediction_shadow_fills
               WHERE shadow_campaign_id=? AND mode='SHADOW'
               ORDER BY event_time_ms ASC, shadow_fill_id ASC""",
            (str(shadow_campaign_id),),
        )

    async def count_shadow_resolved_campaigns(
        self,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
    ) -> int:
        """Count unique settled Shadow campaigns inside one provenance window."""

        row = await self._fetchone(
            """SELECT COUNT(DISTINCT c.campaign_id) AS count
                 FROM prediction_shadow_campaigns c
                 JOIN prediction_shadow_settlements s
                   ON s.shadow_campaign_id=c.shadow_campaign_id
                WHERE c.mode='SHADOW' AND s.status='SETTLED'
                  AND c.config_hash=? AND c.window_start_ms=? AND c.window_end_ms=?""",
            (str(config_hash), int(window_start_ms), int(window_end_ms)),
        )
        return int((row or {}).get("count") or 0)

    async def record_shadow_settlement(
        self,
        *,
        shadow_campaign_id: str,
        resolved_outcome: str,
        simulated_fees: Any = "0",
        simulated_pnl: Any = "0",
        settled_at_ms: int | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append one immutable SETTLED shadow result."""

        row = await self._fetchone(
            "SELECT * FROM prediction_shadow_campaigns WHERE shadow_campaign_id=? AND mode='SHADOW'",
            (str(shadow_campaign_id),),
        )
        if not row:
            raise ValueError("shadow campaign identity does not exist")
        conn = self._require_conn()
        await self._begin(conn)
        try:
            result = await self._insert_shadow_settlement_conn(
                conn,
                shadow_campaign=row,
                resolved_outcome=resolved_outcome,
                simulated_fees=simulated_fees,
                simulated_pnl=simulated_pnl,
                settled_at_ms=int(settled_at_ms if settled_at_ms is not None else _now_ms()),
                payload=payload,
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        return result

    async def _shadow_invariant_metrics(
        self,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
    ) -> dict[str, int]:
        """Compute risk invariants from SQL, scoped to immutable SHADOW rows."""

        scope = """EXISTS (
            SELECT 1 FROM prediction_shadow_campaigns sc
            WHERE sc.campaign_id = i.campaign_id AND sc.mode='SHADOW'
              AND sc.config_hash=? AND sc.window_start_ms=? AND sc.window_end_ms=?
        )"""
        params = (config_hash, window_start_ms, window_end_ms)
        unresolved_intents = await self._fetchone(
            """SELECT COUNT(*) AS count FROM prediction_order_intents i
               WHERE i.created_at_ms >= ? AND i.created_at_ms <= ? AND """ + scope +
            " AND (i.unknown=1 OR UPPER(i.status) NOT IN ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED'))",
            (window_start_ms, window_end_ms, *params),
        )
        unresolved_orders = await self._fetchone(
            """SELECT COUNT(*) AS count FROM prediction_orders o
               WHERE o.updated_at_ms >= ? AND o.updated_at_ms <= ?
                 AND EXISTS (
                   SELECT 1 FROM prediction_shadow_campaigns sc
                   WHERE sc.campaign_id=o.campaign_id AND sc.mode='SHADOW'
                     AND sc.config_hash=? AND sc.window_start_ms=? AND sc.window_end_ms=?
                 )
                 AND UPPER(o.status) NOT IN ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED')""",
            (window_start_ms, window_end_ms, *params),
        )
        duplicate_intents = await self._fetchone(
            """SELECT COUNT(*) AS count FROM (
                   SELECT i.campaign_id,i.action,i.attempt
                   FROM prediction_order_intents i
                   WHERE i.created_at_ms >= ? AND i.created_at_ms <= ? AND """ + scope +
            " GROUP BY i.campaign_id,i.action,i.attempt HAVING COUNT(*) > 1\n"
            "               )",
            (window_start_ms, window_end_ms, *params),
        )
        duplicate_fills = await self._fetchone(
            """SELECT COUNT(*) AS count FROM (
                   SELECT f.shadow_campaign_id,f.fill_identity
                   FROM prediction_shadow_fills f
                   WHERE f.mode='SHADOW' AND f.config_hash=? AND f.window_start_ms=? AND f.window_end_ms=?
                   GROUP BY f.shadow_campaign_id,f.fill_identity HAVING COUNT(*) > 1
               )""",
            params,
        )
        overbuy = await self._fetchone(
            """SELECT COUNT(*) AS count FROM (
                   SELECT i.campaign_id,
                          SUM(CASE WHEN UPPER(i.action) IN ('BUY_INITIAL','BUY_HEDGE','BUY') THEN 1 ELSE 0 END) AS buys,
                          SUM(CASE WHEN UPPER(i.action) IN ('BUY_INITIAL','BUY_HEDGE','BUY')
                                   THEN CAST(COALESCE(i.amount,'0') AS REAL) ELSE 0 END) AS amount
                   FROM prediction_order_intents i
                   WHERE i.created_at_ms >= ? AND i.created_at_ms <= ? AND """ + scope +
            " GROUP BY i.campaign_id HAVING buys > 2 OR amount > 2.0\n"
            "               )",
            (window_start_ms, window_end_ms, *params),
        )
        action_limit = await self._fetchone(
            """SELECT COUNT(*) AS count FROM (
                   SELECT i.campaign_id,COUNT(*) AS attempts
                   FROM prediction_order_intents i
                   WHERE i.created_at_ms >= ? AND i.created_at_ms <= ? AND """ + scope +
            " GROUP BY i.campaign_id HAVING attempts > 8\n"
            "               )",
            (window_start_ms, window_end_ms, *params),
        )
        result = {
            "unresolved_intents": int((unresolved_intents or {}).get("count") or 0),
            "unresolved_orders": int((unresolved_orders or {}).get("count") or 0),
            "duplicate_intents": int((duplicate_intents or {}).get("count") or 0),
            "duplicate_fills": int((duplicate_fills or {}).get("count") or 0),
            "overbuy_violations": int((overbuy or {}).get("count") or 0),
            "action_limit_violations": int((action_limit or {}).get("count") or 0),
        }
        result["invariant_violations"] = sum(result.values())
        result["real_invariant_violations"] = result["invariant_violations"]
        return result

    async def get_shadow_invariant_violations(
        self,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
    ) -> dict[str, int]:
        config, start, end = self._shadow_provenance(
            config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        return await self._shadow_invariant_metrics(config_hash=config, window_start_ms=start, window_end_ms=end)

    async def _shadow_metrics(
        self,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
    ) -> dict[str, Any]:
        params = (config_hash, window_start_ms, window_end_ms)
        row = await self._fetchone(
            """SELECT COUNT(*) AS campaigns,
                      COUNT(DISTINCT CASE WHEN s.status='SETTLED' AND s.resolved_outcome IN ('UP','DOWN','DRAW') THEN c.campaign_id END) AS settled,
                      COALESCE(SUM(CASE WHEN s.status='SETTLED' THEN CAST(s.simulated_pnl AS REAL) ELSE 0 END),0) AS pnl,
                      COALESCE(SUM(CASE WHEN s.status='SETTLED' THEN CAST(s.simulated_fees AS REAL) ELSE 0 END),0) AS fees,
                      COALESCE(SUM(c.expected_fill_count),0) AS expected_fills
               FROM prediction_shadow_campaigns c
               LEFT JOIN prediction_shadow_settlements s ON s.shadow_campaign_id=c.shadow_campaign_id
               WHERE c.mode='SHADOW' AND c.config_hash=? AND c.window_start_ms=? AND c.window_end_ms=?""",
            params,
        )
        # Fill-rate evidence is derived from the immutable fill ledger, never
        # from the campaign's cached ``simulated_fill_count``.  The join and
        # repeated provenance predicates keep fills from a different campaign,
        # window, or config out of both the controller and retirement gate.
        fill_row = await self._fetchone(
            """SELECT COUNT(*) AS simulated_fills
               FROM prediction_shadow_fills f
               JOIN prediction_shadow_campaigns c ON c.shadow_campaign_id=f.shadow_campaign_id
               WHERE f.mode='SHADOW' AND f.config_hash=? AND f.window_start_ms=? AND f.window_end_ms=?
                 AND c.mode='SHADOW' AND c.config_hash=? AND c.window_start_ms=? AND c.window_end_ms=?""",
            params + params,
        )
        campaigns = int((row or {}).get("campaigns") or 0)
        settled = int((row or {}).get("settled") or 0)
        expected_fills = int((row or {}).get("expected_fills") or 0)
        simulated_fills = int((fill_row or {}).get("simulated_fills") or 0)
        invariants = await self._shadow_invariant_metrics(
            config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        coverage = (Decimal(settled) / Decimal(campaigns)) if campaigns else Decimal("0")
        fill_rate = (Decimal(simulated_fills) / Decimal(expected_fills)) if expected_fills else Decimal("0")
        return {
            "shadow_samples": settled,
            "unique_shadow_counterfactual_resolved": settled,
            "unique_settled_markets": settled,
            "campaign_count": campaigns,
            "settled_count": settled,
            "coverage": str(coverage),
            "settlement_rate": str(coverage if campaigns else Decimal("0")),
            "expected_fill_count": expected_fills,
            "simulated_fill_count": simulated_fills,
            "fill_rate": str(fill_rate),
            "simulated_fees": str(row.get("fees", 0) if row else 0),
            "after_fee_pnl": str(row.get("pnl", 0) if row else 0),
            **invariants,
        }

    @staticmethod
    def _shadow_evidence_projection(row: Mapping[str, Any]) -> dict[str, Any]:
        payload = _json_load(row.get("payload_json"), {})
        result = dict(payload) if isinstance(payload, Mapping) else {}
        result.update(
            {
                "evidence_id": row.get("evidence_id"),
                "evidence_identity": row.get("evidence_identity"),
                "mode": row.get("mode", "SHADOW"),
                "config_hash": row.get("config_hash"),
                "window_start_ms": row.get("window_start_ms"),
                "window_end_ms": row.get("window_end_ms"),
                "generated_at_ms": row.get("generated_at_ms"),
                "repository_commit": row.get("repository_commit"),
                "database_path": row.get("database_path"),
                "db_path": row.get("database_path"),
                "unique_shadow_counterfactual_resolved": row.get("unique_resolved_count", 0),
                "shadow_samples": row.get("unique_resolved_count", 0),
                "campaign_count": row.get("campaign_count", 0),
                "settled_count": row.get("settled_count", 0),
                "coverage": row.get("coverage", "0"),
                "settlement_rate": row.get("settlement_rate", "0"),
                "expected_fill_count": row.get("expected_fill_count", 0),
                "simulated_fill_count": row.get("simulated_fill_count", 0),
                "fill_rate": row.get("fill_rate", "0"),
                "simulated_fees": row.get("simulated_fees", "0"),
                "after_fee_pnl": row.get("after_fee_pnl", "0"),
                "unresolved_intents": row.get("unresolved_intents", 0),
                "unresolved_orders": row.get("unresolved_orders", 0),
                "duplicate_violations": row.get("duplicate_violations", 0),
                "overbuy_violations": row.get("overbuy_violations", 0),
                "action_limit_violations": row.get("action_limit_violations", 0),
                "invariant_violations": row.get("invariant_violations", 0),
                "real_invariant_violations": row.get("invariant_violations", 0),
            }
        )
        result["shadow_window"] = {
            "start_ms": result["window_start_ms"],
            "end_ms": result["window_end_ms"],
            "window_start_ms": result["window_start_ms"],
            "window_end_ms": result["window_end_ms"],
        }
        result["canary_window"] = {
            "window_start_ms": result["window_start_ms"],
            "window_end_ms": result["window_end_ms"],
        }
        return result

    async def record_shadow_evidence(
        self,
        *,
        config_hash: str,
        window_start_ms: int,
        window_end_ms: int,
        generated_at_ms: int | None = None,
        repository_commit: str | None = None,
        database_path: str | None = None,
    ) -> dict[str, Any]:
        """Append one immutable SQL-derived evidence snapshot."""

        config, start, end = self._shadow_provenance(
            config_hash=config_hash, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        generated = int(generated_at_ms if generated_at_ms is not None else _now_ms())
        now = _now_ms()
        if generated > now:
            raise ValueError("shadow evidence generated_at_ms cannot be in the future")
        commit = str(repository_commit or self._git_commit()).strip()
        if not commit or commit == "unknown":
            raise ValueError("repository commit identity is unavailable")
        db_path = str(Path(database_path).resolve()) if database_path else self._shadow_database_path()
        metrics = await self._shadow_metrics(config_hash=config, window_start_ms=start, window_end_ms=end)
        stable = {
            "mode": "SHADOW",
            "config_hash": config,
            "window_start_ms": start,
            "window_end_ms": end,
            "generated_at_ms": generated,
            "repository_commit": commit,
            "database_path": db_path,
            **metrics,
        }
        identity = hashlib.sha256(_json_dumps(stable).encode("utf-8")).hexdigest()
        await self._execute(
            """INSERT INTO prediction_shadow_evidence
               (evidence_identity,mode,config_hash,window_start_ms,window_end_ms,generated_at_ms,
                repository_commit,database_path,unique_resolved_count,campaign_count,settled_count,
                coverage,settlement_rate,expected_fill_count,simulated_fill_count,fill_rate,
                simulated_fees,after_fee_pnl,unresolved_intents,unresolved_orders,duplicate_violations,
                overbuy_violations,action_limit_violations,invariant_violations,payload_json,created_at_ms)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(evidence_identity) DO NOTHING""",
            (
                identity,
                "SHADOW",
                config,
                start,
                end,
                generated,
                commit,
                db_path,
                metrics["unique_shadow_counterfactual_resolved"],
                metrics["campaign_count"],
                metrics["settled_count"],
                metrics["coverage"],
                metrics["settlement_rate"],
                metrics["expected_fill_count"],
                metrics["simulated_fill_count"],
                metrics["fill_rate"],
                metrics["simulated_fees"],
                metrics["after_fee_pnl"],
                metrics["unresolved_intents"],
                metrics["unresolved_orders"],
                metrics["duplicate_intents"] + metrics["duplicate_fills"],
                metrics["overbuy_violations"],
                metrics["action_limit_violations"],
                metrics["invariant_violations"],
                _json_dumps(stable),
                _now_ms(),
            ),
        )
        row = await self._fetchone("SELECT * FROM prediction_shadow_evidence WHERE evidence_identity=?", (identity,))
        if row is None:
            raise RuntimeError("shadow evidence was not persisted")
        return self._shadow_evidence_projection(row)

    async def get_shadow_promotion_evidence(
        self,
        *,
        config_hash: str | None = None,
        window_start_ms: int | None = None,
        window_end_ms: int | None = None,
    ) -> dict[str, Any]:
        """Return an immutable SQL-derived snapshot for one SHADOW window.

        A read never rewrites ``generated_at_ms``.  When new ledger rows
        change the metrics, a new append-only evidence identity is recorded;
        a repeated read of unchanged rows returns the original snapshot.
        """

        config = str(config_hash or "").strip() or None
        window = await self.get_shadow_window(
            config_hash=config, window_start_ms=window_start_ms, window_end_ms=window_end_ms
        )
        if window is not None:
            config = str(window["config_hash"])
            start = int(window["window_start_ms"])
            end = int(window["window_end_ms"])
        else:
            runtime_window = await self.get_runtime_config("prediction_shadow_window", None)
            if isinstance(runtime_window, Mapping):
                if config is None and runtime_window.get("config_hash"):
                    config = str(runtime_window["config_hash"])
                if window_start_ms is None:
                    window_start_ms = runtime_window.get("window_start_ms", runtime_window.get("start_ms"))
                if window_end_ms is None:
                    window_end_ms = runtime_window.get("window_end_ms", runtime_window.get("end_ms"))
            if config is None:
                runtime_config = await self.get_runtime_config("prediction_config_hash", None)
                if runtime_config:
                    config = str(runtime_config)
            if config is not None and window_start_ms is not None and window_end_ms is not None:
                start, end = self._shadow_provenance(
                    config_hash=config, window_start_ms=window_start_ms, window_end_ms=window_end_ms
                )[1:]
            else:
                return {
                    "mode": "SHADOW",
                    "shadow_samples": 0,
                    "unique_shadow_counterfactual_resolved": 0,
                    "coverage": "0",
                    "settlement_rate": "0",
                    "fill_rate": "0",
                    "after_fee_pnl": "0",
                    "invariant_violations": 0,
                    "real_invariant_violations": 0,
                    "evidence_error": "immutable shadow window/config identity is missing",
                }
        assert config is not None
        metrics = await self._shadow_metrics(config_hash=config, window_start_ms=start, window_end_ms=end)
        latest = await self._fetchone(
            """SELECT * FROM prediction_shadow_evidence
               WHERE mode='SHADOW' AND config_hash=? AND window_start_ms=? AND window_end_ms=?
               ORDER BY generated_at_ms DESC, evidence_id DESC LIMIT 1""",
            (config, start, end),
        )
        if latest is not None:
            same = all(
                str(latest.get(column, "0")) == str(metrics.get(metric, "0"))
                for column, metric in (
                    ("unique_resolved_count", "unique_shadow_counterfactual_resolved"),
                    ("campaign_count", "campaign_count"),
                    ("settled_count", "settled_count"),
                    ("coverage", "coverage"),
                    ("settlement_rate", "settlement_rate"),
                    ("expected_fill_count", "expected_fill_count"),
                    ("simulated_fill_count", "simulated_fill_count"),
                    ("fill_rate", "fill_rate"),
                    ("simulated_fees", "simulated_fees"),
                    ("after_fee_pnl", "after_fee_pnl"),
                    ("unresolved_intents", "unresolved_intents"),
                    ("unresolved_orders", "unresolved_orders"),
                    ("invariant_violations", "invariant_violations"),
                )
            )
            if same:
                return self._shadow_evidence_projection(latest)
        return await self.record_shadow_evidence(
            config_hash=config,
            window_start_ms=start,
            window_end_ms=end,
        )

    # -- PnL queries ---------------------------------------------------

    async def _sum_net_pnl(self, where: str = "", params: tuple[Any, ...] = ()) -> Decimal:
        clauses = ["status = 'SETTLED'"]
        if where:
            clauses.append(f"({where})")
        rows = await self._fetchall("SELECT net_pnl FROM prediction_settlements WHERE " + " AND ".join(clauses), params)
        return sum((_d(row.get("net_pnl")) for row in rows), Decimal("0"))

    async def get_campaign_pnl(self, campaign_id: str) -> Decimal:
        return await self._sum_net_pnl("campaign_id = ?", (campaign_id,))

    get_campaign_net_pnl = get_campaign_pnl

    async def get_loop_settled_trades(self, loop_id: str) -> list[dict[str, Any]]:
        """Live filled settlements for this loop, excluding net_pnl == 0 Pass/Skip."""

        if not str(loop_id or "").strip():
            return []
        return await self._fetchall(
            """SELECT c.campaign_id, s.net_pnl, s.settled_at_ms, s.winner
               FROM prediction_campaigns c
               JOIN prediction_settlements s ON c.campaign_id = s.campaign_id
               WHERE c.loop_id = ? AND s.status = 'SETTLED'
                 AND CAST(s.net_pnl AS REAL) != 0.0
               ORDER BY s.settled_at_ms ASC""",
            (str(loop_id),),
        )

    async def get_loop_pnl(self, loop_id: str) -> Decimal:
        live = await self._sum_net_pnl("loop_id = ?", (loop_id,))
        shadow_rows = await self._fetchall(
            """SELECT s.simulated_pnl
               FROM prediction_shadow_settlements s
               JOIN prediction_shadow_campaigns sc
                 ON sc.shadow_campaign_id=s.shadow_campaign_id
               JOIN prediction_campaigns c
                 ON c.campaign_id=sc.campaign_id
               WHERE s.status='SETTLED' AND c.loop_id=?""",
            (str(loop_id),),
        )
        shadow = sum((_d(row.get("simulated_pnl")) for row in shadow_rows), Decimal("0"))
        return live + shadow

    get_loop_net_pnl = get_loop_pnl

    async def get_loop_pnl_summary(self, *, limit: int = 10) -> dict[str, Any]:
        """Return a compact, exact-Decimal summary for all Prediction loops."""

        limit = max(1, min(int(limit), 50))
        loops = await self._fetchall(
            """SELECT loop_id,target,completed,state,created_at_ms,updated_at_ms
               FROM prediction_loops
               ORDER BY created_at_ms DESC
               LIMIT ?""",
            (limit,),
        )
        live_rows = await self._fetchall(
            """SELECT s.loop_id,s.net_pnl,
                      CASE WHEN EXISTS (
                          SELECT 1 FROM prediction_fills f
                          WHERE f.campaign_id=s.campaign_id
                      ) THEN 1 ELSE 0 END AS has_fill
                 FROM prediction_settlements s
                WHERE s.status='SETTLED' AND s.loop_id IS NOT NULL"""
        )
        shadow_rows = await self._fetchall(
            """SELECT c.loop_id,s.simulated_pnl,
                      CASE WHEN EXISTS (
                          SELECT 1 FROM prediction_shadow_fills f
                          WHERE f.shadow_campaign_id=s.shadow_campaign_id
                      ) THEN 1 ELSE 0 END AS has_fill
               FROM prediction_shadow_settlements s
               JOIN prediction_shadow_campaigns sc
                 ON sc.shadow_campaign_id=s.shadow_campaign_id
               JOIN prediction_campaigns c
                 ON c.campaign_id=sc.campaign_id
               WHERE s.status='SETTLED' AND c.loop_id IS NOT NULL"""
        )
        pnl_by_loop: dict[str, Decimal] = {}
        stats_by_loop: dict[str, dict[str, int]] = {}
        total_stats = {"wins": 0, "losses": 0, "breakevens": 0, "no_trades": 0}
        total = Decimal("0")
        for row in (*live_rows, *shadow_rows):
            loop_id = str(row.get("loop_id") or "")
            if not loop_id:
                continue
            value = _d(row.get("net_pnl", row.get("simulated_pnl")))
            pnl_by_loop[loop_id] = pnl_by_loop.get(loop_id, Decimal("0")) + value
            total += value
            stats = stats_by_loop.setdefault(
                loop_id,
                {"wins": 0, "losses": 0, "breakevens": 0, "no_trades": 0},
            )
            if value > 0:
                bucket = "wins"
            elif value < 0:
                bucket = "losses"
            elif bool(row.get("has_fill")):
                bucket = "breakevens"
            else:
                bucket = "no_trades"
            stats[bucket] += 1
            total_stats[bucket] += 1

        def with_win_rate(stats: Mapping[str, int] | None = None) -> dict[str, Any]:
            values = dict(stats or {})
            wins = int(values.get("wins", 0))
            losses = int(values.get("losses", 0))
            decisive = wins + losses
            win_rate = (
                str((Decimal(wins) * Decimal("100") / Decimal(decisive)).quantize(Decimal("0.01")))
                if decisive
                else None
            )
            return {
                "wins": wins,
                "losses": losses,
                "breakevens": int(values.get("breakevens", 0)),
                "no_trades": int(values.get("no_trades", 0)),
                "win_rate": win_rate,
            }

        records = [
            {
                "loop_id": str(row.get("loop_id") or ""),
                "target": int(row.get("target") or 0),
                "completed": int(row.get("completed") or 0),
                "state": str(row.get("state") or ""),
                "pnl": str(pnl_by_loop.get(str(row.get("loop_id") or ""), Decimal("0"))),
                "created_at_ms": row.get("created_at_ms"),
                "updated_at_ms": row.get("updated_at_ms"),
                **with_win_rate(stats_by_loop.get(str(row.get("loop_id") or ""))),
            }
            for row in loops
        ]
        active = next((item for item in records if item["state"] == "RUNNING"), None)
        overall = with_win_rate(total_stats)
        current = with_win_rate(active) if active else with_win_rate()
        return {
            "total_loop_pnl": str(total),
            "current_loop_pnl": str(active["pnl"]) if active else "0",
            "active_loop_id": active["loop_id"] if active else None,
            "loop_count": len(records),
            "loops": records,
            "total_wins": overall["wins"],
            "total_losses": overall["losses"],
            "total_breakevens": overall["breakevens"],
            "total_no_trades": overall["no_trades"],
            "total_win_rate": overall["win_rate"],
            "current_wins": current["wins"],
            "current_losses": current["losses"],
            "current_breakevens": current["breakevens"],
            "current_no_trades": current["no_trades"],
            "current_win_rate": current["win_rate"],
        }

    async def get_daily_pnl(
        self,
        day: date | str | int,
        *,
        timezone_name: str = "Asia/Taipei",
    ) -> Decimal:
        if isinstance(day, int):
            # An integer is treated as an epoch millisecond for convenience.
            local_day = datetime.fromtimestamp(day / 1000, ZoneInfo(timezone_name)).date()
        elif isinstance(day, str):
            local_day = date.fromisoformat(day)
        else:
            local_day = day
        zone = ZoneInfo(timezone_name)
        start = datetime.combine(local_day, dt_time.min, tzinfo=zone)
        end = start + timedelta(days=1)
        return await self._sum_net_pnl(
            "settled_at_ms >= ? AND settled_at_ms < ?",
            (int(start.timestamp() * 1000), int(end.timestamp() * 1000)),
        )

    get_daily_net_pnl = get_daily_pnl

    async def get_pnl_summary(self, *, campaign_id: str | None = None, loop_id: str | None = None, day: date | str | int | None = None) -> Decimal:
        if campaign_id is not None:
            return await self.get_campaign_pnl(campaign_id)
        if loop_id is not None:
            return await self.get_loop_pnl(loop_id)
        if day is not None:
            return await self.get_daily_pnl(day)
        return await self._sum_net_pnl()


    async def create_shadow_first_candidate(self, key: str, value: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
        """Commit one immutable candidate before any simulated fill.

        Only the creator may consume this attempt. A restart after this commit
        cannot substitute a later quote, even when the first fill is absent.
        """
        if not str(key).startswith("prediction_shadow_first_candidate:"):
            raise ValueError("first candidate key must be Shadow-scoped")
        inserted = await self._execute(
            """INSERT INTO prediction_runtime_config(config_key, config_value_json, updated_at_ms)
               VALUES (?, ?, ?) ON CONFLICT(config_key) DO NOTHING""",
            (str(key), _json_dumps(value), _now_ms()),
        )
        record = await self.get_runtime_config(key)
        if not isinstance(record, dict):
            raise RuntimeError("durable Shadow first candidate missing or corrupt")
        return inserted == 1, record


    async def save_shadow_observer_market(
        self,
        campaign: Campaign,
        *,
        payload: Mapping[str, Any] | None = None,
        state: str = "ACTIVE",
        last_seen_at_ms: int | None = None,
    ) -> dict[str, Any]:
        """Persist one read-only market identity outside the Live ledger."""

        now = _now_ms()
        market = campaign.market
        observer_payload = payload if payload is not None else market.raw
        await self._execute(
            """INSERT INTO prediction_shadow_observer_markets
               (observer_campaign_id,market_topic_id,market_id,slug,start_time_ms,end_time_ms,
                state,winner,last_quote_at_ms,last_seen_at_ms,last_error,payload_json,created_at_ms,updated_at_ms)
               VALUES(?,?,?,?,?,?,?,NULL,0,?,NULL,?,?,?)
               ON CONFLICT(observer_campaign_id) DO UPDATE SET
                 market_id=excluded.market_id,slug=excluded.slug,
                 start_time_ms=excluded.start_time_ms,end_time_ms=excluded.end_time_ms,
                 state=CASE WHEN prediction_shadow_observer_markets.state='SETTLED'
                            THEN prediction_shadow_observer_markets.state ELSE excluded.state END,
                 last_seen_at_ms=excluded.last_seen_at_ms,
                 payload_json=excluded.payload_json,updated_at_ms=excluded.updated_at_ms""",
            (
                campaign.campaign_id,
                market.market_topic_id,
                market.market_id,
                market.slug,
                int(market.start_time_ms),
                int(market.end_time_ms),
                str(state or "ACTIVE").upper(),
                int(last_seen_at_ms if last_seen_at_ms is not None else now),
                _json_dumps(observer_payload),
                now,
                now,
            ),
        )
        return await self._fetchone(
            "SELECT * FROM prediction_shadow_observer_markets WHERE observer_campaign_id=?",
            (campaign.campaign_id,),
        ) or {}


    async def get_shadow_observer_market(self, observer_campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM prediction_shadow_observer_markets WHERE observer_campaign_id=?",
            (str(observer_campaign_id),),
        )
        if row:
            row["payload"] = _json_load(row.get("payload_json"), {})
        return row


    async def get_active_shadow_observer_market(self) -> dict[str, Any] | None:
        row = await self._fetchone(
            """SELECT * FROM prediction_shadow_observer_markets
               WHERE state IN ('ACTIVE','PENDING_RESOLUTION')
               ORDER BY start_time_ms ASC, updated_at_ms ASC LIMIT 1"""
        )
        if row:
            row["payload"] = _json_load(row.get("payload_json"), {})
        return row


    async def get_latest_shadow_observer_market(self) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM prediction_shadow_observer_markets ORDER BY updated_at_ms DESC LIMIT 1"
        )
        if row:
            row["payload"] = _json_load(row.get("payload_json"), {})
        return row


    async def update_shadow_observer_market(
        self,
        observer_campaign_id: str,
        *,
        state: str | None = None,
        winner: str | None = None,
        last_quote_at_ms: int | None = None,
        last_seen_at_ms: int | None = None,
        last_error: str | None = None,
    ) -> None:
        current = await self.get_shadow_observer_market(observer_campaign_id)
        if not current:
            return
        await self._execute(
            """UPDATE prediction_shadow_observer_markets SET
                 state=?,winner=?,last_quote_at_ms=?,last_seen_at_ms=?,last_error=?,updated_at_ms=?
               WHERE observer_campaign_id=?""",
            (
                str(state or current.get("state") or "ACTIVE").upper(),
                winner if winner is not None else current.get("winner"),
                int(last_quote_at_ms if last_quote_at_ms is not None else current.get("last_quote_at_ms") or 0),
                int(last_seen_at_ms if last_seen_at_ms is not None else current.get("last_seen_at_ms") or 0),
                last_error,
                _now_ms(),
                str(observer_campaign_id),
            ),
        )


    async def save_shadow_observer_state(self, observer_campaign_id: str, state: Mapping[str, Any]) -> None:
        data = _jsonable(state)
        await self._execute(
            """INSERT INTO prediction_shadow_observer_state
               (observer_campaign_id,prior_spot,last_spot,last_spot_at_ms,prior_leader,last_leader,
                leader_since_ms,leader_quotes,cross_count,last_quote_at_ms,payload_json,updated_at_ms)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(observer_campaign_id) DO UPDATE SET
                 prior_spot=excluded.prior_spot,last_spot=excluded.last_spot,
                 last_spot_at_ms=excluded.last_spot_at_ms,prior_leader=excluded.prior_leader,
                 last_leader=excluded.last_leader,leader_since_ms=excluded.leader_since_ms,
                 leader_quotes=excluded.leader_quotes,cross_count=excluded.cross_count,
                 last_quote_at_ms=excluded.last_quote_at_ms,payload_json=excluded.payload_json,
                 updated_at_ms=excluded.updated_at_ms""",
            (
                str(observer_campaign_id),
                str(data.get("prior_spot")) if data.get("prior_spot") is not None else None,
                str(data.get("last_spot")) if data.get("last_spot") is not None else None,
                _int(data.get("last_spot_at_ms")),
                data.get("prior_leader"),
                data.get("last_leader"),
                _int(data.get("leader_since_ms")),
                _int(data.get("leader_quotes")),
                _int(data.get("cross_count")),
                _int(data.get("last_quote_at_ms")),
                _json_dumps(data),
                _now_ms(),
            ),
        )


    async def get_shadow_observer_state(self, observer_campaign_id: str) -> dict[str, Any] | None:
        row = await self._fetchone(
            "SELECT * FROM prediction_shadow_observer_state WHERE observer_campaign_id=?",
            (str(observer_campaign_id),),
        )
        if row:
            payload = _json_load(row.get("payload_json"), {})
            if isinstance(payload, Mapping):
                row.update(payload)
        return row


    async def save_shadow_observer_quote(
        self,
        observer_campaign_id: str,
        quote: QuoteSnapshot | Mapping[str, Any],
    ) -> int:
        values = self._quote_values(observer_campaign_id, quote)
        await self._execute(
            """INSERT INTO prediction_shadow_observer_quotes
               (observer_campaign_id,observed_at_ms,up_bid,up_ask,down_bid,down_ask,leader,
                btc_spot,reference_price,feed_ok,flip_confirmed,btc_crossed_reference,stable_final,
                leader_duration_ms,reference_recross,spot_observed_at_ms,payload_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(observer_campaign_id,observed_at_ms) DO UPDATE SET
                 up_bid=excluded.up_bid,up_ask=excluded.up_ask,down_bid=excluded.down_bid,
                 down_ask=excluded.down_ask,leader=excluded.leader,btc_spot=excluded.btc_spot,
                 reference_price=excluded.reference_price,feed_ok=excluded.feed_ok,
                 flip_confirmed=excluded.flip_confirmed,btc_crossed_reference=excluded.btc_crossed_reference,
                 stable_final=excluded.stable_final,leader_duration_ms=excluded.leader_duration_ms,
                 reference_recross=excluded.reference_recross,spot_observed_at_ms=excluded.spot_observed_at_ms,
                 payload_json=excluded.payload_json""",
            values,
        )
        row = await self._fetchone(
            "SELECT observer_quote_id FROM prediction_shadow_observer_quotes WHERE observer_campaign_id=? AND observed_at_ms=?",
            (observer_campaign_id, values[1]),
        )
        if not row:
            raise RuntimeError("shadow observer quote write did not produce a row")
        return int(row["observer_quote_id"])


    async def get_shadow_observer_quotes(
        self,
        observer_campaign_id: str,
        *,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        return await self._fetchall(
            """SELECT * FROM prediction_shadow_observer_quotes
               WHERE observer_campaign_id=? ORDER BY observed_at_ms ASC LIMIT ?""",
            (str(observer_campaign_id), max(1, int(limit))),
        )


    async def get_shadow_observer_status(self) -> dict[str, Any]:
        latest = await self.get_latest_shadow_observer_market()
        now = _now_ms()
        if not latest:
            return {
                "enabled": True,
                "state": "WAIT_DATA",
                "latest_market": None,
                "last_quote_at_ms": None,
                "quote_age_ms": None,
            }
        last_quote = int(latest.get("last_quote_at_ms") or 0)
        return {
            "enabled": True,
            "state": str(latest.get("state") or "WAIT_DATA").upper(),
            "observer_campaign_id": latest.get("observer_campaign_id"),
            "market_topic_id": latest.get("market_topic_id"),
            "market_start_time_ms": latest.get("start_time_ms"),
            "market_end_time_ms": latest.get("end_time_ms"),
            "last_quote_at_ms": last_quote or None,
            "quote_age_ms": max(0, now - last_quote) if last_quote else None,
            "last_error": latest.get("last_error"),
        }

    async def record_lane_signal(
        self,
        *,
        strategy_lane: str,
        campaign_id: str,
        market_id: str | None,
        window_id: str | None,
        intent_id: str | None,
        signal_ts: int,
        direction: str | None,
        reference_price: float | None,
        spot_price: float | None,
        pre_cross_count: int = 0,
        same_side_seconds: float = 0.0,
        distance_bps: float = 0.0,
        ask_at_signal: float | None = None,
        bid_at_signal: float | None = None,
        status: str = "SIGNAL",
        reject_reason: str | None = None,
        size_usdt: float = 1.0,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
        attribution_id = f"{strategy_lane}::{campaign_id}::{signal_ts}"
        now = _now_ms()
        payload_json = _json_dumps(payload or {})
        conn = self._require_conn()
        await self._begin(conn)
        try:
            await conn.execute(
                """INSERT INTO prediction_lane_attribution
                   (attribution_id, strategy_lane, campaign_id, market_id, window_id, intent_id,
                    signal_ts, direction, reference_price, spot_price, pre_cross_count,
                    same_side_seconds, distance_bps, ask_at_signal, bid_at_signal,
                    status, reject_reason, size_usdt, payload_json, created_at_ms, updated_at_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(attribution_id) DO UPDATE SET
                    status=excluded.status, reject_reason=excluded.reject_reason,
                    intent_id=COALESCE(excluded.intent_id, prediction_lane_attribution.intent_id),
                    updated_at_ms=excluded.updated_at_ms""",
                (
                    attribution_id, strategy_lane, campaign_id, market_id, window_id, intent_id,
                    signal_ts, direction, reference_price, spot_price, pre_cross_count,
                    same_side_seconds, distance_bps, ask_at_signal, bid_at_signal,
                    status, reject_reason, size_usdt, payload_json, now, now,
                ),
            )
            await conn.commit()
            return attribution_id
        except Exception:
            await conn.rollback()
            raise

    async def update_lane_order(
        self,
        attribution_id: str,
        *,
        order_id: str | None = None,
        order_submit_ts: int | None = None,
        fill_ts: int | None = None,
        fill_price: float | None = None,
        fill_shares: float | None = None,
        fill_latency_ms: int | None = None,
        status: str | None = None,
        reject_reason: str | None = None,
    ) -> None:
        conn = self._require_conn()
        now = _now_ms()
        await self._begin(conn)
        try:
            await conn.execute(
                """UPDATE prediction_lane_attribution SET
                   order_id=COALESCE(?, order_id),
                   order_submit_ts=COALESCE(?, order_submit_ts),
                   fill_ts=COALESCE(?, fill_ts),
                   fill_price=COALESCE(?, fill_price),
                   fill_shares=COALESCE(?, fill_shares),
                   fill_latency_ms=COALESCE(?, fill_latency_ms),
                   status=COALESCE(?, status),
                   reject_reason=COALESCE(?, reject_reason),
                   updated_at_ms=?
                   WHERE attribution_id=?""",
                (
                    order_id, order_submit_ts, fill_ts, fill_price, fill_shares,
                    fill_latency_ms, status, reject_reason, now, attribution_id,
                ),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


    async def settle_lane_rejects_hypothetical(
        self,
        campaign_id: str,
        strategy_lane: str,
        *,
        winner: str,
    ) -> int:
        """Settle rejected/signal P3 rows with hypothetical 1U PnL (Spec §13)."""
        import json as _json
        conn = self._require_conn()
        now = _now_ms()
        winner_u = str(winner or "").upper()
        await self._begin(conn)
        try:
            cur = await conn.execute(
                """SELECT attribution_id, direction, ask_at_signal, payload_json
                   FROM prediction_lane_attribution
                   WHERE campaign_id=? AND strategy_lane=?
                     AND status IN ('SIGNAL','CANCELLED','SUBMITTED')
                     AND COALESCE(final_result,'') = ''""",
                (campaign_id, strategy_lane),
            )
            rows = await cur.fetchall()
            updated = 0
            for row in rows:
                direction = str(row["direction"] or "").upper()
                try:
                    ask = float(row["ask_at_signal"] or 0)
                except (TypeError, ValueError):
                    ask = 0.0
                if not direction or ask <= 0 or ask >= 1:
                    hypo = 0.0
                    result = "UNKNOWN"
                else:
                    is_win = direction == winner_u
                    hypo = (1.0 / ask - 1.0) if is_win else -1.0
                    result = "WIN" if is_win else "LOSS"
                try:
                    payload = _json.loads(row["payload_json"] or "{}")
                    if not isinstance(payload, dict):
                        payload = {}
                except Exception:
                    payload = {}
                payload["hypothetical"] = True
                payload["winner"] = winner_u
                await conn.execute(
                    """UPDATE prediction_lane_attribution SET
                       final_result=?,
                       realized_pnl=?,
                       status='REJECT_SETTLED',
                       payload_json=?,
                       updated_at_ms=?
                       WHERE attribution_id=?""",
                    (result, hypo, _json.dumps(payload, ensure_ascii=False), now, row["attribution_id"]),
                )
                updated += 1
            await conn.commit()
            return updated
        except Exception:
            await conn.rollback()
            raise

    async def settle_lane_attribution(
        self,
        campaign_id: str,
        strategy_lane: str,
        *,
        final_result: str,
        realized_pnl: float,
    ) -> None:
        conn = self._require_conn()
        now = _now_ms()
        await self._begin(conn)
        try:
            await conn.execute(
                """UPDATE prediction_lane_attribution SET
                   final_result=?,
                   realized_pnl=?,
                   status='SETTLED',
                   updated_at_ms=?
                   WHERE campaign_id=? AND strategy_lane=? AND status='FILLED'""",
                (final_result, realized_pnl, now, campaign_id, strategy_lane),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def get_lane_performance_summary(
        self,
        strategy_lane: str,
        *,
        since_ms: int = 0,
    ) -> dict[str, Any]:
        rows = await self._fetchall(
            """SELECT status, final_result, realized_pnl, size_usdt, fill_price,
                      same_side_seconds, distance_bps, reject_reason
               FROM prediction_lane_attribution
               WHERE strategy_lane=? AND signal_ts >= ?""",
            (strategy_lane, since_ms),
        )
        total_signals = len(rows)
        accepted = [r for r in rows if r["status"] in ("FILLED", "SETTLED")]
        settled = [r for r in rows if r["status"] == "SETTLED"]
        wins = sum(1 for r in settled if str(r["final_result"]).upper() == "WIN")
        losses = sum(1 for r in settled if str(r["final_result"]).upper() == "LOSS")
        net_pnl = sum(float(r["realized_pnl"] or 0) for r in settled)
        total_invested = sum(float(r["size_usdt"] or 1.0) for r in settled)
        wr = (wins / len(settled) * 100.0) if settled else 0.0
        pnl_100 = (net_pnl / total_invested * 100.0) if total_invested > 0 else 0.0

        avg_same_side = (sum(float(r["same_side_seconds"] or 0) for r in accepted) / len(accepted)) if accepted else 0.0
        avg_dist = (sum(float(r["distance_bps"] or 0) for r in accepted) / len(accepted)) if accepted else 0.0

        return {
            "strategy_lane": strategy_lane,
            "total_signals": total_signals,
            "accepted_trades": len(accepted),
            "settled_trades": len(settled),
            "wins": wins,
            "losses": losses,
            "win_rate": wr,
            "net_pnl": net_pnl,
            "pnl_per_100": pnl_100,
            "avg_same_side": avg_same_side,
            "avg_distance": avg_dist,
        }


__all__ = ["PredictionRepository", "MIGRATIONS_DIR", "ACTIVE_STATES"]
