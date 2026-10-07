"""C180-only SQLite ledger and atomic pre-network entry claim.

This adapter is deliberately separate from the VM's existing repository file.
It uses that repository's connection and operation gate so its snapshots and
claims serialize with existing Prediction writes.  No exchange call is made.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping
import re

from .repository import CAMPAIGN_UPSERT_SQL

from .c180_batch_gate import SettledTrade, block_bounds, evaluate_batch_gate
from .c180_gate_runtime import LiveSettlement, LoopLedgerSnapshot, SLOT_MS
from .regime_lane import STATE_KEY, FINGERPRINT, risk_result


PROFILE = "regime_target6_v1"
TIER = "REGIME_T6"
RISK_PROFILES = (PROFILE, "regime_target6_1_v1", "regime_target6_2_v1",
                 "regime_target6_3_v1", "regime_target6_3a_v1", "regime_target6_3b_v1", "regime_target6_5_v1", "regime_target6_7_v1", 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1')
_RISK_MARKS = ",".join("?" for _ in RISK_PROFILES)
TERMINAL_INTENTS = ("FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED")
TERMINAL_ORDERS = ("FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED")
RESETTABLE_HALTS = ("scheduled20_mdd_3.5",)
NO_FILL_TERMINAL = frozenset(("CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED"))


@dataclass(frozen=True)
class C180ClaimResult:
    claimed: bool
    reason: str
    intent_row: Mapping[str, Any] | None = None
    trigger_run: int | None = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _run(start: int, anchor: int) -> int:
    delta = int(start) - int(anchor)
    if delta < 0 or delta % SLOT_MS:
        raise ValueError("C180 market is off the armed grid")
    return delta // SLOT_MS + 1


def _unit(value: Any) -> Decimal:
    unit = Decimal(str(value))
    if unit not in (Decimal("1"), Decimal("2"), Decimal("3")):
        raise ValueError("Regime unit must be 1/2/3 USDT")
    return unit


def _campaign_matches_slot(campaign: Mapping[str, Any] | None,
                           slot: Mapping[str, Any] | None,
                           loop_id: str, start_ms: int) -> bool:
    """Bind a C180 slot to the UP market ID saved in the campaign payload.

    Binance's current five-minute response leaves the legacy campaign.market_id
    empty while supplying up_market_id.  Never treat an empty column as proof
    of identity; verify the original persisted market snapshot instead.
    """

    if campaign is None or slot is None:
        return False
    try:
        market = json.loads(campaign["payload_json"])["market"]
        up_market_id = str(market["up_market_id"])
        return (
            campaign["loop_id"] == loop_id
            and int(campaign["start_time_ms"]) == start_ms
            and campaign["market_topic_id"] == slot["market_topic_id"]
            and str(market["market_topic_id"]) == slot["market_topic_id"]
            and int(market["start_time_ms"]) == start_ms
            and up_market_id == slot["market_id"]
            and str(campaign["market_id"] or "") in ("", up_market_id)
            and slot["verified_at_ms"] is not None
        )
    except (KeyError, TypeError, ValueError, OverflowError, json.JSONDecodeError):
        return False


class _LoopSnapshotReads:
    """Transaction-local SELECT batching; reuse the exact single-loop validators.

    No cache survives a risk check. Each evidence query is read for every loop
    in chunks on the caller's existing SQLite snapshot, then partitioned by ID.
    Only the explicit one-loop SELECTs used by _snapshot_conn are accepted.
    """
    def __init__(self, ledger, conn, loops):
        self.ledger, self.conn = ledger, conn
        self.loops = {row["loop_id"]: row for row in loops}
        self.cache = {}

    async def rows(self, sql, params):
        if len(params) != 1 or params[0] not in self.loops:
            raise ValueError("invalid batched snapshot query")
        if sql == "SELECT * FROM prediction_loops WHERE loop_id=?":
            return [self.loops[params[0]]]
        if sql not in self.cache:
            match = re.search(r"WHERE (c\.)?loop_id=\?", sql)
            if not match:
                raise ValueError("unreviewed batched snapshot query")
            column = (match.group(1) or "") + "loop_id"
            select = re.match(r"SELECT(?: DISTINCT)? ", sql)
            if not select:
                raise ValueError("unreviewed batched snapshot select")
            grouped = {key: [] for key in self.loops}
            ids = list(self.loops)
            for offset in range(0, len(ids), 200):
                chunk = ids[offset:offset+200]
                query = sql[:match.start()] + "WHERE " + column + " IN (" + ",".join("?" for _ in chunk) + ")" + sql[match.end():]
                query = query[:select.end()] + column + " AS _snapshot_loop_id," + query[select.end():]
                # The orphan probe's LIMIT 1 is per loop in the original path.
                # Fetch all probes here so an orphan in any loop is retained.
                query = re.sub(r" LIMIT 1\s*$", "", query)
                for row in await self.ledger._rows(self.conn, query, tuple(chunk)):
                    key = row.pop("_snapshot_loop_id")
                    grouped[key].append(row)
            self.cache[sql] = grouped
        return self.cache[sql][params[0]]


class RegimeLiveLedger:
    """Use one already-initialized PredictionRepository instance."""

    def __init__(self, repository: Any, *, state_key: str = STATE_KEY,
                 profile: str = PROFILE) -> None:
        if profile not in RISK_PROFILES:
            raise ValueError("unsupported regime profile")
        self.repository = repository
        self.state_key = state_key
        self.profile = profile
        self.tier = ("REGIME_T69A" if profile == "regime_target6_9a_v1" else "REGIME_T69" if profile == "regime_target6_9_v1" else "REGIME_T68A" if profile == "regime_target6_8a_v1" else "REGIME_T68" if profile == "regime_target6_8_v1" else "REGIME_T67C" if profile == "regime_target6_7c_v1" else "REGIME_T67D" if profile == "regime_target6_7d_v1" else "REGIME_T67B" if profile == "regime_target6_7b_v1" else "REGIME_T67A" if profile == "regime_target6_7a_v1" else "REGIME_T67" if profile == "regime_target6_7_v1" else "REGIME_T65" if profile == "regime_target6_5_v1" else
                     "REGIME_T63B" if profile == "regime_target6_3b_v1" else
                     "REGIME_T63A" if profile == "regime_target6_3a_v1" else
                     "REGIME_T63" if profile == "regime_target6_3_v1" else TIER if profile == PROFILE else
                     "REGIME_T62" if profile in ("regime_target6_2_v1", 'regime_target6_3_v1') else "REGIME_T61")
        self.max_price = Decimal("0.80") if profile == "regime_target6_7d_v1" else Decimal("0.65") if profile == PROFILE else Decimal("0.75")

    async def _risk_conn(self, conn, loop_id, start, now):
        row = await self._row(conn,
            "SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?",
            (self.state_key,))
        if row:
            state = json.loads(row["config_value_json"])
        else:
            # Existing claims without their persistent state are an integrity
            # failure. Never establish a fresh epoch to bypass an old loss.
            if await self._row(conn, "SELECT 1 FROM prediction_regime_entry_claims LIMIT 1"):
                return False, "persistent_state_missing_with_history"
            state = {"version": 1, "fingerprint": FINGERPRINT,
                     "first_market_start_ms": start, "unit_usdt": "1",
                     "halt_reason": None}
        loops = await self._rows(conn,
            "SELECT * FROM prediction_loops WHERE strategy_profile IN ("+_RISK_MARKS+") AND mode='LIVE'",
            RISK_PROFILES)
        unknown = await self._row(conn,
            """SELECT 1 FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id
               WHERE l.strategy_profile IN ("""+_RISK_MARKS+""") AND l.mode='LIVE' AND
               (c.pending_unknown=1 OR EXISTS(SELECT 1 FROM prediction_order_intents i
                 WHERE i.campaign_id=c.campaign_id AND i.unknown=1)) LIMIT 1""", RISK_PROFILES)
        if unknown:
            state["halt_reason"] = state.get("halt_reason") or "unknown_order_reconciliation_required"
        settlements, unresolved, complete = [], False, True
        own_snapshot = None
        reads = _LoopSnapshotReads(self, conn, loops)
        for loop in loops:
            snapshot = await self._snapshot_conn(reads, loop["loop_id"], now)
            if loop["loop_id"] == loop_id:
                own_snapshot = snapshot
            complete = complete and snapshot.complete
            settlements.extend(snapshot.settlements)
            unresolved = unresolved or bool(snapshot.unresolved_market_starts)
        allowed, reason = risk_result(state, settlements, start, now,
                                      unresolved=unresolved, unknown=bool(unknown))
        if not complete:
            allowed, reason = False, state.get("halt_reason") or "lane_ledger_incomplete"
        if self.profile in ("regime_target6_3b_v1", "regime_target6_5_v1", "regime_target6_7_v1", 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1') and complete:
            from .regime_t63b_risk import loop_drawdown
            from .regime_t63b_lane import FINGERPRINT as guard_fingerprint
            prefix = "t63b"
            if self.profile == "regime_target6_5_v1":
                from .regime_t65_lane import FINGERPRINT as guard_fingerprint
                prefix = "t65"

            if self.profile == "regime_target6_7_v1":
                from .regime_t67_policy import FINGERPRINT as guard_fingerprint
                prefix = "t67"

            if self.profile == "regime_target6_7a_v1":
                from .regime_t67a_policy import FINGERPRINT as guard_fingerprint
                prefix = "t67a"

            if self.profile == "regime_target6_7b_v1":
                from .regime_t67b_policy import FINGERPRINT as guard_fingerprint
                prefix = "t67b"

            if self.profile == "regime_target6_7d_v1":
                from .regime_t67d_policy import FINGERPRINT as guard_fingerprint
                prefix = "t67d"

            if self.profile == "regime_target6_7c_v1":
                from .regime_t67c_policy import FINGERPRINT as guard_fingerprint
                prefix = "t67c"

            if self.profile == "regime_target6_9_v1":
                from .regime_t69_policy import FINGERPRINT as guard_fingerprint
                prefix = "t69"
            if self.profile == "regime_target6_9a_v1":
                from .regime_t69a_policy import FINGERPRINT as guard_fingerprint
                prefix = "t69a"
            if self.profile == "regime_target6_8a_v1":
                from .regime_t68a_policy import FINGERPRINT as guard_fingerprint
                prefix = "t68a"
            if self.profile == "regime_target6_8_v1":
                from .regime_t68_policy import FINGERPRINT as guard_fingerprint
                prefix = "t68"

            if own_snapshot is None:
                allowed, reason = False, f"{prefix}_loop_ledger_missing"
            else:
                key = self.profile.removesuffix("_v1") + "_loop_risk:" + str(loop_id)
                old = await self._row(conn,
                    "SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?", (key,))
                prior = json.loads(old["config_value_json"]) if old else {}
                if prior and (prior.get("loop_id") != str(loop_id) or prior.get("version") != 1
                              or prior.get("fingerprint") != guard_fingerprint
                              or prior.get("halt_reason") not in (None, f"{prefix}_loop_mdd_3.5")):
                    allowed, reason = False, f"{prefix}_loop_risk_state_invalid"
                else:
                    try:
                        peak, equity, dd, trigger = loop_drawdown(
                            own_snapshot.settlements, start_ms=start, now_ms=now)
                    except (ValueError, TypeError, ArithmeticError):
                        allowed, reason = False, f"{prefix}_loop_risk_unavailable"
                    else:
                        latched = prior.get("halt_reason") == f"{prefix}_loop_mdd_3.5" or trigger is not None
                        guard = {"version": 1, "fingerprint": guard_fingerprint,
                                 "loop_id": str(loop_id),
                                 "peak_1u": str(peak), "equity_1u": str(equity),
                                 "mdd_1u": str(dd), "limit_1u": "3.5",
                                 "halt_reason": f"{prefix}_loop_mdd_3.5" if latched else None,
                                 "trigger_settlement_id": prior.get("trigger_settlement_id") or trigger,
                                 "last_checked_at_ms": now}
                        await conn.execute(
                            """INSERT INTO prediction_runtime_config(config_key,config_value_json,updated_at_ms)
                               VALUES(?,?,?) ON CONFLICT(config_key) DO UPDATE SET
                               config_value_json=excluded.config_value_json,updated_at_ms=excluded.updated_at_ms""",
                            (key, json.dumps(guard, sort_keys=True), now))
                        if latched:
                            allowed, reason = False, f"{prefix}_loop_mdd_3.5"
        state.update(loop_id=loop_id, last_checked_at_ms=now)
        await conn.execute(
            """INSERT INTO prediction_runtime_config(config_key,config_value_json,updated_at_ms)
               VALUES(?,?,?) ON CONFLICT(config_key) DO UPDATE SET
               config_value_json=excluded.config_value_json,updated_at_ms=excluded.updated_at_ms""",
            (self.state_key, json.dumps(state, sort_keys=True), now))
        return allowed, reason

    async def reset_shared_risk(self, *, now_ms: int, reason: str) -> dict[str, Any]:
        """Audited one-time reset of the shared T6 20-run MDD halt.

        Only a scheduled20 MDD halt can be reset (never cumulative loss), and
        only with every T6 LIVE settlement known and no unknown order.  The
        risk count restarts at the next market slot; history is never edited.
        """
        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                result = await self._reset_shared_risk_conn(conn, int(now_ms), str(reason))
                await conn.commit()
                return result
            except BaseException:
                await conn.rollback()
                raise

    async def _reset_shared_risk_conn(self, conn, now, reason):
        row = await self._row(conn,
            "SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?",
            (self.state_key,))
        if not row:
            return {"reset": False, "reason": "risk_state_missing"}
        state = json.loads(row["config_value_json"])
        halt = state.get("halt_reason")
        if state.get("fingerprint") != FINGERPRINT:
            return {"reset": False, "reason": "policy_fingerprint_mismatch"}
        if halt not in RESETTABLE_HALTS:
            return {"reset": False, "reason": "not_resettable", "halt_reason": halt}
        loops = await self._rows(conn,
            "SELECT * FROM prediction_loops WHERE strategy_profile IN ("+_RISK_MARKS+") AND mode='LIVE'",
            RISK_PROFILES)
        unknown = await self._row(conn,
            """SELECT 1 FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id
               WHERE l.strategy_profile IN ("""+_RISK_MARKS+""") AND l.mode='LIVE' AND
               (c.pending_unknown=1 OR EXISTS(SELECT 1 FROM prediction_order_intents i
                 WHERE i.campaign_id=c.campaign_id AND i.unknown=1)) LIMIT 1""", RISK_PROFILES)
        if unknown:
            return {"reset": False, "reason": "unknown_order_reconciliation_required"}
        reads = _LoopSnapshotReads(self, conn, loops)
        for loop in loops:
            snapshot = await self._snapshot_conn(reads, loop["loop_id"], now)
            if not snapshot.complete:
                return {"reset": False, "reason": "lane_ledger_incomplete"}
            if snapshot.unresolved_market_starts:
                return {"reset": False, "reason": "prior_exposure_or_settlement_pending"}
        anchor = int(state["first_market_start_ms"])
        epoch = anchor + -(-max(0, now-anchor) // SLOT_MS) * SLOT_MS
        audit = {"at_ms": now, "reason": reason, "prior_halt_reason": halt,
                 "prior_risk_equity_1u": state.get("risk_equity_1u"),
                 "prior_risk_epoch_start_ms": state.get("risk_epoch_start_ms"),
                 "risk_epoch_start_ms": epoch}
        state.update(halt_reason=None, risk_epoch_start_ms=epoch,
                     risk_resets=[*(state.get("risk_resets") or []), audit][-50:])
        await conn.execute(
            """INSERT INTO prediction_runtime_config(config_key,config_value_json,updated_at_ms)
               VALUES(?,?,?) ON CONFLICT(config_key) DO UPDATE SET
               config_value_json=excluded.config_value_json,updated_at_ms=excluded.updated_at_ms""",
            (self.state_key, json.dumps(state, sort_keys=True), now))
        return {"reset": True, **audit}

    async def check_risk(self, loop_id, start, now):
        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                result = await self._risk_conn(conn, loop_id, start, now)
                await conn.commit()
                return result
            except BaseException:
                await conn.rollback()
                raise

    async def _rows(self, conn: Any, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        if isinstance(conn, _LoopSnapshotReads):
            return await conn.rows(sql, params)
        cursor = await conn.execute(sql, params)
        try:
            return [dict(row) for row in await cursor.fetchall()]
        finally:
            await cursor.close()

    async def _row(self, conn: Any, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        rows = await self._rows(conn, sql, params)
        return rows[0] if rows else None

    async def _loop(self, conn: Any, loop_id: str) -> dict[str, Any]:
        row = await self._row(conn, "SELECT * FROM prediction_loops WHERE loop_id=?", (loop_id,))
        if row is None or str(row.get("strategy_profile") or "").lower() not in RISK_PROFILES:
            raise ValueError("loop is not a regime live profile")
        return row

    async def _slot_has_possible_exposure(self, conn: Any, loop_id: str, start: int) -> bool:
        """Conservatively identify orders that prevent a zero-fill attestation."""
        return int(start) not in await self._empty_slots_without_exposure(conn, loop_id, (start,))

    async def _empty_slots_without_exposure(
        self, conn: Any, loop_id: str, starts: Any,
    ) -> set[int]:
        """Verify all candidate empty slots with one bounded set of SQL reads.

        Every candidate is checked against the same campaign, claim, intent,
        order and fill evidence as the single-slot attestation path.  A bad
        number or incomplete relationship still fails closed.
        """
        wanted = {int(start) for start in starts}
        if not wanted:
            return set()
        campaigns = await self._rows(conn,
            "SELECT campaign_id,start_time_ms,buy_count,pending_unknown,pending_intent_id "
            "FROM prediction_campaigns WHERE loop_id=?", (loop_id,))
        claims = await self._rows(conn,
            "SELECT market_start_ms,campaign_id,intent_id FROM prediction_regime_entry_claims WHERE loop_id=?",
            (loop_id,))
        intents = await self._rows(conn,
            "SELECT i.campaign_id,i.intent_id,i.status,i.unknown FROM prediction_order_intents i "
            "JOIN prediction_campaigns c ON c.campaign_id=i.campaign_id WHERE c.loop_id=?", (loop_id,))
        orders = await self._rows(conn,
            "SELECT o.campaign_id,o.status,o.filled_shares,o.cumulative_gross,o.cumulative_fee "
            "FROM prediction_orders o JOIN prediction_campaigns c ON c.campaign_id=o.campaign_id "
            "WHERE c.loop_id=?", (loop_id,))
        fills = await self._rows(conn,
            "SELECT DISTINCT f.campaign_id FROM prediction_fills f "
            "JOIN prediction_campaigns c ON c.campaign_id=f.campaign_id WHERE c.loop_id=?", (loop_id,))
        by_start: dict[int, list[dict[str, Any]]] = defaultdict(list)
        claims_by_start: dict[int, list[dict[str, Any]]] = defaultdict(list)
        intents_by_campaign: dict[str, list[dict[str, Any]]] = defaultdict(list)
        orders_by_campaign: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in campaigns:
            if int(row["start_time_ms"]) in wanted:
                by_start[int(row["start_time_ms"])].append(row)
        for row in claims:
            if int(row["market_start_ms"]) in wanted:
                claims_by_start[int(row["market_start_ms"])].append(row)
        for row in intents:
            intents_by_campaign[str(row["campaign_id"])].append(row)
        for row in orders:
            orders_by_campaign[str(row["campaign_id"])].append(row)
        filled_ids = {str(row["campaign_id"]) for row in fills}
        safe: set[int] = set()
        for start in wanted:
            slot_campaigns = by_start[start]
            slot_claims = claims_by_start[start]
            campaign_ids = {str(row["campaign_id"]) for row in slot_campaigns}
            if any(str(row["campaign_id"]) not in campaign_ids for row in slot_claims):
                continue
            possible_exposure = False
            for campaign in slot_campaigns:
                campaign_id = str(campaign["campaign_id"])
                if (int(campaign["buy_count"] or 0) or
                    int(campaign["pending_unknown"] or 0) or campaign_id in filled_ids):
                    possible_exposure = True
                    break
                own_intents = intents_by_campaign[campaign_id]
                intent_ids = {str(row["intent_id"]) for row in own_intents}
                if campaign["pending_intent_id"] and str(campaign["pending_intent_id"]) not in intent_ids:
                    possible_exposure = True
                    break
                if any(int(row["unknown"] or 0) or
                       str(row["status"] or "").upper() not in NO_FILL_TERMINAL for row in own_intents):
                    possible_exposure = True
                    break
                if any(str(row["intent_id"]) not in intent_ids for row in slot_claims
                       if str(row["campaign_id"]) == campaign_id):
                    possible_exposure = True
                    break
                for order in orders_by_campaign[campaign_id]:
                    try:
                        if (str(order["status"] or "").upper() not in NO_FILL_TERMINAL or
                            Decimal(str(order["filled_shares"] or "0")) != 0 or
                            Decimal(str(order["cumulative_gross"] or "0")) != 0 or
                            Decimal(str(order["cumulative_fee"] or "0")) != 0):
                            possible_exposure = True
                            break
                    except (ValueError, ArithmeticError):
                        possible_exposure = True
                        break
                if possible_exposure:
                    break
            if not possible_exposure:
                safe.add(start)
        return safe

    async def market_is_registered(self, *, loop_id, market) -> bool:
        """One durable read proves the full schedule, identity and risk epoch.

        A restart or changed/deleted row cannot be hidden by a memory cache.
        Failure falls back to the original registration/epoch initialization.
        Admission and atomic claim still perform current full risk checks.
        """
        rows = await self.repository._fetchall(
            """SELECT sl.*,l.target,l.strategy_profile,r.config_value_json AS risk_state
               FROM prediction_regime_slots sl JOIN prediction_loops l ON l.loop_id=sl.loop_id
               LEFT JOIN prediction_runtime_config r ON r.config_key=?
               WHERE sl.loop_id=? ORDER BY sl.run_ordinal""", (self.state_key, str(loop_id)))
        if not rows or not rows[0]["risk_state"]:
            return False
        anchor = int(rows[0]["market_start_ms"])
        if (anchor <= 0 or anchor % SLOT_MS or len(rows) < int(rows[0]["target"])
                or rows[0]["strategy_profile"] != self.profile
                or any(int(row["run_ordinal"]) != index+1 or
                       int(row["market_start_ms"]) != anchor+index*SLOT_MS
                       for index, row in enumerate(rows))):
            return False
        return any(int(row["market_start_ms"]) == int(market.start_time_ms)
                   and row["verified_at_ms"] is not None
                   and row["market_topic_id"] == market.market_topic_id
                   and row["market_id"] == market.up_market_id for row in rows)

    async def seed_schedule(self, *, loop_id: str, first_market_start_ms: int) -> int:
        """Persist all target 5-minute slots before the first live entry."""

        anchor = int(first_market_start_ms)
        if anchor <= 0 or anchor % SLOT_MS:
            raise ValueError("C180 anchor must be a 5-minute boundary")
        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                loop = await self._loop(conn, str(loop_id))
                target = int(loop["target"])
                if target < 1:
                    raise ValueError("C180 loop target missing")
                existing = await self._rows(
                    conn, "SELECT market_start_ms,run_ordinal FROM prediction_regime_slots WHERE loop_id=?",
                    (str(loop_id),),
                )
                if existing and (len(existing) < target or
                    {int(row["run_ordinal"]) for row in existing} != set(range(1, len(existing) + 1)) or any(
                    int(row["market_start_ms"]) != anchor + (int(row["run_ordinal"]) - 1) * SLOT_MS
                    for row in existing
                )):
                    raise ValueError("C180 schedule anchor or target changed")
                if not existing:
                    await conn.executemany(
                        "INSERT INTO prediction_regime_slots(loop_id,market_start_ms,run_ordinal) VALUES(?,?,?)",
                        [(str(loop_id), anchor + index * SLOT_MS, index + 1) for index in range(target)],
                    )
                await conn.commit()
                return target
            except BaseException:
                await conn.rollback()
                raise

    async def observe_settlement(
        self, *, loop_id: str, settlement_id: str, known_at_ms: int | None = None,
    ) -> None:
        """Durably timestamp the first fee-net SETTLED observation.

        Call immediately after the existing finalize_settlement returns. The
        ledger holds every later buy until this observation is present.
        """

        observed = _now_ms() if known_at_ms is None else int(known_at_ms)
        if abs(_now_ms() - observed) > 2000:
            raise ValueError("settlement observation time is not current")
        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                await self._loop(conn, str(loop_id))
                row = await self._row(conn,
                    """SELECT s.settlement_id,s.campaign_id,s.status,s.net_pnl,
                              c.loop_id,c.start_time_ms,q.intent_id
                       FROM prediction_settlements s
                       JOIN prediction_campaigns c ON c.campaign_id=s.campaign_id
                       JOIN prediction_regime_entry_claims q
                         ON q.loop_id=c.loop_id AND q.market_start_ms=c.start_time_ms
                        AND q.campaign_id=c.campaign_id
                       WHERE s.settlement_id=?""", (str(settlement_id),),
                )
                if (row is None or row["loop_id"] != str(loop_id) or row["status"] != "SETTLED"
                    or not await self._row(conn,
                        "SELECT 1 AS found FROM prediction_fills WHERE campaign_id=? AND order_side='BUY' LIMIT 1",
                        (row["campaign_id"],))):
                    raise ValueError("C180 LIVE fee-net settlement is unconfirmed")
                prior = await self._row(conn,
                    "SELECT campaign_id,net_pnl FROM prediction_regime_settlement_observations WHERE settlement_id=?",
                    (str(settlement_id),),
                )
                if prior and (prior["campaign_id"] != row["campaign_id"] or
                              Decimal(str(prior["net_pnl"])) != Decimal(str(row["net_pnl"]))):
                    raise ValueError("C180 settlement changed after first observation")
                if not prior:
                    await conn.execute(
                        "INSERT INTO prediction_regime_settlement_observations(settlement_id,campaign_id,net_pnl,known_at_ms) VALUES(?,?,?,?)",
                        (str(settlement_id), row["campaign_id"], str(row["net_pnl"]), observed),
                    )
                await self._risk_conn(conn, str(loop_id),
                    max(int(row["start_time_ms"]) + SLOT_MS, observed // SLOT_MS * SLOT_MS), observed)
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def verify_market(
        self, *, loop_id: str, market_start_ms: int, market_topic_id: str,
        market_id: str, verified_at_ms: int,
    ) -> None:
        """Bind one scheduled slot to caller-verified official market identity."""

        if not market_topic_id or not market_id:
            raise ValueError("official C180 market identity missing")
        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                loop = await self._loop(conn, str(loop_id))
                start = int(market_start_ms)
                row = await self._row(conn,
                    "SELECT market_topic_id,market_id FROM prediction_regime_slots WHERE loop_id=? AND market_start_ms=?",
                    (str(loop_id), start),
                )
                if row is None:
                    # Completed campaigns can lag scheduled markets after an
                    # attested empty interval. Extend one official market at a
                    # time so a 200-campaign loop can finish without weakening
                    # the missing-market and exposure checks in the entry gate.
                    slots = await self._rows(conn,
                        "SELECT market_start_ms,run_ordinal FROM prediction_regime_slots WHERE loop_id=? ORDER BY run_ordinal",
                        (str(loop_id),),
                    )
                    target = int(loop["target"])
                    anchor = int(slots[0]["market_start_ms"]) if slots else 0
                    if (str(loop["state"]).upper() != "RUNNING" or
                        str(loop.get("mode") or "").upper() != "LIVE" or
                        int(loop["completed"]) >= target or
                        int(loop.get("new_entries_stopped") or 0) or
                        int(loop.get("hard_stop_latched") or 0) or
                        len(slots) < target or anchor <= 0 or anchor % SLOT_MS or
                        any(int(item["run_ordinal"]) != index + 1 or
                            int(item["market_start_ms"]) != anchor + index * SLOT_MS
                            for index, item in enumerate(slots)) or
                        start != anchor + len(slots) * SLOT_MS or
                        not start <= int(verified_at_ms) < start + SLOT_MS or
                        abs(_now_ms() - int(verified_at_ms)) > 2000):
                        raise ValueError("C180 schedule extension unavailable")
                    await conn.execute(
                        "INSERT INTO prediction_regime_slots(loop_id,market_start_ms,run_ordinal) VALUES(?,?,?)",
                        (str(loop_id), start, len(slots) + 1),
                    )
                    row = {"market_topic_id": None, "market_id": None}
                if row is None or (row["market_topic_id"] and row["market_topic_id"] != str(market_topic_id)) or (
                    row["market_id"] and row["market_id"] != str(market_id)
                ):
                    raise ValueError("C180 slot missing or identity changed")
                await conn.execute(
                    "UPDATE prediction_regime_slots SET market_topic_id=?,market_id=?,verified_at_ms=COALESCE(verified_at_ms,?) WHERE loop_id=? AND market_start_ms=?",
                    (str(market_topic_id), str(market_id), int(verified_at_ms), str(loop_id), start),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def attest_empty(
        self, *, loop_id: str, market_start_ms: int, wallet_reconciled_at_ms: int,
        now_ms: int | None = None,
    ) -> None:
        """Certify a missed completed slot only after a fresh wallet check."""

        now = _now_ms() if now_ms is None else int(now_ms)
        start = int(market_start_ms)
        if now < start + SLOT_MS or not 0 <= now - int(wallet_reconciled_at_ms) <= 2000:
            raise ValueError("C180 slot not ended or wallet check stale")
        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                await self._loop(conn, str(loop_id))
                slot = await self._row(conn,
                    "SELECT market_topic_id,verified_at_ms FROM prediction_regime_slots WHERE loop_id=? AND market_start_ms=?",
                    (str(loop_id), start),
                )
                if slot is None or not slot["market_topic_id"] or slot["verified_at_ms"] is None:
                    raise ValueError("missed C180 market identity unverified")
                if await self._slot_has_possible_exposure(conn, str(loop_id), start):
                    raise ValueError("missed C180 slot has possible exposure")
                await conn.execute(
                    "UPDATE prediction_regime_slots SET empty_attested_at_ms=COALESCE(empty_attested_at_ms,?) WHERE loop_id=? AND market_start_ms=?",
                    (now, str(loop_id), start),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def _snapshot_conn(self, conn: Any, loop_id: str, at_ms: int) -> LoopLedgerSnapshot:
        await self._loop(conn, loop_id)
        slots = await self._rows(conn,
            "SELECT market_start_ms,market_topic_id,market_id,verified_at_ms,empty_attested_at_ms FROM prediction_regime_slots WHERE loop_id=? ORDER BY market_start_ms",
            (loop_id,),
        )
        verified = tuple(int(row["market_start_ms"]) for row in slots if row["market_topic_id"] and row["market_id"] and row["verified_at_ms"] is not None)
        empty_candidates = [int(row["market_start_ms"]) for row in slots if row["empty_attested_at_ms"] is not None]
        safe_empty = await self._empty_slots_without_exposure(conn, loop_id, empty_candidates)
        empty = [start for start in empty_candidates if start in safe_empty]

        settlement_rows = await self._rows(conn,
            """SELECT s.settlement_id,s.campaign_id,s.net_pnl,c.start_time_ms,
                      q.unit_usdt,q.intent_id,o.net_pnl AS observed_net_pnl,
                      o.known_at_ms
               FROM prediction_settlements s
               JOIN prediction_campaigns c ON c.campaign_id=s.campaign_id
               LEFT JOIN prediction_regime_entry_claims q
                 ON q.loop_id=c.loop_id AND q.market_start_ms=c.start_time_ms
                AND q.campaign_id=c.campaign_id
               LEFT JOIN prediction_regime_settlement_observations o
                 ON o.settlement_id=s.settlement_id AND o.campaign_id=c.campaign_id
               WHERE c.loop_id=? AND s.status='SETTLED'
                 AND EXISTS(SELECT 1 FROM prediction_fills f
                            WHERE f.campaign_id=c.campaign_id AND f.order_side='BUY')
               ORDER BY o.known_at_ms,s.settlement_id""",
            (loop_id,),
        )
        settlements: list[LiveSettlement] = []
        complete = True
        settled_campaigns: set[str] = set()
        for row in settlement_rows:
            campaign_id = str(row["campaign_id"])
            if campaign_id in settled_campaigns:
                complete = False
                continue
            settled_campaigns.add(campaign_id)
            if (not row["intent_id"] or row["unit_usdt"] is None or
                row["known_at_ms"] is None or
                Decimal(str(row["net_pnl"])) != Decimal(str(row["observed_net_pnl"]))):
                complete = False
                continue
            settlements.append(LiveSettlement(
                str(row["settlement_id"]), int(row["start_time_ms"]),
                Decimal(str(row["observed_net_pnl"])), int(row["known_at_ms"]), _unit(row["unit_usdt"]),
            ))

        orphan = await self._row(conn,
            """SELECT 1 AS found FROM prediction_fills f
               JOIN prediction_campaigns c ON c.campaign_id=f.campaign_id
               LEFT JOIN prediction_regime_entry_claims q
                 ON q.loop_id=c.loop_id AND q.market_start_ms=c.start_time_ms
                AND q.campaign_id=c.campaign_id
               WHERE c.loop_id=? AND f.order_side='BUY' AND q.intent_id IS NULL LIMIT 1""",
            (loop_id,),
        )
        if orphan:
            complete = False

        unpaired_settlements = await self._rows(conn,
            """SELECT c.start_time_ms FROM prediction_settlements s
               JOIN prediction_campaigns c ON c.campaign_id=s.campaign_id
               WHERE c.loop_id=? AND s.status='SETTLED'
                 AND NOT EXISTS(SELECT 1 FROM prediction_fills f
                                WHERE f.campaign_id=c.campaign_id AND f.order_side='BUY')
                 AND (EXISTS(SELECT 1 FROM prediction_regime_slots sl
                              WHERE sl.loop_id=c.loop_id AND sl.market_start_ms=c.start_time_ms)
                      OR EXISTS(SELECT 1 FROM prediction_regime_entry_claims q
                                WHERE q.loop_id=c.loop_id AND q.campaign_id=c.campaign_id)
                      OR EXISTS(SELECT 1 FROM prediction_order_intents i
                                WHERE i.campaign_id=c.campaign_id AND i.order_side='BUY')
                      OR EXISTS(SELECT 1 FROM prediction_orders o
                                WHERE o.campaign_id=c.campaign_id))
               """, (loop_id,),
        )
        if any(int(row["start_time_ms"]) not in empty for row in unpaired_settlements):
            complete = False

        unresolved_rows = await self._rows(conn,
            """SELECT DISTINCT c.start_time_ms FROM prediction_campaigns c
               WHERE c.loop_id=? AND (
                   c.pending_unknown=1
                   OR EXISTS(SELECT 1 FROM prediction_order_intents i
                             WHERE i.campaign_id=c.campaign_id AND
                               (i.unknown=1 OR (i.order_side='BUY' AND
                                 NOT EXISTS(SELECT 1 FROM prediction_settlements s
                                   WHERE s.campaign_id=c.campaign_id AND s.status='SETTLED'))
                                OR UPPER(i.status) NOT IN
                                   ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED','REJECTED')))
                   OR EXISTS(SELECT 1 FROM prediction_orders o
                             WHERE o.campaign_id=c.campaign_id AND UPPER(o.status) NOT IN
                               ('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED','REJECTED'))
                   OR (EXISTS(SELECT 1 FROM prediction_fills f WHERE f.campaign_id=c.campaign_id AND f.order_side='BUY')
                       AND NOT EXISTS(SELECT 1 FROM prediction_settlements s
                                      WHERE s.campaign_id=c.campaign_id AND s.status='SETTLED'))
               ) ORDER BY c.start_time_ms""",
            (loop_id,),
        )
        return LoopLedgerSnapshot(
            loop_id=loop_id,
            complete=complete,
            verified_market_starts=verified,
            confirmed_empty_market_starts=tuple(empty),
            settlements=tuple(settlements),
            unresolved_market_starts=tuple(int(row["start_time_ms"]) for row in unresolved_rows
                                           if int(row["start_time_ms"]) not in empty),
        )

    async def snapshot(self, loop_id: str, decision_at_ms: int) -> LoopLedgerSnapshot | None:
        """One SQLite read snapshot for C180GateRuntime. Errors fail closed."""

        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn, "DEFERRED")
            try:
                result = await self._snapshot_conn(conn, str(loop_id), int(decision_at_ms))
                await conn.commit()
                return result
            except Exception:
                await conn.rollback()
                return None

    async def reserve_c180_intent(
        self, *, loop_id: str, market_start_ms: int, campaign_id: str,
        intent: Any, decision_at_ms: int, wallet_reconciled_at_ms: int,
        expires_at_ms: int | None = None, entry_campaign: Any = None, trace=None,
    ) -> C180ClaimResult:
        """Recheck gate and insert one BUY intent/claim in one IMMEDIATE tx.

        Caller must use the returned persisted intent row for its existing
        submission path. A denial never permits a network order.
        """

        now = int(decision_at_ms)
        start = int(market_start_ms)
        loop = str(loop_id)
        if abs(_now_ms() - now) > 2000:
            return C180ClaimResult(False, "decision_time_stale")
        if not 0 <= now - int(wallet_reconciled_at_ms) <= 2000:
            return C180ClaimResult(False, "wallet_reconciliation_stale")
        try:
            values = self.repository._intent_values(intent)
            if (values[2], values[4], str(values[14])) != ("BUY_INITIAL", "BUY", self.tier):
                return C180ClaimResult(False, "wrong_c180_intent")
            unit = _unit(values[5])
            if self.profile not in ("regime_target6_2_v1", "regime_target6_3_v1",
                                    "regime_target6_3a_v1", "regime_target6_3b_v1", "regime_target6_5_v1", "regime_target6_7_v1", 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1', 'regime_target6_9_v1', 'regime_target6_9a_v1') and unit != 1:
                return C180ClaimResult(False, "regime_requires_fixed_1_usdt")
            if values[1] != str(campaign_id) or values[0] in ("", "None"):
                return C180ClaimResult(False, "intent_identity_mismatch")
            if abs(int(values[7]) - now) > 2000 or int(values[8]) <= 0 or int(values[9]) != 1:
                return C180ClaimResult(False, "intent_time_or_attempt_invalid")
        except Exception:
            return C180ClaimResult(False, "invalid_c180_intent")

        def measured(stage, began):
            if trace is not None:
                try:
                    trace(stage, time.monotonic_ns() - began)
                except Exception:
                    pass  # Telemetry failure cannot change trading authorization.
        lock_started = time.monotonic_ns()
        async with self.repository._operation_gate.lock:
            measured("claim_lock", lock_started)
            conn = self.repository._require_conn()
            begin_started = time.monotonic_ns()
            await self.repository._begin(conn)
            measured("claim_begin", begin_started)
            try:
                # Lock/BEGIN may have waited. Do not authorize using an old clock.
                now = _now_ms()
                if abs(now - int(decision_at_ms)) > 2000:
                    await conn.rollback()
                    return C180ClaimResult(False, "decision_time_stale_after_lock")
                if not 0 <= now - int(wallet_reconciled_at_ms) <= 2000:
                    await conn.rollback()
                    return C180ClaimResult(False, "wallet_reconciliation_stale_after_lock")
                if expires_at_ms is not None and now >= int(expires_at_ms):
                    await conn.rollback()
                    return C180ClaimResult(False, "execution_expired_after_lock")
                loop_row = await self._loop(conn, loop)
                if str(loop_row.get("strategy_profile") or "").lower() != self.profile:
                    await conn.rollback()
                    return C180ClaimResult(False, "wrong_regime_profile")
                if (str(loop_row["state"]).upper() != "RUNNING" or
                    str(loop_row.get("mode") or "").upper() != "LIVE" or
                    int(loop_row.get("new_entries_stopped") or 0) or
                    int(loop_row.get("hard_stop_latched") or 0)):
                    await conn.rollback()
                    return C180ClaimResult(False, "loop_not_live_or_halted")
                campaign = await self._row(conn,
                    "SELECT * FROM prediction_campaigns WHERE campaign_id=?",
                    (str(campaign_id),),
                )
                slot = await self._row(conn,
                    "SELECT * FROM prediction_regime_slots WHERE loop_id=? AND market_start_ms=?",
                    (loop, start),
                )
                from .loop_market import PROFILES as MULTI_PROFILES
                if self.profile in MULTI_PROFILES:
                    binding = await self._row(conn, "SELECT * FROM prediction_loop_market_bindings WHERE loop_id=?", (loop,))
                    if binding:
                        from .loop_market import market_matches, execution_fingerprint
                        from types import SimpleNamespace
                        try:
                            m = json.loads(campaign["payload_json"])["market"]
                            valid = (market_matches(SimpleNamespace(**m), binding["symbol"])
                                     and binding["profile"] == self.profile
                                     and binding["execution_fingerprint"] == execution_fingerprint(binding["symbol"], self.profile)
                                     and binding["unit"] == str(unit))
                        except (ValueError, KeyError, TypeError, AttributeError):
                            valid = False
                        if not valid:
                            await conn.rollback()
                            return C180ClaimResult(False, "loop_market_binding_mismatch")
                if not _campaign_matches_slot(campaign, slot, loop, start):
                    await conn.rollback()
                    return C180ClaimResult(False, "market_identity_not_verified")
                if (self.profile in ("regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1") and start+180000 <= now < start+183500
                        and await self._row(conn,
                            "SELECT 1 FROM prediction_regime_entry_claims WHERE market_start_ms=? LIMIT 1",
                            (start,))):
                    await conn.rollback()
                    return C180ClaimResult(False, "market_buy_already_claimed")
                if await self._row(conn,
                    """SELECT 1 AS found FROM prediction_regime_entry_claims
                       WHERE loop_id=? AND market_start_ms=?
                       UNION ALL SELECT 1 FROM prediction_order_intents i
                         JOIN prediction_campaigns c ON c.campaign_id=i.campaign_id
                        WHERE c.market_topic_id=? AND c.start_time_ms=? AND i.order_side='BUY'
                       UNION ALL SELECT 1 FROM prediction_fills f
                         JOIN prediction_campaigns c ON c.campaign_id=f.campaign_id
                        WHERE c.market_topic_id=? AND c.start_time_ms=? AND f.order_side='BUY'
                       LIMIT 1""",
                    (loop, start, slot["market_topic_id"], start,
                     slot["market_topic_id"], start),
                ):
                    await conn.rollback()
                    return C180ClaimResult(False, "market_buy_already_claimed")
                begin, end = (60000, 270000) if self.profile == "regime_target6_7_v1" else (124000, 136000)
                if self.profile in ("regime_target6_8_v1", "regime_target6_8a_v1", "regime_target6_9_v1") and start+180000 <= now < start+183500:
                    begin, end = 180000, 183500
                if not start + begin <= now < start + end or not Decimal("0") < Decimal(str(values[6])) <= self.max_price:
                    await conn.rollback()
                    return C180ClaimResult(False, "regime_time_or_price_invalid")
                risk_started = time.monotonic_ns()
                allowed, reason = await self._risk_conn(conn, loop, start, now)
                measured("claim_history_risk", risk_started)
                if not allowed:
                    await conn.commit()
                    return C180ClaimResult(False, reason)
                final_now = _now_ms()
                if (not start + begin <= final_now < start + end
                        or (expires_at_ms is not None and final_now >= int(expires_at_ms))
                        or not 0 <= final_now - int(wallet_reconciled_at_ms) <= 2000):
                    await conn.commit()  # Preserve any newly latched risk state.
                    return C180ClaimResult(False, "execution_expired_during_claim_risk")
                campaign_values = None
                if entry_campaign is not None:
                    campaign_values = list(self.repository._campaign_values(entry_campaign))
                    campaign_values[1] = loop
                    if (campaign_values[0] != str(campaign_id)
                            or campaign_values[2] != campaign["market_topic_id"]
                            or campaign_values[5] != start
                            or campaign_values[7] != "INITIAL_PENDING"
                            or campaign_values[14] != int(campaign["order_attempts"])+1
                            or campaign_values[15] != int(campaign["initial_attempts"])+1
                            or campaign_values[17] != values[0] or campaign_values[18]
                            or campaign["pending_intent_id"] or campaign["pending_unknown"]):
                        await conn.rollback()
                        return C180ClaimResult(False, "entry_campaign_transition_invalid")
                await conn.execute(
                    """INSERT INTO prediction_order_intents
                       (intent_id,campaign_id,action,outcome,order_side,amount,limit_price,
                        created_at_ms,ttl_ms,attempt,order_id,status,unknown,client_order_id,tier,payload_json)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
                await conn.execute(
                    """INSERT INTO prediction_regime_entry_claims
                       (loop_id,market_start_ms,campaign_id,intent_id,unit_usdt,claimed_at_ms)
                       VALUES(?,?,?,?,?,?)""",
                    (loop, start, str(campaign_id), values[0], str(unit), now),
                )
                if campaign_values is not None:
                    await conn.execute(CAMPAIGN_UPSERT_SQL, tuple(campaign_values))
                else:
                    # Standalone callers retain the legacy minimal transition.
                    await conn.execute(
                        "UPDATE prediction_campaigns SET pending_intent_id=?,updated_at_ms=? WHERE campaign_id=?",
                        (values[0], now, str(campaign_id)))
                row = await self._row(conn,
                    "SELECT * FROM prediction_order_intents WHERE intent_id=?", (values[0],),
                )
                commit_started = time.monotonic_ns()
                await conn.commit()
                measured("claim_commit", commit_started)
                return C180ClaimResult(True, "claimed", row)
            except BaseException as exc:
                await conn.rollback()
                if not isinstance(exc, Exception):
                    raise
                logging.getLogger(__name__).exception("regime_claim_transaction_failed")
                return C180ClaimResult(False, "claim_transaction_failed:" + type(exc).__name__)
