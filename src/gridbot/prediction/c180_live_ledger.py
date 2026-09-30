"""C180-only SQLite ledger and atomic pre-network entry claim.

This adapter is deliberately separate from the VM's existing repository file.
It uses that repository's connection and operation gate so its snapshots and
claims serialize with existing Prediction writes.  No exchange call is made.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from .c180_batch_gate import SettledTrade, block_bounds, evaluate_batch_gate
from .c180_gate_runtime import LiveSettlement, LoopLedgerSnapshot, SLOT_MS, STATE_KEY


PROFILE = "c180_favorite_hold_v1"
TIER = "C180"
TERMINAL_INTENTS = ("FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED")
TERMINAL_ORDERS = ("FILLED", "CLOSED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED")
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
        raise ValueError("C180 unit is not 1/2/3 USDT")
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


class C180LiveLedger:
    """Use one already-initialized PredictionRepository instance."""

    def __init__(self, repository: Any, *, state_key: str = STATE_KEY) -> None:
        self.repository = repository
        self.state_key = state_key

    async def _rows(self, conn: Any, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
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
        if row is None or str(row.get("strategy_profile") or "").lower() != PROFILE:
            raise ValueError("loop is not the C180 live profile")
        return row

    async def _slot_has_possible_exposure(self, conn: Any, loop_id: str, start: int) -> bool:
        """Conservatively identify orders that prevent a zero-fill attestation."""

        campaigns = await self._rows(conn,
            "SELECT campaign_id,buy_count,pending_unknown,pending_intent_id FROM prediction_campaigns WHERE loop_id=? AND start_time_ms=?",
            (loop_id, start),
        )
        claims = await self._rows(conn,
            "SELECT campaign_id,intent_id FROM prediction_c180_entry_claims WHERE loop_id=? AND market_start_ms=?",
            (loop_id, start),
        )
        campaign_ids = {str(row["campaign_id"]) for row in campaigns}
        if any(str(row["campaign_id"]) not in campaign_ids for row in claims):
            return True
        for campaign in campaigns:
            campaign_id = str(campaign["campaign_id"])
            if int(campaign["buy_count"] or 0) or int(campaign["pending_unknown"] or 0):
                return True
            if await self._row(conn,
                "SELECT 1 AS found FROM prediction_fills WHERE campaign_id=? LIMIT 1", (campaign_id,),
            ):
                return True
            intents = await self._rows(conn,
                "SELECT intent_id,status,unknown FROM prediction_order_intents WHERE campaign_id=?",
                (campaign_id,),
            )
            intent_ids = {str(row["intent_id"]) for row in intents}
            if campaign["pending_intent_id"] and str(campaign["pending_intent_id"]) not in intent_ids:
                return True
            if any(int(row["unknown"] or 0) or
                   str(row["status"] or "").upper() not in NO_FILL_TERMINAL for row in intents):
                return True
            if any(str(row["intent_id"]) not in intent_ids for row in claims
                   if str(row["campaign_id"]) == campaign_id):
                return True
            orders = await self._rows(conn,
                "SELECT status,filled_shares,cumulative_gross,cumulative_fee FROM prediction_orders WHERE campaign_id=?",
                (campaign_id,),
            )
            for order in orders:
                try:
                    if (str(order["status"] or "").upper() not in NO_FILL_TERMINAL or
                        Decimal(str(order["filled_shares"] or "0")) != 0 or
                        Decimal(str(order["cumulative_gross"] or "0")) != 0 or
                        Decimal(str(order["cumulative_fee"] or "0")) != 0):
                        return True
                except (ValueError, ArithmeticError):
                    return True
        return False

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
                    conn, "SELECT market_start_ms,run_ordinal FROM prediction_c180_slots WHERE loop_id=?",
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
                        "INSERT INTO prediction_c180_slots(loop_id,market_start_ms,run_ordinal) VALUES(?,?,?)",
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
                       JOIN prediction_c180_entry_claims q
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
                    "SELECT campaign_id,net_pnl FROM prediction_c180_settlement_observations WHERE settlement_id=?",
                    (str(settlement_id),),
                )
                if prior and (prior["campaign_id"] != row["campaign_id"] or
                              Decimal(str(prior["net_pnl"])) != Decimal(str(row["net_pnl"]))):
                    raise ValueError("C180 settlement changed after first observation")
                if not prior:
                    await conn.execute(
                        "INSERT INTO prediction_c180_settlement_observations(settlement_id,campaign_id,net_pnl,known_at_ms) VALUES(?,?,?,?)",
                        (str(settlement_id), row["campaign_id"], str(row["net_pnl"]), observed),
                    )
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
                    "SELECT market_topic_id,market_id FROM prediction_c180_slots WHERE loop_id=? AND market_start_ms=?",
                    (str(loop_id), start),
                )
                if row is None:
                    # Completed campaigns can lag scheduled markets after an
                    # attested empty interval. Extend one official market at a
                    # time so a 200-campaign loop can finish without weakening
                    # the missing-market and exposure checks in the entry gate.
                    slots = await self._rows(conn,
                        "SELECT market_start_ms,run_ordinal FROM prediction_c180_slots WHERE loop_id=? ORDER BY run_ordinal",
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
                        "INSERT INTO prediction_c180_slots(loop_id,market_start_ms,run_ordinal) VALUES(?,?,?)",
                        (str(loop_id), start, len(slots) + 1),
                    )
                    row = {"market_topic_id": None, "market_id": None}
                if row is None or (row["market_topic_id"] and row["market_topic_id"] != str(market_topic_id)) or (
                    row["market_id"] and row["market_id"] != str(market_id)
                ):
                    raise ValueError("C180 slot missing or identity changed")
                await conn.execute(
                    "UPDATE prediction_c180_slots SET market_topic_id=?,market_id=?,verified_at_ms=COALESCE(verified_at_ms,?) WHERE loop_id=? AND market_start_ms=?",
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
                    "SELECT market_topic_id,verified_at_ms FROM prediction_c180_slots WHERE loop_id=? AND market_start_ms=?",
                    (str(loop_id), start),
                )
                if slot is None or not slot["market_topic_id"] or slot["verified_at_ms"] is None:
                    raise ValueError("missed C180 market identity unverified")
                if await self._slot_has_possible_exposure(conn, str(loop_id), start):
                    raise ValueError("missed C180 slot has possible exposure")
                await conn.execute(
                    "UPDATE prediction_c180_slots SET empty_attested_at_ms=COALESCE(empty_attested_at_ms,?) WHERE loop_id=? AND market_start_ms=?",
                    (now, str(loop_id), start),
                )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def _snapshot_conn(self, conn: Any, loop_id: str, at_ms: int) -> LoopLedgerSnapshot:
        await self._loop(conn, loop_id)
        slots = await self._rows(conn,
            "SELECT market_start_ms,market_topic_id,market_id,verified_at_ms,empty_attested_at_ms FROM prediction_c180_slots WHERE loop_id=? ORDER BY market_start_ms",
            (loop_id,),
        )
        verified = tuple(int(row["market_start_ms"]) for row in slots if row["market_topic_id"] and row["market_id"] and row["verified_at_ms"] is not None)
        empty_candidates = [int(row["market_start_ms"]) for row in slots if row["empty_attested_at_ms"] is not None]
        empty: list[int] = []
        for start in empty_candidates:
            if not await self._slot_has_possible_exposure(conn, loop_id, start):
                empty.append(start)

        settlement_rows = await self._rows(conn,
            """SELECT s.settlement_id,s.campaign_id,s.net_pnl,c.start_time_ms,
                      q.unit_usdt,q.intent_id,o.net_pnl AS observed_net_pnl,
                      o.known_at_ms
               FROM prediction_settlements s
               JOIN prediction_campaigns c ON c.campaign_id=s.campaign_id
               LEFT JOIN prediction_c180_entry_claims q
                 ON q.loop_id=c.loop_id AND q.market_start_ms=c.start_time_ms
                AND q.campaign_id=c.campaign_id
               LEFT JOIN prediction_c180_settlement_observations o
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
               LEFT JOIN prediction_c180_entry_claims q
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
                 AND (EXISTS(SELECT 1 FROM prediction_c180_slots sl
                              WHERE sl.loop_id=c.loop_id AND sl.market_start_ms=c.start_time_ms)
                      OR EXISTS(SELECT 1 FROM prediction_c180_entry_claims q
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
            if (values[2], values[4], str(values[14])) != ("BUY_INITIAL", "BUY", TIER):
                return C180ClaimResult(False, "wrong_c180_intent")
            unit = _unit(values[5])
            if values[1] != str(campaign_id) or values[0] in ("", "None"):
                return C180ClaimResult(False, "intent_identity_mismatch")
            if abs(int(values[7]) - now) > 2000 or int(values[8]) <= 0 or int(values[9]) != 1:
                return C180ClaimResult(False, "intent_time_or_attempt_invalid")
        except Exception:
            return C180ClaimResult(False, "invalid_c180_intent")

        async with self.repository._operation_gate.lock:
            conn = self.repository._require_conn()
            await self.repository._begin(conn)
            try:
                loop_row = await self._loop(conn, loop)
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
                    "SELECT * FROM prediction_c180_slots WHERE loop_id=? AND market_start_ms=?",
                    (loop, start),
                )
                if not _campaign_matches_slot(campaign, slot, loop, start):
                    await conn.rollback()
                    return C180ClaimResult(False, "market_identity_not_verified")
                if await self._row(conn,
                    """SELECT 1 AS found FROM prediction_c180_entry_claims
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
                state_row = await self._row(conn,
                    "SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?",
                    (self.state_key,),
                )
                state = json.loads(state_row["config_value_json"]) if state_row else None
                if not isinstance(state, dict) or state.get("loop_id") != loop:
                    await conn.rollback()
                    return C180ClaimResult(False, "gate_state_missing")
                policy_version = str(state.get("policy_version") or "1.0")
                if state.get("version") not in (1, 2) or policy_version not in ("1.0", "1.1"):
                    await conn.rollback()
                    return C180ClaimResult(False, "gate_policy_invalid")
                if policy_version == "1.1" and Decimal(str(values[6])) > Decimal("0.90"):
                    await conn.rollback()
                    return C180ClaimResult(False, "price_cap_exceeded")
                anchor = int(state["first_market_start_ms"])
                ordinal = _run(start, anchor)
                if unit != _unit(state["unit_usdt"]) or not start + 120_000 <= now < start + SLOT_MS:
                    await conn.rollback()
                    return C180ClaimResult(False, "unit_or_decision_time_mismatch")
                ledger = await self._snapshot_conn(conn, loop, now)
                if not ledger.complete or start not in ledger.verified_market_starts:
                    await conn.rollback()
                    return C180ClaimResult(False, "ledger_snapshot_missing")
                if any(_unit(row.unit_usdt) != unit for row in ledger.settlements):
                    await conn.rollback()
                    return C180ClaimResult(False, "mixed_loop_units")
                last = int(state["last_market_start_ms"])
                gaps = tuple(range(last + SLOT_MS, start, SLOT_MS))
                if (last > start or (start - last) % SLOT_MS or
                    not set(gaps).issubset(set(ledger.verified_market_starts)) or
                    not set(gaps).issubset(set(ledger.confirmed_empty_market_starts))):
                    await conn.rollback()
                    return C180ClaimResult(False, "unverified_missing_market")
                if start in ledger.unresolved_market_starts:
                    await conn.rollback()
                    return C180ClaimResult(False, "current_market_unknown_intent")
                trades = [SettledTrade(_run(row.market_start_ms, anchor), row.net_pnl_usdt,
                                       row.known_at_ms, row.unit_usdt) for row in ledger.settlements]
                pending = [_run(item, anchor) for item in ledger.unresolved_market_starts]
                block = block_bounds(ordinal)[0]
                recovery = state.get("recovery") or {}
                if recovery and recovery.get("state") not in {"LIVE", "PROBATION"}:
                    await conn.rollback()
                    return C180ClaimResult(False, "recovery_shadow_pending")
                gate = evaluate_batch_gate(
                    run_ordinal=ordinal, decision_at_ms=now,
                    frozen_unit_usdt=unit, settlements=trades,
                    ledger_complete=True, unresolved_filled_runs=pending,
                    persisted_trigger_run=(state.get("latches") or {}).get(str(block)),
                    policy_version=policy_version,
                    persisted_loop_loss_latched=bool(state.get("loop_loss_latched")),
                    persisted_recovery_hold_latched=bool(state.get("recovery_hold_latched")),
                )
                if not gate.allow_entry:
                    if gate.trigger_run is not None or gate.loop_loss_latched or gate.recovery_hold_latched:
                        latches = dict(state.get("latches") or {})
                        if gate.trigger_run is not None:
                            latches[str(block)] = gate.trigger_run
                        state["latches"] = latches
                        state["loop_loss_latched"] = gate.loop_loss_latched
                        state["recovery_hold_latched"] = gate.recovery_hold_latched
                        await conn.execute(
                            "UPDATE prediction_runtime_config SET config_value_json=?,updated_at_ms=? WHERE config_key=?",
                            (json.dumps(state,sort_keys=True,separators=(",",":")), now, self.state_key),
                        )
                        await conn.commit()
                    else:
                        await conn.rollback()
                    return C180ClaimResult(False, gate.reason, trigger_run=gate.trigger_run)
                state["last_market_start_ms"] = start
                await conn.execute(
                    "UPDATE prediction_runtime_config SET config_value_json=?,updated_at_ms=? WHERE config_key=?",
                    (json.dumps(state,sort_keys=True,separators=(",",":")), now, self.state_key),
                )
                await conn.execute(
                    """INSERT INTO prediction_order_intents
                       (intent_id,campaign_id,action,outcome,order_side,amount,limit_price,
                        created_at_ms,ttl_ms,attempt,order_id,status,unknown,client_order_id,tier,payload_json)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
                await conn.execute(
                    """INSERT INTO prediction_c180_entry_claims
                       (loop_id,market_start_ms,campaign_id,intent_id,unit_usdt,claimed_at_ms)
                       VALUES(?,?,?,?,?,?)""",
                    (loop, start, str(campaign_id), values[0], str(unit), now),
                )
                await conn.execute(
                    "UPDATE prediction_campaigns SET pending_intent_id=?,updated_at_ms=? WHERE campaign_id=?",
                    (values[0], now, str(campaign_id)),
                )
                row = await self._row(conn,
                    "SELECT * FROM prediction_order_intents WHERE intent_id=?", (values[0],),
                )
                await conn.commit()
                return C180ClaimResult(True, "claimed", row)
            except Exception:
                await conn.rollback()
                return C180ClaimResult(False, "claim_transaction_failed")
