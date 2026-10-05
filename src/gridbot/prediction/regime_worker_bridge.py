"""Regime lane adapter; reuses durable worker execution without C180 policy."""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from .c180_favorite import C180EntryDecision, C180ExecutionRecheck
from .c180_signal_runtime import read_c180_book, read_c180_signal, _signal_json, _from_signal_json
from .c180_signal_service import C180Signal
from .c180_worker_bridge import C180Ready
from .regime_feature_service import connect, DEFAULT_DB
from .regime_lane import FINGERPRINT, dec, select_side, walk
from .regime_live_ledger import RegimeLiveLedger
from .regime_t61_lane import FINGERPRINT as T61_FINGERPRINT, PROFILE as T61_PROFILE, select_fallback
from .regime_t62_lane import FINGERPRINT as T62_FINGERPRINT, PROFILE as T62_PROFILE, select_primary


from .regime_t63_lane import PROFILE as T63_PROFILE, FINGERPRINT as T63_FINGERPRINT
from .regime_t63a_lane import PROFILE as T63A_PROFILE, FINGERPRINT as T63A_FINGERPRINT
from .regime_t63b_lane import PROFILE as T63B_PROFILE, FINGERPRINT as T63B_FINGERPRINT

from .regime_t65_lane import PROFILE as T65_PROFILE, FINGERPRINT as T65_FINGERPRINT
from .regime_t67_policy import PROFILE as T67_PROFILE, FINGERPRINT as T67_FINGERPRINT

from .regime_t67a_policy import PROFILE as T67A_PROFILE, FINGERPRINT as T67A_FINGERPRINT

from .regime_t67b_policy import PROFILE as T67B_PROFILE, FINGERPRINT as T67B_FINGERPRINT

from .regime_t67c_policy import PROFILE as T67C_PROFILE, FINGERPRINT as T67C_FINGERPRINT
from .regime_t67d_policy import PROFILE as T67D_PROFILE, FINGERPRINT as T67D_FINGERPRINT
from .regime_t68_policy import PROFILE as T68_PROFILE, FINGERPRINT as T68_FINGERPRINT
from .regime_t68a_policy import PROFILE as T68A_PROFILE, FINGERPRINT as T68A_FINGERPRINT
from .regime_t69_policy import PROFILE as T69_PROFILE, FINGERPRINT as T69_FINGERPRINT
from .regime_t69a_policy import PROFILE as T69A_PROFILE, FINGERPRINT as T69A_FINGERPRINT

class RegimeWorkerBridge:
    def __init__(self, repository, signal_db, exposure_checker=None, feature_db=DEFAULT_DB,
                 profile="regime_target6_v1", symbol=None):
        self.symbol = symbol
        self.repository = repository
        self.signal_db = Path(signal_db)
        self.feature_db = Path(feature_db)
        self.profile = profile
        self.decision_fingerprint = (T69A_FINGERPRINT if profile == T69A_PROFILE else T69_FINGERPRINT if profile == T69_PROFILE else T68A_FINGERPRINT if profile == T68A_PROFILE else T68_FINGERPRINT if profile == T68_PROFILE else T67C_FINGERPRINT if profile == T67C_PROFILE else T67D_FINGERPRINT if profile == T67D_PROFILE else T67B_FINGERPRINT if profile == T67B_PROFILE else T67A_FINGERPRINT if profile == T67A_PROFILE else T67_FINGERPRINT if profile == T67_PROFILE else T65_FINGERPRINT if profile == T65_PROFILE else
                                     T63B_FINGERPRINT if profile == T63B_PROFILE else
                                     T63A_FINGERPRINT if profile == T63A_PROFILE else
                                     T63_FINGERPRINT if profile == T63_PROFILE else T62_FINGERPRINT if profile == T62_PROFILE else
                                     T61_FINGERPRINT if profile == T61_PROFILE else FINGERPRINT)
        self.ledger = RegimeLiveLedger(repository, profile=profile)
        self.exposure_checker = exposure_checker

    async def register_market(self, *, loop_id, market, now_ms, unit_usdt):
        if (unit_usdt not in (Decimal(1), Decimal(2), Decimal(3))
                or (self.profile not in (T62_PROFILE, T63_PROFILE, T63A_PROFILE, T63B_PROFILE, T65_PROFILE, T67_PROFILE, T67A_PROFILE, T67B_PROFILE, T67C_PROFILE, T67D_PROFILE, T68_PROFILE, T68A_PROFILE, T69_PROFILE, T69A_PROFILE) and unit_usdt != Decimal(1))):
            return C180Ready(False, "regime_requires_fixed_1_usdt")
        try:
            if self.symbol is not None:
                from .loop_market import market_matches
                if not market_matches(market, self.symbol):
                    return C180Ready(False, "loop_market_identity_mismatch")
                binding = await self.repository.get_loop_market_binding(loop_id)
                if binding and binding["symbol"] != self.symbol:
                    return C180Ready(False, "loop_market_binding_mismatch")
                if self.symbol != "BTCUSDT" and not binding:
                    return C180Ready(False, "loop_market_binding_missing")
            if await self.ledger.market_is_registered(loop_id=loop_id, market=market):
                self._registered_loop_id = str(loop_id)
                return C180Ready(True, "regime_market_registered")
            rows = await self.repository._fetchall(
                "SELECT MIN(market_start_ms) AS anchor FROM prediction_regime_slots WHERE loop_id=?", (loop_id,))
            anchor = rows[0]["anchor"] if rows and rows[0]["anchor"] else int(market.start_time_ms)
            await self.ledger.seed_schedule(loop_id=loop_id, first_market_start_ms=anchor)
            await self.ledger.verify_market(loop_id=loop_id, market_start_ms=market.start_time_ms,
                market_topic_id=market.market_topic_id, market_id=market.up_market_id,
                verified_at_ms=now_ms)
            # Initialize only once; subsequent loops retain the original epoch.
            await self.ledger.check_risk(loop_id, market.start_time_ms, now_ms)
        except Exception:
            return C180Ready(False, "regime_registration_unavailable")
        self._registered_loop_id = str(loop_id)
        return C180Ready(True, "regime_market_registered")

    async def prepare_market(self, *, loop_id, market, now_ms, unit_usdt,
                             already_registered=False, trace=None):
        # The worker registers every campaign before deciding.  Preserve the
        # standalone caller path, but do not repeat schedule/identity/risk
        # registration inside its two-second initial-decision window.
        if not already_registered:
            registered = await self.register_market(loop_id=loop_id, market=market,
                                                  now_ms=now_ms, unit_usdt=unit_usdt)
            if not registered.allowed:
                return registered
        try:
            began = time.monotonic_ns()
            allowed, reason = await self.ledger.check_risk(loop_id, market.start_time_ms, now_ms)
            if trace is not None:
                try:
                    trace("prepare_history_risk", time.monotonic_ns()-began)
                except Exception:
                    pass
            if allowed and (self.exposure_checker is None or not await self.exposure_checker()):
                return C180Ready(False, "account_exposure_not_clear")
            return C180Ready(allowed, reason)
        except Exception:
            return C180Ready(False, "regime_risk_unavailable")

    @staticmethod
    def _book(snapshot, market, at_ms):
        start = int(market.start_time_ms)
        if (not isinstance(snapshot, dict) or snapshot.get("full_depth") is not True
                or snapshot.get("market_start_ms") != start
                or str(snapshot.get("market_topic")) != str(market.market_topic_id)
                or str(snapshot.get("market_id")) != str(market.up_market_id)):
            raise ValueError("book identity/depth missing")
        book_at = int(snapshot["book_at_ms"])
        for key in ("received_at", "received_at_ms", "captured_at_ms"):
            received = int(snapshot[key])
            if not book_at <= received <= at_ms or at_ms-received > 2000:
                raise ValueError("book receipt stale or future")
        if not 0 <= at_ms-book_at <= 2000:
            raise ValueError("book stale or future")
        return book_at

    def _first_book(self, market, at_ms):
        start = int(market.start_time_ms)
        with closing(sqlite3.connect(self.signal_db.resolve().as_uri()+"?mode=ro", uri=True, timeout=1)) as db:
            rows = db.execute("SELECT snapshot_json FROM c180_book_events WHERE market_start_ms=? "
                "AND captured_at_ms>=? AND captured_at_ms<=? ORDER BY captured_at_ms,book_at_ms",
                (start, start+124000, min(at_ms, start+126000)))
            for row in rows:
                snap = json.loads(row[0])
                try:
                    self._book(snap, market, int(snap["captured_at_ms"]))
                    return snap
                except (ValueError, KeyError, TypeError):
                    continue
        return None

    @staticmethod
    def _execution(snapshot, side, cap=Decimal("0.90"), amount=Decimal("1")):
        return walk(snapshot["quote"][side]["ask_levels"], snapshot["fee_bps"], cap, amount)

    def check_signal(self, *, market, unit_usdt, at_ms, last_seen_book_at_ms):
        if self.symbol is not None:
            from .loop_market import market_matches, verify_data_db
            try:
                if not market_matches(market, self.symbol):
                    raise ValueError("market identity mismatch")
                if self.symbol != "BTCUSDT":
                    verify_data_db(self.feature_db, self.symbol)
                    verify_data_db(self.signal_db, self.symbol)
            except (ValueError, OSError, sqlite3.Error, AttributeError):
                return C180Ready(False, "loop_market_data_identity_mismatch")
        if self.profile == T69A_PROFILE:
            from .regime_t69a_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T69_PROFILE:
            from .regime_t69_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T68A_PROFILE:
            from .regime_t68a_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T68_PROFILE:
            from .regime_t68_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T67D_PROFILE:
            from .regime_t67d_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T67C_PROFILE:
            from .regime_t67c_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T67B_PROFILE:
            from .regime_t67b_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T67A_PROFILE:
            from .regime_t67a_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T67_PROFILE:
            from .regime_t67_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T65_PROFILE:
            from .regime_t65_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T63B_PROFILE:
            from .regime_t63b_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T63A_PROFILE:
            from .regime_t63a_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms,
                                last_seen_book_at_ms=last_seen_book_at_ms)
        if self.profile == T63_PROFILE:
            from .regime_t63_bridge import check_signal
            return check_signal(self, market=market, unit_usdt=unit_usdt, at_ms=at_ms, last_seen_book_at_ms=last_seen_book_at_ms)
        start = int(market.start_time_ms)
        if (unit_usdt not in (1, 2, 3) or (self.profile not in (T62_PROFILE, T63_PROFILE) and unit_usdt != 1)
                or not start+124000 <= at_ms < start+136000):
            return C180Ready(False, "regime_unit_or_execution_window")
        try:
            with closing(connect(self.feature_db)) as db:
                row = db.execute("SELECT payload FROM decisions WHERE start=?", (start,)).fetchone()
                if row:
                    decision = json.loads(row[0])
                else:
                    if at_ms > start+126000:
                        return C180Ready(False, "regime_initial_decision_window_missed")
                    row = db.execute("SELECT payload FROM features WHERE start=?", (start,)).fetchone()
                    if not row:
                        return C180Ready(False, "regime_features_missing_skip")
                    features = json.loads(row[0])
                    if (features.get("fingerprint") != FINGERPRINT
                            or features.get("market_start_ms") != start
                            or features.get("cutoff_ms") != start+120000
                            or not start+120000 <= int(features["received_at_ms"]) <= start+123000):
                        return C180Ready(False, "regime_feature_provenance_invalid")
                    original = read_c180_signal(self.signal_db, start)
                    if original and (original.market_topic != market.market_topic_id
                                     or original.market_id != market.up_market_id):
                        return C180Ready(False, "original_market_identity_mismatch")
                    original_payload = json.loads(_signal_json(original)) if original else None
                    base = (select_primary if self.profile in (T62_PROFILE, T63_PROFILE) else select_side)(features, original_payload)
                    base["branch"] = "T6"
                    decision = base
                    decision.update(fingerprint=self.decision_fingerprint, market_topic=market.market_topic_id,
                                    market_id=market.up_market_id, decided_at_ms=at_ms,
                                    unit_usdt=str(unit_usdt))
                    if decision["allowed"] or self.profile in {T61_PROFILE, T62_PROFILE, T63_PROFILE}:
                        snapshot = self._first_book(market, at_ms)
                        if snapshot is None:
                            return C180Ready(False, "regime_initial_book_missing_skip")
                    if decision["allowed"]:
                        try:
                            execution = self._execution(snapshot, decision["side"], amount=unit_usdt)
                            if not dec(decision["lower"]) <= execution["limit"] <= dec(decision["upper"]):
                                decision.update(allowed=False, reason="price_band")
                            elif decision["action"] == "original" and (
                                    dec(decision["probability"])*execution["net_shares"]-execution["cash"] <= Decimal("0.005")*unit_usdt):
                                decision.update(allowed=False, reason="rechecked_ev")
                        except (ValueError, KeyError, TypeError, ArithmeticError):
                            decision.update(allowed=False, reason="initial_depth_or_fee_invalid")
                    if (self.profile in {T61_PROFILE, T62_PROFILE, T63_PROFILE} and not decision["allowed"]
                            and decision["reason"] != "initial_depth_or_fee_invalid"):
                        decision = select_fallback(features, original_payload, decision)
                        decision.update(fingerprint=self.decision_fingerprint, market_topic=market.market_topic_id,
                                        market_id=market.up_market_id, decided_at_ms=at_ms,
                                        unit_usdt=str(unit_usdt))
                        if decision["allowed"]:
                            try:
                                execution = self._execution(snapshot, decision["side"], amount=unit_usdt)
                                if not dec(decision["lower"]) <= execution["limit"] <= dec(decision["upper"]):
                                    decision.update(allowed=False, reason="price_band")
                                elif decision["action"] == "original" and (
                                        dec(decision["probability"])*execution["net_shares"]-execution["cash"] <= Decimal("0.005")*unit_usdt):
                                    decision.update(allowed=False, reason="rechecked_ev")
                            except (ValueError, KeyError, TypeError, ArithmeticError):
                                decision.update(allowed=False, reason="initial_depth_or_fee_invalid")
                    if decision["allowed"]:
                        entry = C180EntryDecision(decision["side"], "regime_entry", decision["side"],
                            unit_usdt, execution["net_shares"], None)
                        signal = C180Signal(start, market.market_topic_id, market.up_market_id,
                            start+120000, at_ms, "regime_frozen_entry", entry,
                            original.original_p_up if original and decision["action"] == "original" else None,
                            self.decision_fingerprint, None, dec(snapshot["fee_bps"]))
                        decision.update(signal=_signal_json(signal), limit=str(execution["limit"]),
                                        initial_book_at_ms=int(snapshot["book_at_ms"]))
                    with db:
                        db.execute("INSERT OR IGNORE INTO decisions VALUES(?,?)", (start, json.dumps(decision, sort_keys=True)))
                    decision = json.loads(db.execute("SELECT payload FROM decisions WHERE start=?", (start,)).fetchone()[0])
            if (decision.get("fingerprint") != self.decision_fingerprint or decision.get("market_topic") != market.market_topic_id
                    or decision.get("market_id") != market.up_market_id
                    or decision.get("unit_usdt") != str(unit_usdt)):
                return C180Ready(False, "regime_decision_identity_mismatch")
            if not decision["allowed"]:
                return C180Ready(False, "regime_frozen_skip:"+decision["reason"])
            signal = _from_signal_json(decision["signal"])
            snapshot = read_c180_book(self.signal_db, start)
            book_at = self._book(snapshot, market, at_ms)
            if book_at <= last_seen_book_at_ms:
                return C180Ready(False, "quote_not_new_after_ready")
            if dec(snapshot["fee_bps"]) != signal.frozen_fee_bps:
                return C180Ready(False, "regime_fee_changed")
            if signal.entry.stake_usdt != unit_usdt:
                return C180Ready(False, "regime_signal_unit_mismatch")
            execution = self._execution(snapshot, signal.entry.side, dec(decision["limit"]), unit_usdt)
            if decision["action"] == "original" and (
                    dec(decision["probability"])*execution["net_shares"]-execution["cash"] <= Decimal("0.005")*unit_usdt):
                return C180Ready(False, "regime_ev_changed")
            recheck = C180ExecutionRecheck(True, "regime_ready", at_ms, start+136000,
                dec(decision["limit"]), execution["cash"], execution["net_shares"], None)
            return C180Ready(True, "regime_ready", signal, recheck, book_at)
        except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
            return C180Ready(False, "regime_inputs_unavailable_skip")
