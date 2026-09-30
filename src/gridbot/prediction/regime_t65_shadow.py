"""Prospective quote observations only; no wallet, claims, positions or orders."""
import json
from types import SimpleNamespace

from .regime_t63_lane import eligible_execution
from .regime_t65_lane import FINGERPRINT


def collect_once(db, start, signal_db, now_ms):
    """Independent of the Live campaign's filled/held state, with a separate table."""
    from .regime_worker_bridge import RegimeWorkerBridge, read_c180_book

    row = db.execute("SELECT payload FROM decisions WHERE start=?", (start,)).fetchone()
    if row is None:
        return "no_decision"
    decision = json.loads(row[0])
    if decision.get("fingerprint") != FINGERPRINT:
        return "other_profile"
    pending = []
    for key in ("shadow_m4", "shadow_m6"):
        frozen = decision.get(key)
        if frozen is None:
            continue
        old = db.execute("SELECT payload FROM t65_shadow_quotes WHERE start=? AND branch=?",
                         (start, frozen["branch"])).fetchone()
        shadow = json.loads(old[0]) if old else dict(frozen, fingerprint=FINGERPRINT,
            market_start_ms=start, market_topic=decision["market_topic"], market_id=decision["market_id"],
            unit_usdt=decision["unit_usdt"], decided_at_ms=decision["decided_at_ms"],
            eligible_books=0, last_checked_at_ms=now_ms)
        if shadow["fill_status"] == "AWAITING_SHADOW_WINDOW":
            pending.append(shadow)
    if not pending:
        return "complete"
    snapshot = None
    if start + 128000 <= now_ms <= start + 134500:
        snapshot = read_c180_book(signal_db, start)
        market = SimpleNamespace(start_time_ms=start, market_topic_id=decision["market_topic"],
                                 up_market_id=decision["market_id"])
        try:
            book_at = RegimeWorkerBridge._book(snapshot, market, now_ms)
            if (now_ms-book_at > 1000 or book_at < start+128000
                    or int(snapshot["captured_at_ms"]) < start+128000
                    or str(snapshot["fee_bps"]) != decision["fee_bps"]):
                snapshot = None
        except (ValueError, TypeError, KeyError):
            snapshot = None
    for shadow in pending:
        shadow["last_checked_at_ms"] = now_ms
        if snapshot is not None:
            shadow["eligible_books"] += 1
            try:
                execution = eligible_execution(shadow["candidate"], snapshot, int(decision["unit_usdt"]))
            except (ValueError, TypeError, KeyError, ArithmeticError):
                pass
            else:
                shadow.update(fill_status="PAPER_QUOTE_ONLY", quoted_at_ms=now_ms,
                              book_at_ms=book_at, received_at_ms=int(snapshot["received_at_ms"]),
                              quote={k: str(execution[k]) for k in ("cash", "net_shares", "limit")})
                shadow["quote"]["fee_bps"] = str(snapshot["fee_bps"])
        if now_ms > start+134500:
            shadow["fill_status"] = ("NO_EXECUTABLE_WINDOW_QUOTE" if shadow["eligible_books"]
                                     else "UNOBSERVED_WINDOW")
        with db:
            db.execute("INSERT INTO t65_shadow_quotes VALUES(?,?,?) ON CONFLICT(start,branch) "
                       "DO UPDATE SET payload=excluded.payload",
                       (start, shadow["branch"], json.dumps(shadow, sort_keys=True)))
    return "observed"
