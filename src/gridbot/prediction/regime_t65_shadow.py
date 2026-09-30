"""Prospective quote observations only; no wallet, claims, positions or orders."""
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

from .regime_t63_lane import eligible_execution
from .regime_t65_lane import FINGERPRINT


def _window_books(signal_db, start, now_ms):
    uri = Path(signal_db).resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
        rows = db.execute('SELECT snapshot_json FROM c180_book_events WHERE market_start_ms=? '
                          'AND captured_at_ms>=? AND captured_at_ms<=? '
                          'ORDER BY captured_at_ms,book_at_ms',
                          (start, start+128000, min(now_ms, start+134500))).fetchall()
    return [json.loads(row[0]) for row in rows]


def collect_once(db, start, signal_db, now_ms):
    """Independent of the Live campaign's filled/held state, with a separate table."""
    from .regime_worker_bridge import RegimeWorkerBridge

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
    snapshots = []
    if start + 128000 <= now_ms <= start + 134500:
        market = SimpleNamespace(start_time_ms=start, market_topic_id=decision["market_topic"],
                                 up_market_id=decision["market_id"])
        for snapshot in _window_books(signal_db, start, now_ms):
            try:
                book_at = RegimeWorkerBridge._book(snapshot, market, now_ms)
                if (now_ms-book_at > 1000 or book_at < start+128000
                        or not start+128000 <= int(snapshot['captured_at_ms']) <= start+134500
                        or str(snapshot['fee_bps']) != decision['fee_bps']):
                    continue
            except (ValueError, TypeError, KeyError):
                continue
            snapshots.append((snapshot, book_at))
    for shadow in pending:
        shadow["last_checked_at_ms"] = now_ms
        for snapshot, book_at in snapshots:
            if book_at <= shadow.get('last_checked_book_at_ms', 0):
                continue
            shadow['last_checked_book_at_ms'] = book_at
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
                shadow['captured_at_ms'] = int(snapshot['captured_at_ms'])
                break
        if now_ms > start+134500:
            shadow["fill_status"] = ("NO_EXECUTABLE_WINDOW_QUOTE" if shadow["eligible_books"]
                                     else "UNOBSERVED_WINDOW")
        with db:
            db.execute("INSERT INTO t65_shadow_quotes VALUES(?,?,?) ON CONFLICT(start,branch) "
                       "DO UPDATE SET payload=excluded.payload",
                       (start, shadow["branch"], json.dumps(shadow, sort_keys=True)))
    return "observed"


def finalize_expired(db, now_ms):
    """Close interrupted observations without replaying an unobserved window."""
    rows = db.execute(
        "SELECT d.start FROM decisions d "
        "LEFT JOIN t65_shadow_quotes m4 ON m4.start=d.start AND m4.branch='M4_first_pullback' "
        "LEFT JOIN t65_shadow_quotes m6 ON m6.start=d.start AND m6.branch='M6_neutral_cheap' "
        "WHERE d.start+134500<? AND json_extract(d.payload,'$.fingerprint')=? AND ("
        "(json_type(d.payload,'$.shadow_m4')='object' AND (m4.start IS NULL OR "
        "json_extract(m4.payload,'$.fill_status')='AWAITING_SHADOW_WINDOW')) OR "
        "(json_type(d.payload,'$.shadow_m6')='object' AND (m6.start IS NULL OR "
        "json_extract(m6.payload,'$.fill_status')='AWAITING_SHADOW_WINDOW'))) "
        "ORDER BY d.start LIMIT 100", (now_ms, FINGERPRINT)).fetchall()
    for row in rows:
        collect_once(db, row[0], None, now_ms)
    return len(rows)


async def resolve_outcome_once(db, now_ms, fetch):
    """Poll one expired shadow market, independently of Live fills and loops."""
    from .models import MarketInfo
    from .worker import PredictionWorker

    row = db.execute(
        "SELECT d.start,d.payload,o.payload FROM decisions d "
        "LEFT JOIN t65_shadow_outcomes o ON o.start=d.start "
        "WHERE d.start+300000<=? AND json_extract(d.payload,'$.fingerprint')=? "
        "AND COALESCE(json_extract(o.payload,'$.complete'),0)=0 "
        "ORDER BY COALESCE(json_extract(o.payload,'$.last_checked_at_ms'),0),d.start LIMIT 1",
        (now_ms, FINGERPRINT)).fetchone()
    if row is None:
        return 'complete'
    start, decision = row[0], json.loads(row[1])
    outcome = dict(fingerprint=FINGERPRINT, market_start_ms=start,
                   market_topic=decision['market_topic'], market_id=decision['market_id'],
                   complete=False, winner=None, last_checked_at_ms=now_ms)
    if not any(decision.get(key) for key in ('shadow_a', 'shadow_flat', 'shadow_b',
                                           'shadow_fallback', 'shadow_m4', 'shadow_m6')):
        outcome['complete'] = True
    else:
        try:
            detail = await fetch(decision['market_topic'])
            market = MarketInfo.from_api(detail)
            if (market.market_topic_id != decision['market_topic']
                    or market.up_market_id != decision['market_id']
                    or market.start_time_ms != start or market.end_time_ms != start+300000):
                raise ValueError('official shadow market identity mismatch')
            if market.status in ('CLOSED', 'RESOLVED', 'SETTLED'):
                outcome['winner'] = PredictionWorker._official_shadow_resolution(detail)
            if outcome['winner'] is not None:
                outcome.update(complete=True, known_at_ms=now_ms, official_detail=detail)
        except Exception as exc:
            outcome['last_error'] = type(exc).__name__
    with db:
        db.execute('INSERT INTO t65_shadow_outcomes VALUES(?,?) ON CONFLICT(start) '
                   'DO UPDATE SET payload=excluded.payload',
                   (start, json.dumps(outcome, sort_keys=True)))
    return 'resolved' if outcome['winner'] else 'pending'
