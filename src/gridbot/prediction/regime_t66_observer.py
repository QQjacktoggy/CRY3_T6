"""Bounded prospective paper observations, independent of Live loop lifecycle.

Only the feature database is written. No wallet, order, intent or risk mutation.
Official resolution is injected by the existing rate-budgeted signal service.
"""
import hashlib
import json
import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from .regime_lane import FINGERPRINT as FEATURE_FINGERPRINT, SLOT_MS, dec, walk
from .regime_t63_lane import eligible_execution
from .regime_t66_policy import FINGERPRINT, PROFILE, RETIRED_KEYS, candidates


def schema(db):
    db.execute("CREATE TABLE IF NOT EXISTS t66_observation_state(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS t66_observation_markets(start INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS t66_shadow_quotes(start INTEGER NOT NULL, branch TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(start,branch))")
    db.execute("CREATE TABLE IF NOT EXISTS t66_shadow_outcomes(start INTEGER PRIMARY KEY, payload TEXT NOT NULL)")


def state(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='t66_observation_state'").fetchone():
        return None
    row = db.execute("SELECT payload FROM t66_observation_state WHERE id=1").fetchone()
    value = json.loads(row[0]) if row else None
    if value is not None and (not isinstance(value, dict) or value.get("fingerprint") != FINGERPRINT):
        raise ValueError("T6.6 observation fingerprint mismatch")
    return value


def activate(db, now_ms, target=500):
    """Explicit deployment operation, idempotent without resetting prior samples."""
    if target != 500:
        raise ValueError("frozen 500-market cohort required")
    schema(db)
    old = state(db)
    if old:
        return old
    start = (now_ms // SLOT_MS + 1) * SLOT_MS
    value = dict(profile=PROFILE, fingerprint=FINGERPRINT, first_start_ms=start,
                 target=target, enabled=True, activated_at_ms=now_ms, auto_promote=False)
    with db:
        db.execute("INSERT INTO t66_observation_state VALUES(1,?)", (json.dumps(value, sort_keys=True),))
        for i in range(target):
            db.execute("INSERT INTO t66_observation_markets VALUES(?,?)", (start+i*SLOT_MS,
                json.dumps(dict(status="SCHEDULED", fingerprint=FINGERPRINT), sort_keys=True)))
    return value


def filter_retired_shadows(db, start, shadows):
    try:
        config = state(db)
    except (sqlite3.Error, ValueError, TypeError, KeyError):
        # An observation-state failure must never block unchanged core trading
        # or silently reactivate retired paper branches.
        return {k: v for k, v in shadows.items() if k not in RETIRED_KEYS}
    if config and config["enabled"] and start >= config["first_start_ms"]:
        return {k: v for k, v in shadows.items() if k not in RETIRED_KEYS}
    return shadows


def _read_books(signal_db, start, low, high):
    with closing(sqlite3.connect(Path(signal_db).resolve().as_uri()+"?mode=ro", uri=True, timeout=1)) as db:
        return [json.loads(r[0]) for r in db.execute(
            "SELECT snapshot_json FROM c180_book_events WHERE market_start_ms=? AND captured_at_ms>=? AND captured_at_ms<=? ORDER BY captured_at_ms,book_at_ms",
            (start, start+low, start+high))]


def _valid(book, market, now, max_age=1000):
    from .regime_worker_bridge import RegimeWorkerBridge
    at = RegimeWorkerBridge._book(book, market, now)
    if now-at > max_age:
        raise ValueError("stale observation book")
    return at


def freeze_once(db, start, signal_db, now):
    if not start+124000 <= now <= start+126000:
        return
    row = db.execute("SELECT payload FROM t66_observation_markets WHERE start=?", (start,)).fetchone()
    if not row or json.loads(row[0])["status"] != "SCHEDULED":
        return
    feature = db.execute("SELECT payload FROM features WHERE start=?", (start,)).fetchone()
    if not feature:
        return
    f = json.loads(feature[0])
    if (f.get("fingerprint") != FEATURE_FINGERPRINT or f.get("market_start_ms") != start
            or f.get("cutoff_ms") != start+120000 or not start+120000 <= int(f["received_at_ms"]) <= start+123000):
        raise ValueError("invalid T6.6 frozen features")
    with closing(sqlite3.connect(Path(signal_db).resolve().as_uri()+"?mode=ro", uri=True, timeout=1)) as signals:
        row = signals.execute("SELECT signal_json FROM c180_signals WHERE market_start_ms=?", (start,)).fetchone()
        original = json.loads(row[0]) if row else None
    for initial in _read_books(signal_db, start, 124000, min(now-start, 126000)):
        market = SimpleNamespace(start_time_ms=start, market_topic_id=initial["market_topic"], up_market_id=initial["market_id"])
        try:
            _valid(initial, market, int(initial["captured_at_ms"]), 2000)
        except (ValueError, TypeError, KeyError):
            continue
        if original and (original["market_topic"], original["market_id"]) != (initial["market_topic"], initial["market_id"]):
            raise ValueError("original market identity mismatch")
        core, observations = candidates(f, original, initial)
        value = dict(status="FROZEN", fingerprint=FINGERPRINT, profile=PROFILE, market_start_ms=start,
            market_topic=initial["market_topic"], market_id=initial["market_id"], fee_bps=str(initial["fee_bps"]),
            features=f, core_candidates=core, candidates=observations, unit_usdt="1", frozen_at_ms=now,
            initial_captured_at_ms=initial["captured_at_ms"],
            initial_sha256=hashlib.sha256(json.dumps(initial, sort_keys=True).encode()).hexdigest())
        with db:
            db.execute("UPDATE t66_observation_markets SET payload=? WHERE start=?", (json.dumps(value, sort_keys=True), start))
        return


def _quote(candidate, book):
    ex = eligible_execution(candidate, book, Decimal(1))
    result = {k: str(ex[k]) for k in ("cash", "net_shares", "limit")}
    result["fee_bps"] = str(book["fee_bps"])
    stressed = [[str(dec(p)+Decimal('.02')), q]
                for p, q in book["quote"][candidate["side"]]["ask_levels"]
                if dec(p)+Decimal('.02') < 1]
    # Same frozen candidate at a worse price; do not select a new favorable market.
    try:
        ex = walk(stressed, book["fee_bps"], cap=Decimal('.99'), amount=Decimal(1))
        result["stress_tick2"] = {k: str(ex[k]) for k in ("cash", "net_shares", "limit")}
    except (ValueError, ArithmeticError):
        result["stress_tick2"] = None
    return result


def observe_once(db, start, signal_db, now):
    row = db.execute("SELECT payload FROM t66_observation_markets WHERE start=?", (start,)).fetchone()
    if not row:
        return
    frozen = json.loads(row[0])
    if frozen["status"] != "FROZEN":
        return
    if frozen["fingerprint"] != FINGERPRINT:
        raise ValueError("T6.6 frozen observation identity changed")
    market = SimpleNamespace(start_time_ms=start, market_topic_id=frozen['market_topic'], up_market_id=frozen['market_id'])
    books = []
    if start+128000 <= now <= start+135500:
        for b in _read_books(signal_db, start, 128000, min(now-start, 135500)):
            try:
                bt = _valid(b, market, now)
                if bt < start+128000 or str(b['fee_bps']) != frozen['fee_bps']:
                    continue
                books.append(b)
            except (ValueError, KeyError, TypeError):
                continue
    for branch, c in frozen['candidates'].items():
        old = db.execute('SELECT payload FROM t66_shadow_quotes WHERE start=? AND branch=?', (start, branch)).fetchone()
        q = json.loads(old[0]) if old else dict(fingerprint=FINGERPRINT, market_start_ms=start,
            market_topic=frozen['market_topic'], market_id=frozen['market_id'], candidate=c,
            status='AWAITING_WINDOW' if c['control'] or c['incremental_eligible'] else 'CORE_OVERLAP',
            quote=None, checked_books=0, delays={})
        if q['status'] == 'CORE_OVERLAP':
            pass
        else:
            for b in books:
                if b['book_at_ms'] <= q.get('last_checked_book_at_ms', 0):
                    continue
                q['last_checked_book_at_ms'] = b['book_at_ms']
                if q['status'] == 'AWAITING_WINDOW' and now <= start+134500:
                    q['checked_books'] += 1
                    try:
                        quoted = _quote(c, b)
                    except (ValueError, TypeError, KeyError, ArithmeticError):
                        continue
                    q.update(status='PAPER_QUOTE_ONLY', quote=quoted, quoted_at_ms=now,
                             book_at_ms=b['book_at_ms'], captured_at_ms=b['captured_at_ms'])
                elif q['status'] == 'PAPER_QUOTE_ONLY':
                    for delay in (300, 1000):
                        key = str(delay)
                        if key in q['delays'] or int(b['captured_at_ms']) < q['quoted_at_ms']+delay:
                            continue
                        if int(b['captured_at_ms']) > q['quoted_at_ms']+delay+300:
                            q['delays'][key] = dict(status='UNOBSERVED', quote=None)
                            continue
                        try:
                            delayed = _quote(c, b)
                            status = 'EXECUTABLE'
                        except (ValueError, TypeError, KeyError, ArithmeticError):
                            delayed, status = None, 'UNEXECUTABLE'
                        q['delays'][key] = dict(status=status, quote=delayed, observed_at_ms=now,
                                               captured_at_ms=b['captured_at_ms'], book_at_ms=b['book_at_ms'])
            if now > start+134500 and q['status'] == 'AWAITING_WINDOW':
                q['status'] = 'NO_EXECUTABLE_QUOTE' if q['checked_books'] else 'UNOBSERVED_WINDOW'
            if now > start+135500 and q['status'] == 'PAPER_QUOTE_ONLY':
                for delay in ('300', '1000'):
                    q['delays'].setdefault(delay, dict(status='UNOBSERVED', quote=None))
        raw = json.dumps(q, sort_keys=True)
        if not old or raw != old[0]:
            with db:
                db.execute('INSERT INTO t66_shadow_quotes VALUES(?,?,?) ON CONFLICT(start,branch) DO UPDATE SET payload=excluded.payload', (start, branch, raw))


def tick(db, signal_db, now):
    config = state(db)
    if not config or not config['enabled']:
        return 'disabled'
    start = now//SLOT_MS*SLOT_MS
    freeze_once(db, start, signal_db, now)
    observe_once(db, start, signal_db, now)
    # A restart never backfills a missed decision/quote from future-known history.
    rows = db.execute("SELECT start,payload FROM t66_observation_markets WHERE start+135500<? AND json_extract(payload,'$.status') IN ('SCHEDULED','FROZEN') ORDER BY start LIMIT 50", (now,)).fetchall()
    for s, raw in rows:
        value = json.loads(raw)
        if value['status'] == 'FROZEN':
            observe_once(db, s, signal_db, now)
            value['status'] = 'OBSERVED'
        else:
            value['status'] = 'UNOBSERVED_DECISION'
        value['closed_at_ms'] = now
        with db:
            db.execute('UPDATE t66_observation_markets SET payload=? WHERE start=?', (json.dumps(value, sort_keys=True), s))
    return 'bounded_cohort'


async def resolve_once(db, now, fetch):
    from .models import MarketInfo
    from .worker import PredictionWorker
    config = state(db)
    if not config or not config['enabled']:
        return 'disabled'
    row = db.execute("SELECT m.start,m.payload FROM t66_observation_markets m LEFT JOIN t66_shadow_outcomes o ON o.start=m.start WHERE m.start+300000<=? AND json_extract(m.payload,'$.market_topic') IS NOT NULL AND COALESCE(json_extract(o.payload,'$.complete'),0)=0 ORDER BY COALESCE(json_extract(o.payload,'$.last_checked_at_ms'),0),m.start LIMIT 1", (now,)).fetchone()
    if not row:
        return 'complete'
    start, frozen = row[0], json.loads(row[1])
    value = dict(fingerprint=FINGERPRINT, market_start_ms=start, market_topic=frozen['market_topic'],
                 market_id=frozen['market_id'], last_checked_at_ms=now, complete=False, winner=None)
    try:
        detail = await fetch(frozen['market_topic'])
        market = MarketInfo.from_api(detail)
        if (market.market_topic_id != frozen['market_topic'] or market.up_market_id != frozen['market_id']
                or market.start_time_ms != start or market.end_time_ms != start+SLOT_MS):
            raise ValueError('official T6.6 market identity mismatch')
        winner = PredictionWorker._official_shadow_resolution(detail)
        if market.status in ('CLOSED', 'RESOLVED', 'SETTLED') and winner in ('UP', 'DOWN', 'DRAW'):
            value.update(complete=True, winner=winner, known_at_ms=now,
                         official_sha256=hashlib.sha256(json.dumps(detail, sort_keys=True).encode()).hexdigest())
    except Exception as exc:
        value['last_error'] = type(exc).__name__
    with db:
        db.execute('INSERT INTO t66_shadow_outcomes VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload', (start, json.dumps(value, sort_keys=True)))
    return 'resolved' if value['complete'] else 'pending'
