"""Public-data-only feature collector. No wallet, signer, or trading API."""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path

from .regime_lane import SLOT_MS, freeze_features

DEFAULT_DB = "prediction/data/regime-target6/features.sqlite3"


def connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=1)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS features(start INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS decisions(start INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS t65_shadow_quotes(start INTEGER NOT NULL, branch TEXT NOT NULL, "
               "payload TEXT NOT NULL, PRIMARY KEY(start,branch))")
    db.execute("CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY, at_ms INTEGER, status TEXT)")
    db.commit()
    return db


def collect_once(db, start, *, clock=lambda: time.time_ns()//1000000, fetch=None):
    now = clock()
    if not start + 120000 <= now < start + 123000:
        return "outside_cutoff"
    if db.execute("SELECT 1 FROM features WHERE start=?", (start,)).fetchone():
        return "already_frozen"
    params = urllib.parse.urlencode({"symbol": "BTCUSDT", "interval": "1m",
        "startTime": start-900000, "endTime": start+119999, "limit": 17})
    if fetch is None:
        def fetch():
            with urllib.request.urlopen("https://api.binance.com/api/v3/klines?"+params, timeout=1.5) as response:
                return json.load(response)
    try:
        payload = freeze_features(start, fetch(), clock())
        with db:
            db.execute("INSERT OR IGNORE INTO features VALUES(?,?)",
                       (start, json.dumps(payload, sort_keys=True)))
        return "frozen"
    except Exception as exc:
        return "unavailable:" + type(exc).__name__


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--signal-db", default="prediction/data/c180-favorite-live/signals.sqlite3")
    args = parser.parse_args()
    db = connect(args.db)
    try:
        while True:
            now = time.time_ns()//1000000
            start = now//SLOT_MS*SLOT_MS
            status = collect_once(db, start)
            if start+124000 <= now <= start+137000:
                from .regime_t65_shadow import collect_once as observe_shadow
                try:
                    observe_shadow(db, start, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ";t65_shadow_unavailable:" + type(exc).__name__
            with db:
                db.execute("INSERT OR REPLACE INTO health VALUES(1,?,?)", (now, status))
            time.sleep(0.25 if 119500 <= now-start <= 137000 else 1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
