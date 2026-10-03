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
from .http_bounds import KLINES_BODY_BYTES, read_bounded

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
    db.execute("CREATE TABLE IF NOT EXISTS t65_shadow_outcomes(start INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    db.execute("CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY, at_ms INTEGER, status TEXT)")
    from .regime_t66_observer import schema
    schema(db)
    db.commit()
    return db


def collect_once(db, start, *, clock=lambda: time.time_ns()//1000000, fetch=None, symbol="BTCUSDT"):
    now = clock()
    if not start + 120000 <= now < start + 123000:
        return "outside_cutoff"
    if db.execute("SELECT 1 FROM features WHERE start=?", (start,)).fetchone():
        return "already_frozen"
    params = urllib.parse.urlencode({"symbol": symbol, "interval": "1m",
        "startTime": start-900000, "endTime": start+119999, "limit": 17})
    if fetch is None:
        def fetch():
            with urllib.request.urlopen("https://api.binance.com/api/v3/klines?"+params, timeout=1.5) as response:
                return json.loads(read_bounded(response, response.headers, KLINES_BODY_BYTES))
    try:
        payload = freeze_features(start, fetch(), clock(), symbol=symbol)
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
    parser.add_argument("--symbol", default="BTCUSDT", choices=("BTCUSDT", "ETHUSDT", "BNBUSDT"))
    parser.add_argument("--prediction-db", default=None)
    args = parser.parse_args()
    from .loop_market import bind_data_db, data_paths
    if args.symbol != "BTCUSDT":
        if not args.prediction_db:
            raise ValueError("non-BTC collector requires explicit prediction DB")
        feature_path, signal_path = data_paths(args.prediction_db, args.symbol)
        if Path(args.db).resolve() != feature_path or Path(args.signal_db).resolve() != signal_path:
            raise ValueError("non-BTC data paths must be isolated")
    # Mark identity before creating feature/observer schemas.
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(args.db) as identity_db:
        bind_data_db(identity_db, args.symbol)
    db = connect(args.db)
    from .regime_t67_evidence import selected_profile
    from .regime_t67_policy import PROFILE as T67_PROFILE
    prediction_db = Path(args.prediction_db) if args.prediction_db else Path(args.db).resolve().parent.parent / 'prediction.sqlite3'
    if args.symbol != 'BTCUSDT' and not args.prediction_db:
        raise ValueError('non-BTC collector requires explicit prediction DB')
    last_finalized_at_ms = 0
    try:
        while True:
            now = time.time_ns()//1000000
            start = now//SLOT_MS*SLOT_MS
            status = collect_once(db, start, symbol=args.symbol)
            try:
                profile = selected_profile(prediction_db)
                shadows_enabled = profile not in (T67_PROFILE, 'regime_target6_7a_v1', 'regime_target6_7b_v1', 'regime_target6_7c_v1', 'regime_target6_7d_v1', 'regime_target6_8_v1', 'regime_target6_8a_v1')
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
                profile = None
                shadows_enabled = False
            if args.symbol != "BTCUSDT":
                shadows_enabled = False
                if profile != "regime_target6_7c_v1":
                    profile = None
            if profile == 'regime_target6_8a_v1':
                from .regime_t68a_shadow import observe
                try:
                    status += ';t68a:' + observe(db, prediction_db, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ';t68a_shadow_unavailable:' + type(exc).__name__
            if profile == 'regime_target6_8_v1':
                from .regime_t68_shadow import observe
                try:
                    status += ';t68:' + observe(db, prediction_db, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ';t68_shadow_unavailable:' + type(exc).__name__
            if profile == 'regime_target6_7d_v1':
                from .regime_t67d_shadow import observe
                try:
                    status += ';t67d:' + observe(db, prediction_db, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ';t67d_shadow_unavailable:' + type(exc).__name__
            if profile == 'regime_target6_7c_v1':
                from .regime_t67c_shadow import observe
                try:
                    status += ';t67c:' + observe(db, prediction_db, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ';t67c_shadow_unavailable:' + type(exc).__name__
            if profile == 'regime_target6_7b_v1':
                from .regime_t67b_shadow import observe
                try:
                    status += ';t67b:' + observe(db, prediction_db, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ';t67b_shadow_unavailable:' + type(exc).__name__
            if profile == 'regime_target6_7a_v1':
                from .regime_t67a_shadow import observe
                try:
                    status += ';t67a:' + observe(db, prediction_db, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ';t67a_shadow_unavailable:' + type(exc).__name__
            if shadows_enabled and now-last_finalized_at_ms >= 1000:
                from .regime_t65_shadow import finalize_expired
                finalize_expired(db, now)
                last_finalized_at_ms = now
            if shadows_enabled and start+124000 <= now <= start+137000:
                from .regime_t65_shadow import collect_once as observe_shadow
                try:
                    observe_shadow(db, start, args.signal_db, time.time_ns()//1000000)
                except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    status += ";t65_shadow_unavailable:" + type(exc).__name__
            try:
                from .regime_t66_observer import tick
                if shadows_enabled:
                    tick(db, args.signal_db, time.time_ns()//1000000)
            except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError) as exc:
                status += ";t66_observation_unavailable:" + type(exc).__name__
            with db:
                db.execute("INSERT OR REPLACE INTO health VALUES(1,?,?)", (now, status))
            time.sleep(0.25 if 119500 <= now-start <= 137000 else 1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
