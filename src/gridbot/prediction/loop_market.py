"""T6.7c/T6.9/T6.9a asset identity. Public data is isolated; the account ledger is shared."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
PROFILE = "regime_target6_7c_v1"
T69_PROFILE = "regime_target6_9_v1"
T69A_PROFILE = "regime_target6_9a_v1"
PROFILES = (PROFILE, T69_PROFILE, T69A_PROFILE)


def symbol(value):
    value = str(value).strip().upper()
    if value not in SYMBOLS:
        raise ValueError("unsupported loop market")
    return value


def execution_fingerprint(asset, profile=PROFILE):
    if profile == T69_PROFILE:
        from .regime_t69_policy import FINGERPRINT
        routing = "t69_nine_branches"
    elif profile == T69A_PROFILE:
        from .regime_t69a_policy import FINGERPRINT
        routing = "t69a_seven_branches"
    elif profile == PROFILE:
        from .regime_t67c_policy import FINGERPRINT
        routing = "t67c_seven_branches"
    else:
        raise ValueError("profile has no loop market binding")
    return hashlib.sha256(json.dumps(dict(version=1, symbol=symbol(asset),
        parent=FINGERPRINT, routing=routing, isolation="asset_database_v1"),
        sort_keys=True).encode()).hexdigest()


def data_paths(prediction_db, asset):
    parent = Path(prediction_db).resolve().parent
    asset = symbol(asset)
    if asset == "BTCUSDT":
        return parent / "regime-target6/features.sqlite3", parent / "c180-favorite-live/signals.sqlite3"
    parent = parent / "t67c-multimarket" / asset
    return parent / "features.sqlite3", parent / "signals.sqlite3"


def bind_data_db(db, asset):
    """Never relabel populated legacy data as another asset."""
    asset = symbol(asset)
    db.execute("CREATE TABLE IF NOT EXISTS asset_identity(id INTEGER PRIMARY KEY CHECK(id=1),symbol TEXT NOT NULL)")
    row = db.execute("SELECT symbol FROM asset_identity WHERE id=1").fetchone()
    if row and row[0] != asset:
        raise ValueError("data database asset mismatch")
    if not row:
        if asset != "BTCUSDT":
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='asset_identity'").fetchall()
            for (table,) in tables:
                if db.execute('SELECT 1 FROM "'+table.replace('"','""')+'" LIMIT 1').fetchone():
                    raise ValueError("cannot relabel populated database")
        db.execute("INSERT INTO asset_identity VALUES(1,?)", (asset,))
        db.commit()


def verify_data_db(path, asset):
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro", uri=True, timeout=1)) as db:
        row = db.execute("SELECT symbol FROM asset_identity WHERE id=1").fetchone()
        if not row or row[0] != symbol(asset):
            raise ValueError("data database asset mismatch")


def market_matches(market, asset):
    raw = market.raw
    raw = raw.get("data", raw)
    return (raw.get("symbol") == symbol(asset)
            and raw.get("variantData", {}).get("priceFeedSymbol") == asset
            and market.end_time_ms - market.start_time_ms == 300000)


def report_asset(root, loop_id):
    dbpath = Path(root)/"prediction/data/prediction.sqlite3"
    with closing(sqlite3.connect(dbpath.resolve().as_uri()+"?mode=ro", uri=True)) as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='prediction_loop_market_bindings'").fetchone()
        row = db.execute("SELECT symbol FROM prediction_loop_market_bindings WHERE loop_id=?", (loop_id,)).fetchone() if exists else None
    return symbol(row[0]) if row else None


def report_feature_path(root, loop_id):
    asset = report_asset(root, loop_id)
    return data_paths(Path(root)/"prediction/data/prediction.sqlite3", asset or "BTCUSDT")[0]


def signal_asset_active(prediction_db, asset):
    """Do not multiply paid Original calls across idle asset collectors."""
    asset = symbol(asset)
    with closing(sqlite3.connect(Path(prediction_db).resolve().as_uri()+"?mode=ro", uri=True, timeout=1)) as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE name='prediction_loop_market_bindings'").fetchone()
        if not exists:
            return asset == 'BTCUSDT'
        row = db.execute("""SELECT b.symbol FROM prediction_loops l
            LEFT JOIN prediction_loop_market_bindings b ON l.loop_id=b.loop_id
            WHERE l.state='RUNNING' ORDER BY l.created_at_ms DESC LIMIT 1""").fetchone()
        if row and row[0]:
            return row[0] == asset
        return asset == 'BTCUSDT'
