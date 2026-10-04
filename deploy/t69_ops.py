"""Shared T6.9 operator helpers. Read-only except where a caller passes --apply.

Ship beside t69_manual_install.py, t69_verify.py, t69_rollback.py and
release_verifier.py, outside STAGE. Nothing here arms, selects a strategy,
creates a loop or sends Telegram; official APIs are only queried (GET).
"""
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

ROOT = Path('/home/jack_shih/cry3')
MANIFEST = 'prediction/release-manifest.json'
PIN = 'prediction/release-pin.env'
GUARD = 'prediction/hs-recovery-startup.env'
SERVICES = ('cry3-predict-user.service', 'cry3-regime-feature.service', 'cry3-c180-favorite-signal.service')
VENV_PYTHON = 'testnet/.venv/bin/python'
PRODUCER_MARKERS = ('run_t67c_asset.sh', 'regime_feature_service', 'c180_signal_runtime')
# Reviewed offline from main 15c67fb; a different value means different policy bytes.
POLICY_FINGERPRINT = '19b065791563f411faee5df68979c2e3d8c7c19de3088f0406a921c7c215f1bd'
T69_TABLES = ('t69_flat_shadow_states', 't69_shadow_outcomes', 't69_shadow_quotes', 't69_shadow_states')
FEATURE_DBS = {'BTCUSDT': 'prediction/data/regime-target6/features.sqlite3',
               'ETHUSDT': 'prediction/data/t67c-multimarket/ETHUSDT/features.sqlite3',
               'BNBUSDT': 'prediction/data/t67c-multimarket/BNBUSDT/features.sqlite3'}
TERMINAL = "('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED','REJECTED')"
LEDGER_TABLES = ('prediction_loops', 'prediction_fills', 'prediction_order_intents', 'prediction_orders',
                 'prediction_risk_ledger', 'prediction_settlements', 'prediction_regime_entry_claims',
                 'prediction_regime_settlement_observations')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest() if Path(path).is_file() else None


def service_name(value):
    """Extra units (for example ETH/BNB producers) must be plain cry3 user units."""
    if not isinstance(value, str) or re.fullmatch(r'cry3-[a-z0-9-]{1,64}\.service', value) is None:
        raise ValueError('service must look like cry3-<name>.service')
    return value


def prediction_db(root=None):
    path = Path(root or ROOT) / 'prediction/data/prediction.sqlite3'
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


def ledger(db):
    """Order-independent hashes of trading records plus protected runtime config."""
    hashes = {}
    for table in LEDGER_TABLES:
        rows = [dict(r) for r in db.execute('SELECT * FROM ' + table)]
        canonical = json.dumps(sorted(rows, key=lambda r: json.dumps(r, sort_keys=True)),
                               sort_keys=True, separators=(',', ':'))
        hashes[table] = hashlib.sha256(canonical.encode()).hexdigest()
    protected = {r[0]: r[1] for r in db.execute(
        "SELECT config_key,config_value_json FROM prediction_runtime_config WHERE "
        "config_key IN ('prediction_hard_stop_latched','prediction_risk_state','prediction_selected_strategy',"
        "'prediction_selected_order_unit','prediction_pending_strategy') OR config_key LIKE 'regime_target6%risk%'")}
    return hashes, protected


def snapshot(loop_id, *, allow_cancelled=False, allow_historical_closed=False):
    """T6.8a boundary: target loop finished, nothing RUNNING, no open or UNKNOWN exposure."""
    with closing(prediction_db()) as db:
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM prediction_loops WHERE loop_id=?', (loop_id,)).fetchone()
        done = row and row['state'] == 'DONE' and row['completed'] == row['target'] == 100
        cancelled = (row and allow_cancelled and row['state'] == 'CANCELLED'
                     and row['new_entries_stopped'] == 1 and 0 <= row['completed'] <= row['target'])
        if not (done or cancelled):
            raise RuntimeError('Target loop must be DONE 100/100 or explicitly authorized stopped CANCELLED loop')
        if db.execute("SELECT 1 FROM prediction_loops WHERE state='RUNNING'").fetchone():
            raise RuntimeError('Another loop is RUNNING')
        if db.execute("SELECT 1 FROM prediction_order_intents WHERE unknown=1 OR COALESCE(status,'') NOT IN " + TERMINAL).fetchone():
            raise RuntimeError('Nonterminal/UNKNOWN intent')
        if db.execute("SELECT 1 FROM prediction_orders WHERE COALESCE(status,'') NOT IN " + TERMINAL).fetchone():
            raise RuntimeError('Nonterminal order')
        closed_exception = (" AND NOT COALESCE((c.state='DONE' AND c.loop_id<>? AND c.end_time_ms>0 AND c.end_time_ms<?),0)"
                            if allow_historical_closed else "")
        params = (loop_id, row['created_at_ms']) if allow_historical_closed else ()
        # pending_intent_id mirrors repository.loop_market_local_clear().
        if db.execute("SELECT 1 FROM prediction_campaigns c WHERE pending_unknown=1 OR pending_intent_id IS NOT NULL OR "
                      "(buy_count>0 AND NOT EXISTS(SELECT 1 FROM prediction_settlements s WHERE s.campaign_id=c.campaign_id AND s.status='SETTLED')"
                      + closed_exception + ")", params).fetchone():
            raise RuntimeError('Unsettled campaign')
        for key in ('prediction_hard_stop_latched', 'prediction_risk_state', 'regime_target6_risk_v1'):
            risk_row = db.execute('SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?', (key,)).fetchone()
            if risk_row:
                risk = json.loads(risk_row[0])
                if risk.get('latched') or risk.get('hard_stop_latched') or risk.get('halt_reason'):
                    raise RuntimeError('Risk latch remains; deployment must not reset it')
        hashes, protected = ledger(db)
        return dict(loop=dict(row), protected=protected, ledger=hashes)


def service(*args):
    env = dict(os.environ, XDG_RUNTIME_DIR=f'/run/user/{os.getuid()}',
               DBUS_SESSION_BUS_ADDRESS=f'unix:path=/run/user/{os.getuid()}/bus')
    # systemd's configured stop timeout is 90s; allow its normal teardown to finish.
    timeout = 120 if args and args[0] == 'stop' else 30
    return subprocess.check_output(['systemctl', '--user', *args], env=env, text=True, timeout=timeout).strip()


def producer_units():
    """Loaded cry3 user units whose ExecStart runs a feature/signal producer.

    ETH/BNB producers (scripts/run_t67c_asset.sh) import T6.9 code, so they must
    be cold-reloaded with the BTC services. Names come from systemd, not guesses.
    """
    listed = service('list-units', '--all', '--plain', '--no-legend', '--type=service', 'cry3-*')
    found = []
    for line in listed.splitlines():
        name = line.split()[0] if line.split() else ''
        if not name.startswith('cry3-') or not name.endswith('.service'):
            continue
        exec_start = service('show', name, '-p', 'ExecStart')
        if any(marker in exec_start for marker in PRODUCER_MARKERS):
            found.append(service_name(name))
    return tuple(found)


class Interrupted(Exception):
    """SIGHUP/SIGTERM (for example a dropped SSH session) during --apply."""


def raise_on_hangup():
    """Turn hangup/terminate into an exception so the restore path runs."""
    def handler(signum, _frame):
        raise Interrupted('received signal ' + str(signum))
    for sig in (signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, handler)


def ignore_hangup():
    """While restoring, a second hangup must not abort the restore itself."""
    for sig in (signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)


def require_runtime():
    """official_clear imports predict_main, which needs the app venv packages."""
    try:
        import dotenv  # noqa: F401
        import telegram  # noqa: F401
    except ImportError as exc:
        raise RuntimeError('Run with ' + str(ROOT / VENV_PYTHON) + ' (missing ' + str(exc.name) + ')') from exc


def service_state(name):
    """Active state, MainPID and restart count; a cold reload must change MainPID."""
    active = service('is-active', name)
    values = dict(line.split('=', 1) for line in service('show', name, '-p', 'MainPID', '-p', 'NRestarts').splitlines() if '=' in line)
    return dict(active=active, main_pid=values.get('MainPID'), restarts=values.get('NRestarts'))


def official_clear():
    """Read-only official GETs; never creates, cancels or redeems orders."""
    sys.path.insert(0, str(ROOT))
    from predict_main import load_prediction_environment, read_binance_credentials
    from src.gridbot.prediction.settings import PredictionSettings
    from src.gridbot.prediction.client import BinancePredictionClient, DEFAULT_BASE_URL
    from src.gridbot.prediction.worker import PredictionWorker
    load_prediction_environment(ROOT / 'prediction/live.env')
    settings = PredictionSettings.from_env(os.environ)
    if not settings.wallet_address:
        raise RuntimeError('Wallet not configured')
    key, secret = read_binance_credentials()
    client = BinancePredictionClient(key, secret, base_url=os.environ.get('PREDICTION_BASE_URL') or DEFAULT_BASE_URL,
                                     recv_window=settings.recv_window, order_unit_usdt=settings.order_unit_usdt)
    orders = client.query_active_orders(wallet_address=settings.wallet_address)
    positions = client.query_positions(wallet_address=settings.wallet_address)
    if not (isinstance(orders, (dict, list)) and isinstance(positions, (dict, list))):
        raise RuntimeError('Official response unknown')
    if PredictionWorker._official_order_rows(orders):
        raise RuntimeError('Official active orders remain')
    if any(PredictionWorker._official_position_shares(p) > 0 for p in PredictionWorker._official_position_rows(positions)):
        raise RuntimeError('Official positions remain')


def evidence_preflight():
    """PR14 rejects oversized legacy evidence; fail before any service stop."""
    limit = 256 * 1024 * 1024
    for relative in ('prediction/data/c180-favorite-live/signals.sqlite3',
                     'prediction/data/c180-favorite-live/t67-evidence.sqlite3'):
        path = ROOT / relative
        if not path.is_file():
            raise RuntimeError('Required evidence database missing: ' + relative)
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            size = db.execute('PRAGMA page_count').fetchone()[0] * db.execute('PRAGMA page_size').fetchone()[0]
        if size > limit:
            raise RuntimeError('Evidence maintenance required before deployment: ' + relative)
    if shutil.disk_usage(ROOT).free < 128 * 1024 * 1024:
        raise RuntimeError('Evidence free-space reserve unavailable')


def guard_bytes():
    guard = (ROOT / GUARD).read_bytes()
    if not (b'PREDICTION_LIVE_ARM_ON_START=false' in guard and b'PREDICTION_AUTO_START_LOOP=false' in guard):
        raise RuntimeError('Autoarm/autoloop guard is not disabled')
    return guard


FRESH_CHECK = r'''
import json, sqlite3, sys
from pathlib import Path
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT, LIVE_BRANCHES, SHADOW_BRANCHES
from src.gridbot.prediction import regime_t69_shadow, regime_t69_flat_shadow
from src.gridbot.prediction.live_report import format_live_report, T69_PROFILE
db = sqlite3.connect(':memory:')
regime_t69_shadow.schema(db)
regime_t69_flat_shadow.schema(db)
tables = sorted(r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'"))
report = format_live_report(Path(sys.argv[1]), profile_filter=T69_PROFILE)
print(json.dumps(dict(policy=FINGERPRINT, live=len(LIVE_BRANCHES), shadow=len(SHADOW_BRANCHES),
                      tables=tables, report=report), ensure_ascii=False))
'''


def fresh_check():
    """Import deployed T6.9 in a new interpreter; render the report without sending it."""
    result = subprocess.run([str(ROOT / VENV_PYTHON), '-B', '-c', FRESH_CHECK, str(ROOT)],
                            cwd=ROOT, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError('Fresh deployed T6.9 import/report failed')
    value = json.loads(result.stdout)
    if value.get('policy') != POLICY_FINGERPRINT:
        raise RuntimeError('Deployed T6.9 policy fingerprint differs from reviewed value')
    if (value.get('live'), value.get('shadow')) != (8, 6):
        raise RuntimeError('Deployed T6.9 lanes are not eight Live plus six Shadow')
    if not set(T69_TABLES) <= set(value.get('tables') or ()):
        raise RuntimeError('Deployed T6.9 Shadow schema is incomplete')
    if 'T6.9 Report' not in (value.get('report') or ''):
        raise RuntimeError('Deployed T6.9 report did not render')
    return value


def t69_tables():
    """Read-only state of live feature DBs. Tables appear only after T6.9 is selected."""
    result = {}
    for asset, relative in FEATURE_DBS.items():
        path = ROOT / relative
        if not path.is_file():
            result[asset] = dict(database='missing')
            continue
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
            db.execute('PRAGMA query_only=ON')
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 't69_%'")}
            row = dict(database='present', tables=sorted(names))
            if 't69_shadow_quotes' in names:
                count, latest = db.execute('SELECT COUNT(*),MAX(start) FROM t69_shadow_quotes').fetchone()
                row.update(quotes=count, latest_quote_start=latest)
        result[asset] = row
    return result


def selected_profile(key='prediction_selected_strategy'):
    with closing(prediction_db()) as db:
        row = db.execute('SELECT config_value_json FROM prediction_runtime_config WHERE config_key=?', (key,)).fetchone()
    return json.loads(row[0]).get('profile') if row else None
