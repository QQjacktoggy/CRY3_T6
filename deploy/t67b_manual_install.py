"""Operator-run T6.7b installer. Default is read-only; never arms or starts a loop."""
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

# The reviewed installer and verifier must be shipped together outside STAGE.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_verifier import safe_path, verify_release, validate_candidate

ROOT = Path('/home/jack_shih/cry3')
STAGE = ROOT/'prediction/t67b-entry-staged-v1-20261001'
MANIFEST = 'prediction/release-manifest.json'
PIN = 'prediction/release-pin.env'
SERVICES = ('cry3-predict-user.service','cry3-regime-feature.service','cry3-c180-favorite-signal.service')
TERMINAL = "('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED','REJECTED')"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def snapshot(loop_id, *, allow_cancelled=False, allow_historical_closed=False):
    with closing(sqlite3.connect((ROOT/'prediction/data/prediction.sqlite3').as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM prediction_loops WHERE loop_id=?',(loop_id,)).fetchone()
        done = row and row['state']=='DONE' and row['completed']==row['target']==100
        cancelled = (row and allow_cancelled and row['state']=='CANCELLED'
                     and row['new_entries_stopped']==1 and 0 <= row['completed'] <= row['target'])
        if not (done or cancelled):
            raise RuntimeError('Target loop must be DONE 100/100 or explicitly authorized stopped CANCELLED loop')
        if (db.execute("SELECT 1 FROM prediction_loops WHERE state='RUNNING'").fetchone()):
            raise RuntimeError('Another loop is RUNNING')
        if (db.execute('SELECT 1 FROM prediction_order_intents WHERE unknown=1 OR COALESCE(status,\'\') NOT IN '+TERMINAL).fetchone()):
            raise RuntimeError('Nonterminal/UNKNOWN intent')
        if (db.execute('SELECT 1 FROM prediction_orders WHERE COALESCE(status,\'\') NOT IN '+TERMINAL).fetchone()):
            raise RuntimeError('Nonterminal order')
        # Legacy paper/closed campaigns predate the target and may have no
        # settlement row. Do not edit that ledger or treat it as current exposure.
        # This exception is explicit and main() still requires official zero
        # positions/orders before and immediately before any source replacement.
        closed_exception = (" AND NOT COALESCE((c.state='DONE' AND c.loop_id<>? AND c.end_time_ms>0 AND c.end_time_ms<?),0)"
                            if allow_historical_closed else "")
        params = (loop_id,row['created_at_ms']) if allow_historical_closed else ()
        if (db.execute("SELECT 1 FROM prediction_campaigns c WHERE pending_unknown=1 OR "
            "(buy_count>0 AND NOT EXISTS(SELECT 1 FROM prediction_settlements s WHERE s.campaign_id=c.campaign_id AND s.status='SETTLED')"
            +closed_exception+")",params).fetchone()):
            raise RuntimeError('Unsettled campaign')
        protected = {r[0]:r[1] for r in db.execute("SELECT config_key,config_value_json FROM prediction_runtime_config WHERE "
            "config_key IN ('prediction_hard_stop_latched','prediction_risk_state','prediction_selected_strategy','prediction_selected_order_unit','prediction_pending_strategy') "
            "OR config_key LIKE 'regime_target6%risk%'")}
        ledger = {}
        for table in ('prediction_loops','prediction_fills','prediction_order_intents','prediction_orders','prediction_risk_ledger','prediction_settlements',
                      'prediction_regime_entry_claims','prediction_regime_settlement_observations'):
            rows = [dict(r) for r in db.execute('SELECT * FROM '+table)]
            canonical = json.dumps(sorted(rows,key=lambda r:json.dumps(r,sort_keys=True)),sort_keys=True,separators=(',',':'))
            ledger[table] = hashlib.sha256(canonical.encode()).hexdigest()
        return dict(loop=dict(row),protected=protected,ledger=ledger)


def service(*args):
    env = dict(os.environ,XDG_RUNTIME_DIR=f'/run/user/{os.getuid()}',
               DBUS_SESSION_BUS_ADDRESS=f'unix:path=/run/user/{os.getuid()}/bus')
    # systemd's configured stop timeout is 90s; allow its normal teardown
    # to finish before treating a stop job as failed. No process injection.
    timeout = 120 if args and args[0]=='stop' else 30
    return subprocess.check_output(['systemctl','--user',*args],env=env,text=True,timeout=timeout).strip()


def official_clear():
    sys.path.insert(0,str(ROOT))
    from predict_main import load_prediction_environment, read_binance_credentials
    from src.gridbot.prediction.settings import PredictionSettings
    from src.gridbot.prediction.client import BinancePredictionClient, DEFAULT_BASE_URL
    from src.gridbot.prediction.worker import PredictionWorker
    load_prediction_environment(ROOT/'prediction/live.env')
    settings = PredictionSettings.from_env(os.environ)
    if not (settings.wallet_address):
        raise RuntimeError('Wallet not configured')
    key, secret = read_binance_credentials()
    client = BinancePredictionClient(key,secret,base_url=os.environ.get('PREDICTION_BASE_URL') or DEFAULT_BASE_URL,
        recv_window=settings.recv_window,order_unit_usdt=settings.order_unit_usdt)
    orders = client.query_active_orders(wallet_address=settings.wallet_address)
    positions = client.query_positions(wallet_address=settings.wallet_address)
    if not (isinstance(orders,(dict,list)) and isinstance(positions,(dict,list))):
        raise RuntimeError('Official response unknown')
    if (PredictionWorker._official_order_rows(orders)):
        raise RuntimeError('Official active orders remain')
    if (any(PredictionWorker._official_position_shares(p)>0 for p in PredictionWorker._official_position_rows(positions))):
        raise RuntimeError('Official positions remain')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--loop-id',default='loop:1790839356188')
    parser.add_argument('--apply',action='store_true',help='Operator explicitly installs code/reloads services; no Live activation')
    parser.add_argument('--allow-cancelled-loop',action='store_true',help='Explicit user-authorized cancellation boundary; all exposure checks still required')
    parser.add_argument('--allow-historical-closed-ledger',action='store_true',help='Allow only DONE campaigns ended before target creation; official zero exposure remains mandatory')
    args = parser.parse_args()
    if not (os.getuid()!=0):
        raise RuntimeError('Run as application user jack_shih')
    candidate = json.loads(safe_path(STAGE, 'candidate.json').read_text())
    validation = json.loads(safe_path(STAGE, 'validation.json').read_text())
    if not (validation['status']=='STAGED_VERIFIED_NOT_DEPLOYED'):
        raise RuntimeError('Installer safety check failed')
    old = json.loads(safe_path(ROOT, MANIFEST).read_text())
    new = json.loads(safe_path(STAGE, MANIFEST).read_text())
    if not (old['release_fingerprint']==candidate['parent']==validation['parent']):
        raise RuntimeError('Parent changed; inspect before installing')
    if not (new['release_fingerprint']==validation['fingerprint']):
        raise RuntimeError('Installer safety check failed')
    old_pin = safe_path(ROOT, PIN).read_text()
    new_pin = safe_path(STAGE, PIN).read_text()
    old_bytes = verify_release(ROOT, old, pin_text=old_pin)
    new_bytes = verify_release(STAGE, new, pin_text=new_pin)
    validate_candidate(ROOT, STAGE, candidate, old_bytes, new_bytes)
    source_modes = {row['path']: safe_path(STAGE, row['path']).stat().st_mode & 0o777
                    for row in candidate['files']}
    guard_path = ROOT/'prediction/hs-recovery-startup.env'
    guard = guard_path.read_bytes()
    if not (b'PREDICTION_LIVE_ARM_ON_START=false' in guard and b'PREDICTION_AUTO_START_LOOP=false' in guard):
        raise RuntimeError('Installer safety check failed')
    before = snapshot(args.loop_id,allow_cancelled=args.allow_cancelled_loop,allow_historical_closed=args.allow_historical_closed_ledger)
    official_clear()
    if not (snapshot(args.loop_id,allow_cancelled=args.allow_cancelled_loop,allow_historical_closed=args.allow_historical_closed_ledger)==before):
        raise RuntimeError('Trading records changed during preflight')
    if not (all(service('is-active',s)=='active' for s in SERVICES)):
        raise RuntimeError('Installer safety check failed')
    if not args.apply:
        print(json.dumps(dict(status='READ_ONLY_PREFLIGHT_PASSED',candidate=new['release_fingerprint'],live_activated=False)))
        return
    verify_release(ROOT, old, pin_text=old_pin)
    backup = ROOT/'prediction'/('t67b-rollback-'+str(time.time_ns()//1000000))
    backup.mkdir(mode=0o700)
    for path in [r['path'] for r in candidate['files'] if r['before'] is not None]+[MANIFEST,PIN]:
        dest=backup/path
        dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(safe_path(ROOT, path),dest)
    (backup/'before.json').write_text(json.dumps(dict(snapshot=before,candidate=candidate),indent=2)+'\n')
    try:
        for name in SERVICES:service('stop',name)
        if not (snapshot(args.loop_id,allow_cancelled=args.allow_cancelled_loop,allow_historical_closed=args.allow_historical_closed_ledger)==before):
            raise RuntimeError('Installer safety check failed')
        # Recheck official exposure immediately before installing. The APIs
        # here are read-only; this helper never creates/cancels/redeems orders.
        official_clear()
        for row in candidate['files']:
            dest=safe_path(ROOT, row['path'])
            temp=dest.with_name(dest.name+'.t67b-new')
            dest.parent.mkdir(parents=True,exist_ok=True)
            # Use the immutable bytes checked before stopping services.
            with temp.open('xb') as output:
                output.write(new_bytes[row['path']])
            temp.chmod(source_modes[row['path']])
            os.replace(temp,dest)
        verify_release(ROOT, new, pin_text=new_pin)
        safe_path(ROOT, MANIFEST).write_text(json.dumps(new, indent=2)+'\n')
        safe_path(ROOT, PIN).write_text(new_pin)
        if not (guard_path.read_bytes()==guard and snapshot(args.loop_id,allow_cancelled=args.allow_cancelled_loop,allow_historical_closed=args.allow_historical_closed_ledger)==before):
            raise RuntimeError('Installer safety check failed')
        for name in reversed(SERVICES):service('start',name)
        if not (all(service('is-active',s)=='active' for s in SERVICES)):
            raise RuntimeError('Installer safety check failed')
        # Check the deployed report in a fresh interpreter, not this helper's
        # preflight import cache. Store the result without sending Telegram.
        code="from pathlib import Path;from src.gridbot.prediction.live_report import format_live_report,T67B_PROFILE;print(format_live_report(Path('/home/jack_shih/cry3'),profile_filter=T67B_PROFILE))"
        report=subprocess.run([str(ROOT/'testnet/.venv/bin/python'),'-c',code],cwd=ROOT,capture_output=True,text=True,timeout=30)
        if not (report.returncode==0):
            raise RuntimeError('Fresh deployed report failed')
        (backup/'report.txt').write_text(report.stdout)
        if not (snapshot(args.loop_id,allow_cancelled=args.allow_cancelled_loop,allow_historical_closed=args.allow_historical_closed_ledger)==before and guard_path.read_bytes()==guard):
            raise RuntimeError('Installer safety check failed')
        print(json.dumps(dict(status='CODE_INSTALLED_LIVE_NOT_ACTIVATED',fingerprint=new['release_fingerprint'],
                              backup=str(backup),next_step='Operator selects T6.7b/amount and confirms Live in Telegram')))
    except BaseException:
        for name in SERVICES:service('stop',name)
        for row in candidate['files']:
            dest=safe_path(ROOT, row['path'])
            if row['before'] is None:dest.unlink(missing_ok=True)
            else:shutil.copy2(backup/row['path'],dest)
        for path in (MANIFEST,PIN):shutil.copy2(backup/path,ROOT/path)
        verify_release(ROOT, old, pin_text=old_pin)
        for name in reversed(SERVICES):service('start',name)
        print('Source/manifest/pin rolled back; trading DB retained; Live not activated.',file=sys.stderr)
        raise


if __name__=='__main__':
    main()
