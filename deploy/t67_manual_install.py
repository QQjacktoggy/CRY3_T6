"""Operator-run T6.7 installer. Default is read-only; never arms or starts a loop."""
import argparse
import hashlib
import json
import os
import runpy
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

ROOT = Path('/home/jack_shih/cry3')
STAGE = ROOT/'prediction/t67-live-staged-v2-20261001'
MANIFEST = 'prediction/release-manifest.json'
PIN = 'prediction/release-pin.env'
SERVICES = ('cry3-predict-user.service','cry3-regime-feature.service','cry3-c180-favorite-signal.service')
TERMINAL = "('FILLED','CLOSED','CANCELLED','CANCELED','EXPIRED','FAILED','REJECTED')"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def snapshot(loop_id):
    with closing(sqlite3.connect((ROOT/'prediction/data/prediction.sqlite3').as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM prediction_loops WHERE loop_id=?',(loop_id,)).fetchone()
        assert row and row['state']=='DONE' and row['completed']==row['target']==100, 'Target loop must be DONE 100/100'
        assert not db.execute("SELECT 1 FROM prediction_loops WHERE state='RUNNING'").fetchone(), 'Another loop is RUNNING'
        assert not db.execute('SELECT 1 FROM prediction_order_intents WHERE unknown=1 OR COALESCE(status,\'\') NOT IN '+TERMINAL).fetchone(), 'Nonterminal/UNKNOWN intent'
        assert not db.execute('SELECT 1 FROM prediction_orders WHERE COALESCE(status,\'\') NOT IN '+TERMINAL).fetchone(), 'Nonterminal order'
        assert not db.execute("SELECT 1 FROM prediction_campaigns c WHERE pending_unknown=1 OR "
            "(buy_count>0 AND NOT EXISTS(SELECT 1 FROM prediction_settlements s WHERE s.campaign_id=c.campaign_id AND s.status='SETTLED'))").fetchone(), 'Unsettled campaign'
        protected = {r[0]:r[1] for r in db.execute("SELECT config_key,config_value_json FROM prediction_runtime_config WHERE "
            "config_key IN ('prediction_hard_stop_latched','prediction_selected_strategy','prediction_selected_order_unit','prediction_pending_strategy') "
            "OR config_key LIKE 'regime_target6%risk%'")}
        ledger = {}
        for table in ('prediction_loops','prediction_fills','prediction_order_intents','prediction_settlements',
                      'prediction_regime_entry_claims','prediction_regime_settlement_observations'):
            rows = [dict(r) for r in db.execute('SELECT * FROM '+table)]
            canonical = json.dumps(sorted(rows,key=lambda r:json.dumps(r,sort_keys=True)),sort_keys=True,separators=(',',':'))
            ledger[table] = hashlib.sha256(canonical.encode()).hexdigest()
        return dict(loop=dict(row),protected=protected,ledger=ledger)


def service(*args):
    env = dict(os.environ,XDG_RUNTIME_DIR=f'/run/user/{os.getuid()}',
               DBUS_SESSION_BUS_ADDRESS=f'unix:path=/run/user/{os.getuid()}/bus')
    return subprocess.check_output(['systemctl','--user',*args],env=env,text=True,timeout=30).strip()


def official_clear():
    sys.path.insert(0,str(ROOT))
    from predict_main import load_prediction_environment, read_binance_credentials
    from src.gridbot.prediction.settings import PredictionSettings
    from src.gridbot.prediction.client import BinancePredictionClient, DEFAULT_BASE_URL
    from src.gridbot.prediction.worker import PredictionWorker
    load_prediction_environment(ROOT/'prediction/live.env')
    settings = PredictionSettings.from_env(os.environ)
    assert settings.wallet_address, 'Wallet not configured'
    key, secret = read_binance_credentials()
    client = BinancePredictionClient(key,secret,base_url=os.environ.get('PREDICTION_BASE_URL') or DEFAULT_BASE_URL,
        recv_window=settings.recv_window,order_unit_usdt=settings.order_unit_usdt)
    orders = client.query_active_orders(wallet_address=settings.wallet_address)
    positions = client.query_positions(wallet_address=settings.wallet_address)
    assert isinstance(orders,(dict,list)) and isinstance(positions,(dict,list)), 'Official response unknown'
    assert not PredictionWorker._official_order_rows(orders), 'Official active orders remain'
    assert not any(PredictionWorker._official_position_shares(p)>0 for p in PredictionWorker._official_position_rows(positions)), 'Official positions remain'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--loop-id',default='loop:1790817223795')
    parser.add_argument('--apply',action='store_true',help='Operator explicitly installs code/reloads services; no Live activation')
    args = parser.parse_args()
    assert os.getuid()!=0, 'Run as application user jack_shih'
    candidate = json.loads((STAGE/'candidate.json').read_text())
    validation = json.loads((STAGE/'validation.json').read_text())
    assert validation['status']=='STAGED_VERIFIED_NOT_DEPLOYED'
    old = json.loads((ROOT/MANIFEST).read_text())
    new = json.loads((STAGE/MANIFEST).read_text())
    assert old['release_fingerprint']==candidate['parent']==validation['parent'], 'Parent changed; inspect before installing'
    assert new['release_fingerprint']==validation['fingerprint']
    parent = runpy.run_path(str(ROOT/'src/gridbot/prediction/release.py'))
    release = runpy.run_path(str(STAGE/'src/gridbot/prediction/release.py'))
    assert not parent['verify_release_manifest'](ROOT,old,pin_path=ROOT/PIN)
    assert not release['verify_release_manifest'](STAGE,new,pin_path=STAGE/PIN)
    for row in candidate['files']:
        assert digest(ROOT/row['path'])==row['before'], 'Deployed source changed: '+row['path']
        assert digest(STAGE/row['path'])==row['after'], 'Candidate source changed: '+row['path']
    guard_path = ROOT/'prediction/hs-recovery-startup.env'
    guard = guard_path.read_bytes()
    assert b'PREDICTION_LIVE_ARM_ON_START=false' in guard and b'PREDICTION_AUTO_START_LOOP=false' in guard
    before = snapshot(args.loop_id)
    official_clear()
    assert snapshot(args.loop_id)==before, 'Trading records changed during preflight'
    assert all(service('is-active',s)=='active' for s in SERVICES)
    if not args.apply:
        print(json.dumps(dict(status='READ_ONLY_PREFLIGHT_PASSED',candidate=new['release_fingerprint'],live_activated=False)))
        return
    backup = ROOT/'prediction'/('t67-rollback-'+str(time.time_ns()//1000000))
    backup.mkdir(mode=0o700)
    for path in [r['path'] for r in candidate['files'] if r['before'] is not None]+[MANIFEST,PIN]:
        dest=backup/path
        dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(ROOT/path,dest)
    (backup/'before.json').write_text(json.dumps(dict(snapshot=before,candidate=candidate),indent=2)+'\n')
    try:
        for name in SERVICES:service('stop',name)
        assert snapshot(args.loop_id)==before
        # Recheck official exposure immediately before installing. The APIs
        # here are read-only; this helper never creates/cancels/redeems orders.
        official_clear()
        for row in candidate['files']:
            dest=ROOT/row['path']
            temp=dest.with_name(dest.name+'.t67-new')
            shutil.copy2(STAGE/row['path'],temp)
            os.replace(temp,dest)
        deployed=runpy.run_path(str(ROOT/'src/gridbot/prediction/release.py'))
        assert deployed['build_release_manifest'](ROOT)==new
        for path in (MANIFEST,PIN):shutil.copy2(STAGE/path,ROOT/path)
        assert not deployed['verify_release_manifest'](ROOT,new,pin_path=ROOT/PIN)
        assert guard_path.read_bytes()==guard and snapshot(args.loop_id)==before
        for name in reversed(SERVICES):service('start',name)
        assert all(service('is-active',s)=='active' for s in SERVICES)
        # Check the deployed report in a fresh interpreter, not this helper's
        # preflight import cache. Store the result without sending Telegram.
        code="from pathlib import Path;from src.gridbot.prediction.live_report import format_live_report;print(format_live_report(Path('/home/jack_shih/cry3')))"
        report=subprocess.run([str(ROOT/'testnet/.venv/bin/python'),'-c',code],cwd=ROOT,capture_output=True,text=True,timeout=30)
        assert report.returncode==0, 'Fresh deployed report failed'
        (backup/'report.txt').write_text(report.stdout)
        assert snapshot(args.loop_id)==before and guard_path.read_bytes()==guard
        print(json.dumps(dict(status='CODE_INSTALLED_LIVE_NOT_ACTIVATED',fingerprint=new['release_fingerprint'],
                              backup=str(backup),next_step='Operator selects T6.7/amount and confirms Live in Telegram')))
    except BaseException:
        for name in SERVICES:service('stop',name)
        for row in candidate['files']:
            dest=ROOT/row['path']
            if row['before'] is None:dest.unlink(missing_ok=True)
            else:shutil.copy2(backup/row['path'],dest)
        for path in (MANIFEST,PIN):shutil.copy2(backup/path,ROOT/path)
        assert not parent['verify_release_manifest'](ROOT,old,pin_path=ROOT/PIN)
        for name in reversed(SERVICES):service('start',name)
        print('Source/manifest/pin rolled back; trading DB retained; Live not activated.',file=sys.stderr)
        raise


if __name__=='__main__':
    main()
