"""Operator recovery using fresh signed history; defaults to a database copy.

Use --apply only after validating the printed copy result. Overlay imports run
in this separate process and never inject code into the running Live worker.
"""
import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import sys
import time


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--archive',type=Path,required=True)
    p.add_argument('--order-id',required=True)
    p.add_argument('--expected-release',required=True)
    p.add_argument('--overlay',type=Path)
    p.add_argument('--expected-overlay',help='Independent SHA256 of overlay hashes JSON')
    p.add_argument('--apply',action='store_true')
    a=p.parse_args();root=a.root.resolve();sys.path.insert(0,str(root))
    release=runpy.run_path(str(root/'src/gridbot/prediction/release.py'))
    manifest=json.loads((root/'prediction/release-manifest.json').read_text())
    if manifest['release_fingerprint']!=a.expected_release or release['verify_release_manifest'](root,manifest,pin_path=root/'prediction/release-pin.env'):
        raise ValueError('deployed release differs')
    if a.overlay:
        hashes_file=a.overlay/'hashes.json'
        if hashlib.sha256(hashes_file.read_bytes()).hexdigest()!=a.expected_overlay:
            raise ValueError('overlay identity differs')
        hashes=json.loads(hashes_file.read_text())
        for relative,digest in hashes.items():
            if hashlib.sha256((a.overlay/relative).read_bytes()).hexdigest()!=digest:
                raise ValueError('overlay source differs')
        import src.gridbot.prediction
        for relative,name in [('src/gridbot/prediction/repository.py','src.gridbot.prediction.repository'),('src/gridbot/prediction/late_fill_repair.py','src.gridbot.prediction.late_fill_repair')]:
            spec=importlib.util.spec_from_file_location(name,a.overlay/relative)
            module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module)
    from predict_main import load_prediction_environment,read_binance_credentials
    from src.gridbot.prediction.settings import PredictionSettings
    from src.gridbot.prediction.client import BinancePredictionClient,DEFAULT_BASE_URL
    from src.gridbot.prediction.repository import PredictionRepository
    load_prediction_environment(root/'prediction/live.env');s=PredictionSettings.from_env(os.environ)
    key,secret=read_binance_credentials()
    client=BinancePredictionClient(key,secret,base_url=os.environ.get('PREDICTION_BASE_URL') or DEFAULT_BASE_URL,recv_window=s.recv_window,timeout=10)
    order=None
    for offset in range(0,150,30):
        rows=client.query_order_history(wallet_address=s.wallet_address,limit=30,offset=offset)['orders']
        matches=[r for r in rows if str(r['orderId'])==a.order_id]
        if matches:order=matches[0];break
        if len(rows)<30:break
    if order is None:raise ValueError('exact official order missing')
    positions=client.query_settled_position_history(wallet_address=s.wallet_address,limit=30)['positions']
    matches=[r for r in positions if str(r['marketId'])==str(order['marketId']) and str(r['marketTopicId'])==str(order['marketTopicId']) and str(r['outcomeName']).upper()==str(order['outcome']).upper()]
    if len(matches)!=1:raise ValueError('exact settled position ambiguous or missing')
    position=matches[0]
    order_keys=('orderId','marketId','marketTopicId','side','outcome','status','price','filledShareQty','filledUsdtAmount','marketProviderFee','networkFee','createTime','modifyTime','terminalTime')
    position_keys=('marketId','marketTopicId','tokenId','outcomeName','shares','totalCost','realizedPnl','positionStatus','isWinner','canClaim','finalOutcome','settledDate','endDate','claimAmount')
    evidence={'source':'authenticated_official_history','checked_at_ms':int(time.time()*1000),'order':{k:order[k] for k in order_keys if k in order},'position':{k:position[k] for k in position_keys if k in position}}
    a.archive.mkdir(mode=0o700,parents=True,exist_ok=True)
    stamp=str(evidence['checked_at_ms']);folder=a.archive/stamp;folder.mkdir(mode=0o700)
    proof=folder/'evidence.json';proof.write_text(json.dumps(evidence,sort_keys=True));proof.chmod(0o600)
    source=root/'prediction/data/prediction.sqlite3';backup=folder/'accounting-before.sqlite3'
    if shutil.disk_usage(folder).free < 256*1024*1024:
        raise ValueError('not enough archive space for accounting backup')
    # Back up every row in the accounting tables, not gigabytes of unrelated
    # telemetry. This is a scoped accounting snapshot, not a full DB restore.
    accounting = {'prediction_loops','prediction_campaigns','prediction_orders',
        'prediction_order_intents','prediction_fills','prediction_settlements',
        'prediction_risk_ledger','prediction_runtime_config','prediction_regime_slots',
        'prediction_regime_entry_claims','prediction_regime_settlement_observations'}
    with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as src,sqlite3.connect(backup) as dst:
        src.execute('PRAGMA query_only=ON');src.execute('BEGIN')
        schema=src.execute("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'").fetchall()
        for kind,name,sql in schema:
            if kind=='table':dst.execute(sql)
        for name in sorted(accounting):
            cursor=src.execute('SELECT * FROM '+name)
            placeholders=','.join('?' for _ in cursor.description)
            while rows:=cursor.fetchmany(500):dst.executemany('INSERT INTO '+name+' VALUES('+placeholders+')',rows)
        for kind,name,sql in schema:
            if kind in ('index','trigger','view'):dst.execute(sql)
        dst.commit()
        assert dst.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        assert not dst.execute('PRAGMA foreign_key_check').fetchall()
    backup.chmod(0o600)
    target=source if a.apply else folder/'dry-run.sqlite3'
    if not a.apply:shutil.copy2(backup,target)
    async def repair():
        repo=PredictionRepository(target)
        # Open the existing schema without migrations or startup repairs.
        import aiosqlite
        repo._conn=await aiosqlite.connect(str(target),timeout=10,isolation_level=None)
        repo._conn.row_factory=aiosqlite.Row
        await repo._conn.execute('PRAGMA foreign_keys=ON')
        try:
            result=await repo.repair_cancelled_fill(evidence)
            repeated=await repo.repair_cancelled_fill(evidence)
            assert repeated['status']=='ALREADY_REPAIRED'
            row=await repo._fetchone('SELECT loop_id FROM prediction_campaigns WHERE campaign_id=?',(result['campaign_id'],))
            result['loop']=await repo.get_loop(row['loop_id'])
            result['mode']='APPLIED' if a.apply else 'COPY_VALIDATED'
            result['backup']=str(backup)
            (folder/'result.json').write_text(json.dumps(result,sort_keys=True))
            print(json.dumps(result))
        finally:await repo.close()
    asyncio.run(repair())


if __name__=='__main__':
    try:main()
    except Exception as ex:
        print('Recovery failed:',type(ex).__name__,file=sys.stderr)
        raise SystemExit(1)
