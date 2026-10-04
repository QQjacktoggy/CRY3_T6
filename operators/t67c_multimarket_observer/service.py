"""Read-only source consumer. All writes stay in the independent observer DB.

No credentials, trading client, order repository, claims, arm or HS actions.
Public K-lines are fetched only inside their original causal window when missing.
The existing per-symbol producers own public feeds and authorized Original calls.
"""
import argparse
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import signal
import sqlite3
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from src.gridbot.prediction.loop_market import SYMBOLS,data_paths,verify_data_db
from src.gridbot.prediction.c180_signal_runtime import read_c180_book,read_c180_signal,_signal_json
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.regime_t67_evidence import read_inputs
from .engine import VERSION,FINGERPRINT,new_state,evaluate,observe_shadow,settle,SLOT
from .report import snapshot
from src.gridbot.prediction.regime_lane import freeze_features
from src.gridbot.prediction.http_bounds import KLINES_BODY_BYTES,read_bounded


def now():return time.time_ns()//1000000


def read_row(path,sql,args):
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=.05)) as db:
        db.execute('PRAGMA query_only=ON')
        r=db.execute(sql,args).fetchone()
        return json.loads(r[0]) if r else None


def fetch_features(symbol,start,clock=now):
    """Public-only fallback, bounded body/time, original closed-candle contract."""
    if symbol not in SYMBOLS or not start+120000<=clock()<start+122000:
        raise ValueError('feature_dispatch_deadline')
    params=urllib.parse.urlencode(dict(symbol=symbol,interval='1m',startTime=start-900000,endTime=start+119999,limit=17))
    with urllib.request.urlopen('https://api.binance.com/api/v3/klines?'+params,timeout=1.2) as response:
        raw=json.loads(read_bounded(response,response.headers,KLINES_BODY_BYTES))
    received=clock()
    return freeze_features(start,raw,received,symbol=symbol),raw


class Collector:
    def __init__(self,root,directory):
        self.root=Path(root).resolve();self.directory=Path(directory).resolve()
        if self.directory.name!='t67c-multimarket-observer':raise ValueError('observer_output_path')
        self.directory.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(self.directory/'observer.sqlite3',timeout=1)
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY,value TEXT);'
            'CREATE TABLE IF NOT EXISTS windows(symbol TEXT,start INTEGER,payload TEXT,PRIMARY KEY(symbol,start));')
        for key,value in (('version',VERSION),('policy',FINGERPRINT)):
            row=self.db.execute('SELECT value FROM config WHERE key=?',(key,)).fetchone()
            if row and row[0]!=value:raise ValueError('observer_policy_changed')
            self.db.execute('INSERT OR IGNORE INTO config VALUES(?,?)',(key,value))
        # Restart preserves epoch, preventing retrospective creation of simulations.
        self.db.execute("INSERT OR IGNORE INTO config VALUES('epoch',?)",(str(now()//SLOT*SLOT),));self.db.commit()
        self.epoch=int(self.db.execute("SELECT value FROM config WHERE key='epoch'").fetchone()[0])
        self.first_db=self.root/'prediction/data/first-multimarket-v1/first-observer.sqlite3'
        self.paths={s:data_paths(self.root/'prediction/data/prediction.sqlite3',s) for s in SYMBOLS}
        self.bridges={};self.states={};self.last_shadow=0;self.errors={};self.last_report=0;self.last_start=None
        self.feature_pool=ThreadPoolExecutor(max_workers=3,thread_name_prefix='observer-public-klines');self.feature_jobs={};self.feature_cache={}
        for s,(feature,signal_path) in self.paths.items():
            verify_data_db(feature,s);verify_data_db(signal_path,s)
            # Avoid __init__, which creates the unused Live ledger adapter.
            bridge=RegimeWorkerBridge.__new__(RegimeWorkerBridge)
            bridge.symbol=s;bridge.signal_db=signal_path;self.bridges[s]=bridge

    def error(self,phase,exc):
        key=phase+':'+type(exc).__name__;self.errors[key]=self.errors.get(key,0)+1

    def official(self,symbol,start):
        return read_row(self.first_db,'SELECT payload FROM windows WHERE symbol=? AND start=?',(symbol,start))

    def market(self,state,at):
        if not state.get('meta'):
            row=self.official(state['symbol'],state['start'])
            if not row or not row.get('meta') or row.get('identified_at_ms',at+1)>at:return None
            meta=row['meta']
            if meta['symbol']!=state['symbol'] or meta['start']!=state['start'] or meta['end']!=state['end']:return None
            state['meta']=meta
        m=state['meta']
        return SimpleNamespace(start_time_ms=m['start'],market_topic_id=m['topic'],up_market_id=m['market_id'])

    def tick(self,at):
        start=at//SLOT*SLOT;offset=at-start
        if self.last_start!=start:
            prior=self.db.execute('SELECT MAX(start) FROM windows').fetchone()[0]
            begin=max(self.epoch,prior+SLOT if prior is not None else self.epoch)
            if start-begin>30*86400000:raise RuntimeError('observer_offline_over_30_days')
            for t in range(begin,start+1,SLOT):
                for s in SYMBOLS:
                    self.db.execute('INSERT OR IGNORE INTO windows VALUES(?,?,?)',(s,t,json.dumps(new_state(s,t))))
            self.db.commit()
            for s in SYMBOLS:
                r=self.db.execute('SELECT payload FROM windows WHERE symbol=? AND start=?',(s,start)).fetchone()
                self.states[s]=json.loads(r[0]);self.states[s]['observed_since_ms']=at
            self.last_start=start
            self.feature_jobs={k:v for k,v in self.feature_jobs.items() if k[1]>=start};self.feature_cache={k:v for k,v in self.feature_cache.items() if k[1]>=start}
        shadows=at-self.last_shadow>=900 and not 117000<=offset<=140000
        for s,state in self.states.items():
            try:
                if not 60000<=offset<270000:continue
                market=self.market(state,at)
                if not market:
                    state['reason']='market_metadata_missing';continue
                feature_path,signal_path=self.paths[s]
                if 120000<=offset<136000 and not state.get('quote'):
                    features=read_row(feature_path,'SELECT payload FROM features WHERE start=?',(start,))
                    key=(s,start)
                    if not features and key not in self.feature_jobs and 121000<=offset<122000:
                        self.feature_jobs[key]=self.feature_pool.submit(fetch_features,s,start)
                    job=self.feature_jobs.get(key)
                    if job and job.done() and key not in self.feature_cache:
                        try:
                            fallback,raw=job.result();self.feature_cache[key]=fallback
                            state['fallback_feature_evidence']=dict(features=fallback,candles=raw)
                        except Exception as exc:
                            self.feature_cache[key]=None;state['feature_fallback_error']=type(exc).__name__
                    if not features and self.feature_cache.get(key):
                        features=self.feature_cache[key];state['feature_source']='observer_public_klines'
                    elif features:state['feature_source']='existing_feature_producer'
                    state['features_present']=bool(features and features.get('symbol','BTCUSDT')==s and start+120000<=int(features.get('received_at_ms',0))<=min(now(),start+123000))
                    original=read_c180_signal(signal_path,start)
                    state['original_present']=bool(original and original.original_p_up is not None and original.market_topic==market.market_topic_id and original.market_id==market.up_market_id and original.cutoff_ms==start+120000 and start+120000<=original.completed_at_ms<=min(now(),start+123000))
                    if original and state['original_present']:state['original_evidence']=json.loads(_signal_json(original))
                    if offset>=124000:
                        book=read_c180_book(signal_path,start);clock=now()
                        if book and str(book.get('fee_bps'))!=str(state['meta']['fee_bps']):
                            state['reason']='metadata_fee_mismatch';continue
                        evaluate(state,self.bridges[s],market,features,book,clock)
                        finished=now()
                        if state.get('selected') and finished>=state['selected']['expires_at_ms']:
                            state['quote']=None;state['reason']='processing_exceeded_deadline'
                if shadows:
                    books,spots=read_inputs(signal_path,start,at);observe_shadow(state,market,books,spots,at)
            except (OSError,sqlite3.Error,ValueError,KeyError,TypeError,ArithmeticError) as exc:
                self.error(s,exc);state['reason']='source_unavailable:'+type(exc).__name__
        if shadows:self.last_shadow=at
        # No fsync/report scan inside the shared capture/decision windows.
        if not 117000<=offset<=140000 and at-self.last_report>=15000:
            self.flush()
            self.resolve(at)
            self.publish(now());self.last_report=at

    def flush(self):
        with self.db:
            for s,r in self.states.items():self.db.execute('INSERT OR REPLACE INTO windows VALUES(?,?,?)',(s,r['start'],json.dumps(r,sort_keys=True)))

    def resolve(self,at):
        rows=self.db.execute("SELECT symbol,start,payload FROM windows WHERE start+300000<=? AND json_extract(payload,'$.winner') IS NULL ORDER BY start DESC LIMIT 300",(at,)).fetchall()
        with self.db:
            for s,start,raw in rows:
                state=json.loads(raw)
                if not state.get('meta'):continue
                official=self.official(s,start)
                if official:
                    settle(state,official,at)
                    self.db.execute('UPDATE windows SET payload=? WHERE symbol=? AND start=?',(json.dumps(state,sort_keys=True),s,start))

    def publish(self,at):
        # Bounded rolling history instead of loading the full trading ledger.
        rows=[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM windows WHERE start>=? ORDER BY start,symbol',(at//SLOT*SLOT-100*SLOT,))]
        health=dict(at_ms=at,errors=self.errors,readonly_sources=True,auto_selector=False,paid_original='authorized_all_three_producers')
        result=snapshot(rows,at,self.epoch,health)
        temp=self.directory/'latest.json.tmp';temp.write_text(json.dumps(result,ensure_ascii=False));temp.replace(self.directory/'latest.json')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True);parser.add_argument('--data',type=Path,required=True)
    args=parser.parse_args();args.data.mkdir(parents=True,exist_ok=True)
    with (args.data/'observer.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        collector=Collector(args.root,args.data);stopping=False
        def stop(*_):
            nonlocal stopping
            stopping=True
        signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
        try:
            while not stopping:
                at=now();collector.tick(at)
                time.sleep(.1 if 119000<=at%SLOT<=136000 else 1)
        finally:
            collector.feature_pool.shutdown(wait=True,cancel_futures=True)
            collector.flush();collector.publish(now());collector.db.close()

if __name__=='__main__':main()
