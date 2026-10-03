"""Independent signed GET-only observer. Never imports the Live application."""
import argparse
import asyncio
from collections import deque
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import signal
import shutil
import sqlite3
import time
from urllib.parse import urlencode
import zlib

from policy import SYMBOLS, SLOT, POLICY, FINGERPRINT, metadata, features, book, initial_quote, recheck, resolution, pnl

PREFIX='/sapi/v1/w3w/wallet/prediction'
ALLOWED={PREFIX+'/market/list',PREFIX+'/market/detail',PREFIX+'/order-book','/api/v3/klines','/api/v3/time'}

def now():return time.time_ns()//1000000

def dumps(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)

class ReadError(Exception):pass

class Reader:
    def __init__(self,session,key,secret):
        self.session=session;self.key=key;self.secret=secret;self.calls=deque();self.cooldown=0;self.offset=0
        self.semaphore=asyncio.Semaphore(6)
    async def get(self,path,params=None):
        if path not in ALLOWED:raise PermissionError('observer_endpoint_denied')
        at=now()
        while self.calls and self.calls[0]<at-60000:self.calls.popleft()
        if at<self.cooldown or len(self.calls)>=40:raise ReadError('observer_budget_or_cooldown')
        self.calls.append(at)
        params=dict(params or {});headers={}
        if path.startswith(PREFIX):
            params.update(timestamp=at+self.offset,recvWindow=5000)
            query=urlencode(params);signature=hmac.new(self.secret.encode(),query.encode(),hashlib.sha256).hexdigest()
            query+='&signature='+signature;headers['X-MBX-APIKEY']=self.key
        else:query=urlencode(params)
        async with self.semaphore:
            # Redirects cannot carry credentials to another endpoint or host.
            async with self.session.get('https://api.binance.com'+path+'?'+query,headers=headers,allow_redirects=False) as response:
                if response.status in (418,429):
                    self.cooldown=now()+max(900000,int(response.headers.get('Retry-After','900'))*1000)
                    raise ReadError('server_cooldown')
                if response.status!=200:raise ReadError('http_'+str(response.status))
                chunks=[];size=0
                async for chunk in response.content.iter_chunked(16384):
                    size+=len(chunk)
                    if size>1048576:raise ReadError('response_size')
                    chunks.append(chunk)
                body=b''.join(chunks)
                result=json.loads(body)
                if isinstance(result,dict) and result.get('success') is False:raise ReadError('api_rejected')
                if isinstance(result,dict) and result.get('code') not in (None,0,'0'):raise ReadError('api_rejected')
                return result

def connect(directory):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    # Only this explicitly named observer DB can be opened writable.
    db=sqlite3.connect(directory/'first-observer.sqlite3',timeout=1)
    db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=FULL');db.row_factory=sqlite3.Row
    db.execute('CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS windows(symbol TEXT,start INTEGER,payload TEXT NOT NULL,PRIMARY KEY(symbol,start))')
    db.execute('CREATE TABLE IF NOT EXISTS evidence(symbol TEXT,start INTEGER,stage TEXT,received INTEGER,body BLOB,PRIMARY KEY(symbol,start,stage))')
    db.execute('CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY,at_ms INTEGER,payload TEXT)')
    old=db.execute("SELECT value FROM config WHERE key='policy'").fetchone()
    if old and old[0]!=FINGERPRINT:raise ValueError('observer_policy_mismatch')
    with db:
        db.execute("INSERT OR IGNORE INTO config VALUES('policy',?)",(FINGERPRINT,))
        db.execute("INSERT OR IGNORE INTO config VALUES('definition',?)",(dumps(POLICY),))
        db.execute("INSERT OR IGNORE INTO config VALUES('epoch',?)",(str(now()//SLOT*SLOT),))
    return db

class Observer:
    def __init__(self,db,reader,directory):
        self.db=db;self.reader=reader;self.directory=Path(directory);self.stop=asyncio.Event()
        self.last_discovery=0;self.last_outcomes=0;self.last_report=0;self.last_start=None
        self.timeline={};self.tasks=set();self.errors={};self.phase_started=set();self.last_housekeeping=0
    def error(self,phase,exc):
        # Exception messages may contain signed URLs: retain only bounded classes/codes.
        key=phase+':'+(str(exc) if isinstance(exc,ReadError) else type(exc).__name__)
        self.errors[key]=self.errors.get(key,0)+1
    def row(self,symbol,start):
        row=self.db.execute('SELECT payload FROM windows WHERE symbol=? AND start=?',(symbol,start)).fetchone()
        return json.loads(row[0]) if row else dict(symbol=symbol,start=start,end=start+SLOT,reason='awaiting',policy=FINGERPRINT)
    def save(self,row):
        with self.db:self.db.execute('INSERT OR REPLACE INTO windows VALUES(?,?,?)',(row['symbol'],row['start'],dumps(row)))
    def evidence(self,symbol,start,stage,raw,received):
        with self.db:self.db.execute('INSERT OR IGNORE INTO evidence VALUES(?,?,?,?,?)',(symbol,start,stage,received,zlib.compress(dumps(raw).encode())))
    def spawn(self,key,coroutine):
        if key in self.phase_started:coroutine.close();return
        self.phase_started.add(key)
        task=asyncio.create_task(coroutine);self.tasks.add(task)
        def done(t):
            self.tasks.discard(t)
            if not t.cancelled() and t.exception():self.error('task',t.exception())
        task.add_done_callback(done)
    async def discover(self,start):
        try:
            raw=await self.reader.get(PREFIX+'/market/list',dict(l1Category='crypto',l2Category='up-down',limit=100))
            data=raw.get('data',raw)
            if isinstance(data,dict):data=data.get('marketTopics',data.get('list',data.get('items',[])))
            for item in data:
                symbol=item.get('symbol')
                if symbol not in SYMBOLS or int(item.get('endDate',0))-int(item.get('startDate',0))!=SLOT:continue
                for t in [item]+item.get('timeline',[]):
                    if int(t.get('endDate',0))-int(t.get('startDate',0))==SLOT:self.timeline[(symbol,int(t['startDate']))]=str(t['marketTopicId'])
            await asyncio.gather(*(self.load_market(s,start) for s in SYMBOLS))
        except Exception as exc:self.error('discovery',exc)
    async def load_market(self,symbol,start):
        row=self.row(symbol,start)
        if row.get('meta'):return
        topic=self.timeline.get((symbol,start))
        if not topic:return
        try:
            raw=await self.reader.get(PREFIX+'/market/detail',dict(marketTopicId=topic));received=now()
            meta=metadata(raw,symbol,start)
            if meta['topic']!=topic:raise ValueError('topic_mismatch')
            self.evidence(symbol,start,'market',raw,received)
            row=self.row(symbol,start);row['meta']=meta;row['identified_at_ms']=received;self.save(row)
        except Exception as exc:self.error(symbol+'_metadata',exc)
    async def freeze(self,symbol,start):
        row=self.row(symbol,start);row['feature_attempted_at_ms']=now();self.save(row)
        try:
            raw=await self.reader.get('/api/v3/klines',dict(symbol=symbol,interval='1m',startTime=start-900000,endTime=start+119999,limit=17))
            received=now();self.evidence(symbol,start,'features',raw,received);f=features(symbol,start,raw,received)
            row=self.row(symbol,start);row['features']=f;row['reason']=f['reason'];self.save(row)
        except Exception as exc:
            self.error(symbol+'_features',exc)
            row=self.row(symbol,start);row['reason']='feature_missing';row['feature_error']=type(exc).__name__;self.save(row)
    async def capture_book(self,symbol,start,side,stage):
        meta=self.row(symbol,start)['meta']
        raw=await self.reader.get(PREFIX+'/order-book',dict(vendor='predict_fun',marketId=meta['market_id'],tokenId=meta['tokens'][side]))
        received=now();self.evidence(symbol,start,stage+'_'+side,raw,received)
        return book(raw,meta,side,received)
    async def initial(self,symbol,start):
        row=self.row(symbol,start)
        if not row.get('meta') or not row.get('features'):
            row['reason']='initial_inputs_missing';self.save(row);return
        quotes=await asyncio.gather(*(self.capture_book(symbol,start,s,'initial') for s in ('UP','DOWN')),return_exceptions=True)
        row=self.row(symbol,start);row['initial_books']={}
        for side,q in zip(('UP','DOWN'),quotes):
            if isinstance(q,Exception):self.error(symbol+'_initial_'+side,q)
            else:row['initial_books'][side]=q
        f=row['features']
        if f['trend_pass']:
            try:
                quote=row['initial_books'][f['side']]
                row['initial_quote']=initial_quote(row['meta'],f,quote,now());row['reason']='initial_quote_eligible'
            except Exception as exc:
                row['reason']=str(exc) if isinstance(exc,ValueError) else 'initial_book_missing'
        self.save(row)
    async def confirm(self,symbol,start):
        row=self.row(symbol,start)
        if not row.get('initial_quote') or row.get('recheck_attempted'):return
        row['recheck_attempted']=now();self.save(row)
        try:
            side=row['features']['side'];quote=await self.capture_book(symbol,start,side,'recheck')
            at=now();row['recheck_book']=quote
            row['sim_quote']=recheck(row['meta'],row['initial_quote'],quote,at)
            row['sim_at_ms']=at;row['reason']='QUOTE_SIMULATED_NOT_FILLED'
        except Exception as exc:
            self.error(symbol+'_recheck',exc);row['reason']='recheck_unavailable_or_rejected'
        self.save(row)
    async def outcomes(self):
        # Round robin oldest check avoids permanent starvation by one unresolved market.
        pending=[]
        for r in self.db.execute('SELECT payload FROM windows WHERE start<=?',(now()-SLOT-15000,)):
            row=json.loads(r[0])
            if row.get('meta') and not row.get('winner') and now()-row.get('last_resolution_check',0)>=60000:pending.append(row)
        pending.sort(key=lambda x:(x.get('last_resolution_check',0),x['start'],x['symbol']))
        async def one(row):
            symbol,start=row['symbol'],row['start']
            try:
                raw=await self.reader.get(PREFIX+'/market/detail',dict(marketTopicId=row['meta']['topic']));at=now()
                winner=resolution(raw,row['meta'],at)
                row=self.row(symbol,start);row['last_resolution_check']=at
                if winner:
                    self.evidence(symbol,start,'resolution',raw,at);row['winner']=winner;row['known_at_ms']=at
                    side=row.get('features',{}).get('side')
                    for key,dest in [('initial_quote','initial_sim_pnl'),('sim_quote','sim_pnl')]:
                        if row.get(key):row[dest]=pnl(row[key],winner,side)
                self.save(row)
            except Exception as exc:
                self.error(symbol+'_resolution',exc)
                row=self.row(symbol,start);row['last_resolution_check']=now();self.save(row)
        await asyncio.gather(*(one(r) for r in pending[:3]))
    def schedule_phases(self,start,at):
        # Dispatch once throughout the admissible window, leaving request time.
        # Receipt deadlines remain enforced by the frozen policy functions.
        offset=at-start
        for symbol in SYMBOLS:
            row=self.row(symbol,start)
            if 120100<=offset<=122000 and not row.get('features') and not row.get('feature_attempted_at_ms'):
                self.spawn((start,'features',symbol),self.freeze(symbol,start))
            if 124100<=offset<=125500 and not row.get('initial_books'):
                self.spawn((start,'initial',symbol),self.initial(symbol,start))
            if 128100<=offset<=129000 and not row.get('sim_quote'):
                self.spawn((start,'confirm',symbol),self.confirm(symbol,start))
            if offset>123000 and not row.get('features') and not row.get('feature_attempted_at_ms'):
                if row.get('feature_capture_status')!='dispatch_window_missed':
                    row['feature_capture_status']='dispatch_window_missed'
                    row['feature_deadline_missed_at_ms']=at
                    row['reason']='missed_feature_window';self.save(row)

    async def run(self):
        from report import write_reports
        try:
            while not self.stop.is_set():
                at=now();start=at//SLOT*SLOT;offset=at-start
                if self.last_start!=start:
                    previous=self.db.execute('SELECT MAX(start) FROM windows').fetchone()[0]
                    begin=previous+SLOT if previous is not None and previous<start else start
                    # Refuse huge offline gaps instead of silently changing the denominator.
                    if start-begin>30*86400000:raise RuntimeError('observer_offline_over_30_days')
                    for t in range(begin,start+1,SLOT):
                        for symbol in SYMBOLS:
                            row=self.row(symbol,t)
                            if not row.get('features') and at>t+123000:row['reason']='missed_feature_window'
                            self.save(row)
                    self.last_start=start
                    self.phase_started={k for k in self.phase_started if k[0]>=start-SLOT}
                    self.timeline={k:v for k,v in self.timeline.items() if k[1]>=start-SLOT}
                if not 119000<=offset<=137000:
                    if at-self.last_discovery>=30000:
                        self.last_discovery=at;self.spawn((start,'discover',at//30000),self.discover(start))
                    if at-self.last_outcomes>=60000:
                        self.last_outcomes=at;self.spawn((start,'outcomes',at//60000),self.outcomes())
                self.schedule_phases(start,at)
                # Full-history report serialization and filesystem work must not
                # block the sub-second capture loop during any quote checkpoint.
                if not 119000<=offset<=137000:
                    if at-self.last_housekeeping>=15000:
                        self.last_housekeeping=at
                        if shutil.disk_usage(self.directory).free<1024**3 or sum(p.stat().st_size for p in self.directory.glob('first-observer.sqlite3*'))>512*1024**2:
                            raise RuntimeError('observer_storage_budget')
                    if at-self.last_report>=15000:
                        self.last_report=at;write_reports(self.db,self.directory,at)
                health=dict(policy=FINGERPRINT,errors=self.errors,inflight=len(self.tasks),cooldown_until_ms=self.reader.cooldown,readonly=True,selector_enabled=False)
                with self.db:self.db.execute('INSERT OR REPLACE INTO health VALUES(1,?,?)',(at,dumps(health)))
                try:await asyncio.wait_for(self.stop.wait(),timeout=.1 if 119000<=offset<=130000 else 1)
                except asyncio.TimeoutError:pass
        finally:
            for task in list(self.tasks):task.cancel()
            await asyncio.gather(*self.tasks,return_exceptions=True)
            write_reports(self.db,self.directory,now());self.db.close()

async def main_async(args):
    import aiohttp
    from dotenv import dotenv_values
    cfg=dotenv_values(args.credential_file,interpolate=False)
    key=os.environ.get('PREDICTION_BINANCE_API_KEY') or cfg.get('PREDICTION_BINANCE_API_KEY')
    secret=os.environ.get('PREDICTION_BINANCE_API_SECRET') or cfg.get('PREDICTION_BINANCE_API_SECRET')
    if not key or not secret:raise RuntimeError('prediction_credentials_unavailable')
    del cfg
    timeout=aiohttp.ClientTimeout(total=1.2,connect=.6,sock_read=.6)
    async with aiohttp.ClientSession(timeout=timeout,connector=aiohttp.TCPConnector(limit=6),trust_env=True,auto_decompress=False,headers={'Accept-Encoding':'identity'}) as session:
        reader=Reader(session,key,secret)
        # Verify VM/API clock before signing; local arrival times define the windows.
        sent=now();server=await reader.get('/api/v3/time');received=now()
        offset=int(server['serverTime'])-(sent+received)//2
        if abs(offset)>500:raise RuntimeError('observer_clock_skew')
        reader.offset=offset
        observer=Observer(connect(args.data),reader,args.data)
        loop=asyncio.get_running_loop()
        for sig in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(sig,observer.stop.set)
        await observer.run()

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--data',required=True);parser.add_argument('--credential-file',required=True)
    args=parser.parse_args();directory=Path(args.data);directory.mkdir(parents=True,exist_ok=True)
    with open(directory/'observer.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:asyncio.run(main_async(args))
        except Exception as exc:
            print('observer stopped: '+type(exc).__name__,flush=True);raise SystemExit(1)

if __name__=='__main__':main()
