"""Finite read-only market collector and paper shadow. No live order route."""
import argparse
import asyncio
import gzip
import hashlib
import json
import os
import pathlib
import shutil
import signal
from urllib.parse import urlsplit
import aiohttp
from dotenv import dotenv_values
from frozen.prediction.client import BinancePredictionClient,UrllibTransport,PREDICTION_PREFIX
from frozen.prediction.models import MarketInfo
from frozen.prediction.spot import Reversal5Feeds,reversal5_orientation
from frozen.prediction.official_resolution import official_market_topic_id,parse_official_resolution
from logic import SLOT,Tape,advance,dumps,CONFIG
def metadata(raw,identified_at):
    m=MarketInfo.from_api(raw)
    return {'topic':str(m.market_topic_id),'market_id':str(m.up_market_id),'start':m.start_time_ms,
            'end':m.end_time_ms,'reference':float(m.reference_price) if m.reference_price else None,
            'yes':reversal5_orientation(m),'fee_bps':float(raw['feeRateBps']) if raw.get('feeRateBps') is not None else None,
            'identified_at':identified_at}

from store import Store,now
from candidate_engine import Engine,ACCOUNT
from calibration import ProbabilityModel
from candidate_report import write_report

TRADE_URLS={'spot':'wss://stream.binance.com:9443/ws/btcusdt@aggTrade',
            'futures':'wss://fstream.binance.com/market/ws/btcusdt@aggTrade'}

class ReadOnlyTransport(UrllibTransport):
    def request(self,method,url,**kwargs):
        u=urlsplit(url)
        if method!='GET' or u.scheme!='https' or u.netloc!='api.binance.com' or u.path not in {PREDICTION_PREFIX+'/market/list',PREDICTION_PREFIX+'/market/detail'}:
            raise PermissionError('shadow REST endpoint denied')
        return super().request(method,url,**kwargs)

class ReadOnlyClient(BinancePredictionClient):
    def _request(self,path,method='GET',**kwargs):
        if method!='GET' or path not in {PREDICTION_PREFIX+'/market/list',PREDICTION_PREFIX+'/market/detail'}:
            raise PermissionError('shadow cannot trade')
        return super()._request(path,method,**kwargs)

class Feeds(Reversal5Feeds):
    def __init__(self,*args,callback,**kwargs):
        self.callback=callback;super().__init__(*args,**kwargs)
    def __setattr__(self,key,value):
        if key=='_book' and value and hasattr(self,'callback'):
            self.callback({'received_at':now(),'kind':'prediction_book','body':{'market_id':self._market_id,**value}})
        super().__setattr__(key,value)

def verify_source():
    root=pathlib.Path(__file__).parent;manifest=json.loads((root/'manifest.json').read_text())
    actual={str(p.relative_to(root)).replace('\\','/') for p in root.rglob('*.py')}
    if actual!=set(manifest['files']): raise ValueError('unexpected source files')
    for path,sha in manifest['files'].items():
        if hashlib.sha256((root/path).read_bytes()).hexdigest()!=sha: raise ValueError('source hash mismatch: '+path)
    return hashlib.sha256(dumps(manifest).encode()).hexdigest()

class Runner:
    def __init__(self,args):
        self.args=args;self.directory=pathlib.Path(args.data);self.directory.mkdir(parents=True,exist_ok=True)
        self.store=Store(self.directory/'research.sqlite3','preflight' if args.preflight else 'shadow',args.budget,verify_source())
        if self.store.get('start') is None:
            self.store.set('start',((now()+30000)//SLOT+1)*SLOT)
            self.store.set('target',1 if args.preflight else args.windows)
        if self.store.get('target')!=(1 if args.preflight else args.windows): raise ValueError('target mismatch')
        self.start=self.store.get('start');self.end=self.start+self.store.get('target')*SLOT
        artifact=json.loads(pathlib.Path(args.model_artifact).read_text()) if args.model_artifact else None
        self.engine=Engine(self.store,ProbabilityModel(artifact));self.tape=Tape();self.markets={m['start']:m for m in self.store.rows('markets')}
        self.frozen={p['id'] for p in self.store.rows('packets')};self.tasks=set();self.stop=asyncio.Event()
        self.next_freeze=0;self.recovered=False
        self.raw=None;self.raw_bucket=None;self.raw_count=0;self.errors={};self.current_market=None;self.session=None
        # Never reconstruct an interrupted live decision with later data.
        self.began=now()
    def task_done(self,task):
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.error('decision',task.exception());self.stop.set()
    def error(self,kind,exc):
        self.errors[kind+':'+type(exc).__name__]=self.errors.get(kind+':'+type(exc).__name__,0)+1
    def freeze_due(self,at):
        if not self.recovered:
            self.recovered=True
            done={d['id'] for d in self.store.rows('decisions') if d['status']=='complete' and (d['phase']=='E10' or ACCOUNT in d.get('branches',{}))}
            for packet in self.store.rows('packets'):
                if packet['id'] not in done:
                    task=asyncio.create_task(self.engine.decide(packet,self.session,self.key,recover=True))
                    self.tasks.add(task);task.add_done_callback(self.task_done)
        while self.next_freeze<self.store.get('target')*3:
            i,stage=divmod(self.next_freeze,3);phase,offset=CONFIG['phases'][stage]
            start=self.start+i*SLOT;cut=start+offset
            if at<cut: break
            self.next_freeze+=1;ident=f'{start}:{phase}'
            if ident in self.frozen: continue
            m=self.markets.get(start)
            if m is None:
                m={'topic':'missing:'+str(start),'start':start,'end':start+SLOT,'reference':None,
                   'market_id':'missing','yes':None,'fee_bps':None,'identified_at':None}
            packet=self.tape.packet(m,cut)
            if at>cut+500: packet['reasons'].append('missed_cutoff')
            self.engine.freeze(packet,phase);self.frozen.add(ident)
            task=asyncio.create_task(self.engine.decide(packet,self.session,self.key))
            self.tasks.add(task);task.add_done_callback(self.task_done)
    def ingest(self,event):
        at=event['received_at']
        self.freeze_due(at)
        self.tape.ingest(event)
        bucket=at//SLOT
        if bucket!=self.raw_bucket:
            if self.raw: self.raw.close()
            if shutil.disk_usage(self.directory).free<1024**3:
                self.stop.set();raise RuntimeError('raw disk reserve')
            self.raw=gzip.open(self.directory/f'raw-{bucket}-{os.getpid()}.jsonl.gz','at',encoding='utf-8',compresslevel=1)
            self.raw_bucket=bucket
        self.raw.write(dumps(event)+'\n');self.raw_count+=1
        if self.raw_count%500==0: self.raw.flush()
        if event['kind']=='prediction_book':
            self.execute(at)
    def execute(self,at):
        for o in self.engine.orders.values():
            if o['status']!='open': continue
            m=next((m for m in self.markets.values() if m['topic']==o['topic']),None)
            if m is None:
                if at>o['expires']: o['status']='unknown';self.store.put('orders',o['id'],o)
                continue
            old=dumps(o);advance(o,self.tape.books.get(m['market_id']),m,at)
            if old!=dumps(o): self.store.put('orders',o['id'],o)
    async def trades(self,source,url):
        while not self.stop.is_set():
            try:
                async with self.session.ws_connect(url,heartbeat=20,timeout=aiohttp.ClientWSTimeout(ws_receive=15,ws_close=5)) as ws:
                    async for msg in ws:
                        if msg.type==aiohttp.WSMsgType.TEXT:
                            self.ingest({'received_at':now(),'kind':'binance_'+source+'_aggTrade','body':json.loads(msg.data)})
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
            except Exception as exc: self.error(source,exc)
            await asyncio.sleep(2)
    async def catalog(self):
        timeline={};last_list=0;seeded=set();prefetched={}
        async def accept(raw):
            if raw.get('symbol')!='BTCUSDT': return
            m=MarketInfo.from_api(raw)
            if m.end_time_ms-m.start_time_ms!=SLOT or not reversal5_orientation(m): return
            t=now()
            for item in raw.get('timeline',[]):
                if isinstance(item.get('startDate'),int): timeline[item['startDate']]=str(item['marketTopicId'])
            if self.start<=m.start_time_ms<self.end and m.start_time_ms<=t and m.reference_price is not None and m.reference_price>0:
                candidate=metadata(raw,t);prior=self.markets.get(m.start_time_ms)
                if prior and any(prior[k]!=candidate[k] for k in ('topic','reference','market_id','yes')):
                    self.error('identity',ValueError());return
                if not prior: self.markets[m.start_time_ms]=candidate;self.store.put('markets',candidate['topic'],candidate)
            if m.start_time_ms-2000<=t<m.end_time_ms-2000 and t<self.end:
                if self.current_market!=str(m.market_topic_id):
                    await self.feeds.select(m.up_market_id);self.current_market=str(m.market_topic_id)
        while not self.stop.is_set():
            try:
                t=now();slot=t//SLOT*SLOT
                if t-last_list>=20000:
                    payload=await asyncio.to_thread(self.client.list_prediction_markets,l1_category='crypto',l2_category='up-down',limit=100)
                    data=payload.get('data',payload);items=data.get('marketTopics',[]) if isinstance(data,dict) else data
                    last_list=now()
                    for raw in items:
                        await accept(raw)
                        if raw.get('symbol')=='BTCUSDT':
                            m=MarketInfo.from_api(raw)
                            if m.end_time_ms-m.start_time_ms==SLOT and m.start_time_ms<=now()<m.end_time_ms and m.market_topic_id not in seeded:
                                detail=await asyncio.to_thread(self.client.get_market_detail,m.market_topic_id)
                                await accept(detail.get('data',detail));seeded.add(m.market_topic_id)
                if slot in timeline and self.start<=slot<self.end and slot not in self.markets:
                    detail=await asyncio.to_thread(self.client.get_market_detail,timeline[slot]);await accept(detail.get('data',detail))
                upcoming=slot+SLOT
                if upcoming in timeline and upcoming<self.end and now()>=upcoming-5000:
                    if upcoming not in prefetched:
                        detail=await asyncio.to_thread(self.client.get_market_detail,timeline[upcoming]);prefetched[upcoming]=detail.get('data',detail)
                    await accept(prefetched[upcoming])
            except Exception as exc: self.error('catalog',exc)
            await asyncio.sleep(.5 if now()%SLOT<20000 or now()%SLOT>290000 else 2)
    async def resolutions(self):
        while not self.stop.is_set():
            known={r[0] for r in self.store.db.execute('SELECT topic FROM resolutions')}
            for m in list(self.markets.values()):
                if m['topic'] in known or now()<m['end']+15000: continue
                try:
                    payload=await asyncio.to_thread(self.client.get_market_detail,m['topic'])
                    if official_market_topic_id(payload)!=m['topic']: continue
                    resolved=parse_official_resolution(payload)
                    if resolved.ambiguous or not resolved.winners: continue
                    outcome='TIE' if resolved.exact_dual_tie else str(resolved.winners[0])
                    self.engine.resolve(m['topic'],outcome,now())
                except Exception as exc: self.error('resolution',exc)
                await asyncio.sleep(.2)
            await asyncio.sleep(15)
    async def run(self):
        if self.store.get('complete'): return
        values=dotenv_values(self.args.root+'/prediction/shadow-api-credentials.env')
        self.key=dotenv_values(self.args.root+'/jev_shadow_lane/.env').get('OPENROUTER_API_KEY') or values.get('OPENROUTER_API_KEY')
        if not self.key: raise ValueError('missing Jev credential')
        key,secret=values['PREDICTION_BINANCE_API_KEY'],values['PREDICTION_BINANCE_API_SECRET']
        os.environ['PREDICTION_SHARED_WEIGHT_DB']=self.args.root+'/prediction/data/request-weight.sqlite3'
        self.client=ReadOnlyClient(key,secret,transport=ReadOnlyTransport(),timeout=5)
        self.feeds=Feeds(key,secret,callback=self.ingest)
        runtime={};last_report=0
        async with aiohttp.ClientSession() as session:
            self.session=session
            jobs=[asyncio.create_task(self.catalog()),asyncio.create_task(self.resolutions()),
                  asyncio.create_task(self.trades('spot',TRADE_URLS['spot'])),
                  asyncio.create_task(self.trades('futures',TRADE_URLS['futures']))]
            try:
                while not self.stop.is_set():
                    at=now();self.freeze_due(at);self.execute(at)
                    phase='warmup' if at<self.start else 'sampling' if at<self.end else 'settling'
                    book=self.tape.books.get(self.feeds._market_id) or {}
                    book_age=at-book['book_at_ms'] if book.get('book_at_ms') else None
                    runtime={'asof_ms':at,'start_ms':self.start,'end_ms':self.end,'target':self.store.get('target'),
                             'book_health':{'exchange_age_ms':book_age,'fresh':book_age is not None and 0<=book_age<=CONFIG['max_book_age_ms'] and bool(self.feeds.health.get('book_connected')),
                                            'connected':bool(self.feeds.health.get('book_connected')),'empty_side':bool(book.get('empty_side')),'last_rejection':self.feeds.health.get('book_last_rejection')},
                             'phase':phase,'errors':self.errors,'raw_records':self.raw_count,
                             'trade_sources':{s:{'retained':len(xs),'last_received_at':xs[-1][0] if xs else None} for s,xs in self.tape.trades.items()},
                             'free_bytes':shutil.disk_usage(self.directory).free,'feed':self.feeds.health}
                    if at-last_report>=10000:
                        write_report(self.store,self.directory,runtime);last_report=at
                        (self.directory/'heartbeat.json').write_text(dumps(runtime),encoding='utf-8')
                    if at>=self.end:
                        if not jobs[0].done():
                            for j in (jobs[0],jobs[2],jobs[3]): j.cancel()
                            await self.feeds.close()
                        resolved=self.store.db.execute('SELECT count(*) FROM resolutions').fetchone()[0]
                        if resolved>=len(self.markets) and not self.tasks or at>=self.end+7200000:
                            self.store.set('complete',True);break
                    if runtime['free_bytes']<512*1024**2: raise RuntimeError('disk reserve')
                    await asyncio.sleep(.1)
            finally:
                self.stop.set()
                for job in jobs: job.cancel()
                await asyncio.gather(*jobs,return_exceptions=True)
                if self.tasks: await asyncio.gather(*list(self.tasks),return_exceptions=True)
                if self.store.pending: await asyncio.gather(*list(self.store.pending.values()),return_exceptions=True)
                await self.feeds.close()
                if self.raw: self.raw.close()
                runtime['phase']='complete' if self.store.get('complete') else 'stopped'
                runtime['asof_ms']=now()
                write_report(self.store,self.directory,runtime)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--data',required=True)
    p.add_argument('--budget',type=float,default=5.);p.add_argument('--windows',type=int,default=200);p.add_argument('--preflight',action='store_true');p.add_argument('--model-artifact')
    args=p.parse_args()
    if args.budget<=0 or args.budget>5 or not 1<=args.windows<=200: raise ValueError('bounded cohort/budget')
    import fcntl
    pathlib.Path(args.data).mkdir(parents=True,exist_ok=True)
    with open(pathlib.Path(args.data)/'writer.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        runner=Runner(args)
        async def main():
            loop=asyncio.get_running_loop()
            for sig in (signal.SIGTERM,signal.SIGINT): loop.add_signal_handler(sig,runner.stop.set)
            await runner.run()
        asyncio.run(main())
