"""Durable single-writer research ledger and cost-limited Jev client."""
import asyncio
import json
import pathlib
import sqlite3
import time
import aiohttp
from logic import CONFIG,DEFINITION,MODEL,QUESTIONS,digest,dumps,number,parse_response

def now(): return int(time.time()*1000)

class Store:
    def __init__(self,path,mode,budget,source_hash):
        pathlib.Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path);self.db.row_factory=sqlite3.Row
        self.db.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
          CREATE TABLE IF NOT EXISTS calls(hash TEXT PRIMARY KEY,state TEXT,status TEXT,response TEXT,started INTEGER,completed INTEGER,cost REAL);
          CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,body TEXT);
          CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY,body TEXT);
          CREATE TABLE IF NOT EXISTS markets(topic TEXT PRIMARY KEY,body TEXT);
          CREATE TABLE IF NOT EXISTS packets(topic TEXT PRIMARY KEY,body TEXT);
          CREATE TABLE IF NOT EXISTS resolutions(topic TEXT PRIMARY KEY,outcome TEXT,known_at INTEGER);
        ''')
        identity={'mode':mode,'budget':budget,'definition':DEFINITION,'source_hash':source_hash}
        if self.get('identity') not in (None,identity):
            self.db.close();raise ValueError('cohort identity mismatch')
        self.set('identity',identity)
        self.db.execute("UPDATE calls SET status='interrupted',completed=? WHERE status='inflight'",(now(),));self.db.commit()
        self.budget=budget;self.pending={}
    def get(self,key):
        r=self.db.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone();return json.loads(r[0]) if r else None
    def set(self,key,value):
        self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(key,dumps(value)));self.db.commit()
    def put(self,table,key,value):
        if table not in ('orders','decisions','markets','packets'): raise ValueError('table')
        self.db.execute('INSERT OR REPLACE INTO '+table+' VALUES(?,?)',(key,dumps(value)));self.db.commit()
    def put_many(self,items):
        with self.db:
            for table,key,value in items:
                if table not in ('orders','decisions','markets','packets'): raise ValueError('table')
                self.db.execute('INSERT OR REPLACE INTO '+table+' VALUES(?,?)',(key,dumps(value)))
    def rows(self,table):
        if table not in ('orders','decisions','markets','packets'): raise ValueError('table')
        return [json.loads(r[0]) for r in self.db.execute('SELECT body FROM '+table)]
    def spent(self): return self.db.execute('SELECT coalesce(sum(cost),0) FROM calls').fetchone()[0]
    async def infer(self,session,key,state,questions=None):
        questions=questions or QUESTIONS
        h=digest({'state':state,'questions':questions,'model':MODEL})
        if h in self.pending: return await asyncio.shield(self.pending[h])
        cached=self.db.execute('SELECT * FROM calls WHERE hash=?',(h,)).fetchone()
        if cached: return dict(cached)
        if len(dumps(state).encode())>CONFIG['input_bytes']: return {'hash':h,'status':'input_too_large','cost':0}
        if self.get('billing_stop') or self.spent()+.05>self.budget+1e-9: return {'hash':h,'status':'budget_stop','cost':0}
        self.db.execute('INSERT INTO calls VALUES(?,?,?,?,?,?,?)',(h,dumps({'model':MODEL,'questions':questions,'state':state}),'inflight',None,now(),None,.05));self.db.commit()
        task=asyncio.create_task(self._request(session,key,h,state,questions));self.pending[h]=task
        try: return await asyncio.shield(task)
        finally: self.pending.pop(h,None)
    async def _request(self,session,key,h,state,questions):
        raw={};status='request_error';began=time.monotonic()
        try:
            async with session.post('https://openrouter.ai/api/alpha/decisions',
                 headers={'Authorization':'Bearer '+key,'X-OpenRouter-Title':'cry3 early10 jev v2 shadow'},
                 json={'model':MODEL,'questions':questions,'state':state,'user':'cry3-early10-jev-v2-shadow'},
                 timeout=aiohttp.ClientTimeout(total=max(.001,min(3,(state['observed_at']+3000-now())/1000)))) as response:
                if response.status!=200: raw={'http_status':response.status};status='http_error'
                else:
                    response_data=await response.json()
                    # Keep only documented, nonsecret fields even for malformed provider responses.
                    raw={k:response_data[k] for k in ('id','model','answers','usage') if k in response_data}
                    try: parse_response(raw,questions);status='ok'
                    except (ValueError,TypeError,KeyError): status='invalid_response'
        except asyncio.CancelledError: status='cancelled'
        except Exception as exc: raw={'error_type':type(exc).__name__}
        elapsed=int((time.monotonic()-began)*1000)
        if elapsed>3000 and status=='ok': status='late'
        raw['elapsed_ms']=elapsed
        charge=number((raw.get('usage') or {}).get('cost'))
        cost=charge if charge is not None and charge>=0 else .05
        if cost>.05: self.set('billing_stop','reported_charge_exceeded_reservation')
        self.db.execute('UPDATE calls SET status=?,response=?,completed=?,cost=? WHERE hash=?',
                        (status,dumps(raw),now(),cost,h));self.db.commit()
        return dict(self.db.execute('SELECT * FROM calls WHERE hash=?',(h,)).fetchone())
