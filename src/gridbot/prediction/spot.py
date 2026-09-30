"""Read-only BTCUSDT spot provider with freshness and retry backoff."""
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class SpotQuote:
    price: str
    observed_at_ms: int
    source: str = "binance-public"


class BinanceSpotProvider:
    def __init__(self, *, base_url: str = "https://api.binance.com", symbol: str = "BTCUSDT", timeout: float = 5.0, max_age_ms: int = 1500):
        normalized = str(symbol).upper()
        _ALLOWED = {"BTCUSDT", "ETHUSDT"}
        if normalized not in _ALLOWED:
            raise ValueError(f"Prediction spot feed restricted to {_ALLOWED}")
        self.base_url, self.symbol, self.timeout, self.max_age_ms = base_url.rstrip("/"), normalized, timeout, max_age_ms
        self._last: SpotQuote | None = None
        self._backoff = 0.0
        self._last_error: str | None = None

    def __call__(self, *_args, **_kwargs) -> dict[str, str | int]:
        now = int(time.time() * 1000)
        if self._last and now - self._last.observed_at_ms <= self.max_age_ms:
            return {"price": self._last.price, "observed_at_ms": self._last.observed_at_ms}
        if self._backoff:
            time.sleep(self._backoff + random.uniform(0, min(0.25, self._backoff)))
        request = Request(f"{self.base_url}/api/v3/ticker/price?symbol={self.symbol}", headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            price = str(payload["price"])
            self._last = SpotQuote(price, int(time.time() * 1000))
            self._backoff = 0.0
            self._last_error = None
            return {"price": price, "observed_at_ms": self._last.observed_at_ms}
        except Exception as exc:
            self._backoff = min(10.0, max(0.25, self._backoff * 2))
            self._last_error = str(exc)
            raise

    def health(self, *, now_ms: int | None = None) -> dict[str, object]:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        age = None if self._last is None else max(0, now - self._last.observed_at_ms)
        return {
            "symbol": self.symbol,
            "fresh": age is not None and age <= self.max_age_ms,
            "age_ms": age,
            "backoff_seconds": self._backoff,
            "last_error": self._last_error,
        }


"""Read-only WS feeds. No order endpoints; no signed URL/exception logging.

Full canonical YES books are converted to NO by complementing/swapping sides.
After reconnect no previous book is reused; wait for a fresh complete packet.
Binance aggTrade is a directional proxy, NOT the official settlement feed.
"""
import asyncio
import hashlib
import hmac
import json
import secrets
import time
from decimal import Decimal as D
import aiohttp


def reversal5_command_ok(payload):
    return str(payload.get('code','')) in ('0','000000','00000000','200','SUCCESS') and payload.get('data')=='SUCCESS'


def reversal5_orientation(market):
    raw=market.raw
    data=raw.get('data',raw) if isinstance(raw,dict) else {}
    mid=str(market.up_market_id)
    if mid!=str(market.down_market_id) or str(market.vendor).upper()!='PREDICT_FUN': return None
    nodes=[m for m in data.get('markets',[]) if str(m.get('marketId'))==mid]
    if len(nodes)!=1: return None
    outcomes=nodes[0].get('outcomes',[])
    if len(outcomes)!=2: return None
    indexed={str(o.get('index')):o for o in outcomes}
    if set(indexed)!={'0','1'}: return None
    yes,no=indexed['0'],indexed['1']
    tokens={str(market.up_token_id),str(market.down_token_id)}
    if len(tokens)!=2 or tokens!={str(yes.get('tokenId')),str(no.get('tokenId'))}: return None
    labels={str(o.get('tokenId')):str(o.get('name')).upper() for o in outcomes}
    if labels.get(str(market.up_token_id))!='UP' or labels.get(str(market.down_token_id))!='DOWN': return None
    return 'UP' if str(yes['tokenId'])==str(market.up_token_id) else 'DOWN'


def reversal5_parse_book(payload, *, market_id, now_ms, last_ms=0):
    try:
        if payload.get('type')=='COMMAND': return None
        data=payload.get('data',payload)
        if isinstance(data,str): data=json.loads(data)
        if data.get('msgType')!='orderbook' or str(data.get('marketId'))!=str(market_id): return None
        stamp=D(str(data['updateTimestampMs']))
        if not stamp.is_finite() or stamp!=stamp.to_integral_value(): return None
        at=int(stamp)
        if not 0<at<=now_ms or at<=last_ms: return None
        result={'book_at_ms':at,'received_at_ms':now_ms}
        for name in ('bids','asks'):
            levels=data[name]
            if not isinstance(levels,list) or not 1<=len(levels)<=100: return None
            parsed=[(D(str(p)),D(str(q))) for p,q in levels]
            if any(not p.is_finite() or not q.is_finite() or not 0<p<1 or q<=0 for p,q in parsed): return None
            top=(max if name=='bids' else min)(p for p,_ in parsed)
            result[name]=[str(top),str(sum(q for p,q in parsed if p==top))]
            result[name+'_levels']=[[str(p),str(q)] for p,q in sorted(parsed,reverse=name=='bids')]
        if D(result['bids'][0])>D(result['asks'][0]): return None
        return result
    except (ValueError,TypeError,KeyError,ArithmeticError,AttributeError): return None


class Reversal5Feeds:
    def __init__(self, api_key, api_secret, *, clock_ms=None, symbol: str = "BTCUSDT"):
        self.symbol = str(symbol or "BTCUSDT").upper()
        self._key=api_key;self._secret=api_secret
        self._now=clock_ms or (lambda:int(time.time()*1000))
        self._market_id=None;self._book=None;self._spot=None
        self._book_task=None;self._spot_task=None
        self._retired_book_tasks=set();self._closed=False
        self.health={'book_packets':0,'spot_packets':0,'book_connections':0,'spot_connections':0,
                     'spot_raw_packets':0,'spot_rejected_packets':0,'spot_connected':False}
        self._spot_last_trade_id=-1
        self._spot_last_trade_at_ms=0
        self._spot_last_event_at_ms=0

    def prewarm_spot(self):
        if self._spot_task is None or self._spot_task.done():
            self._spot_task=asyncio.create_task(self._spot_loop())

    def _owns_book(self,mid,task):
        return not self._closed and self._market_id==mid and self._book_task is task

    def _retired_done(self,task):
        self._retired_book_tasks.discard(task)
        self.health['retired_book_tasks']=len(self._retired_book_tasks)
        self.health['book_cancel_completed_at_ms']=self._now()
        if not task.cancelled():
            error=task.exception()
            if error:self.health['retired_book_error']=type(error).__name__

    async def select(self,market_id):
        if self._closed:raise RuntimeError('Feed already closed')
        mid=str(market_id)
        self.prewarm_spot()
        if mid==self._market_id and self._book_task and not self._book_task.done():return
        if len(self._retired_book_tasks)>=8:
            self._market_id=None;self._book=None
            raise RuntimeError('Old book cleanup backlog; fail closed')
        old=self._book_task
        # Revoke ownership before cancellation; late cleanup/packets cannot
        # mutate the new market, even when selecting the same market again.
        self._book_task=None;self._market_id=mid;self._book=None
        self.health['book_switch_started_at_ms']=self._now()
        self.health.pop('first_book_at_ms',None)
        self.health.pop('book_error',None)
        if old:
            self._retired_book_tasks.add(old)
            old.add_done_callback(self._retired_done)
            old.cancel()
            self.health['book_cancel_started_at_ms']=self._now()
        self.health['retired_book_tasks']=len(self._retired_book_tasks)
        self._book_task=asyncio.create_task(self._book_loop(mid))
        self.health['book_switch_returned_at_ms']=self._now()

    async def close(self):
        self._closed=True;self._market_id=None
        tasks={t for t in (self._book_task,self._spot_task) if t}|self._retired_book_tasks
        self._book_task=None;self._spot_task=None;self._book=None;self._spot=None
        for task in tasks:task.cancel()
        if tasks:
            done,pending=await asyncio.wait(tasks,timeout=2)
            self.health['close_pending_tasks']=len(pending)
            for task in done:
                if not task.cancelled():task.exception()

    async def _book_loop(self, mid):
        owner=asyncio.current_task()
        backoff=1
        while self._owns_book(mid,owner):
            self._book=None
            try:
                if not self._key or not self._secret: raise ValueError('dedicated credentials missing')
                topic='web3_prediction_orderbook_'+mid
                params={'random':secrets.token_hex(12),'recvWindow':'30000','timestamp':str(self._now()),'topic':topic}
                query='&'.join(f'{k}={params[k]}' for k in sorted(params))
                sig=hmac.new(self._secret.encode(),query.encode(),hashlib.sha256).hexdigest()
                url='wss://api.binance.com/sapi/wss?'+query+'&signature='+sig
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None,connect=10)) as session:
                    async with session.ws_connect(url,headers={'X-MBX-APIKEY':self._key},heartbeat=25,max_msg_size=262144) as ws:
                        await ws.send_json({'command':'SUBSCRIBE','value':topic})
                        if not self._owns_book(mid,owner): return
                        self.health['book_connections']+=1
                        self.health['book_subscribed_at_ms']=self._now()
                        opened=time.monotonic()
                        while time.monotonic()-opened<23*3600:
                            msg=await asyncio.wait_for(ws.receive(),timeout=45)
                            if msg.type!=aiohttp.WSMsgType.TEXT: break
                            if not self._owns_book(mid,owner): return
                            payload=json.loads(msg.data)
                            if payload.get('type')=='COMMAND':
                                if not reversal5_command_ok(payload):
                                    raise ValueError('subscription rejected')
                                continue
                            book=reversal5_parse_book(payload,market_id=mid,now_ms=self._now(),last_ms=(self._book or {}).get('book_at_ms',0))
                            if book:
                                if not self._owns_book(mid,owner): return
                                self.health.setdefault('first_book_at_ms',self._now())
                                self._book=book;self.health['book_packets']+=1;backoff=1
                                self.health['book_error']=None
            except asyncio.CancelledError: raise
            except Exception as exc:
                if self._owns_book(mid,owner): self.health['book_error']=type(exc).__name__
            finally:
                if self._owns_book(mid,owner): self._book=None
            await asyncio.sleep(backoff)
            backoff=min(30,backoff*2)

    def _accept_spot_packet(self, data, *, received_at_ms):
        """Keep exchange trade/event time separate from local receipt time.

        Silence and heartbeats never advance the last trade. An increasing
        aggregate ID permits multiple real trades with the same millisecond.
        Sequence checks survive a reconnect, although the snapshot is cleared.
        """
        self.health['spot_raw_packets']+=1
        try:
            if not isinstance(data,dict) or data.get('e')!='aggTrade' or data.get('s')!=self.symbol:
                raise ValueError('Unexpected spot packet')
            price=D(str(data['p']))
            fields=[D(str(data[key])) for key in ('T','E','a')]
            if any(not v.is_finite() or v!=v.to_integral_value() for v in fields):
                raise ValueError('Nonintegral spot metadata')
            at,event_at,trade_id=map(int,fields)
            if (not price.is_finite() or price<=0 or not 0<at<=event_at<=received_at_ms
                    or trade_id<=self._spot_last_trade_id or at<self._spot_last_trade_at_ms
                    or event_at<self._spot_last_event_at_ms):
                raise ValueError('Noncausal spot packet')
        except (ValueError,TypeError,KeyError,ArithmeticError) as exc:
            self.health['spot_rejected_packets']+=1
            self.health['spot_last_reject_reason']=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
            return False
        self._spot={'spot':str(price),'spot_at_ms':at,'spot_event_at_ms':event_at,
                    'spot_received_at_ms':int(received_at_ms),'spot_trade_id':trade_id,
                    'spot_connection_generation':self.health['spot_connections']}
        self._spot_last_trade_id=trade_id
        self._spot_last_trade_at_ms=at
        self._spot_last_event_at_ms=event_at
        self.health['spot_packets']+=1
        self.health['spot_error']=None
        return True

    async def _spot_loop(self):
        backoff=1
        while True:
            self._spot=None
            self._mid=None
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None,connect=10)) as session:
                    stream_sym = self.symbol.lower()
                    async with session.ws_connect(f'wss://stream.binance.com:9443/stream?streams={stream_sym}@aggTrade/{stream_sym}@bookTicker',heartbeat=25,max_msg_size=65536) as ws:
                        self.health['spot_connections']+=1
                        self.health['spot_connected']=True
                        opened=time.monotonic()
                        while time.monotonic()-opened<23*3600:
                            msg=await asyncio.wait_for(ws.receive(),timeout=30)
                            if msg.type!=aiohttp.WSMsgType.TEXT: break
                            received_at_ms=self._now()
                            try:
                                envelope=json.loads(msg.data)
                                data=envelope.get('data',envelope)
                                if envelope.get('stream')==f'{self.symbol.lower()}@bookTicker':
                                    self._accept_mid_packet(data,received_at_ms=received_at_ms)
                                    continue
                            except (ValueError,TypeError):
                                self.health['spot_raw_packets']+=1
                                self.health['spot_rejected_packets']+=1
                                self.health['spot_last_reject_reason']='Malformed JSON'
                                continue
                            if self._accept_spot_packet(data,received_at_ms=received_at_ms):backoff=1
            except asyncio.CancelledError: raise
            except Exception as exc: self.health['spot_error']=type(exc).__name__
            finally:
                self._spot=None
                self.health['spot_connected']=False
            await asyncio.sleep(backoff)
            backoff=min(30,backoff*2)

    def _accept_mid_packet(self,data,*,received_at_ms):
        # Observation only: bookTicker has no exchange event timestamp.
        try:
            bid,ask=D(str(data['b'])),D(str(data['a']));uid=int(data['u'])
            if data.get('s')!=self.symbol or not bid.is_finite() or not ask.is_finite() or not 0<bid<=ask:return False
            old=getattr(self,'_mid',None)
            if old and uid<=old['spot_mid_update_id']:return False
            self._mid={'spot_mid':str((bid+ask)/2),'spot_mid_received_at_ms':received_at_ms,'spot_mid_update_id':uid}
            return True
        except (ValueError,TypeError,KeyError,ArithmeticError):return False

    def snapshot(self, market, now_ms):
        q={'source':'binance_prediction_ws','feed_ok':False,'orientation_verified':False,
           'book_at_ms':0,'spot_at_ms':0,'received_at_ms':0,'spot':None,
           'reference':str(market.reference_price) if market.reference_price is not None else None,
           'spot_source':'binance_aggTrade_proxy_not_settlement',
           'spot_connected':bool(self.health.get('spot_connected')),
           'spot_raw_packets':self.health['spot_raw_packets'],
           'spot_rejected_packets':self.health['spot_rejected_packets'],
           'spot_last_reject_reason':self.health.get('spot_last_reject_reason'),
           'UP':{'bid':None,'ask':None,'ask_shares':'0'},'DOWN':{'bid':None,'ask':None,'ask_shares':'0'}}
        yes=reversal5_orientation(market)
        if not yes or self._market_id!=str(market.up_market_id): return q
        q['orientation_verified']=True
        if self._spot: q.update(self._spot)
        if getattr(self,'_mid',None): q.update(self._mid)
        if not self._book: return q
        book=self._book
        bid,bs=map(D,book['bids']);ask,ass=map(D,book['asks'])
        no='DOWN' if yes=='UP' else 'UP'
        q[yes]={'bid':str(bid),'ask':str(ask),'ask_shares':str(ass)}
        q[no]={'bid':str(1-ask),'ask':str(1-bid),'ask_shares':str(bs)}
        q.update({k:book[k] for k in ('book_at_ms','received_at_ms')})
        q['canonical_yes_levels']={'bids':book.get('bids_levels',[]),'asks':book.get('asks_levels',[])}
        q['feed_ok']=bool(self._spot and all(0<q[k]<=now_ms and now_ms-q[k]<=1500 for k in ('book_at_ms','spot_at_ms','received_at_ms')))
        return q
