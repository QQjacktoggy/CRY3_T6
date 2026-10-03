"""Read-only signed catalog plus public spot/futures tape and Prediction WS books.

There is deliberately no BinancePredictionClient, wallet endpoint or POST transport.
Credentials are supplied explicitly and are never read from legacy environments.
"""
import asyncio
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.parse
import urllib.request

import aiohttp

from .eth_t67c_core import identity
from .eth_t67c_policy import SYMBOL
from .http_bounds import KLINES_BODY_BYTES, PREDICTION_BODY_BYTES, read_bounded
from .regime_lane import dec
from .spot import Reversal5Feeds, reversal5_orientation

PREFIX = '/sapi/v1/w3w/wallet/prediction'
READ_WEIGHTS = {'market/list': 1, 'market/detail': 1}  # Same audited catalog as the BTC worker.


class ReadOnlyDataError(RuntimeError):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ReadOnlyDataError('eth_http_redirect_denied')


class EthCatalog:
    def __init__(self, api_key, api_secret, budget, *, clock_ms=None, transport=None):
        if not api_key or not api_secret:
            raise ValueError('eth_existing_prediction_credentials_required')
        self._key, self._secret, self.budget = api_key, api_secret, budget
        self.clock = clock_ms or (lambda: time.time_ns()//1_000_000)
        self._transport = transport

    @staticmethod
    def _get(url, headers, response_hook):
        request = urllib.request.Request(url, headers=headers, method='GET')
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=1.5) as response:
            # Preserve known status/Retry-After even if the body decoder rejects it.
            response_hook(response.status, dict(response.headers))
            return response.status, dict(response.headers), read_bounded(response, response.headers, PREDICTION_BODY_BYTES)

    def read(self, endpoint, params):
        if endpoint not in READ_WEIGHTS:
            raise PermissionError('eth_read_endpoint_denied')
        if not self.budget.acquire(READ_WEIGHTS[endpoint], priority='normal', headroom=100):
            raise ReadOnlyDataError('eth_shared_budget_deferred')
        token = self.budget.begin_request()
        if token is None:
            raise ReadOnlyDataError('eth_shared_budget_deferred')
        query = urllib.parse.urlencode(dict(params, timestamp=self.clock(), recvWindow=5000))
        signature = hmac.new(self._secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = 'https://api.binance.com'+PREFIX+'/'+endpoint+'?'+query+'&signature='+signature
        response_seen = False

        def complete_response(status, headers):
            nonlocal response_seen
            self.budget.note_response(status, headers, token=token)
            response_seen = True

        try:
            headers_in = {'X-MBX-APIKEY': self._key, 'Accept': 'application/json'}
            if self._transport is None:
                status, headers, body = self._get(url, headers_in,
                    complete_response)
                token = None  # The real transport already completed the durable journal.
            else:
                status, headers, body = self._transport(url, headers_in)
        except urllib.error.HTTPError as exc:
            self.budget.note_response(exc.code, dict(exc.headers or {}), token=token)
            exc.close()
            raise ReadOnlyDataError('eth_catalog_http_error') from None
        except Exception:
            # Read-only transport failure cannot leave BTC's shared journal blocked.
            if token is not None and not response_seen:
                self.budget.transport_failed(token)
            raise ReadOnlyDataError('eth_catalog_transport_error') from None
        if token is not None:
            self.budget.note_response(status, headers, token=token)
        if status != 200:
            raise ReadOnlyDataError('eth_catalog_http_error')
        if len(body) > PREDICTION_BODY_BYTES:
            raise ReadOnlyDataError('eth_catalog_body_too_large')
        value = json.loads(body)
        if not isinstance(value, dict) or value.get('code', 0) not in (0, '0', None):
            raise ReadOnlyDataError('eth_catalog_payload_rejected')
        return value.get('data', value)

    def markets(self):
        return self.read('market/list', dict(l1Category='crypto', l2Category='up-down', limit=100))

    def detail(self, topic):
        return self.read('market/detail', {'marketTopicId': str(topic)})


def kline_query(symbol, start):
    if symbol not in ('BTCUSDT', SYMBOL):
        raise ValueError('eth_public_symbol_invalid')
    return urllib.parse.urlencode(dict(symbol=symbol, interval='1m', startTime=start-900000,
                                      endTime=start+119999, limit=17))


def fetch_klines(start, *, budget, symbol=SYMBOL):
    # Official Spot GET /api/v3/klines weight is 2; share the existing BTC reserve/ban.
    url = 'https://api.binance.com/api/v3/klines?'+kline_query(symbol, start)
    if not budget.acquire(2, priority='normal', headroom=100):
        raise ReadOnlyDataError('eth_shared_budget_deferred')
    token = budget.begin_request()
    if token is None:
        raise ReadOnlyDataError('eth_shared_budget_deferred')
    response_seen = False
    try:
        request = urllib.request.Request(url, headers={'Accept': 'application/json'}, method='GET')
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=1.5) as response:
            budget.note_response(response.status, dict(response.headers), token=token)
            response_seen = True
            if response.status != 200:
                raise ReadOnlyDataError('eth_klines_http_error')
            return json.loads(read_bounded(response, response.headers, KLINES_BODY_BYTES))
    except urllib.error.HTTPError as exc:
        budget.note_response(exc.code, dict(exc.headers or {}), token=token)
        exc.close()
        raise ReadOnlyDataError('eth_klines_http_error') from None
    except Exception:
        if not response_seen:
            budget.transport_failed(token)
        raise ReadOnlyDataError('eth_klines_transport_error') from None


def trade_urls(symbol):
    if symbol not in ('BTCUSDT', SYMBOL):
        raise ValueError('eth_public_symbol_invalid')
    lower = symbol.lower()
    return {'spot': f'wss://stream.binance.com:9443/ws/{lower}@aggTrade',
            'futures': f'wss://fstream.binance.com/market/ws/{lower}@aggTrade'}


class PublicTape:
    def __init__(self, engine, *, symbol=SYMBOL, clock_ms=None):
        if symbol != SYMBOL:
            raise ValueError('eth_tape_asset_invalid')
        self.engine, self.urls = engine, trade_urls(symbol)
        self.clock = clock_ms or (lambda: time.time_ns()//1_000_000)
        self.tasks = []

    async def start(self):
        self.tasks = [asyncio.create_task(self._loop(source, url)) for source, url in self.urls.items()]

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _loop(self, source, url):
        generation = self.engine.watermarks.get(source, {}).get('generation', 0)
        backoff = 1
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=25, max_msg_size=65536) as ws:
                        generation += 1
                        self.engine.disconnect(source, self.clock())
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                break
                            received = self.clock()
                            try:
                                self.engine.trade(source, generation, json.loads(msg.data), received)
                                backoff = 1
                            except (ValueError, KeyError, TypeError, ArithmeticError):
                                self.engine.diagnostic('eth_trade_packet_rejected', received)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.engine.diagnostic('eth_public_feed_disconnected', self.clock())
            finally:
                self.engine.disconnect(source, self.clock())
            await asyncio.sleep(backoff)
            backoff = min(30, backoff*2)


def book_snapshot(feed, raw, spec, start, at_ms):
    """Use the existing read-only WS parser, including its real source clocks/depth."""
    from .eth_t67c_core import validate_market
    market = validate_market(raw, spec, start)
    if feed._market_id != str(market.up_market_id) or not feed._book:
        raise ValueError('eth_ws_book_unavailable')
    yes = reversal5_orientation(market)
    if yes not in ('UP', 'DOWN'):
        raise ValueError('eth_book_orientation_unverified')
    book = feed._book
    opposite = 'DOWN' if yes == 'UP' else 'UP'
    quote = {yes: {'ask_levels': book['asks_levels']}, opposite: {'ask_levels':
        [[str(1-dec(p)), q] for p, q in book['bids_levels']]}}
    return dict(identity(raw, spec, start), quote=quote, full_depth=True,
                book_at_ms=book['book_at_ms'], received_at_ms=book['received_at_ms'], captured_at_ms=at_ms)


def book_feed(api_key, api_secret, *, clock_ms=None):
    return Reversal5Feeds(api_key, api_secret, symbol=SYMBOL, clock_ms=clock_ms)
