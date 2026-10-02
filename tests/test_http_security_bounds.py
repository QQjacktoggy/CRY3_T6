"""Offline regression coverage for response caps and durable server bans."""
import asyncio
import gzip
import importlib
import io
import json
import sqlite3
import urllib.error
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.gridbot.prediction import client, c180_jev_client as jev, http_bounds as bounds
from src.gridbot.prediction import rate_limit as rate


@pytest.fixture(params=['runtime', 'frozen'])
def modules(request, monkeypatch):
    if request.param == 'runtime':
        return client, rate
    root = Path(__file__).resolve().parents[1] / 'prediction/experiments/c180-original-mix75-v1-bda3e5a85a98'
    monkeypatch.syspath_prepend(str(root))
    return importlib.import_module('frozen.prediction.client'), importlib.import_module('frozen.prediction.rate_limit')


@pytest.mark.parametrize('header', [None, '0', '-1', 'garbage', '1'])
def test_actual_count_wins_over_length(header):
    headers = {} if header is None else {'Content-Length': header}
    with pytest.raises(bounds.ResponseBodyError):
        bounds.read_bounded(io.BytesIO(b'x' * 1001), headers, 1000)


@pytest.mark.parametrize('encoding,compress', [('gzip', gzip.compress), ('deflate', zlib.compress)])
def test_compressed_count_and_decoded_count(encoding, compress):
    data = compress(b'x' * 10000)
    assert len(data) < 1000
    with pytest.raises(bounds.ResponseBodyError, match='decoded'):
        bounds.read_bounded(io.BytesIO(data), {'Content-Encoding': encoding}, 1000)
    assert bounds.read_bounded(io.BytesIO(compress(b'{}')), {'Content-Encoding': encoding}, 1000) == b'{}'
    # Empty/concatenated streams still consume the wire allowance.
    with pytest.raises(bounds.ResponseBodyError):
        bounds.read_bounded(io.BytesIO(compress(b'') * 100), {'Content-Encoding': encoding}, 1000)


def test_truncated_and_unsupported_compression():
    with pytest.raises(bounds.ResponseBodyError, match='Incomplete'):
        bounds.read_bounded(io.BytesIO(gzip.compress(b'{}')[:-3]), {'Content-Encoding': 'gzip'}, 1000)
    with pytest.raises(bounds.ResponseBodyError, match='Unsupported'):
        bounds.read_bounded(io.BytesIO(b'{}'), {'Content-Encoding': 'br'}, 1000)


def test_oversize_length_rejected_before_read():
    class Unreadable:
        def read(self, n):
            pytest.fail('oversize advertised response must not be read')
    with pytest.raises(bounds.ResponseBodyError):
        bounds.read_bounded(Unreadable(), {'Content-Length': '1001'}, 1000)


class Response(io.BytesIO):
    status = 200
    def __init__(self, data, headers=None):
        super().__init__(data)
        self.headers = headers or {}


@pytest.mark.parametrize('error', [False, True])
def test_urllib_caps_success_and_error_and_closes(modules, monkeypatch, error):
    mod, _ = modules
    monkeypatch.setattr(mod, 'PREDICTION_BODY_BYTES', 1000)
    monkeypatch.setattr(mod, 'ERROR_BODY_BYTES', 100)
    response = Response(b'x' * 1001, {'Content-Length': '1'})
    def open_fake(*args, **kwargs):
        if error:
            raise urllib.error.HTTPError('https://example.invalid', 429, 'limited', response.headers, response)
        return response
    monkeypatch.setattr(mod.urllib.request, 'urlopen', open_fake)
    result = mod.UrllibTransport().request('GET', 'https://example.invalid', headers={})
    assert result.body_error and result.body is None
    assert result.status_code == (429 if error else 200)
    assert response.closed


def test_requests_streams_compressed_raw_and_closes(modules):
    mod, _ = modules
    raw = io.BytesIO(gzip.compress(b'{"ok":true}'))
    seen = {}
    response = SimpleNamespace(raw=raw, status_code=200, headers={'Content-Encoding': 'gzip'}, close=lambda: seen.update(closed=True))
    class Session:
        def request(self, *args, **kwargs):
            seen.update(kwargs)
            return response
    result = mod.RequestsTransport(Session()).request('GET', 'https://example.invalid', headers={})
    assert result.json() == {'ok': True}
    assert seen['stream'] is True and seen['closed'] is True
    assert raw.decode_content is False


def test_oversized_429_still_persists_ban(modules, tmp_path):
    mod, limiter = modules
    transport = SimpleNamespace(request=lambda *args, **kwargs: mod.TransportResponse(429, None, {'Retry-After': '3600'}, 'too large'))
    obj = mod.BinancePredictionClient('', '', transport=transport)
    obj.request_budget = limiter.SharedRequestBudget(tmp_path / 'budget.db', clock_ms=lambda: 1000)
    with pytest.raises(mod.PredictionTransportError):
        obj._request('/anything', signed=False)
    assert not obj.request_budget.acquire(priority='exit')
    assert obj.request_budget.health()['backoff_until_ms'] == 3601000


@pytest.mark.parametrize('value', ['1e1000', '9223372036854775807000', '9'*200])
def test_enormous_retry_after_is_durable_fail_fast(modules, tmp_path, value):
    _, limiter = modules
    path = tmp_path / 'budget.db'
    obj = limiter.SharedRequestBudget(path, clock_ms=lambda: 1000)
    obj.note_response(418, {'Retry-After': value})
    assert obj.health()['backoff_until_ms'] == limiter.MAX_DEADLINE_MS
    restarted = limiter.SharedRequestBudget(path, clock_ms=lambda: 86401000)
    for priority in ['normal', 'management', 'exit']:
        assert not restarted.acquire(priority=priority)
    assert not restarted.can_send_prepaid()
    local = limiter.PredictionRateLimiter(clock_ms=lambda: 1000, sleep=lambda _: pytest.fail('must not sleep through ban'))
    local.note_rate_limit(value)
    assert not local.acquire(emergency=True)


@pytest.mark.parametrize('value', ['nan', 'inf', '-inf', '-1', '', 'invalid'])
def test_invalid_retry_after_falls_back_and_recovers(modules, tmp_path, value):
    _, limiter = modules
    clock = [1000]
    path = tmp_path / 'budget.db'
    obj = limiter.SharedRequestBudget(path, clock_ms=lambda: clock[0])
    obj.note_response(429, {'Retry-After': value})
    assert obj.health()['backoff_until_ms'] == 61000
    clock[0] = 60999
    restarted = limiter.SharedRequestBudget(path, clock_ms=lambda: clock[0])
    assert not restarted.acquire(priority='exit')
    clock[0] = 61000
    assert restarted.acquire(priority='exit')


def test_retry_after_http_date_and_success_do_not_lift_ban(modules, tmp_path):
    _, limiter = modules
    obj = limiter.SharedRequestBudget(tmp_path / 'budget.db', clock_ms=lambda: 1000)
    obj.note_response(429, {'Retry-After': 'Thu, 01 Jan 1970 00:02:00 GMT'})
    obj.note_response(200, {})
    assert obj.health()['backoff_until_ms'] == 120000
    assert not obj.acquire(priority='exit')


@pytest.mark.parametrize('old', ['NaN', float('inf'), -1, 1e100])
def test_corrupt_persisted_deadline_is_fail_closed_on_restart(modules, tmp_path, old):
    _, limiter = modules
    path = tmp_path / 'budget.db'
    limiter.SharedRequestBudget(path, clock_ms=lambda: 1000)
    with sqlite3.connect(path) as db:
        db.execute('UPDATE weight_meta SET backoff_until_ms=?', (old,))
    restarted = limiter.SharedRequestBudget(path, clock_ms=lambda: 1000)
    assert restarted.health()['backoff_until_ms'] == limiter.MAX_DEADLINE_MS
    assert not restarted.acquire(priority='exit')


class AsyncStream:
    def __init__(self, data):
        self.stream = io.BytesIO(data)
    async def read(self, n):
        return self.stream.read(n)


@pytest.mark.asyncio
async def test_jev_disables_auto_decompression_and_rejects_bomb(monkeypatch):
    monkeypatch.setattr(jev, '_frozen_state', lambda _: ({}, 0, 'hash'))
    seen = {}
    class AsyncResponse:
        status = 200
        headers = {'Content-Encoding': 'gzip'}
        content = AsyncStream(gzip.compress(b'x' * (bounds.JEV_BODY_BYTES + 1)))
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            seen['closed'] = True
    class Session:
        def post(self, *args, **kwargs):
            seen.update(kwargs)
            return AsyncResponse()
    result = await jev.infer_original_jev_p_up(Session(), api_key='test-key', frozen_state={}, now_ms=lambda: 1000)
    assert result.status == 'transport_error'
    assert seen['auto_decompress'] is False and seen['closed'] is True


def test_klines_body_cap_returns_unavailable(monkeypatch, tmp_path):
    from src.gridbot.prediction import regime_feature_service as feature
    response = Response(b' ' * (bounds.KLINES_BODY_BYTES + 1))
    monkeypatch.setattr(feature.urllib.request, 'urlopen', lambda *a, **kw: response)
    db = feature.connect(tmp_path / 'features.db')
    try:
        assert feature.collect_once(db, 0, clock=lambda: 121000) == 'unavailable:ResponseBodyError'
        assert db.execute('SELECT count(*) FROM features').fetchone()[0] == 0
    finally:
        db.close()
    assert response.closed


def test_fractional_retry_after_rounds_up():
    assert rate.retry_after_deadline('1.0001', 1000) == 2001


def test_bounded_wait_with_stalled_clock():
    sleeps = []
    local = rate.PredictionRateLimiter(clock_ms=lambda: 1000, sleep=sleeps.append)
    local.note_rate_limit(1000)
    assert not local.acquire()
    assert sleeps == [1.0]
