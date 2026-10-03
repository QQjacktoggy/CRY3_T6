"""ETH isolation, parent parity, read-only capabilities and causal paper evidence."""
import copy
import json
import sqlite3
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.gridbot.prediction.eth_t67c_core import (
    choose, freeze_eth_features, identity, official_winner, validate_market, validate_spec,
)
from src.gridbot.prediction.eth_t67c_data import EthCatalog, ReadOnlyDataError, book_snapshot, kline_query, trade_urls
from src.gridbot.prediction.eth_t67c_policy import FINGERPRINT, PROFILE, SYMBOL
from src.gridbot.prediction.eth_t67c_service import EthShadow, next_resolution, process_lock, replay, shared_budget
from src.gridbot.prediction.eth_t67c_store import ShadowStore
from src.gridbot.prediction.eth_t67c_telegram import EthShadowTelegram, build_application
from src.gridbot.prediction.regime_lane import FINGERPRINT as BTC_FP
from src.gridbot.prediction.telegram import selectable_lanes_for_market
from src.gridbot.prediction.worker import PredictionWorker
from test_t67c import setup as btc_setup, state as btc_state

S = 1790532600000


def spec():
    # Synthetic fixtures are deliberately not an official ETH specification.
    return dict(verified=True, evidence_sha256='a'*64, symbol=SYMBOL, underlying='ETH',
                duration_ms=300000, oracle_provider='fixture-oracle', oracle_feed_id='fixture-ETH-USD',
                vendor='predict_fun', chain_id='56', tick_size='.01', min_cash_usdt='.90',
                min_shares='.01', share_step='.01', fee_bps='200')


def market():
    return dict(symbol=SYMBOL, underlying='ETH', l1Category='crypto', l2Category='up-down',
                marketTopicId='eth-topic', slug='eth-fixture', startDate=S, endDate=S+300000,
                variantData={'startPrice': '1000'}, vendor='predict_fun', chainId='56', status='OPEN',
                settlementOracle={'provider': 'fixture-oracle', 'feedId': 'fixture-ETH-USD'},
                tickSize='.01', minOrderAmount='.90', minShares='.01', shareStep='.01', feeRateBps='200',
                markets=[dict(marketId='eth-market', status='OPEN', outcomes=[
                    {'name': 'UP', 'tokenId': 'eth-up', 'index': 0},
                    {'name': 'DOWN', 'tokenId': 'eth-down', 'index': 1}])])


def candles(first=2, last=-1, prior=2):
    result = [[S-900000+i*60000, '100', '100', '100', '100', '1',
               S-900000+i*60000+59999] for i in range(17)]
    result[14][4] = str(D(100)*(1+D(str(prior))/10000))
    result[15][4] = str(D(100)*(1+D(str(first))/10000))
    result[16][1] = result[15][4]
    result[16][4] = str(D(result[16][1])*(1+D(str(last))/10000))
    return result


def book(up='.30', down='.70', offset=124000):
    return dict(identity(market(), spec(), S), full_depth=True, book_at_ms=S+offset,
                received_at_ms=S+offset, captured_at_ms=S+offset,
                quote={side: {'ask_levels': [[price, '100']]} for side, price in [('UP', up), ('DOWN', down)]})


def trade(at, seq, *, symbol=SYMBOL):
    return dict(e='aggTrade', s=symbol, T=at, E=at, a=seq, p='1000', q='1')


@pytest.fixture
def engine(tmp_path):
    store = ShadowStore(tmp_path/'eth-t67c-shadow', spec=spec())
    value = EthShadow(store, spec())
    value.trade('spot', 1, trade(S+1000, 1), S+1000)
    value.observe(S, S+120500, raw=market(), candles=candles())
    value.trade('spot', 1, trade(S+124000, 2), S+124000)
    yield value
    store.close()


def test_core_shadow_and_parent_parity(engine, tmp_path):
    engine.observe(S, S+124000, raw=market(), book=book())
    quote = engine.store.get('quotes', S)
    assert quote['branch'] == 'core_first_up' and quote['fill_status'] == 'PAPER_QUOTE_ONLY'
    assert quote['fingerprint'] == FINGERPRINT and quote['expires_at_ms'] == S+136000
    features = engine.store.get('features', S)
    parent_feature = dict(features, fingerprint=BTC_FP)
    parent_book = book()
    parent_book.update(market_topic='topic', market_id='up', received_at=S+124000)
    bridge, check = btc_setup(tmp_path/'parent', parent_feature, parent_book)
    # The parent temp feature collector creates its directory itself.
    ready = check()
    assert ready.allowed
    assert btc_state(bridge)['branch'] == quote['branch']
    assert str(ready.execution.expected_shares) == quote['net_shares']


@pytest.mark.parametrize('first,last,prior,up,down,branch', [
    (-2, 1, -2, '.70', '.30', 'core_first_down'),
    (2, '.2', 2, '.60', '.40', 'core_stall_down'),
    (2, -4, -2, '.30', '.70', 'core_c_down'),
    (-1, 3, 2, '.70', '.30', 'c_mirror_up_prior'),
    (2, -1, -2, '.60', '.40', 'shallow_retracement'),
])
def test_deterministic_t67c_branches(first, last, prior, up, down, branch):
    features = freeze_eth_features(S, candles(first, last, prior), S+120500)
    result = choose({}, features, book(up, down), identity(market(), spec(), S), spec(), S+124000)
    assert result['candidate']['branch'] == branch


def test_missing_original_is_unknown_not_empty():
    features = freeze_eth_features(S, candles(2, 1, 2), S+120500)
    result = choose({}, features, book('.8', '.2'), identity(market(), spec(), S), spec(), S+124000)
    assert result['reason'] == 'eth_original_probability_unavailable'
    assert result['terminal'] and 'core_guard' not in result and 'candidate' not in result


def test_reserved_core_never_falls_through_to_additions():
    features = freeze_eth_features(S, candles(), S+120500)
    initial = choose({}, features, book(), identity(market(), spec(), S), spec(), S+124000)
    guard = initial['core_guard']
    reserved = {'core_guard': guard}
    result = choose(reserved, features, book('.6', '.4', 125000), identity(market(), spec(), S), spec(), S+125000)
    assert not result['core_guard']['empty'] and 'candidate' not in result


@pytest.mark.parametrize('key,value', [('symbol', 'BTCUSDT'), ('underlying', 'BTC'), ('feeRateBps', '201'),
                                      ('tickSize', '.001'), ('minOrderAmount', '2'), ('shareStep', '.1'),
                                      ('vendor', 'unreviewed'), ('chainId', '1'), ('endDate', S+900000)])
def test_official_metadata_mismatch_fail_closed(key, value):
    raw = market(); raw[key] = value
    with pytest.raises(ValueError):
        validate_market(raw, spec(), S)


@pytest.mark.parametrize('key', ['settlementOracle', 'tickSize', 'minShares', 'feeRateBps', 'underlying'])
def test_official_metadata_missing_fail_closed(key):
    raw = market(); del raw[key]
    with pytest.raises((ValueError, KeyError)):
        validate_market(raw, spec(), S)


def test_unverified_spec_and_asset_backend_blocked(engine):
    with pytest.raises(ValueError): validate_spec(None)
    bad = spec(); bad['verified'] = False
    with pytest.raises(ValueError): validate_spec(bad)
    assert PROFILE not in PredictionWorker._selectable_strategy_profiles()
    assert not selectable_lanes_for_market('ETHUSDT')
    assert not selectable_lanes_for_market(None)
    assert 'regime_target6_7c_v1' in dict(selectable_lanes_for_market('BTCUSDT'))
    assert engine.preflight(require_live=True)['passed'] is False
    with pytest.raises(PermissionError): engine.set_shadow_mode(False)
    assert engine.set_shadow_mode(True)['mode'] == 'SHADOW'
    for capability in ('place_order', 'redeem', 'start_loop', 'get_quote', 'create_intent'):
        assert not hasattr(engine, capability)


def test_deadline_late_feature_and_missing_initial_book(engine):
    engine.observe(S, S+126001, raw=market(), book=book(offset=126001))
    row = engine.store.get('windows', S)
    assert row['state']['terminal'] and row['state']['reason'] == 'initial_window_missed'
    engine.observe(S, S+126100, raw=market(), book=book(offset=126100))
    assert engine.store.get('quotes', S) is None
    with pytest.raises(ValueError): freeze_eth_features(S, candles(), S+123001)
    with pytest.raises(ValueError): freeze_eth_features(S, candles(), S+119999)


@pytest.mark.parametrize('mutate', [
    lambda b: b.update(symbol='BTCUSDT'),
    lambda b: b.update(fingerprint=BTC_FP),
    lambda b: b.update(market_id='btc-market'),
    lambda b: b.update(book_at_ms=S+122999),
    lambda b: b.update(received_at_ms=S+124001),
    lambda b: b.update(full_depth=False),
    lambda b: b['quote']['UP'].update(ask_levels=[['.301', '100']]),
])
def test_invalid_book_never_becomes_candidate(engine, mutate):
    value = book(); mutate(value)
    engine.observe(S, S+124000, raw=market(), book=value)
    assert engine.store.get('quotes', S) is None


def test_disconnect_reconnect_does_not_reuse_opening_anchor(engine):
    engine.disconnect('spot', S+123500)
    engine.trade('spot', 2, trade(S+124000, 3), S+124000)
    engine.observe(S, S+124000, raw=market(), book=book())
    assert engine.store.get('quotes', S) is None
    assert any(d['code'] == 'eth_opening_anchor_or_generation_unavailable' for d in engine.store.report()['diagnostics'])
    with pytest.raises(ValueError): engine.trade('spot', 1, trade(S+1000, 10), S+1000)


def test_non_core_side_thin_depth_preserves_parent_core_and_empty_needs_both():
    thin = book(); thin['quote']['DOWN']['ask_levels'] = [['.70', '.01']]
    value = choose({}, freeze_eth_features(S, candles(), S+120500), thin, identity(market(), spec(), S), spec(), S+124000)
    assert value['candidate']['branch'] == 'core_first_up'
    # The same insufficient opposing depth cannot attest verified-empty core.
    with pytest.raises(ValueError):
        choose({}, freeze_eth_features(S, candles(2, -1, -2), S+120500), thin,
               identity(market(), spec(), S), spec(), S+124000)


def test_trade_sequences_clocks_and_cross_asset_rejected(engine):
    for packet in (trade(S+124000, 1), trade(S+124001, 3), trade(S+124000, 3, symbol='BTCUSDT')):
        with pytest.raises(ValueError): engine.trade('spot', 1, packet, S+124000)
    with pytest.raises(ValueError): engine.trade('spot', 0, trade(S+124000, 3), S+124000)


def test_quote_duplicate_restart_and_outcome_are_immutable(engine):
    engine.observe(S, S+124000, raw=market(), book=book())
    before = engine.store.get('quotes', S)
    engine.observe(S, S+124000, raw=market(), book=book())
    restarted = EthShadow(engine.store, spec())
    restarted.observe(S, S+125000, raw=market(), book=book('.4', '.6', 125000))
    assert engine.store.get('quotes', S) == before
    raw = market(); raw['status'] = raw['markets'][0]['status'] = 'RESOLVED'
    raw['markets'][0]['outcomes'][0]['winner'] = True
    engine.resolve(S, raw, S+301000)
    engine.resolve(S, raw, S+302000)
    outcome = engine.store.get('outcomes', S)
    assert outcome['winner'] == 'UP' and outcome['known_at_ms'] == S+301000
    assert outcome['fill_status'] == 'PAPER_QUOTE_ONLY'
    changed = copy.deepcopy(raw)
    changed['markets'][0]['outcomes'][0]['winner'] = False
    changed['markets'][0]['outcomes'][1]['winner'] = True
    with pytest.raises(ValueError): engine.resolve(S, changed, S+303000)


def test_draw_ambiguous_and_wrong_asset_resolution(engine):
    raw = market(); raw['status'] = raw['markets'][0]['status'] = 'RESOLVED'
    assert official_winner(raw) is None
    for o in raw['markets'][0]['outcomes']: o.update(winner=True, payout='.5')
    assert official_winner(raw) == 'DRAW'
    for o in raw['markets'][0]['outcomes']: o.pop('payout')
    assert official_winner(raw) is None
    raw['symbol'] = 'BTCUSDT'
    with pytest.raises(ValueError): engine.resolve(S, raw, S+301000)


def test_canonical_market_status_and_draw_price_schema():
    raw = market(); raw.pop('status'); raw['markets'][0].pop('status')
    with pytest.raises(ValueError): validate_market(raw, spec(), S)
    raw = market(); raw['markets'] = []
    with pytest.raises(ValueError): validate_market(raw, spec(), S)
    raw = market(); raw.pop('status'); raw['markets'][0]['status'] = 'RESOLVED'
    for outcome in raw['markets'][0]['outcomes']: outcome['price'] = '.5'
    assert official_winner(raw) == 'DRAW'


@pytest.mark.parametrize('change', [
    lambda m: m.update(settlementOracle=None),
    lambda m: m.update(markets=[None]),
    lambda m: m['markets'][0].update(outcomes=[None, None]),
    lambda m: m['variantData'].update(startPrice='Infinity'),
])
def test_malformed_metadata_is_diagnostic_not_collector_crash(engine, change):
    raw = market(); change(raw)
    engine.observe(S, S+124000, raw=raw, book=book())
    assert engine.store.get('quotes', S) is None
    assert engine.store.report()['diagnostics']


def test_feature_late_actual_receipt_and_50_to_145_second_gap(tmp_path):
    store = ShadowStore(tmp_path/'eth-t67c-shadow', spec=spec())
    try:
        engine = EthShadow(store, spec())
        engine.observe(S, S+50000, raw=market())
        engine.observe(S, S+124000, candles=candles(), feature_received_ms=S+123001)
        assert store.get('features', S) is None
        assert any(r['code'] == 'eth_feature_freeze_deadline_missed' for r in store.report()['diagnostics'])
        engine.observe(S, S+145000, raw=market(), book=book(offset=145000))
        assert store.get('windows', S)['state']['reason'] == 'initial_window_missed'
        assert store.get('quotes', S) is None
    finally: store.close()


def test_permanently_pending_first_outcome_does_not_starve_later(engine):
    later = engine.store.admit(S+300000)
    raw = market(); raw.update(startDate=S+300000, endDate=S+600000, marketTopicId='eth-later')
    later['identity'] = identity(raw, spec(), S+300000)
    engine.store.save_window(S+300000, later)
    assert next_resolution(engine, -1, S+601000)[0] == S
    assert next_resolution(engine, S, S+601000)[0] == S+300000
    assert next_resolution(engine, S+300000, S+601000)[0] == S


def test_quote_and_window_transaction_roll_back_together(engine):
    engine.store.db.execute("CREATE TEMP TRIGGER reject_window BEFORE UPDATE ON eth_shadow_windows BEGIN SELECT RAISE(ABORT,'fixture crash'); END")
    with pytest.raises(sqlite3.IntegrityError): engine.observe(S, S+124000, raw=market(), book=book())
    assert engine.store.get('quotes', S) is None
    assert 'candidate' not in engine.store.get('windows', S)['state']


def test_namespace_refuses_btc_symlink_config_and_double_process(tmp_path):
    btc = tmp_path/'prediction.sqlite3'
    with sqlite3.connect(btc) as db: db.execute('CREATE TABLE prediction_loops(id TEXT)')
    before = btc.read_bytes()
    with pytest.raises(ValueError): ShadowStore(tmp_path, spec=spec())
    root = tmp_path/'eth-t67c-shadow'; root.mkdir(); (root/'shadow.sqlite3').symlink_to(btc)
    with pytest.raises(ValueError): ShadowStore(root, spec=spec())
    assert btc.read_bytes() == before
    with pytest.raises(ValueError): shared_budget(btc)
    (root/'shadow.sqlite3').unlink(); root.rmdir()
    store = ShadowStore(root, spec=spec())
    try:
        with process_lock(root), pytest.raises(RuntimeError), process_lock(root): pass
        with pytest.raises(ValueError): ShadowStore(root, spec=spec(), windows=21)
        assert all(r[0].startswith('eth_shadow_') for r in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'"))
    finally: store.close()


def test_worker_gap_counts_skipped_windows_and_target_is_finite(tmp_path):
    store = ShadowStore(tmp_path/'eth-t67c-shadow', windows=3)
    try:
        engine = EthShadow(store)
        engine.observe(S, S+100)
        engine.observe(S+900000, S+900100)
        report = store.report()
        assert report['observed_windows'] == 3
        assert [w['market_start_ms'] for w in report['windows']] == [S, S+300000, S+600000]
        assert report['windows'][1]['state']['reason'] == 'worker_window_gap'
        assert report['paper_quotes'] == 0
    finally: store.close()


def test_readonly_transport_only_signed_allowlisted_gets():
    budget = Mock(); budget.acquire.return_value = True; budget.begin_request.return_value = object()
    transport = Mock(return_value=(200, {}, b'{"data":{"marketTopics":[]}}'))
    client = EthCatalog('fixture-key', 'fixture-secret', budget, clock_ms=lambda: S, transport=transport)
    assert client.markets() == {'marketTopics': []}
    url = transport.call_args.args[0]
    assert '/market/list?' in url and 'signature=' in url
    for endpoint in ('trade/get-quote', 'trade/place-order', 'redeem', '../trade/place-order'):
        with pytest.raises(PermissionError): client.read(endpoint, {})
    assert transport.call_count == 1
    budget.acquire.return_value = False
    with pytest.raises(ReadOnlyDataError): client.detail('eth-topic')
    assert transport.call_count == 1
    budget.acquire.return_value = True
    transport.side_effect = RuntimeError('https://secret/?token=fixture-secret')
    with pytest.raises(ReadOnlyDataError, match='^eth_catalog_transport_error$'): client.markets()


def test_collectors_use_eth_and_real_ws_clocks():
    assert all('ethusdt@aggTrade' in u for u in trade_urls(SYMBOL).values())
    assert 'symbol=ETHUSDT' in kline_query(SYMBOL, S)
    feed = SimpleNamespace(_market_id='eth-market', _book=dict(book_at_ms=S+124000,
        received_at_ms=S+124010, asks_levels=[['.3','100']], bids_levels=[['.29','100']]))
    value = book_snapshot(feed, market(), spec(), S, S+124020)
    assert value['book_at_ms'] == S+124000 and value['received_at_ms'] == S+124010
    assert value['quote']['DOWN']['ask_levels'] == [['0.71','100']]


@pytest.mark.asyncio
async def test_tg_auth_stale_and_repeated_callbacks_do_not_write(engine):
    service = EthShadowTelegram(engine, ['1'])
    service.authority._reply = AsyncMock()
    query = SimpleNamespace(data='eth_shadow:report:'+FINGERPRINT[:16], answer=AsyncMock())
    update = SimpleNamespace(effective_chat=SimpleNamespace(id=1), callback_query=query)
    before = list(engine.store.db.iterdump())
    await service.callback(update, None); await service.callback(update, None)
    assert list(engine.store.db.iterdump()) == before
    assert service.authority._reply.await_count == 2
    query.data = 'predict_shadow:confirm:old'
    await service.callback(update, None)
    assert '已失效' in service.authority._reply.await_args.args[1]
    update.effective_chat.id = 2
    await service.callback(update, None)
    assert '授權' in service.authority._reply.await_args.args[1]
    with pytest.raises(ValueError): build_application(engine, {'ETH_SHADOW_TELEGRAM_BOT_TOKEN':'same',
        'ETH_SHADOW_TELEGRAM_CHAT_IDS':'1','PREDICTION_TELEGRAM_BOT_TOKEN':'same'})


def test_replay_requires_asset_and_bounded_lines(engine, tmp_path):
    path = tmp_path/'replay.jsonl'
    path.write_text(json.dumps(dict(symbol='BTCUSDT', kind='observe', start=S, received_at_ms=S+124000)))
    with pytest.raises(ValueError): replay(engine, path)
    path.write_bytes(b'x'*262145)
    with pytest.raises(ValueError): replay(engine, path)


def test_replay_mode_clock_and_resume_are_bound(tmp_path):
    root = tmp_path/'eth-t67c-shadow'
    store = ShadowStore(root, input_mode='replay')
    path = tmp_path/'replay.jsonl'
    rows = [dict(symbol=SYMBOL, kind='observe', start=S, received_at_ms=S+100),
            dict(symbol=SYMBOL, kind='observe', start=S, received_at_ms=S+99)]
    path.write_text('\n'.join(json.dumps(r) for r in rows)+'\n')
    with pytest.raises(ValueError, match='clock_reversed'): replay(EthShadow(store), path)
    with pytest.raises(ValueError): ShadowStore(root, input_mode='collect')
    store.close()
    store = ShadowStore(tmp_path/'resume'/'eth-t67c-shadow')
    try:
        rows.pop()
        path.write_text(json.dumps(rows[0])+'\n')
        replay(EthShadow(store), path)
        before = list(store.db.iterdump())
        replay(EthShadow(store), path)
        assert list(store.db.iterdump()) == before
        path.write_text(json.dumps(dict(rows[0], received_at_ms=S+101))+'\n')
        with pytest.raises(ValueError, match='source_changed'): replay(EthShadow(store), path)
    finally: store.close()


def test_response_body_failure_and_transport_failure_release_shared_budget(tmp_path, monkeypatch):
    from src.gridbot.prediction.rate_limit import SharedRequestBudget
    from src.gridbot.prediction.eth_t67c_data import EthCatalog
    budget = SharedRequestBudget(tmp_path/'budget.sqlite3', clock_ms=lambda: S)
    client = EthCatalog('fixture', 'fixture', budget, clock_ms=lambda: S)
    def known_response_failure(url, headers, hook):
        hook(200, {})
        raise ValueError('body too large')
    monkeypatch.setattr(client, '_get', known_response_failure)
    with pytest.raises(ReadOnlyDataError): client.markets()
    assert budget.can_send_prepaid()
    client._transport = Mock(side_effect=OSError('fixture network failure'))
    with pytest.raises(ReadOnlyDataError): client.markets()
    assert budget.can_send_prepaid()


def test_replay_binds_source_before_first_event_crash(tmp_path, monkeypatch):
    store = ShadowStore(tmp_path/'eth-t67c-shadow')
    engine = EthShadow(store)
    source = tmp_path/'input.jsonl'
    source.write_text(json.dumps(dict(symbol=SYMBOL, kind='observe', start=S, received_at_ms=S+100))+'\n')
    monkeypatch.setattr(engine, 'observe', Mock(side_effect=RuntimeError('fixture crash')))
    with pytest.raises(RuntimeError): replay(engine, source)
    cursor = json.loads(store.db.execute('SELECT payload FROM eth_shadow_replay_cursor').fetchone()[0])
    assert cursor['ordinal'] == 0
    source.write_text(json.dumps(dict(symbol=SYMBOL, kind='observe', start=S, received_at_ms=S+101))+'\n')
    with pytest.raises(ValueError, match='source_changed'): replay(engine, source)
    store.close()


@pytest.mark.parametrize('failure', ['deferred', 'body', 'transport', 'ban'])
def test_public_klines_share_budget_reserves_and_response_journal(tmp_path, monkeypatch, failure):
    import io
    import urllib.error
    from src.gridbot.prediction import eth_t67c_data as data
    from src.gridbot.prediction.rate_limit import SharedRequestBudget
    budget = SharedRequestBudget(tmp_path/'budget.sqlite3', clock_ms=lambda: S)
    opener = Mock()
    monkeypatch.setattr(data.urllib.request, 'build_opener', Mock(return_value=opener))
    if failure == 'deferred':
        assert budget.acquire(900)
    elif failure == 'transport':
        opener.open.side_effect = OSError('fixture network failure')
    elif failure == 'ban':
        opener.open.side_effect = urllib.error.HTTPError('https://example.test', 429, 'fixture',
                                                       {'Retry-After': '60'}, io.BytesIO(b''))
    else:
        response = Mock(status=200, headers={})
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        opener.open.return_value = response
        monkeypatch.setattr(data, 'read_bounded', Mock(side_effect=ValueError('fixture oversized body')))
    with pytest.raises(ReadOnlyDataError): data.fetch_klines(S, budget=budget)
    if failure == 'deferred':
        opener.open.assert_not_called()
    else:
        assert budget.health()['used'] == 2
    assert budget.can_send_prepaid() == (failure != 'ban')


def test_frozen_btc_source_and_strategy_are_unchanged():
    import hashlib
    root = Path(__file__).resolve().parents[1]
    files = [p for p in (root/'prediction/experiments').rglob('*')
             if p.is_file() and '__pycache__' not in p.parts]
    files.extend(root/p for p in ['src/gridbot/prediction/regime_t67c_policy.py',
                 'src/gridbot/prediction/regime_t67c_bridge.py', 'src/gridbot/prediction/worker.py'])
    frozen = hashlib.sha256()
    for path in sorted(files, key=lambda p: p.relative_to(root).as_posix()):
        frozen.update(path.relative_to(root).as_posix().encode()+b'\0'+path.read_bytes()+b'\0')
    # SHA of the 30 protected files at baseline a3cd398; works with shallow CI checkouts.
    assert frozen.hexdigest() == 'e7d9777798ce4f1644a6687a4d43374246043457f699f6c03320dd9a596664e4'


@pytest.mark.parametrize('signum', ['SIGTERM', 'SIGINT', 'CANCEL', 'NATURAL'])
@pytest.mark.parametrize('response', ['success', 'transport', 'timeout', 'ban'])
def test_cli_signal_drains_armed_read_before_exit_without_poisoning_btc_budget(tmp_path, signum, response):
    import os
    import signal
    import subprocess
    import sys
    import time
    from src.gridbot.prediction.rate_limit import SharedRequestBudget
    script = r'''
import asyncio, json, os, sys, time
from pathlib import Path
from src.gridbot.prediction import eth_t67c_data as data, eth_t67c_service as svc
from src.gridbot.prediction.rate_limit import SharedRequestBudget
base, response, mode = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
SharedRequestBudget(base/'request-weight.sqlite3')  # Existing shared budget is fixture setup, not the ETH CLI.
db_stat = (base/'request-weight.sqlite3').stat()
journal_stat = (base/'request-weight.sqlite3.cooldown').stat()
budget_identity = f'{db_stat.st_dev}:{db_stat.st_ino}:{journal_stat.st_dev}:{journal_stat.st_ino}'
original = data.EthCatalog
def transport(url, headers):
    with (base/'calls').open('a') as stream: stream.write('GET\n')
    (base/'ready').touch()  # The real shared journal is already armed.
    deadline = time.monotonic()+10
    while not (base/'release').exists():
        if time.monotonic() > deadline: raise RuntimeError('fixture timeout')
        time.sleep(.01)
    if response == 'transport': raise OSError('fixture transport failure')
    if response == 'timeout': raise TimeoutError('fixture socket timeout')
    if response == 'ban': return 429, {'Retry-After':'60'}, b'{}'
    return 200, {}, json.dumps({'data':{'marketTopics':[{'symbol':'ETHUSDT','marketTopicId':'fixture'}]}}).encode()
data.EthCatalog = lambda key, secret, budget: original(key, secret, budget, transport=transport)
class NoNetwork:
    def __init__(self, *args, **kwargs): pass
    async def start(self): pass
    async def close(self): pass
data.PublicTape, data.book_feed = NoNetwork, NoNetwork
svc.now_ms = lambda: 1790532600000+50000
if mode == 'NATURAL':
    svc.now_ms = lambda: 1790532600000+(600000 if (base/'ready').exists() else 50000)
elif mode == 'CANCEL':
    original_collect = svc.collect
    async def cancelled_collect(engine, budget_path, grace_seconds, *, stop=None, budget_identity=None):
        task = asyncio.create_task(original_collect(engine, budget_path, grace_seconds, stop=stop,
                                                  budget_identity=budget_identity))
        while not (base/'ready').exists(): await asyncio.sleep(.01)
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass
    svc.collect = cancelled_collect
os.environ['PREDICTION_BINANCE_API_KEY'] = 'fixture'
os.environ['PREDICTION_BINANCE_API_SECRET'] = 'fixture'
svc.main(['--collect','--root',str(base/'eth-t67c-shadow'),
          '--shared-weight-db',str(base/'request-weight.sqlite3'),'--windows','1',
          '--expected-shared-budget-identity',budget_identity,
          '--resolution-grace-seconds','0'])
'''
    proc = subprocess.Popen([sys.executable, '-c', script, str(tmp_path), response, signum],
                            cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic()+5
        while not (tmp_path/'ready').exists():
            assert proc.poll() is None
            assert time.monotonic() < deadline
            time.sleep(.01)
        if signum.startswith('SIG'):
            os.kill(proc.pid, getattr(signal, signum))
        if signum == 'SIGKILL':
            proc.communicate(timeout=5)
            assert proc.returncode == -signal.SIGKILL
            budget = SharedRequestBudget(tmp_path/'request-weight.sqlite3')
            assert not budget.can_send_prepaid()  # Retain the unknown pending safety gate.
            assert list((tmp_path/'request-weight.sqlite3.cooldown').glob('request-*'))
            return
        time.sleep(.2)
        assert proc.poll() is None  # Must wait for the armed HTTP completion.
        if signum.startswith('SIG'):
            os.kill(proc.pid, getattr(signal, signum))  # Repeated signals still request graceful stop.
        (tmp_path/'release').touch()
        stdout, stderr = proc.communicate(timeout=5)
        assert proc.returncode == 0, stderr.decode()
        assert (tmp_path/'calls').read_text() == 'GET\n'  # Never start detail after stop.
        budget = SharedRequestBudget(tmp_path/'request-weight.sqlite3')
        assert budget.can_send_prepaid() == (response != 'ban')
        if response != 'ban':
            assert not list((tmp_path/'request-weight.sqlite3.cooldown').glob('request-*'))
        assert json.loads(stdout)['mode'] == 'SHADOW'
    finally:
        if proc.poll() is None:
            (tmp_path/'release').touch()
            proc.communicate(timeout=5)


def test_forced_kill_remains_a_shared_budget_coexistence_blocker(tmp_path):
    test_cli_signal_drains_armed_read_before_exit_without_poisoning_btc_budget(tmp_path, 'SIGKILL', 'success')


def test_shared_budget_requires_existing_complete_database_and_journal(tmp_path):
    from src.gridbot.prediction.rate_limit import SharedRequestBudget
    path = tmp_path/'request-weight.sqlite3'
    with pytest.raises(ValueError, match='missing_database'): shared_budget(path)
    assert not path.exists() and not Path(str(path)+'.cooldown').exists()
    path.touch()
    before = path.read_bytes()
    with pytest.raises(ValueError, match='foreign_database'): shared_budget(path)
    assert path.read_bytes() == before
    path.unlink()
    SharedRequestBudget(path)
    assert shared_budget(path).can_send_prepaid()
    db_stat, journal_stat = path.stat(), Path(str(path)+'.cooldown').stat()
    expected = {'database': [db_stat.st_dev, db_stat.st_ino], 'journal': [journal_stat.st_dev, journal_stat.st_ino]}
    assert shared_budget(path, expected_identity=expected).can_send_prepaid()
    wrong = dict(expected, database=[db_stat.st_dev, db_stat.st_ino+1])
    before = path.read_bytes()
    with pytest.raises(ValueError, match='identity_mismatch'): shared_budget(path, expected_identity=wrong)
    assert path.read_bytes() == before
    (Path(str(path)+'.cooldown')/'ready').unlink()
    with pytest.raises(ValueError, match='journal_unverified'): shared_budget(path)
    assert not (Path(str(path)+'.cooldown')/'ready').exists()


@pytest.mark.parametrize('reference', [1000, '1000.0', '1.00000E3'])
def test_equal_numeric_reference_reconciles_but_changed_value_fails(engine, reference):
    terminal = market()
    terminal['variantData']['startPrice'] = reference
    terminal['markets'][0]['status'] = 'RESOLVED'
    terminal['markets'][0]['outcomes'][0]['winner'] = True
    engine.resolve(S, terminal, S+301000)
    assert engine.store.get('outcomes', S)['winner'] == 'UP'
    terminal['variantData']['startPrice'] = '1000.01'
    with pytest.raises(ValueError, match='identity_mismatch'): engine.resolve(S, terminal, S+302000)


@pytest.mark.asyncio
async def test_final_resolution_sweep_records_late_result_and_respects_shutdown(engine, monkeypatch):
    from src.gridbot.prediction import eth_t67c_service as svc
    monkeypatch.setattr(svc, 'now_ms', lambda: S+910000)
    terminal = market()
    terminal['markets'][0]['status'] = 'SETTLED'
    terminal['markets'][0]['outcomes'][0]['isWinner'] = True
    read_detail = AsyncMock(return_value=terminal)
    stop = svc.StopFlag()
    stop.set()
    await svc.final_resolution_pass(engine, read_detail, stop)
    read_detail.assert_not_awaited()
    await svc.final_resolution_pass(engine, read_detail, svc.StopFlag())
    assert engine.store.get('outcomes', S)['winner'] == 'UP'
    assert engine.store.get('outcomes', S)['known_at_ms'] == S+910000
    await svc.final_resolution_pass(engine, read_detail, svc.StopFlag())
    assert read_detail.await_count == 1


@pytest.mark.asyncio
async def test_finite_collector_runs_final_reconciliation_after_schedulers_stop(tmp_path, monkeypatch):
    from src.gridbot.prediction import eth_t67c_service as svc, eth_t67c_data as data
    from src.gridbot.prediction.rate_limit import SharedRequestBudget
    store = ShadowStore(tmp_path/'eth-t67c-shadow', spec=spec(), windows=1, input_mode='collect')
    engine = EthShadow(store, spec())
    engine.observe(S, S+50000, raw=market())
    terminal = market()
    terminal['variantData']['startPrice'] = '1000.0'
    terminal['markets'][0]['status'] = 'RESOLVED'
    terminal['markets'][0]['outcomes'][0]['winner'] = True
    catalog = SimpleNamespace(markets=Mock(side_effect=AssertionError('discovery must stop')),
                              detail=Mock(return_value=terminal))
    monkeypatch.setattr(data, 'EthCatalog', lambda *args: catalog)
    offline_feed = SimpleNamespace(start=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(data, 'PublicTape', lambda *args: offline_feed)
    monkeypatch.setattr(data, 'book_feed', lambda *args: offline_feed)
    monkeypatch.setattr(svc, 'now_ms', lambda: S+910000)
    budget_path = tmp_path/'request-weight.sqlite3'
    SharedRequestBudget(budget_path)
    db_stat, journal_stat = budget_path.stat(), Path(str(budget_path)+'.cooldown').stat()
    expected = {'database': [db_stat.st_dev, db_stat.st_ino], 'journal': [journal_stat.st_dev, journal_stat.st_ino]}
    try:
        await svc.collect(engine, budget_path, 0, budget_identity=expected)
        catalog.detail.assert_called_once_with('eth-topic')
        assert store.get('outcomes', S)['winner'] == 'UP'
    finally:
        store.close()
