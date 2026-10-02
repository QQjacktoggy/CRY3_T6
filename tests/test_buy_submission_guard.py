"""Offline races at the final BUY method and native HTTP boundaries."""
import asyncio
import json
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from src.gridbot.prediction.client import BinancePredictionClient, PredictionEntryNotSubmitted, TransportResponse
from src.gridbot.prediction.models import ActionType, Campaign, CampaignState, OrderSide, OutcomeSide, QuoteSnapshot
from src.gridbot.prediction.risk import RiskSnapshot
from src.gridbot.prediction.strategy import StrategyDecision
from src.gridbot.prediction.worker import PredictionWorker
from test_t6_entry_critical_path import EntryHarness, LedgerFixture, START, D


@pytest_asyncio.fixture
async def worker_env():
    fixture = LedgerFixture()
    await fixture.asyncSetUp()
    worker = EntryHarness(fixture.repo, fixture.ledger)
    worker._call_api = PredictionWorker._call_api.__get__(worker)
    worker._risk_snapshot = AsyncMock(return_value=RiskSnapshot())
    worker._persist_risk_state = AsyncMock()
    worker._selected_strategy_profile = 'baseline'
    await fixture.repo._execute("UPDATE prediction_loops SET strategy_profile='baseline'")
    try:
        yield worker, fixture.repo, Campaign('c1', fixture.market)
    finally:
        await fixture.asyncTearDown()


async def change_control(worker, repo, fault):
    if fault == 'local_stop':
        worker._allow_new_buys = False
    elif fault == 'local_hs':
        worker._hard_stop_latched = True
    elif fault == 'risk_hs':
        await repo.set_runtime_config('prediction_risk_state', {'hard_stop_latched': True})
    elif fault == 'legacy_hs':
        await repo.set_runtime_config('prediction_hard_stop_latched', {'latched': True})
    elif fault == 'pause':
        await repo._execute("UPDATE prediction_loops SET new_entries_stopped=1")
    elif fault == 'loop_hs':
        await repo._execute("UPDATE prediction_loops SET hard_stop_latched=1")
    elif fault == 'profile':
        worker._selected_strategy_profile = 'changed'
        await repo._execute("UPDATE prediction_loops SET strategy_profile='changed'")
    elif fault == 'missing_loop':
        worker._loop_id = 'absent'


@pytest.mark.asyncio
@pytest.mark.parametrize('lane', ['baseline', 'add', 'hedge', 'p3'])
@pytest.mark.parametrize('fault', ['local_stop', 'local_hs', 'risk_hs', 'legacy_hs', 'pause', 'loop_hs', 'profile', 'none'])
async def test_stop_while_quote_is_blocked_never_calls_order(worker_env, lane, fault):
    worker, repo, campaign = worker_env
    started = asyncio.Event()
    release = threading.Event()
    event_loop = asyncio.get_running_loop()
    def quote(**kwargs):
        event_loop.call_soon_threadsafe(started.set)
        if not release.wait(5):
            raise AssertionError('test did not release quote')
        return {'quoteId': 'offline-quote'}
    worker.client = SimpleNamespace(request_budget=None, get_quote=Mock(side_effect=quote),
                                    place_order=Mock(return_value={'orderId': 'offline-order'}))
    if lane == 'p3':
        worker._selected_strategy_profile = 'fav_p3'
        await repo._execute("UPDATE prediction_loops SET strategy_profile='fav_p3'")
        worker._fav_p3_arm_override = 'live'
        worker._p3_active_campaigns = {}
        worker._s3s5_engine = {START: {'legs': {'FAV': {'pending': {'side': 'UP', 'ask': '.65'}}}}}
        worker._p3_gate_evaluator = SimpleNamespace(evaluate=Mock(return_value=SimpleNamespace(
            allowed=True, cross_count=0, same_side_seconds=30, distance_bps=20)))
        worker._record_p3_lane_signal = AsyncMock(return_value=None)
        worker._poll_order_terminal = AsyncMock(return_value='FILLED')
        snapshot = QuoteSnapshot(worker.clock, up_bid=D('.64'), up_ask=D('.65'), down_bid=D('.34'),
                                 down_ask=D('.35'), btc_spot=D('101'), reference_price=D('100'))
        # This test targets execution admission, after the existing strategy gates.
        with patch('src.gridbot.prediction.s3s5_pair.quote_from_snapshot', return_value={}), \
             patch('src.gridbot.prediction.s3s5_pair.ask_allowed', return_value=True):
            task = asyncio.create_task(worker._manage_p3_live_lane(campaign, snapshot, worker.clock))
            try:
                await asyncio.wait_for(started.wait(), 5)
                await change_control(worker, repo, fault)
            finally:
                release.set()
            await task
        checked_campaign = worker._p3_active_campaigns['c1::p3']
    else:
        action = {'baseline': ActionType.BUY_INITIAL, 'add': ActionType.BUY_ADD, 'hedge': ActionType.BUY_HEDGE}[lane]
        decision = StrategyDecision(action, CampaignState.INITIAL_PENDING,
            'offline baseline entry', outcome=OutcomeSide.UP, order_side=OrderSide.BUY,
            amount=D(1), limit_price=D('.35'), ttl_ms=2000, trade_allowed=True)
        task = asyncio.create_task(worker._handle_decision(campaign, decision))
        try:
            await asyncio.wait_for(started.wait(), 5)
            await change_control(worker, repo, fault)
        finally:
            release.set()
        await task
        checked_campaign = campaign
    intents = await repo._fetchall('SELECT status, unknown, payload_json FROM prediction_order_intents')
    assert len(intents) == 1
    if fault == 'none':
        worker.client.place_order.assert_called_once()
        assert intents[0]['status'] == 'SUBMITTED'
    else:
        worker.client.place_order.assert_not_called()
        assert intents[0]['status'] == 'REJECTED'
        assert not intents[0]['unknown']
        assert json.loads(intents[0]['payload_json'])['not_submitted']
        assert checked_campaign.pending_intent_id is None
        assert not checked_campaign.pending_unknown
        saved = await repo.get_campaign(checked_campaign.campaign_id)
        assert saved['pending_intent_id'] is None and not saved['pending_unknown']


def order_args():
    return dict(_entry_buy=True, _weight_pre_acquired=True, _shared_pre_acquired=True,
        wallet_address='offline', wallet_id='offline', quote_id='offline', account_type='SPOT',
        order_type='LIMIT', time_in_force='GTC', slippage_bps=0)


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['budget', 'signing'])
@pytest.mark.parametrize('fault', ['local_stop', 'risk_hs', 'legacy_hs', 'pause'])
async def test_native_http_rechecks_controls_after_budget_and_signing(worker_env, boundary, fault):
    worker, repo, _ = worker_env
    transport = Mock()
    transport.request.return_value = TransportResponse(200, {'orderId': 'offline'}, {})
    worker.client = BinancePredictionClient('offline-key', 'offline-secret', transport=transport)
    def mutate():
        if fault == 'local_stop':
            worker._allow_new_buys = False
        else:
            with sqlite3.connect(repo.db_path) as db:
                if fault == 'pause':
                    db.execute('UPDATE prediction_loops SET new_entries_stopped=1')
                else:
                    key, payload = ('prediction_risk_state', {'hard_stop_latched': True}) if fault == 'risk_hs' else (
                        'prediction_hard_stop_latched', {'latched': True})
                    db.execute('INSERT OR REPLACE INTO prediction_runtime_config '
                               '(config_key,config_value_json,updated_at_ms) VALUES(?,?,?)',
                               (key, json.dumps(payload), worker.clock))
    class Budget:
        def can_send_prepaid(self):
            if boundary == 'budget': mutate()
            return True
        def begin_request(self): return object()
        def transport_failed(self, token): pass
        def note_response(self, *args, **kwargs): pass
    worker.client.request_budget = Budget()
    from src.gridbot.prediction.client import hmac_sha256_signature
    def sign(*args):
        result = hmac_sha256_signature(*args)
        if boundary == 'signing': mutate()
        return result
    with patch('src.gridbot.prediction.client.hmac_sha256_signature', side_effect=sign):
        with pytest.raises(PredictionEntryNotSubmitted):
            await worker._call_api('place_order', **order_args())
    transport.request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['risk_hs', 'legacy_hs', 'pause', 'missing_loop'])
async def test_custom_client_rejected_before_invocation_and_sell_remains_available(worker_env, fault):
    worker, repo, _ = worker_env
    worker.client = SimpleNamespace(place_order=Mock(return_value={'orderId': 'offline'}))
    await change_control(worker, repo, fault)
    with pytest.raises(PredictionEntryNotSubmitted):
        await worker._call_api('place_order', **order_args())
    worker.client.place_order.assert_not_called()
    worker._allow_new_buys = False
    worker._hard_stop_latched = True
    args = order_args()
    args['_entry_buy'] = False
    args['_emergency'] = True
    await worker._call_api('place_order', **args)
    worker.client.place_order.assert_called_once()
