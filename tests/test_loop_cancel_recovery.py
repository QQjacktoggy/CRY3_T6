"""Denied cancellation keeps settlement alive without reopening Live admission."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.gridbot.prediction.models import Campaign, CampaignState, MarketInfo, Position
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.worker import PredictionWorker, WorkerHeartbeat
from test_t63 import S


async def setup_worker(tmp_path, *, shares='2', state=CampaignState.FINAL_HOLD):
    repo = PredictionRepository(tmp_path/'prediction.sqlite3')
    await repo.initialize()
    row = await repo.start_loop('existing', 100, mode='LIVE', strategy_profile='regime_target6_7a_v1')
    market = MarketInfo('topic', 'up', 'test', S, S+300000,
                        up_market_id='up', down_market_id='down')
    campaign = Campaign('held', market, state=state, position=Position(up_shares=Decimal(shares)))
    await repo.save_campaign(campaign, loop_id='existing')
    worker = object.__new__(PredictionWorker)
    worker.repository = repo
    worker.settings = SimpleNamespace(wallet_address='test-wallet', poll_interval_seconds=.01)
    worker.client = SimpleNamespace(query_positions=Mock(), place_order=AsyncMock())
    worker._lock = asyncio.Lock()
    worker._task = None
    worker._loop_id = 'existing'
    worker._loop_created_at_ms = row['created_at_ms']
    worker._target_markets = 100
    worker._active_campaigns = {campaign.campaign_id: campaign}
    worker._accept_new_markets = worker._allow_new_orders = worker._allow_new_buys = True
    worker._allow_reductions = True
    worker._hard_stop_latched = worker._cancel_requested = False
    clock = [S+299000]
    worker._now_ms = lambda: clock[0]
    worker.heartbeat = WorkerHeartbeat(clock[0])
    worker._selected_strategy_profile = 'regime_target6_7a_v1'
    worker._status = Mock(return_value={})
    worker.status = AsyncMock(return_value={})
    worker.reconcile = AsyncMock(return_value={'known': True, 'orders': 0})
    worker._query_active_order_rows = AsyncMock(return_value=[])
    worker._call_api = AsyncMock(return_value=[{'shares': '2', 'marketId': 'up'}])
    worker._begin_shadow_tick = Mock()
    worker._check_adaptive_jump_stop = AsyncMock()
    worker._check_loop_loss_guard = AsyncMock()
    worker._persist_heartbeat = AsyncMock()
    worker._run_market_once = AsyncMock(side_effect=AssertionError('new market admission forbidden'))
    worker._observe_campaign = Mock()
    worker._trace_event = Mock()
    worker._settlement_attempt_at_ms = {}
    worker._settlement_poll_cadence_ms = 0
    worker._shadow_lane_experiment_enabled = Mock(return_value=False)
    worker._settle_p3_live_lane = AsyncMock()
    worker._settle_baseline_lane = AsyncMock()
    worker._clear_v3_gate_history = Mock()

    async def official_settlement(campaign):
        campaign.state = CampaignState.DONE
        campaign.position.up_shares = Decimal(0)
        await repo.save_campaign(campaign)
        return {'status': 'SETTLED', 'winner': 'UP'}

    worker.settle_campaign = AsyncMock(side_effect=official_settlement)
    return repo, worker, campaign, clock, row


async def cleanup(repo, worker):
    task = worker._task
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    await repo.close()


def assert_admission_closed(worker):
    assert worker._hard_stop_latched
    assert not worker._accept_new_markets
    assert not worker._allow_new_orders and not worker._allow_new_buys
    assert worker._allow_reductions
    assert not worker._cancel_requested
    worker._run_market_once.assert_not_awaited()
    worker.client.place_order.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('prior_task', ['none', 'done'])
async def test_open_position_denial_restores_normal_settlement_without_new_loop_or_buy(tmp_path, prior_task):
    repo, worker, campaign, clock, original_loop = await setup_worker(tmp_path)
    try:
        if prior_task == 'done':
            worker._task = asyncio.create_task(asyncio.sleep(0))
            await worker._task
        result = await worker.cancel_loop()
        assert result['action_denied'] and result['open_position_count'] == 1
        assert worker._task is not None
        assert_admission_closed(worker)
        assert worker._loop_id == original_loop['loop_id']
        assert worker._target_markets == original_loop['target']
        assert worker._loop_created_at_ms == original_loop['created_at_ms']
        clock[0] = S+310000
        await asyncio.wait_for(worker._task, timeout=1)
        worker.settle_campaign.assert_awaited_once_with(campaign)
        assert not worker._active_campaigns
        assert campaign.state == CampaignState.DONE
        assert_admission_closed(worker)
        assert (await repo.get_runtime_config('prediction_risk_state'))['hard_stop_latched']
        loops = await repo._fetchall('SELECT loop_id,target,state,new_entries_stopped FROM prediction_loops')
        assert loops == [{'loop_id': 'existing', 'target': 100, 'state': 'HARD_STOP', 'new_entries_stopped': 1}]
        assert not await repo._fetchall('SELECT 1 FROM prediction_order_intents')
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_timeout_denial_keeps_existing_shielded_task_without_duplicate(tmp_path):
    repo, worker, _, _, _ = await setup_worker(tmp_path)
    release = asyncio.Event()
    original_task = asyncio.create_task(release.wait())
    worker._task = original_task
    worker._run_loop = AsyncMock()
    try:
        with patch('src.gridbot.prediction.worker.asyncio.wait_for', AsyncMock(side_effect=TimeoutError)):
            result = await worker.cancel_loop()
        assert result['action_denied'] and 'quiesce' in result['reason']
        assert worker._task is original_task and not original_task.done()
        worker._run_loop.assert_not_called()
        assert_admission_closed(worker)
        assert (await repo.get_loop('existing'))['new_entries_stopped'] == 1
    finally:
        release.set()
        await original_task
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_unknown_denial_keeps_reconciliation_barrier_and_does_not_settle_or_buy(tmp_path):
    repo, worker, campaign, clock, _ = await setup_worker(tmp_path, shares='0')
    try:
        campaign.pending_intent_id = 'unknown'
        campaign.pending_unknown = True
        await repo.save_campaign(campaign)
        await repo._execute('INSERT INTO prediction_order_intents '
            '(intent_id,campaign_id,action,outcome,order_side,amount,limit_price,created_at_ms,ttl_ms,status,unknown,payload_json) '
            'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
            ('unknown', 'held', 'BUY_INITIAL', 'UP', 'BUY', '1', '.4', S+124000, 2000, 'UNKNOWN', 1, '{}'))
        worker.reconcile = AsyncMock(return_value={'known': False, 'orders': 0, 'unresolved': 1})
        reconciled = asyncio.Event()

        async def retain_unknown(*args):
            reconciled.set()
            return None

        worker.reconcile_intent_without_order_id = AsyncMock(side_effect=retain_unknown)
        result = await worker.cancel_loop()
        assert result['action_denied'] and 'clean exchange reconciliation' in result['reason']
        assert worker._task is not None
        clock[0] = S+310000
        await asyncio.wait_for(reconciled.wait(), timeout=1)
        assert_admission_closed(worker)
        worker.settle_campaign.assert_not_awaited()
        assert campaign.pending_unknown and campaign.pending_intent_id == 'unknown'
        unresolved = await repo.load_unresolved_intents()
        assert len(unresolved) == 1 and unresolved[0]['status'] == 'UNKNOWN' and unresolved[0]['unknown'] == 1
        assert (await repo.get_loop('existing'))['new_entries_stopped'] == 1
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('state,shares', [(CampaignState.OBSERVE, '0'), (CampaignState.DONE, '2')])
async def test_denial_without_nonterminal_known_exposure_does_not_spawn_management(tmp_path, state, shares):
    repo, worker, _, _, _ = await setup_worker(tmp_path, shares=shares, state=state)
    worker._run_loop = AsyncMock()
    try:
        result = await worker.cancel_loop()
        assert result['action_denied'] and result['open_position_count'] == 1
        assert_admission_closed(worker)
        assert worker._task is None
        worker._run_loop.assert_not_called()
        worker.settle_campaign.assert_not_awaited()
        assert (await repo.get_loop('existing'))['state'] == 'RUNNING'
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_clean_cancellation_remains_terminal_without_restarting_management(tmp_path):
    repo, worker, campaign, _, _ = await setup_worker(tmp_path, shares='0')
    worker._run_loop = AsyncMock()
    worker._call_api = AsyncMock(return_value=[])
    try:
        result = await worker.cancel_loop()
        assert result['loop_cancelled']
        assert worker._task is None and worker._loop_id is None
        assert not worker._cancel_requested and not worker._hard_stop_latched
        assert not worker._accept_new_markets and not worker._allow_new_buys
        worker._run_loop.assert_not_called()
        assert campaign.state == CampaignState.CANCELLED
        assert (await repo.get_loop('existing'))['state'] == 'CANCELLED'
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_denial_with_missing_durable_loop_identity_does_not_start_task(tmp_path):
    repo, worker, _, _, _ = await setup_worker(tmp_path)
    worker._run_loop = AsyncMock()
    repo.get_active_loop = AsyncMock(return_value={'loop_id': ''})
    try:
        result = await worker.cancel_loop()
        assert result['action_denied'] and 'no durable loop id' in result['reason']
        assert_admission_closed(worker)
        assert worker._task is None
        worker._run_loop.assert_not_called()
    finally:
        await cleanup(repo, worker)
