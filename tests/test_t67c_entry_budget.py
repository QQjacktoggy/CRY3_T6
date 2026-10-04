"""Causal book retries, diagnostic refusals and historical BUY lookup safety."""
import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.c180_worker_bridge import C180Ready
from src.gridbot.prediction.client import (
    BinancePredictionClient, PredictionEntryNotSubmitted, TransportResponse,
    entry_http_guard_scope, request_timing_scope,
)
from src.gridbot.prediction.models import ActionType, Campaign, CampaignState, OrderSide, OutcomeSide
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.repository import MIGRATIONS_DIR, PredictionRepository
from src.gridbot.prediction.strategy import StrategyDecision
from src.gridbot.prediction.worker import PredictionWorker
from test_t63 import S, feature, book
from test_t67c import setup, state
from test_t6_entry_critical_path import EntryHarness, LedgerFixture, START, D


@pytest.mark.parametrize('failure,reason', [
    ('missing', 't67c_book_missing'),
    ('identity', 't67c_book_identity_or_depth_invalid'),
    ('receipt_future', 't67c_book_receipt_future'),
    ('receipt_stale', 't67c_book_receipt_stale'),
    ('freshness', 't67c_fresh_book_required'),
    ('price', 't67c_execution_price_band'),
    ('depth', 't67c_execution_insufficient_depth'),
    ('unit', 't67c_frozen_identity_unit_fee_mismatch'),
])
def test_selected_refusal_is_specific_and_never_changes_frozen_selection(tmp_path, failure, reason):
    bridge, check = setup(tmp_path, feature(2,-1,2), book('.30','.70',124000))
    original = check()
    assert original.allowed
    frozen = state(bridge)
    current = book('.29','.71',125000)
    unit = 1
    at = S+125000
    if failure == 'missing': current = None
    elif failure == 'identity': current['market_topic'] = 'other'
    elif failure == 'receipt_future': current['received_at_ms'] += 1
    elif failure == 'receipt_stale': at += 2001
    elif failure == 'freshness': at += 1001
    elif failure == 'price': current['quote']['UP']['ask_levels'] = [['.60','100']]
    elif failure == 'depth': current['quote']['UP']['ask_levels'] = [['.29','.01']]
    elif failure == 'unit': unit = 2
    # The selected path must remain read-only even while another writer holds WAL.
    with closing(sqlite3.connect(bridge.feature_db)) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('BEGIN IMMEDIATE')
        with patch.object(b,'read_c180_book',return_value=current), patch.object(
                b,'connect',side_effect=AssertionError('write during selected refresh')):
            result = bridge.check_signal(market=SimpleNamespace(start_time_ms=S,
                market_topic_id='topic',up_market_id='up'), unit_usdt=unit,
                at_ms=at,last_seen_book_at_ms=original.book_at_ms)
        assert not result.allowed and result.reason == reason
        assert state(bridge) == frozen


def harness():
    worker = PredictionWorker.__new__(PredictionWorker)
    worker.clock = S+124998
    worker._now_ms = lambda: worker.clock
    worker._selected_strategy_profile = 'regime_target6_7c_v1'
    worker._selected_order_unit_usdt = 1
    campaign = SimpleNamespace(market=SimpleNamespace(start_time_ms=S,
        market_topic_id='topic',up_market_id='up'))
    return worker, campaign


@pytest.mark.asyncio
async def test_initial_book_arriving_between_one_second_ticks_can_freeze_inside_window(tmp_path):
    worker, campaign = harness()
    initial = book('.30','.70',125183)
    bridge, _ = setup(tmp_path, feature(2,-1,2), initial)
    times = []
    def read(*args):
        times.append(worker.clock)
        return initial if worker.clock >= S+125183 else None
    async def sleep(seconds): worker.clock += round(seconds*1000)
    with patch.object(b,'read_c180_book',side_effect=read), patch.object(
            bridge,'_first_book',return_value=initial), patch.object(
            b,'read_c180_signal',return_value=None), patch(
            'src.gridbot.prediction.worker.asyncio.sleep',side_effect=sleep):
        ready = await worker._entry_signal_within_window(bridge,campaign,S)
    assert ready.allowed and ready.signal.entry.side == 'UP'
    assert times == [S+124998,S+125098,S+125198]
    assert state(bridge)['selected_at_ms'] == S+125198
    assert ready.execution.expires_at_ms == S+136000


@pytest.mark.asyncio
async def test_initial_retry_does_not_call_bridge_after_delayed_wakeup():
    worker, campaign = harness()
    bridge = SimpleNamespace(check_signal=Mock(return_value=C180Ready(False,'t67c_book_missing')))
    async def sleep(seconds): worker.clock = S+126001
    with patch('src.gridbot.prediction.worker.asyncio.sleep',side_effect=sleep):
        ready = await worker._entry_signal_within_window(bridge,campaign,S)
    assert not ready.allowed
    assert bridge.check_signal.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('reason', ['t67c_execution_price_band','t67c_execution_insufficient_depth',
    't67c_execution_ev','t67c_book_receipt_future','t67c_book_identity_or_depth_invalid',
    't67c_frozen_identity_unit_fee_mismatch','t67c_no_live_candidate:initial_window_missed'])
async def test_structural_rejection_is_not_retried(reason):
    worker, campaign = harness()
    bridge = SimpleNamespace(check_signal=Mock(return_value=C180Ready(False,reason)))
    ready = SimpleNamespace(book_at_ms=S+124000,execution=SimpleNamespace(expires_at_ms=S+126000))
    with patch('src.gridbot.prediction.worker.asyncio.sleep') as sleep:
        result = await worker._entry_refresh_within_deadline(bridge,campaign,ready)
        assert result.reason == reason
        sleep.assert_not_called()
    assert bridge.check_signal.call_count == 1


@pytest.mark.asyncio
async def test_selected_retry_keeps_side_cap_and_two_second_expiry(tmp_path):
    worker, campaign = harness()
    bridge, check = setup(tmp_path,feature(2,-1,-2),book('.60','.40',124000))
    ready = check()
    frozen = state(bridge)
    assert ready.execution.expires_at_ms == S+126000
    worker.clock = S+125100
    current = book('.61','.39',125183)
    def read(*args): return current if worker.clock >= S+125183 else book('.60','.40',124000)
    async def sleep(seconds): worker.clock += round(seconds*1000)
    with patch.object(b,'read_c180_book',side_effect=read), patch.object(
            b,'connect',side_effect=AssertionError('selected retry wrote')), patch(
            'src.gridbot.prediction.worker.asyncio.sleep',side_effect=sleep):
        fresh = await worker._entry_refresh_within_deadline(bridge,campaign,ready)
    assert fresh.allowed and fresh.signal == ready.signal
    assert fresh.execution.expires_at_ms == ready.execution.expires_at_ms
    assert fresh.execution.worst_ask_limit == ready.execution.worst_ask_limit
    assert state(bridge) == frozen


@pytest.mark.asyncio
async def test_retry_never_extends_expiry_or_calls_bridge_after_oversleep():
    worker, campaign = harness()
    worker.clock = S+125980
    ready = SimpleNamespace(book_at_ms=S+124000,execution=SimpleNamespace(expires_at_ms=S+126000))
    bridge = SimpleNamespace(check_signal=Mock(return_value=C180Ready(False,'t67c_fresh_book_required')))
    waits = []
    async def sleep(seconds):
        waits.append(seconds)
        worker.clock = S+126010
    with patch('src.gridbot.prediction.worker.asyncio.sleep',side_effect=sleep):
        result = await worker._entry_refresh_within_deadline(bridge,campaign,ready)
    assert not result.allowed and result.reason == 'execution_expired_during_book_wait'
    assert bridge.check_signal.call_count == 1 and waits == [.02]


@pytest.mark.asyncio
async def test_populated_database_migration_is_index_only_and_survives_reopen(tmp_path):
    repo = PredictionRepository(tmp_path/'history.db')
    await repo.initialize()
    try:
        await repo._execute("INSERT INTO prediction_campaigns(campaign_id,market_topic_id,market_id,"
            "start_time_ms,end_time_ms,state,payload_json,created_at_ms,updated_at_ms) "
            "VALUES('historical','topic','up',1,2,'DONE','{}',1,2)")
        await repo._execute('DROP INDEX idx_prediction_campaigns_market_buy_lookup')
        await repo._execute("DELETE FROM prediction_migrations WHERE filename='027_campaign_market_buy_lookup.sql'")
        before = [dict(r) for r in await repo._fetchall('SELECT * FROM prediction_campaigns')]
        await repo.close()
        await repo.initialize()
        assert [dict(r) for r in await repo._fetchall('SELECT * FROM prediction_campaigns')] == before
        indexes = await repo._fetchall('PRAGMA index_info(idx_prediction_campaigns_market_buy_lookup)')
        assert [r['name'] for r in indexes] == ['market_topic_id','start_time_ms','campaign_id']
        await repo.close()
        await repo.initialize()
        assert len(await repo._fetchall('PRAGMA index_info(idx_prediction_campaigns_market_buy_lookup)')) == 3
    finally: await repo.close()


class HistoricalBuyLookupTests(LedgerFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # The legacy ledger fixture uses a minimal schema; install the indexes
        # from the real baseline migration before checking the production plan.
        await self.repo._require_conn().executescript((MIGRATIONS_DIR/'001_initial.sql').read_text())
        await self.repo._require_conn().executescript(
            (MIGRATIONS_DIR/'027_campaign_market_buy_lookup.sql').read_text())

    async def test_real_claim_plan_avoids_full_intent_and_fill_scans(self):
        statements = []
        conn = self.repo._require_conn()
        await conn.set_trace_callback(statements.append)
        try: assert (await self.reserve()).claimed
        finally: await conn.set_trace_callback(None)
        sql = next(s for s in statements if 'SELECT 1 AS found FROM prediction_regime_entry_claims' in s)
        plans = await self.repo._fetchall('EXPLAIN QUERY PLAN '+sql)
        details = [r['detail'] for r in plans]
        assert sum('COVERING INDEX idx_prediction_campaigns_market_buy_lookup' in d for d in details) == 2
        assert not any(d.startswith(('SCAN i','SCAN f')) for d in details), details

    async def test_rejected_buy_from_another_loop_still_blocks_same_official_market(self):
        await self.repo.save_campaign(Campaign('old',self.market),loop_id='closed-old-loop')
        old = {**self.intent,'campaign_id':'old','intent_id':'old-buy'}
        await self.repo.create_intent(old)
        await self.repo.update_intent('old-buy',status='REJECTED',unknown=False)
        result = await self.reserve()
        assert not result.claimed and result.reason == 'market_buy_already_claimed'
        assert not await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')

    async def test_other_official_market_does_not_block_same_start(self):
        other = replace(self.market,market_topic_id='eth-topic')
        await self.repo.save_campaign(Campaign('eth',other),loop_id='closed-eth-loop')
        await self.repo.create_intent({**self.intent,'campaign_id':'eth','intent_id':'eth-buy'})
        assert (await self.reserve()).claimed


@pytest.mark.parametrize('telemetry_raises', [False,True])
def test_journal_expiry_guard_still_prevents_http_and_telemetry_is_safe(telemetry_raises):
    transport = Mock()
    client = BinancePredictionClient('offline-key','offline-secret',transport=transport)
    spans = []
    order = []
    class Budget:
        def acquire(self,*args,**kwargs): return True
        def begin_request(self): order.append('journal'); return object()
        def transport_failed(self,token): order.append('failed')
    client.request_budget = Budget()
    def timing(stage,ns):
        spans.append((stage,ns))
        if telemetry_raises: raise ValueError('telemetry unavailable')
    def guard():
        order.append('guard')
        raise PredictionEntryNotSubmitted('expired')
    with request_timing_scope(timing), entry_http_guard_scope(guard):
        with pytest.raises(PredictionEntryNotSubmitted,match='expired'):
            client._request('/sapi/v1/w3w/wallet/prediction/trade/place-order-bundle','POST')
    transport.request.assert_not_called()
    assert order == ['journal','guard','failed']
    assert [s for s,_ in spans] == ['shared_weight_admission','request_signing',
        'request_journal','final_http_admission']
    assert all(ns >= 0 for _,ns in spans)


@pytest.mark.asyncio
async def test_worker_durable_admission_spans_are_buffered_without_request_payloads():
    worker = EntryHarness(None,SimpleNamespace(profile='regime_target6_7c_v1'))
    worker._call_api = PredictionWorker._call_api.__get__(worker)
    worker._entry_durable_http_guard = Mock()
    worker.client = BinancePredictionClient('offline-key','offline-secret',transport=Mock())
    worker.client.transport.request.return_value = TransportResponse(200,{'orderId':'offline'}, {})
    # Use the native request through a small fake method; production final guard applies.
    worker.client.place_order = lambda **kw: worker.client._request(
        '/sapi/v1/w3w/wallet/prediction/trade/place-order-bundle','POST',params={'private':'do-not-log'})
    await worker._call_api('place_order',_entry_buy=True,_trace_campaign_id='c1',
        _entry_deadline_ms=worker.clock+1000,_entry_book_at_ms=worker.clock)
    rows = [payload for event,cid,payload in worker._observability_events if event=='entry_pre_http_stage']
    assert sum(e['stage']=='durable_buy_admission' for e in rows) == 2
    assert 'do-not-log' not in json.dumps(rows) and 'offline-secret' not in json.dumps(rows)


class SelectedRetryAdmissionTests(LedgerFixture):
    async def test_hs_during_post_claim_book_wait_keeps_single_durable_barrier_and_sends_no_order(self):
        profile = 'regime_target6_7c_v1'
        await self.repo._execute('UPDATE prediction_loops SET strategy_profile=?',(profile,))
        ledger = RegimeLiveLedger(self.repo,profile=profile)
        worker = EntryHarness(self.repo,ledger)
        campaign = Campaign('c1',self.market)
        worker._start_entry_attempt(campaign,worker.ready)
        fresh = worker.bridge.check_signal.return_value
        # Before claim: fresh. After claim: stale, then a fresh book.
        worker.bridge.check_signal.side_effect = [fresh,C180Ready(False,'t67c_fresh_book_required'),fresh]
        async def sleep(seconds):
            worker.clock += round(seconds*1000)
            await self.repo.set_runtime_config('prediction_risk_state',{'hard_stop_latched':True})
        decision = StrategyDecision(ActionType.BUY_INITIAL,CampaignState.INITIAL_PENDING,
            'c180 durable entry',outcome=OutcomeSide.UP,order_side=OrderSide.BUY,
            amount=D(1),limit_price=D('.35'),ttl_ms=2000,trade_allowed=True)
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms',side_effect=lambda:worker.clock),patch(
                'src.gridbot.prediction.worker.asyncio.sleep',side_effect=sleep):
            await worker._handle_decision(campaign,decision)
        assert not worker.sent
        intents = await self.repo._fetchall('SELECT * FROM prediction_order_intents')
        assert len(intents) == 1 and intents[0]['status']=='REJECTED'
        assert len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')) == 1
        assert worker.bridge.check_signal.call_count == 3
