"""Offline fault tests for shared T6 admission, durability and HTTP timing."""
import asyncio
import json
import sqlite3
import unittest
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from src.gridbot.prediction.client import (
    BinancePredictionClient, PredictionEntryNotSubmitted, TransportResponse,
    entry_http_guard_scope,
)
from src.gridbot.prediction.models import (
    ActionType, Campaign, CampaignState, MarketInfo, OrderSide, OutcomeSide,
)
from src.gridbot.prediction.regime_live_ledger import (
    RegimeLiveLedger, RISK_PROFILES, _LoopSnapshotReads,
)
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.risk import RiskEngine
from src.gridbot.prediction.strategy import StrategyDecision
from src.gridbot.prediction.worker import PredictionWorker
import test_regime_lane as baseline

START = baseline.START


class LedgerFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await baseline.LedgerTests.asyncSetUp(self)
        self.clock = START+125000

    async def asyncTearDown(self):
        await baseline.LedgerTests.asyncTearDown(self)

    async def reserve(self, **kwargs):
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', side_effect=lambda: self.clock):
            return await self.ledger.reserve_c180_intent(
                loop_id='loop1', market_start_ms=START, campaign_id='c1', intent=self.intent,
                decision_at_ms=START+125000, wallet_reconciled_at_ms=START+125000, **kwargs)


class BatchRiskTests(LedgerFixture):
    async def snapshots(self, ids):
        conn = self.repo._require_conn()
        loops = await self.ledger._rows(conn, 'SELECT * FROM prediction_loops')
        batch = _LoopSnapshotReads(self.ledger, conn, loops)
        await self.repo._begin(conn, 'DEFERRED')
        try:
            old = [await self.ledger._snapshot_conn(conn, key, self.clock) for key in ids]
            fresh = [await self.ledger._snapshot_conn(batch, key, self.clock) for key in ids]
            self.assertEqual(old, fresh)
        finally:
            await conn.rollback()
        return old

    async def test_all_profiles_and_twenty_loops_use_bounded_reads(self):
        for n in range(2, 21):
            key = f'loop{n}'
            await self.repo.start_loop(key, 20, mode='LIVE', strategy_profile=RISK_PROFILES[n % len(RISK_PROFILES)])
            await self.ledger.seed_schedule(loop_id=key, first_market_start_ms=START)
        await self.repo._execute('UPDATE prediction_regime_slots SET empty_attested_at_ms=market_start_ms+300000')
        ids = [f'loop{n}' for n in range(1, 21)]
        await self.snapshots(ids)
        queries = []
        conn = self.repo._require_conn()
        await conn.set_trace_callback(queries.append)
        try:
            loops = await self.ledger._rows(conn, 'SELECT * FROM prediction_loops')
            batch = _LoopSnapshotReads(self.ledger, conn, loops)
            for key in ids:
                snap = await self.ledger._snapshot_conn(batch, key, self.clock)
                self.assertEqual(len(snap.confirmed_empty_market_starts), 20)
            self.assertLessEqual(len(queries), 12, queries)
        finally:
            await conn.set_trace_callback(None)

    async def test_orphans_unknown_unpaired_settlement_and_bad_empty_remain_visible(self):
        await self.repo.start_loop('loop2', 20, mode='LIVE', strategy_profile=RISK_PROFILES[-1])
        await self.ledger.seed_schedule(loop_id='loop2', first_market_start_ms=START)
        await self.repo.save_campaign(Campaign('c2', self.market), loop_id='loop2')
        await self.repo._execute('UPDATE prediction_regime_slots SET empty_attested_at_ms=market_start_ms+300000')
        await self.repo._execute('UPDATE prediction_campaigns SET pending_unknown=1 WHERE campaign_id=\'c2\'')
        await self.repo._execute("INSERT INTO prediction_fills(fill_id,order_id,campaign_id,outcome,order_side,shares,price,gross_amount,fee,event_time_ms,payload_json) VALUES('f1','o1','c2','UP','BUY','2','.35','1','.02',?,'{}')", (self.clock,))
        await self.repo._execute("INSERT INTO prediction_settlements(settlement_id,campaign_id,loop_id,settled_at_ms,status,net_pnl,payload_json) VALUES('s1','c1','loop1',?,'SETTLED','0','{}')", (self.clock,))
        snapshots = await self.snapshots(['loop1','loop2'])
        self.assertFalse(snapshots[1].complete)
        self.assertIn(START, snapshots[1].unresolved_market_starts)
        self.assertNotIn(START, snapshots[1].confirmed_empty_market_starts)
        self.assertFalse((await self.ledger.check_risk('loop1', START, self.clock))[0])

    async def test_settlement_observation_and_later_edits_are_not_cached(self):
        result = await self.reserve()
        self.assertTrue(result.claimed, result.reason)
        await self.repo._execute("UPDATE prediction_order_intents SET status='FILLED' WHERE intent_id='i1'")
        await self.repo._execute("UPDATE prediction_campaigns SET pending_intent_id=NULL WHERE campaign_id='c1'")
        await self.repo._execute("INSERT INTO prediction_fills(fill_id,order_id,campaign_id,outcome,order_side,shares,price,gross_amount,fee,event_time_ms,payload_json) VALUES('f1','o1','c1','UP','BUY','2.8','.35','1','.02',?,'{}')", (self.clock,))
        await self.repo._execute("INSERT INTO prediction_settlements(settlement_id,campaign_id,loop_id,settled_at_ms,status,net_pnl,payload_json) VALUES('s1','c1','loop1',?,'SETTLED','-1','{}')", (START+300000,))
        self.clock = START+300000
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=self.clock):
            await self.ledger.observe_settlement(loop_id='loop1', settlement_id='s1')
        snap = (await self.snapshots(['loop1']))[0]
        self.assertTrue(snap.complete)
        self.assertEqual(snap.settlements[0].net_pnl_usdt, D('-1'))
        await self.repo._execute("UPDATE prediction_order_intents SET unknown=1 WHERE intent_id='i1'")
        snap = (await self.snapshots(['loop1']))[0]
        self.assertIn(START, snap.unresolved_market_starts)
        self.assertFalse((await self.ledger.check_risk('loop1', START, self.clock))[0])


class AtomicEntryTests(LedgerFixture):
    def entry(self):
        return Campaign('c1', self.market, state=CampaignState.INITIAL_PENDING,
                        initial_attempts=1, order_attempts=1, pending_intent_id='i1')

    async def test_campaign_counters_payload_intent_and_claim_commit_together(self):
        spans=[]
        result = await self.reserve(entry_campaign=self.entry(), expires_at_ms=START+127000,
                                    trace=lambda stage, ns: spans.append((stage,ns)))
        self.assertTrue(result.claimed, result.reason)
        row=await self.repo.get_campaign('c1')
        self.assertEqual((row['state'],row['initial_attempts'],row['order_attempts'],row['pending_intent_id']),
                         ('INITIAL_PENDING',1,1,'i1'))
        self.assertEqual(row['payload']['pending_intent_id'],'i1')
        # Restart after commit but before HTTP retains the exact market barrier.
        self.ledger=RegimeLiveLedger(self.repo)
        self.assertFalse((await self.reserve()).claimed)
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_order_intents')),1)
        self.assertEqual({stage for stage, _ in spans},
                         {'claim_lock','claim_begin','claim_history_risk','claim_commit'})

    async def test_campaign_failure_rolls_back_intent_and_claim(self):
        conn=self.repo._require_conn()
        await conn.execute("CREATE TRIGGER fail_entry BEFORE UPDATE ON prediction_campaigns WHEN NEW.initial_attempts=1 BEGIN SELECT RAISE(ABORT,'test disk failure'); END")
        await conn.commit()
        result=await self.reserve(entry_campaign=self.entry(), expires_at_ms=START+127000)
        self.assertFalse(result.claimed)
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims'),[])
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_order_intents'),[])
        self.assertEqual((await self.repo.get_campaign('c1'))['initial_attempts'],0)

    async def test_lock_wait_crosses_short_ttl(self):
        lock=self.repo._operation_gate.lock
        await lock.acquire()
        task=asyncio.create_task(self.reserve(expires_at_ms=START+125500))
        await asyncio.sleep(0)
        self.clock=START+125500
        lock.release()
        result=await task
        self.assertEqual(result.reason,'execution_expired_after_lock')
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_order_intents'),[])

    async def test_risk_read_crosses_expiry_or_wallet_freshness(self):
        old=self.ledger._risk_conn
        async def slow(*args):
            result=await old(*args)
            self.clock=START+127001
            return result
        with patch.object(self.ledger,'_risk_conn',side_effect=slow):
            result=await self.reserve(expires_at_ms=START+127000)
        self.assertEqual(result.reason,'execution_expired_during_claim_risk')
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_order_intents'),[])

    async def test_persistent_registration_is_read_only_and_detects_tampering(self):
        await self.ledger.check_risk('loop1',START,self.clock)
        bridge=RegimeWorkerBridge(self.repo,'unused.db')
        with (patch.object(bridge.ledger,'seed_schedule',side_effect=AssertionError('repeat write')),
              patch.object(bridge.ledger,'check_risk',side_effect=AssertionError('repeat history read'))):
            self.assertTrue((await bridge.register_market(loop_id='loop1',market=self.market,
                now_ms=self.clock,unit_usdt=D(1))).allowed)
        await self.repo._execute("UPDATE prediction_regime_slots SET market_id='changed' WHERE market_start_ms=?",(START,))
        self.assertFalse(await self.ledger.market_is_registered(loop_id='loop1',market=self.market))


class EntryHarness(PredictionWorker):
    @property
    def live_capability(self):
        return True

    def __init__(self, repo, ledger):
        self.repository=repo
        self.clock=START+125000
        self._now_ms=lambda: self.clock
        self._selected_strategy_profile=ledger.profile
        self._selected_order_unit_usdt=D(1)
        self._loop_id='loop1'
        self._hard_stop_latched=False
        self._allow_new_buys=True
        self._allow_reductions=True
        self._observability_events=[]
        self._observability_dropped=0
        self._entry_attempts={}
        self.heartbeat=SimpleNamespace(last_error=None,last_api_ok_at_ms=None)
        self.settings=SimpleNamespace(wallet_address='fake-wallet',wallet_id='fake-id',
            recv_window=5000, required_balance_usdt=D(1),order_type='LIMIT',
            slippage_bps=0,chain_id='56',account_type='SPOT',time_in_force='GTC')
        self._rate_limiter=Mock()
        self._rate_limiter.acquire.return_value=True
        self.client=SimpleNamespace(request_budget=None)
        self.risk_engine=RiskEngine()
        self._can_prioritize_initial_entry=Mock(return_value=False)
        self._check_regime_entry_gate=AsyncMock(return_value=True)
        self._poll_order_terminal=AsyncMock(return_value=None)
        self._record_order_result=AsyncMock()
        self.sent=[]
        signal=SimpleNamespace(entry=SimpleNamespace(side='UP',stake_usdt=D(1)))
        self.ready=SimpleNamespace(allowed=True,reason='ready',signal=signal,book_at_ms=START+124000,
            execution=SimpleNamespace(worst_ask_limit=D('.35'),expires_at_ms=START+127000))
        fresh=SimpleNamespace(**vars(self.ready))
        fresh.book_at_ms=START+124999
        self.bridge=SimpleNamespace(ledger=ledger,check_signal=Mock(return_value=fresh))
        self._c180_bridge_for_worker=Mock(return_value=self.bridge)
        self._c180_ready={'c1':self.ready}

    async def _call_api(self, method, *args, **kwargs):
        if method=='get_quote':return {'quoteId':'fake-quote'}
        if method=='query_payment_option_balances':return {'items':[{'accountType':'CeDeFi','enabled':True,'availableBalanceDisplay':'10'}]}
        if method=='place_order':
            self.sent.append(kwargs)
            return {'orderId':'fake-order'}
        raise AssertionError(method)


class WorkerBoundaryTests(LedgerFixture):
    async def run_entry(self, fault=None):
        worker=EntryHarness(self.repo,self.ledger)
        campaign=Campaign('c1', self.market)
        worker._start_entry_attempt(campaign,worker.ready)
        decision=StrategyDecision(ActionType.BUY_INITIAL,CampaignState.INITIAL_PENDING,
            'c180 durable entry', outcome=OutcomeSide.UP,order_side=OrderSide.BUY,
            amount=D(1),limit_price=D('.35'),ttl_ms=2000,trade_allowed=True)
        if fault:
            old=self.ledger.reserve_c180_intent
            async def reserve(**kwargs):
                result=await old(**kwargs)
                fault(worker)
                return result
            self.ledger.reserve_c180_intent=reserve
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms',side_effect=lambda: worker.clock):
            await worker._handle_decision(campaign,decision)
        return worker,campaign

    async def test_success_persists_before_single_post_without_extra_campaign_save(self):
        with patch.object(self.repo,'save_campaign',wraps=self.repo.save_campaign) as save:
            worker,campaign=await self.run_entry()
        self.assertEqual(len(worker.sent),1,worker._observability_events)
        self.assertEqual(save.await_count,0)
        row=await self.repo.get_intent(campaign.pending_intent_id)
        self.assertEqual(row['status'],'SUBMITTED')
        self.assertEqual(row['ttl_deadline_ms'],START+127000)
        self.assertFalse(worker._entry_attempts)

    async def test_commit_crosses_expiry_known_not_sent_and_no_retry(self):
        worker,campaign=await self.run_entry(lambda w:setattr(w,'clock',START+127000))
        self.assertEqual(worker.sent,[])
        self.assertFalse(campaign.pending_unknown)
        self.assertIsNone(campaign.pending_intent_id)
        row=(await self.repo._fetchall('SELECT * FROM prediction_order_intents'))[0]
        self.assertEqual(row['status'],'REJECTED')
        self.assertEqual(campaign.initial_attempts,1)
        self.assertTrue(json.loads(row['payload_json'])['not_submitted'])
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')),1)

    async def test_control_and_adverse_book_after_claim_block_http(self):
        worker,campaign=await self.run_entry(lambda w:setattr(w,'_allow_new_buys',False))
        self.assertEqual(worker.sent,[])
        self.assertFalse(campaign.pending_unknown)
        self.assertEqual(campaign.initial_attempts,1)

    async def test_operator_hs_after_claim_is_not_cleared(self):
        old=self.ledger.reserve_c180_intent
        async def reserve(**kwargs):
            result=await old(**kwargs)
            await self.repo.set_runtime_config('prediction_risk_state',{'hard_stop_latched':True})
            return result
        self.ledger.reserve_c180_intent=reserve
        worker,_=await self.run_entry()
        self.assertEqual(worker.sent,[])
        self.assertTrue((await self.repo.get_runtime_config('prediction_risk_state',{}))['hard_stop_latched'])

    async def test_adverse_book_after_claim_is_known_unsubmitted(self):
        def change(w):
            bad=SimpleNamespace(**vars(w.ready))
            bad.execution=SimpleNamespace(worst_ask_limit=D('.36'),expires_at_ms=START+127000)
            w.bridge.check_signal.return_value=bad
        worker,campaign=await self.run_entry(change)
        self.assertEqual(worker.sent,[])
        self.assertFalse(campaign.pending_unknown)
        self.assertEqual(campaign.initial_attempts,1)
        self.assertEqual((await self.repo._fetchall('SELECT status FROM prediction_order_intents'))[0]['status'],'REJECTED')

    async def test_possible_http_failure_retains_unknown_and_claim_barrier(self):
        from src.gridbot.prediction.client import PredictionTransportError
        captured={}
        def fail(w):
            original=w._call_api
            captured['worker']=w
            async def call(method,*args,**kwargs):
                if method=='place_order':
                    w.sent.append(kwargs)
                    raise PredictionTransportError('test response lost after POST')
                return await original(method,*args,**kwargs)
            w._call_api=call
        with self.assertRaises(PredictionTransportError):
            await self.run_entry(fail)
        self.assertEqual(len(captured['worker'].sent),1)
        row=(await self.repo._fetchall('SELECT * FROM prediction_order_intents'))[0]
        self.assertEqual((row['status'],row['unknown']),('UNKNOWN',1))
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')),1)
        self.assertFalse((await self.ledger.check_risk('loop1',START,START+125000))[0])


class HTTPGuardTests(unittest.TestCase):
    def test_post_guard_runs_after_signing_and_budget_and_never_retries(self):
        transport=Mock()
        transport.request.return_value=TransportResponse(200,b'{"orderId":"o"}',{})
        client=BinancePredictionClient('fake-key','fake-secret',transport=transport)
        checked=[]
        def guard():
            checked.append(True)
            raise PredictionEntryNotSubmitted('expired after signing')
        with entry_http_guard_scope(guard), self.assertRaises(PredictionEntryNotSubmitted):
            client.place_order(wallet_address='fake',wallet_id='fake',quote_id='fake', account_type='SPOT',order_type='LIMIT',time_in_force='GTC',slippage_bps=0)
        self.assertEqual(len(checked),1)
        transport.request.assert_not_called()


class TelemetryAndExposureTests(LedgerFixture):
    async def test_telemetry_one_commit_and_failed_batch_is_retained(self):
        events=[('entry_stage','c1',{'stage':'claim_lock','duration_ns':n}) for n in range(32)]
        conn=self.repo._require_conn()
        with patch.object(conn,'commit',wraps=conn.commit) as commit:
            await self.repo.record_execution_timings(events)
        self.assertEqual(commit.await_count,1)
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_risk_events')),32)
        worker=EntryHarness(self.repo,self.ledger)
        worker._observability_flush_lock=asyncio.Lock()
        worker._observation_repository=AsyncMock(return_value=self.repo)
        worker._observability_events=list(events)
        with patch.object(self.repo,'record_execution_timings',side_effect=sqlite3.OperationalError('busy')):
            await worker._flush_observability()
        self.assertEqual(worker._observability_events,events)
        await worker._flush_observability()
        self.assertEqual(worker._observability_events,[])

    async def test_full_telemetry_queue_does_not_drop_transaction_evidence(self):
        worker=EntryHarness(self.repo,self.ledger)
        worker._observability_events=[('sentinel',None,{})]*2000
        worker._trace_event('extra')
        self.assertEqual(worker._observability_dropped,1)
        self.assertTrue((await self.reserve()).claimed)

    async def test_parallel_reads_wait_for_both_and_fail_closed(self):
        worker=EntryHarness(self.repo,self.ledger)
        worker.client.concurrent_reads=True
        started=asyncio.Event()
        finished=asyncio.Event()
        async def orders():
            started.set()
            await finished.wait()
            return []
        async def positions(*args,**kwargs):
            await started.wait()
            finished.set()
            return []
        worker._query_active_order_rows=orders
        worker._call_api=positions
        self.assertTrue(await worker._c180_recovery_exposure_clear())
        worker._call_api=AsyncMock(return_value=[{'shares':'1'}])
        self.assertFalse(await worker._c180_recovery_exposure_clear())

class AdditionalSafetyTests(LedgerFixture):
    async def test_cancellation_during_claim_rolls_back_and_releases_gate(self):
        entered=asyncio.Event()
        async def stalled(*args):
            entered.set()
            await asyncio.Event().wait()
        with patch.object(self.ledger,'_risk_conn',side_effect=stalled):
            task=asyncio.create_task(self.reserve())
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(self.repo._require_conn().in_transaction)
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_order_intents'),[])
        self.assertTrue((await self.reserve()).claimed)

    async def test_telemetry_failure_does_not_change_claim_outcome(self):
        def fail(*args):raise RuntimeError('telemetry only')
        self.assertTrue((await self.reserve(trace=fail)).claimed)

    async def test_risk_persistence_preserves_new_operator_hs_and_reset_fields(self):
        payload={'day':'2026-10-01','hard_stop_latched':False,'daily_net_pnl':'-2'}
        await self.repo.set_runtime_config('prediction_risk_state',
            {'day':'2026-10-01','hard_stop_latched':True,'hard_stop_reset_count':4})
        await self.repo.save_risk_snapshot(payload)
        saved=await self.repo.get_runtime_config('prediction_risk_state',{})
        self.assertTrue(saved['hard_stop_latched'])
        self.assertEqual(saved['hard_stop_reset_count'],4)
        self.assertEqual(saved['daily_net_pnl'],'-2')

    async def test_competing_workers_have_exactly_one_durable_claim(self):
        first,second=await asyncio.gather(self.reserve(),self.reserve())
        self.assertEqual(sum(row.claimed for row in (first,second)),1)
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')),1)

    async def test_http_thread_checks_expiry_after_budget_and_latest_durable_hs(self):
        worker=EntryHarness(self.repo,self.ledger)
        transport=Mock()
        transport.request.return_value=TransportResponse(200,{'orderId':'order'}, {})
        client=BinancePredictionClient('fake-key','fake-secret',transport=transport,
                                      clock_ms=lambda:worker.clock)
        worker.client=client
        class Budget:
            def __init__(self, mutate):self.mutate=mutate
            def can_send_prepaid(self):self.mutate();return True
            def begin_request(self):return object()
            def transport_failed(self, token):pass
            def note_response(self,*args,**kwargs):pass
        async def call():
            return await PredictionWorker._call_api(worker,'place_order',
                _trace_campaign_id='c1',_trace_intent_id='i1',_weight_pre_acquired=True,
                _shared_pre_acquired=True,_entry_deadline_ms=START+127000,
                _entry_book_at_ms=START+124999,wallet_address='fake-wallet',wallet_id='fake-id',
                quote_id='fake-quote',account_type='SPOT',order_type='LIMIT',time_in_force='GTC',slippage_bps=0)
        client.request_budget=Budget(lambda:setattr(worker,'clock',START+127000))
        with self.assertRaises(PredictionEntryNotSubmitted):await call()
        transport.request.assert_not_called()
        worker.clock=START+125000
        def committed_hs():
            with sqlite3.connect(self.repo.db_path) as db:
                db.execute("UPDATE prediction_loops SET hard_stop_latched=1 WHERE loop_id='loop1'")
        client.request_budget=Budget(committed_hs)
        with self.assertRaises(PredictionEntryNotSubmitted):await call()
        transport.request.assert_not_called()
        self.assertNotIn('fake-wallet',json.dumps(worker._observability_events))

    async def test_late_lane_halt_and_unknown_deny_at_actual_http_boundary(self):
        from src.gridbot.prediction.regime_lane import STATE_KEY
        worker=EntryHarness(self.repo,self.ledger)
        worker._entry_durable_http_guard('loop1',self.ledger.profile)
        await self.repo.set_runtime_config(STATE_KEY,{'halt_reason':'scheduled20_mdd_3.5'})
        with self.assertRaises(PredictionEntryNotSubmitted):
            worker._entry_durable_http_guard('loop1',self.ledger.profile)
        await self.repo.set_runtime_config(STATE_KEY,{'halt_reason':None})
        await self.repo._execute("UPDATE prediction_campaigns SET pending_unknown=1 WHERE campaign_id='c1'")
        with self.assertRaises(PredictionEntryNotSubmitted):
            worker._entry_durable_http_guard('loop1',self.ledger.profile)

    async def test_actual_http_start_and_ack_share_attempt_id(self):
        worker=EntryHarness(self.repo,self.ledger)
        transport=Mock()
        transport.request.return_value=TransportResponse(200,{'orderId':'order'}, {})
        worker.client=BinancePredictionClient('fake-key','fake-secret',transport=transport)
        worker._start_entry_attempt(Campaign('c1',self.market),worker.ready)
        result=await PredictionWorker._call_api(worker,'place_order',
            _trace_campaign_id='c1',_trace_intent_id='i1',_weight_pre_acquired=True,
            _shared_pre_acquired=True,_entry_deadline_ms=START+127000,
            _entry_book_at_ms=START+124999,wallet_address='fake-wallet',wallet_id='fake-id',
            quote_id='fake-quote',account_type='SPOT',order_type='LIMIT',time_in_force='GTC',slippage_bps=0)
        self.assertEqual(result['orderId'],'order')
        transport.request.assert_called_once()
        events={name:fields for name,_,fields in worker._observability_events}
        self.assertEqual(events['api_ack']['attempt_id'],events['entry_http_start']['attempt_id'])
        self.assertEqual(events['entry_finished']['reason'],'post_started')
        self.assertLess(events['entry_http_start']['http_at_ms'],START+127000)
        self.assertNotIn('fake-secret',json.dumps(worker._observability_events))


# The atomic worker integration is shared by every supported T6 profile.
import pytest
@pytest.mark.asyncio
@pytest.mark.parametrize('profile', RISK_PROFILES)
async def test_every_t6_profile_retains_one_entry_barrier(profile):
    fixture=WorkerBoundaryTests('test_success_persists_before_single_post_without_extra_campaign_save')
    await fixture.asyncSetUp()
    try:
        await fixture.repo._execute('UPDATE prediction_loops SET strategy_profile=? WHERE loop_id=\'loop1\'',(profile,))
        fixture.ledger=RegimeLiveLedger(fixture.repo,profile=profile)
        worker,campaign=await fixture.run_entry()
        assert len(worker.sent)==1
        assert campaign.initial_attempts==campaign.order_attempts==1
        assert len(await fixture.repo._fetchall('SELECT * FROM prediction_regime_entry_claims'))==1
    finally:
        await fixture.asyncTearDown()

@pytest.mark.asyncio
@pytest.mark.parametrize('profile', RISK_PROFILES)
async def test_native_http_guard_preserves_profile_book_freshness(profile):
    fixture=LedgerFixture()
    await fixture.asyncSetUp()
    try:
        await fixture.repo._execute('UPDATE prediction_loops SET strategy_profile=? WHERE loop_id=\'loop1\'',(profile,))
        worker=EntryHarness(fixture.repo,RegimeLiveLedger(fixture.repo,profile=profile))
        transport=Mock()
        transport.request.return_value=TransportResponse(200,{'orderId':'o'}, {})
        worker.client=BinancePredictionClient('fake-key','fake-secret',transport=transport)
        worker.clock=START+125100
        args=dict(_weight_pre_acquired=True,_shared_pre_acquired=True,
            _entry_deadline_ms=START+127000,_entry_book_at_ms=START+124000,
            wallet_address='fake',wallet_id='fake',quote_id='fake',account_type='SPOT',
            order_type='LIMIT',time_in_force='GTC',slippage_bps=0)
        if profile in RISK_PROFILES[:3]:
            await PredictionWorker._call_api(worker,'place_order',**args)
            transport.request.assert_called_once()
        else:
            with pytest.raises(PredictionEntryNotSubmitted):
                await PredictionWorker._call_api(worker,'place_order',**args)
            transport.request.assert_not_called()
    finally:
        await fixture.asyncTearDown()
