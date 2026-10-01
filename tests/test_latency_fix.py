"""Focused timing-order and fail-closed ledger regressions for T6/T6.1."""
import unittest
from decimal import Decimal
from types import SimpleNamespace

from src.gridbot.prediction.models import Campaign, MarketInfo
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.worker import PredictionWorker

START = 1790265600000


class FakeBridge:
    def __init__(self, *, signal_allowed=True, gate_allowed=True):
        self.calls = []
        self.signal_allowed = signal_allowed
        self.gate_allowed = gate_allowed

    async def register_market(self, **kwargs):
        self.calls.append('register')
        return SimpleNamespace(allowed=True, reason='registered')

    def check_signal(self, **kwargs):
        self.calls.append('signal')
        if not self.signal_allowed:
            return SimpleNamespace(allowed=False, reason='frozen_skip', signal=None, execution=None)
        return SimpleNamespace(
            allowed=True, reason='regime_ready',
            signal=SimpleNamespace(entry=SimpleNamespace(side='UP')),
            execution=SimpleNamespace(worst_ask_limit=Decimal('.40'),
                                      expires_at_ms=START+136000),
            book_at_ms=START+124100,
        )

    async def prepare_market(self, **kwargs):
        self.calls.append('prepare')
        if kwargs.get('already_registered') is not True:
            raise AssertionError('worker repeated market registration')
        return SimpleNamespace(allowed=self.gate_allowed,
                               reason='gate_pass' if self.gate_allowed else 'risk_halt')


class FakeWorker:
    _c180_decide = PredictionWorker._c180_decide
    _finish_entry_attempt = PredictionWorker._finish_entry_attempt
    _start_entry_attempt = PredictionWorker._start_entry_attempt
    _entry_stage = PredictionWorker._entry_stage
    _entry_signal_within_window = PredictionWorker._entry_signal_within_window
    _trace_event = PredictionWorker._trace_event

    def __init__(self, bridge, times):
        self.bridge = bridge
        self.times = iter(times)
        self._loop_id = 'loop1'
        self._selected_order_unit_usdt = Decimal(1)
        self._selected_strategy_profile = 'regime_target6_1_v1'
        self.live_capability = True
        self._c180_ready = {}

    def _c180_bridge_for_worker(self):
        return self.bridge

    def _now_ms(self):
        return next(self.times)


def campaign():
    market = MarketInfo('topic','up','test',START,START+300000,
                        up_market_id='up',down_market_id='down')
    return Campaign('c1',market)


class WorkerTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_refreshes_time_and_freezes_before_wallet_gate(self):
        bridge = FakeBridge()
        worker = FakeWorker(bridge,[START+124200]*5)
        decision = await worker._c180_decide(campaign(),START+123900)
        self.assertEqual(decision.action.value,'BUY_INITIAL')
        self.assertEqual(bridge.calls,['register','signal','prepare'])
        self.assertEqual(decision.amount,Decimal(1))
        self.assertEqual(decision.limit_price,Decimal('.40'))

    async def test_frozen_skip_never_queries_wallet_gate(self):
        bridge = FakeBridge(signal_allowed=False)
        worker = FakeWorker(bridge,[START+124200]*4)
        decision = await worker._c180_decide(campaign(),START+124000)
        self.assertEqual(decision.action.value,'HOLD')
        self.assertEqual(bridge.calls,['register','signal'])

    async def test_risk_halt_blocks_frozen_entry(self):
        bridge = FakeBridge(gate_allowed=False)
        worker = FakeWorker(bridge,[START+124200]*5)
        decision = await worker._c180_decide(campaign(),START+124000)
        self.assertEqual(decision.action.value,'HOLD')
        self.assertIn('risk_halt',decision.reason)
        self.assertEqual(bridge.calls,['register','signal','prepare'])

    async def test_gate_overrun_cannot_place_after_expiry(self):
        bridge = FakeBridge()
        worker = FakeWorker(bridge,[START+124200,START+124300,START+124300,START+136001])
        decision = await worker._c180_decide(campaign(),START+124000)
        self.assertEqual(decision.action.value,'HOLD')
        self.assertIn('expired',decision.reason)


class BatchedExposureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from stage_test import baseline_tests
        await baseline_tests.LedgerTests.asyncSetUp(self)

    async def asyncTearDown(self):
        from stage_test import baseline_tests
        await baseline_tests.LedgerTests.asyncTearDown(self)

    async def safe(self):
        return await self.ledger._empty_slots_without_exposure(
            self.repo._require_conn(),'loop1',(START,))

    async def test_pending_intent_and_nonzero_order_fail_closed(self):
        self.assertEqual(await self.safe(),{START})
        await self.repo._execute(
            "INSERT INTO prediction_order_intents(intent_id,campaign_id,action,outcome,order_side,amount,limit_price,created_at_ms,ttl_ms,attempt,status,payload_json) "
            "VALUES('i1','c1','BUY_INITIAL','UP','BUY','1','.4',?,1000,1,'PENDING','{}')",(START+125000,))
        self.assertEqual(await self.safe(),set())
        await self.repo._execute("UPDATE prediction_order_intents SET status='REJECTED' WHERE intent_id='i1'")
        self.assertEqual(await self.safe(),{START})
        await self.repo._execute(
            "INSERT INTO prediction_orders(order_id,intent_id,campaign_id,status,updated_at_ms,payload_json,cumulative_fee) "
            "VALUES('o1','i1','c1','CANCELLED',?,'{}','0.01')",(START+126000,))
        self.assertEqual(await self.safe(),set())
        await self.repo._execute("UPDATE prediction_orders SET cumulative_fee='0' WHERE order_id='o1'")
        self.assertEqual(await self.safe(),{START})

    async def test_20_empty_slots_use_bounded_queries(self):
        await self.repo._execute(
            "UPDATE prediction_regime_slots SET empty_attested_at_ms=market_start_ms+300000 WHERE loop_id='loop1'")
        class CountingLedger(RegimeLiveLedger):
            def __init__(self, repo):
                super().__init__(repo)
                self.calls=0
            async def _rows(self, conn, sql, params=()):
                self.calls += 1
                return await super()._rows(conn,sql,params)
        ledger=CountingLedger(self.repo)
        snap=await ledger._snapshot_conn(self.repo._require_conn(),'loop1',START+21*300000)
        self.assertEqual(len(snap.confirmed_empty_market_starts),20)
        self.assertLessEqual(ledger.calls,15)


if __name__ == '__main__':
    unittest.main(verbosity=2)
