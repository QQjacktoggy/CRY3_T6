"""Pre-HTTP durable admission must not age a fresh entry book past its limit."""
import json
from types import SimpleNamespace
from unittest.mock import Mock

from src.gridbot.prediction.client import (
    BinancePredictionClient, PredictionAPIError, PredictionEntryNotSubmitted, TransportResponse,
)
from src.gridbot.prediction.models import ActionType, Campaign, CampaignState, OrderSide, OutcomeSide
from src.gridbot.prediction.regime_live_ledger import RISK_PROFILES, RegimeLiveLedger
from src.gridbot.prediction.repository import MIGRATIONS_DIR
from src.gridbot.prediction.strategy import StrategyDecision
from src.gridbot.prediction.worker import PredictionWorker, durable_admission_sql
from test_t6_entry_critical_path import EntryHarness, LedgerFixture, START, D

COLD_READ_MS = 1100  # Observed cold reads on the VM were 720-1160 ms.
T67C = 'regime_target6_7c_v1'


class SlowAdmissionHarness(EntryHarness):
    """Real _call_api for place_order; the first durable read is cold."""

    def __init__(self, repo, ledger):
        super().__init__(repo, ledger)
        self.transport = Mock()
        self.transport.request.return_value = TransportResponse(200, {'orderId': 'fake-order'}, {})
        self.client = BinancePredictionClient('fake-key', 'fake-secret', transport=self.transport,
                                              clock_ms=lambda: self.clock)
        self.durable_reads = 0
        fresh = SimpleNamespace(**vars(self.ready))
        def check_signal(**kwargs):
            # The local websocket book is always current when it is read.
            fresh.book_at_ms = self.clock
            return fresh
        self.bridge.check_signal = Mock(side_effect=check_signal)

    def _entry_durable_http_guard(self, loop_id, profile):
        self.durable_reads += 1
        if self.durable_reads == 1:
            self.clock += COLD_READ_MS
        return super()._entry_durable_http_guard(loop_id, profile)

    async def _call_api(self, method, *args, **kwargs):
        if method == 'place_order':
            self.sent.append(kwargs)
            return await PredictionWorker._call_api(self, method, *args, **kwargs)
        return await super()._call_api(method, *args, **kwargs)


class AdmissionLatencyTests(LedgerFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # The ledger fixture uses a minimal schema; install the real indexes.
        conn = self.repo._require_conn()
        await conn.executescript((MIGRATIONS_DIR/'001_initial.sql').read_text())
        await conn.executescript((MIGRATIONS_DIR/'028_entry_admission_unknown_lookup.sql').read_text())
        # T6.7c is the Live profile that lost entries; it uses the 1000 ms book limit.
        await self.repo._execute("UPDATE prediction_loops SET strategy_profile=? WHERE loop_id='loop1'", (T67C,))
        self.ledger = RegimeLiveLedger(self.repo, profile=T67C)

    async def run_entry(self, worker):
        campaign = Campaign('c1', self.market)
        worker._start_entry_attempt(campaign, worker.ready)
        decision = StrategyDecision(ActionType.BUY_INITIAL, CampaignState.INITIAL_PENDING,
            'c180 durable entry', outcome=OutcomeSide.UP, order_side=OrderSide.BUY,
            amount=D(1), limit_price=D('.35'), ttl_ms=2000, trade_allowed=True)
        from unittest.mock import patch
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', side_effect=lambda: worker.clock):
            await worker._handle_decision(campaign, decision)
        return campaign

    async def test_cold_durable_read_no_longer_ages_final_book(self):
        worker = SlowAdmissionHarness(self.repo, self.ledger)
        campaign = await self.run_entry(worker)
        worker.transport.request.assert_called_once()
        self.assertEqual(worker.durable_reads, 3)  # prefetch, invoke and final HTTP boundary
        row = await self.repo.get_intent(campaign.pending_intent_id)
        self.assertEqual(row['status'], 'SUBMITTED')
        stages = [f['stage'] for name, _, f in worker._observability_events if name == 'entry_stage']
        self.assertIn('durable_admission_prefetch', stages)

    async def test_prefetch_rejection_is_known_not_submitted(self):
        from src.gridbot.prediction.regime_lane import STATE_KEY
        worker = SlowAdmissionHarness(self.repo, self.ledger)
        reserve = self.ledger.reserve_c180_intent
        async def reserve_then_halt(**kwargs):
            result = await reserve(**kwargs)
            await self.repo.set_runtime_config(STATE_KEY, {'halt_reason': 'test_halt_after_claim'})
            return result
        self.ledger.reserve_c180_intent = reserve_then_halt
        campaign = await self.run_entry(worker)
        worker.transport.request.assert_not_called()
        self.assertEqual(worker.sent, [])
        self.assertFalse(campaign.pending_unknown)
        row = (await self.repo._fetchall('SELECT * FROM prediction_order_intents'))[0]
        self.assertEqual(row['status'], 'REJECTED')
        payload = json.loads(row['payload_json'])
        self.assertTrue(payload['not_submitted'])
        self.assertEqual(payload['reason'], 'durable_admission_rejected_before_http')

    async def test_unknown_intent_in_any_live_t6_loop_still_denies(self):
        worker = EntryHarness(self.repo, self.ledger)
        worker._entry_durable_http_guard('loop1', self.ledger.profile)
        await self.repo.create_intent({**self.intent, 'intent_id': 'old'})
        await self.repo._execute("UPDATE prediction_order_intents SET unknown=1 WHERE intent_id='old'")
        with self.assertRaises(PredictionEntryNotSubmitted):
            worker._entry_durable_http_guard('loop1', self.ledger.profile)
        # An unknown intent in a non-LIVE loop does not block this loop.
        await self.repo._execute("UPDATE prediction_loops SET mode='SHADOW' WHERE loop_id='loop1'")
        await self.repo.start_loop('loop2', 20, mode='LIVE', strategy_profile=self.ledger.profile)
        worker._loop_id = 'loop2'
        worker._entry_durable_http_guard('loop2', self.ledger.profile)

    async def test_unknown_lookups_use_partial_indexes(self):
        conn = self.repo._require_conn()
        args = (*RISK_PROFILES, *RISK_PROFILES, 'lane', 'guard', 'loop1')
        plans = await conn.execute_fetchall('EXPLAIN QUERY PLAN '+durable_admission_sql(len(RISK_PROFILES)), args)
        details = [r[3] for r in plans]
        self.assertTrue(any('idx_prediction_campaigns_pending_unknown' in d for d in details), details)
        self.assertTrue(any('idx_prediction_intents_unknown' in d for d in details), details)
        # The only scans left are of the partial indexes, which hold unknown rows only.
        scans = [d for d in details if d.startswith('SCAN ')]
        self.assertEqual(scans, ['SCAN c USING COVERING INDEX idx_prediction_campaigns_pending_unknown',
                                 'SCAN i USING COVERING INDEX idx_prediction_intents_unknown'])


class PreSendErrorCodeTests(LedgerFixture):
    async def call(self, worker, *, book_at):
        return await PredictionWorker._call_api(worker, 'place_order',
            _trace_campaign_id='c1', _trace_intent_id='i1', _weight_pre_acquired=True,
            _shared_pre_acquired=True, _entry_deadline_ms=START+127000, _entry_book_at_ms=book_at,
            wallet_address='fake', wallet_id='fake', quote_id='fake', account_type='SPOT',
            order_type='LIMIT', time_in_force='GTC', slippage_bps=0)

    async def harness(self):
        profile = T67C
        await self.repo._execute("UPDATE prediction_loops SET strategy_profile=? WHERE loop_id='loop1'", (profile,))
        worker = EntryHarness(self.repo, self.ledger)
        worker._selected_strategy_profile = profile
        worker.transport = Mock()
        worker.client = BinancePredictionClient('fake-key', 'fake-secret', transport=worker.transport)
        return worker

    def events(self, worker, name):
        return [f for n, _, f in worker._observability_events if n == name]

    async def test_stale_book_is_named_with_its_age_and_is_not_api_error(self):
        worker = await self.harness()
        with self.assertRaises(PredictionEntryNotSubmitted) as ctx:
            await self.call(worker, book_at=START+123900)
        self.assertEqual(str(ctx.exception), 'entry_book_stale_before_http book_age_ms=1100 max_ms=1000')
        worker.transport.request.assert_not_called()
        self.assertEqual(self.events(worker, 'api_error'), [])
        [event] = self.events(worker, 'entry_not_submitted')
        self.assertTrue(event['reason'].startswith('entry_book_stale_before_http'))

    async def test_deadline_is_named_separately_from_stale_book(self):
        worker = await self.harness()
        worker.clock = START+127050
        with self.assertRaises(PredictionEntryNotSubmitted) as ctx:
            await self.call(worker, book_at=START+127000)
        self.assertEqual(str(ctx.exception), 'entry_deadline_expired_before_http late_ms=50')
        worker.transport.request.assert_not_called()


class ApiErrorLabelTests(LedgerFixture):
    def harness(self, status):
        worker = EntryHarness(self.repo, self.ledger)
        def history(**kwargs):
            raise PredictionAPIError('history failed', status_code=status, payload={}, headers={})
        worker.client = SimpleNamespace(request_budget=None, query_order_history=history)
        return worker

    async def test_expected_status_filter_failure_is_history_fallback(self):
        worker = self.harness(500)
        with self.assertRaises(PredictionAPIError):
            await PredictionWorker._call_api(worker, 'query_order_history', _fallback_statuses={400, 404, 500})
        names = [n for n, _, _ in worker._observability_events]
        self.assertIn('history_fallback', names)
        self.assertNotIn('api_error', names)

    async def test_other_failures_stay_api_error_with_status_code(self):
        worker = self.harness(503)
        with self.assertRaises(PredictionAPIError):
            await PredictionWorker._call_api(worker, 'query_order_history', _fallback_statuses={400, 404, 500})
        [event] = [f for n, _, f in worker._observability_events if n == 'api_error']
        self.assertEqual(event['status_code'], 503)
