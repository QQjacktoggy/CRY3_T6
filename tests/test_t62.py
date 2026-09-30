import json
import unittest
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

import stage_test as old
import test_latency_fix as latency
import test_live_report as reports
from src.gridbot.prediction import regime_t62_lane as t62
from src.gridbot.prediction.c180_favorite import C180EntryDecision
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.regime_lane import FINGERPRINT as RISK_FINGERPRINT, risk_result, walk
from src.gridbot.prediction.c180_gate_runtime import LiveSettlement
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.telegram import selectable_lanes_for_market
from src.gridbot.prediction.worker import PredictionWorker


class T62PolicyTests(unittest.TestCase):
    def test_only_primary_flat_price_changes(self):
        for first, last in ((2, 2), (2, -2), (.1, 2), (2, .1), (.1, .1)):
            f = old.feature(first, last, 2, first + last)
            before = old.t6.select_side(f, old.original('UP'))
            after = t62.select_primary(f, old.original('UP'))
            expected = {**before, 'upper': '.60'} if before['state'] == 'flat' else before
            if before['state'] == 'flat':
                self.assertEqual(D(after['upper']), D('.60'))
                after = {**after, 'upper': expected['upper']}
            self.assertEqual(after, expected)
        self.assertEqual(old.t6.RULES['flat'][-1], '0.65')

    def test_configuration_identity_and_selectable_units(self):
        for unit in (D(1), D(2), D(3)):
            config = PredictionWorker._sized_strategy_config(t62.PROFILE, unit)
            self.assertEqual((config.max_buy_usdt, config.max_market_buy_usdt), (unit, unit))
        self.assertIn(t62.PROFILE, PredictionWorker._selectable_strategy_profiles())
        self.assertIn(t62.PROFILE, dict(selectable_lanes_for_market('BTCUSDT')))
        self.assertNotIn(t62.PROFILE, dict(selectable_lanes_for_market('ETHUSDT')))
        self.assertEqual(StrategyConfig.for_profile(t62.PROFILE).provenance_payload['regime_policy_fingerprint'], t62.FINGERPRINT)
        self.assertNotEqual(t62.FINGERPRINT, old.t61.FINGERPRINT)
        ledger = RegimeLiveLedger(None, profile=t62.PROFILE)
        self.assertEqual((ledger.tier, ledger.state_key, ledger.max_price),
                         (t62.TIER, old.t6.STATE_KEY, D('.75')))


class T62AmountSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_t62_accepts_two_three_and_old_regime_lanes_stay_fixed(self):
        for profile, unit, accepted in ((t62.PROFILE, '2', True),
                                        (t62.PROFILE, '3', True),
                                        (old.t6.PROFILE, '2', False),
                                        (old.t61.PROFILE, '3', False)):
            with self.subTest(profile=profile, unit=unit):
                worker = object.__new__(PredictionWorker)
                worker.restore_order_unit = AsyncMock(return_value=D(1))
                worker.restore_selected_strategy = AsyncMock(return_value=profile)
                worker._activate_order_unit = AsyncMock(return_value=False)
                worker._status = lambda: {}
                worker._selected_strategy_profile = profile
                worker._selected_order_unit_usdt = D(1)
                worker.repository = SimpleNamespace(get_active_loop=AsyncMock(return_value=None))
                worker._task = None
                result = await worker.select_order_unit(unit)
                self.assertEqual(not result.get('action_denied', False), accepted)
                self.assertEqual(worker._activate_order_unit.called, accepted)


class T62BridgeTests(unittest.TestCase):
    setUp = old.BridgeTests.setUp
    tearDown = old.BridgeTests.tearDown
    check = old.BridgeTests.check
    seed = old.BridgeTests.seed

    def activate(self):
        self.bridge = RegimeWorkerBridge(None, self.path/'signals.db',
                                        feature_db=self.path/'features.db', profile=t62.PROFILE)

    def flat(self, price):
        self.activate()
        self.seed(old.feature(.1, .1, 0, .2), price)
        snap = old.book(price)
        snap['quote']['DOWN']['ask_levels'] = [['.39', '100']]
        signal = C180Signal(old.START, 'topic', 'up', old.START+120000, old.START+120500,
            'entry_positive_cost_after_ev', C180EntryDecision('UP', 'approved', 'UP', D(1), D(2), None),
            D('.9'), 'original', None, D(200))
        with patch.object(self.bridge, '_first_book', return_value=snap), \
             patch('src.gridbot.prediction.regime_worker_bridge.read_c180_book', return_value=snap), \
             patch('src.gridbot.prediction.regime_worker_bridge.read_c180_signal', return_value=signal):
            return self.check()

    def test_flat_at_060_is_allowed(self):
        result = self.flat('.60')
        self.assertTrue(result.allowed, result.reason)
        self.assertEqual(result.execution.worst_ask_limit, D('.60'))

    def test_flat_above_060_cannot_reenter_on_later_cheaper_quote(self):
        result = self.flat('.61')
        self.assertFalse(result.allowed, result.reason)
        saved = json.loads(self.features.execute('SELECT payload FROM decisions').fetchone()[0])
        self.assertFalse(saved['allowed'])
        self.assertEqual(saved['fingerprint'], t62.FINGERPRINT)
        self.store.persist_book(old.book('.55', 125000))
        self.assertFalse(self.check(125100).allowed)

    def test_t61_fallback_remains_allowed_at_070(self):
        self.activate()
        self.seed(old.feature(.1, .1, 0, .2), '.70')
        result = self.check()
        self.assertTrue(result.allowed, result.reason)
        saved = json.loads(self.features.execute('SELECT payload FROM decisions').fetchone()[0])
        self.assertEqual((saved['branch'], saved['fingerprint']), ('fallback', t62.FINGERPRINT))

    def test_other_lane_frozen_decision_is_not_reused(self):
        self.seed(old.feature(2, 2, 0, 4), '.40')
        self.assertTrue(self.check().allowed)
        self.activate()
        self.assertEqual(self.check().reason, 'regime_decision_identity_mismatch')

    def _check_scaled_entry(self, unit):
        self.activate()
        self.seed(old.feature(2, 2, 0, 4), '.40')
        snap = old.book('.40')
        with patch.object(self.bridge, '_first_book', return_value=snap), \
             patch('src.gridbot.prediction.regime_worker_bridge.read_c180_book', return_value=snap):
            ready = self.bridge.check_signal(market=self.market, unit_usdt=unit,
                at_ms=old.START+124200, last_seen_book_at_ms=old.START+120000)
        self.assertTrue(ready.allowed, ready.reason)
        self.assertEqual(ready.signal.entry.stake_usdt, unit)
        self.assertGreater(ready.execution.expected_cash_usdt, unit-D('.01'))
        self.assertFalse(self.bridge.check_signal(market=self.market, unit_usdt=D(1),
            at_ms=old.START+124200, last_seen_book_at_ms=old.START+120000).allowed)

    def test_two_unit_depth_and_frozen_signal(self):
        self._check_scaled_entry(D(2))

    def test_three_unit_depth_and_frozen_signal(self):
        self._check_scaled_entry(D(3))

    def test_three_unit_candidate_requires_three_unit_depth(self):
        self.activate()
        self.seed(old.feature(2, 2, 0, 4), '.40')
        snap = old.book('.40')
        snap['quote']['DOWN']['ask_levels'] = [['.40', '5']]
        with patch.object(self.bridge, '_first_book', return_value=snap):
            ready = self.bridge.check_signal(market=self.market, unit_usdt=D(3),
                at_ms=old.START+124200, last_seen_book_at_ms=old.START+120000)
        self.assertFalse(ready.allowed)
        self.assertIn('depth', ready.reason)


class T62ScaledRiskTests(unittest.TestCase):
    def state(self):
        return {'fingerprint': RISK_FINGERPRINT, 'first_market_start_ms': old.START,
                'unit_usdt': '1', 'halt_reason': None}

    def settlement(self, n, unit, pnl):
        return LiveSettlement(str(n), old.START+n*300000, D(pnl), old.START+n*300000+300000, D(unit))

    def test_mixed_units_preserve_old_history_and_normalize_risk(self):
        state = self.state()
        rows = [self.settlement(0, 1, '-1'), self.settlement(1, 2, '-2')]
        allowed, reason = risk_result(state, rows, old.START+3*300000, old.START+4*300000)
        self.assertTrue(allowed, reason)
        self.assertEqual((state['net_pnl_usdt'], state['risk_equity_1u']), ('-3', '-2'))
        rows.append(self.settlement(2, 3, '-3'))
        allowed, reason = risk_result(state, rows, old.START+4*300000, old.START+5*300000)
        self.assertTrue(allowed, reason)
        self.assertEqual(state['risk_equity_1u'], '-3')
        rows.append(self.settlement(3, 3, '-3'))
        allowed, reason = risk_result(state, rows, old.START+5*300000, old.START+6*300000)
        self.assertFalse(allowed)
        self.assertEqual(reason, 'scheduled20_mdd_3.5')

    def test_old_halt_remains_latched(self):
        state = self.state()
        state['halt_reason'] = 'scheduled20_mdd_3.5'
        self.assertEqual(risk_result(state, [], old.START+300000, old.START+300000),
                         (False, 'scheduled20_mdd_3.5'))

    def test_pure_two_and_three_unit_mdd_scales_exactly(self):
        for unit in (D(2), D(3)):
            with self.subTest(unit=unit):
                state = self.state()
                rows = [self.settlement(n, unit, str(-unit)) for n in range(3)]
                allowed, reason = risk_result(state, rows, old.START+4*300000, old.START+5*300000)
                self.assertTrue(allowed, reason)
                rows.append(self.settlement(3, unit, str(-unit)))
                allowed, reason = risk_result(state, rows, old.START+5*300000, old.START+6*300000)
                self.assertFalse(allowed)
                self.assertEqual(reason, 'scheduled20_mdd_3.5')
                self.assertEqual(D(state['net_pnl_usdt']), -4*unit)

    def test_depth_walk_scales_cash_and_rejects_insufficient_book(self):
        for unit in (D(1), D(2), D(3)):
            fill = walk([('.40', '10')], D(0), amount=unit)
            self.assertEqual(fill['cash'], unit)
        with self.assertRaises(ValueError):
            walk([('.40', '5')], D(0), amount=D(3))


class T62LedgerTests(old.CrossLaneLedgerTests):
    async def test_claim_uses_shared_epoch_and_lock(self):
        with patch.object(old, 't61', t62):
            await super().test_claim_uses_shared_epoch_and_lock()

    async def _claim_scaled_unit(self, unit):
        await self.ledger.check_risk('loop1', old.START, old.START+125000)
        start = old.START+20*old.t6.SLOT_MS
        await self.repo.start_loop('loop2', 20, mode='LIVE', strategy_profile=t62.PROFILE)
        market = old.MarketInfo('topic2', 'up2', 'test', start, start+old.t6.SLOT_MS,
                                up_market_id='up2', down_market_id='down2')
        await self.repo.save_campaign(old.Campaign('c2', market), loop_id='loop2')
        ledger = RegimeLiveLedger(self.repo, profile=t62.PROFILE)
        await ledger.seed_schedule(loop_id='loop2', first_market_start_ms=start)
        await ledger.verify_market(loop_id='loop2', market_start_ms=start,
            market_topic_id='topic2', market_id='up2', verified_at_ms=start+124000)
        intent = {**self.intent, 'intent_id':'i2', 'campaign_id':'c2',
                  'created_at_ms':start+125000, 'limit_price':'.70',
                  'amount':str(unit), 'tier':t62.TIER}
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=start+125000):
            claimed = await ledger.reserve_c180_intent(loop_id='loop2', market_start_ms=start,
                campaign_id='c2', intent=intent, decision_at_ms=start+125000,
                wallet_reconciled_at_ms=start+125000)
        self.assertTrue(claimed.claimed, claimed.reason)
        rows = await self.repo._fetchall('SELECT unit_usdt FROM prediction_regime_entry_claims')
        self.assertEqual([D(row['unit_usdt']) for row in rows], [unit])

    async def test_two_unit_claim_persists_exact_amount(self):
        await self._claim_scaled_unit(D(2))

    async def test_three_unit_claim_persists_exact_amount(self):
        await self._claim_scaled_unit(D(3))


class T62TimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_t62_freezes_before_wallet_and_obeys_risk_halt(self):
        for allowed in (True, False):
            bridge = latency.FakeBridge(gate_allowed=allowed)
            worker = latency.FakeWorker(bridge, [old.START+124200]*5)
            worker._selected_strategy_profile = t62.PROFILE
            result = await worker._c180_decide(latency.campaign(), old.START+123900)
            self.assertEqual(bridge.calls, ['register', 'signal', 'prepare'])
            self.assertEqual(result.action.value, 'BUY_INITIAL' if allowed else 'HOLD')


class T62ReportTests(unittest.TestCase):
    setUp = reports.ReportTests.setUp
    tearDown = reports.ReportTests.tearDown
    loop = reports.ReportTests.loop
    gate = reports.ReportTests.gate
    fill = reports.ReportTests.fill
    render = reports.ReportTests.render

    def test_t62_report_keeps_own_pnl_and_shared_risk(self):
        self.db.execute('UPDATE prediction_loops SET strategy_profile=? WHERE loop_id=?', (t62.PROFILE, 'new'))
        self.db.commit()
        self.loop('prior', old.t61.PROFILE, 'DONE', 0)
        self.fill('before', loop='prior', start=reports.START, pnl='-1')
        self.fill('after', loop='new', start=reports.START+20*300000, pnl='2')
        self.gate(net_pnl_usdt='1')
        text = self.render()
        self.assertIn('Regime T6.2 Live Report', text)
        self.assertIn('上限0.60', text)
        self.assertIn('本輪已知淨 PnL +2.0000', text)
        self.assertIn('累計已知淨 PnL +1.0000', text)
        self.assertNotIn('風控累計與可核對結算不一致', text)

    def test_two_unit_fill_uses_cash_pnl_and_normalized_risk(self):
        self.db.execute('UPDATE prediction_loops SET strategy_profile=? WHERE loop_id=?', (t62.PROFILE, 'new'))
        self.db.commit()
        self.fill('two', pnl='2')
        self.db.execute("UPDATE prediction_regime_entry_claims SET unit_usdt='2' WHERE campaign_id='two'")
        self.db.commit()
        self.gate(net_pnl_usdt='2', risk_equity_1u='1')
        text = self.render()
        self.assertIn('本輪已知淨 PnL +2.0000', text)
        self.assertIn('風控1U等值 +1.0000', text)
        self.assertNotIn('風控1U等值與可核對結算不一致', text)
