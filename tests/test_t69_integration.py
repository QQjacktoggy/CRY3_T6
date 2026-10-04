"""T6.9 registration, existing risk durability and actionable admission diagnostics."""
import json
import sqlite3
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

import test_t6_entry_critical_path as critical
from src.gridbot.prediction.c180_gate_runtime import LiveSettlement, LoopLedgerSnapshot
from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
from src.gridbot.prediction import live_report, regime_worker_bridge
from src.gridbot.prediction.regime_live_ledger import RISK_PROFILES, RegimeLiveLedger
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT, PROFILE, TIER
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.telegram import (
    SELECTABLE_LANES, _regime_risk_text, selectable_lanes_for_market,
)
from src.gridbot.prediction.worker import PredictionWorker
from test_t63 import S
from test_t67c_report import main_database


def test_t69_is_selectable_on_three_markets_with_original_units_and_no_sibling_trades():
    assert PROFILE in PredictionWorker._selectable_strategy_profiles()
    assert SELECTABLE_LANES[0][0] == 'regime_target6_9_v1'
    assert PROFILE in dict(selectable_lanes_for_market('BTCUSDT'))
    assert PROFILE in dict(selectable_lanes_for_market('ETHUSDT'))
    assert PROFILE in dict(selectable_lanes_for_market('BNBUSDT'))
    assert tuple(RISK_PROFILES) == tuple(live_report.RISK_PROFILES)
    assert len(RISK_PROFILES) == len(set(RISK_PROFILES)) == 15
    cfg = StrategyConfig.for_profile(PROFILE)
    assert cfg.provenance_payload['regime_policy_fingerprint'] == FINGERPRINT
    assert cfg.provenance_payload['t69_policy']['profile'] == PROFILE
    assert (cfg.entry_start_seconds, cfg.entry_end_seconds) == (120, 184)
    assert cfg.max_initial_attempts == 1
    assert cfg.max_scale_in_attempts == cfg.max_hedge_attempts == 0
    assert not cfg.protective_exit_enabled and not cfg.profit_lock_enabled
    assert RegimeLiveLedger(None, profile=PROFILE).tier == TIER == 'REGIME_T69'
    worker = object.__new__(PredictionWorker)
    worker._selected_strategy_profile = PROFILE
    worker._fav_p3_arm_override = 'live'
    worker._shadow_lane_strategies = {'old': object()}
    assert not worker._fav_p3_live_orders_enabled()
    assert not worker._shadow_lane_experiment_enabled()
    for unit in (D(1), D(2), D(3)):
        sized = PredictionWorker._sized_strategy_config(PROFILE, unit)
        assert sized.max_buy_usdt == sized.max_market_buy_usdt == unit
        assert 'T6.9' in _regime_risk_text(PROFILE, unit)
        assert '本輪MDD' in _regime_risk_text(PROFILE, unit)


def test_bridge_dispatches_to_t69_and_uses_t69_fingerprint():
    bridge = regime_worker_bridge.RegimeWorkerBridge(None, 'unused', profile=PROFILE)
    sentinel = object()
    market = SimpleNamespace(start_time_ms=S)
    with patch('src.gridbot.prediction.regime_t69_bridge.check_signal', return_value=sentinel) as check:
        assert bridge.check_signal(market=market, unit_usdt=D(2), at_ms=S+124000,
                                   last_seen_book_at_ms=S+123000) is sentinel
    assert bridge.decision_fingerprint == FINGERPRINT
    assert check.call_args.kwargs['unit_usdt'] == D(2)


def test_t69_selected_before_first_loop_gets_own_empty_report(tmp_path):
    with main_database(tmp_path) as db:
        db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',
                   ('prediction_selected_strategy', json.dumps({'profile': PROFILE})))
    assert live_report.t67_family_report_profile(tmp_path) == PROFILE
    with patch('src.gridbot.prediction.regime_t69_report.empty_report', return_value='T6.9 empty'):
        report = live_report.format_live_report(tmp_path, profile_filter=PROFILE, now_ms=S)
    assert report == 'T6.9 empty'


def test_t69_historical_loop_is_in_report_family_fallback(tmp_path):
    with main_database(tmp_path) as db:
        db.execute('UPDATE prediction_loops SET state=\'DONE\',strategy_profile=?', (PROFILE,))
    assert live_report.t67_family_report_profile(tmp_path) == PROFILE


def test_t69_retains_original_and_public_evidence_collection(tmp_path):
    path = tmp_path/'db'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE prediction_runtime_config(config_key TEXT PRIMARY KEY,config_value_json TEXT)')
        db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',
                   ('prediction_selected_strategy', json.dumps({'profile': PROFILE})))
    runtime = object.__new__(C180SignalRuntime)
    runtime.prediction_db = path
    runtime.signals = SimpleNamespace(on_frozen=Mock())
    runtime._t67_profile_checked_ms = 0
    runtime._t67_selected = False
    runtime._t67_last_error_ms = 0
    with patch('src.gridbot.prediction.c180_signal_runtime._now_ms', return_value=S+120000):
        assert runtime._t67_active(S+120000) is True
        runtime._on_frozen({'frozen': 'cutoff evidence'})
    runtime.signals.on_frozen.assert_called_once()


@pytest.mark.asyncio
async def test_t69_own_loop_guard_survives_restart_and_checks_policy_fingerprint(tmp_path):
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo.start_loop('current', 100, mode='LIVE', strategy_profile=PROFILE)
        ledger = RegimeLiveLedger(repo, profile=PROFILE)
        await ledger.seed_schedule(loop_id='current', first_market_start_ms=S)
        assert (await ledger.check_risk('current', S, S+124000))[0]
        rows = (LiveSettlement('profit', S, D(3), S+300000, D(1)),
                LiveSettlement('loss', S+300000, D('-3.5'), S+600000, D(1)))
        value = LoopLedgerSnapshot('current', True, (), (), rows, ())
        with patch.object(ledger, '_snapshot_conn', AsyncMock(return_value=value)):
            assert await ledger.check_risk('current', S+600000, S+724000) == (False, 't69_loop_mdd_3.5')
        key = 'regime_target6_9_loop_risk:current'
        guard = await repo.get_runtime_config(key)
        assert guard['fingerprint'] == FINGERPRINT and D(guard['mdd_1u']) == D('3.5')
        assert await repo.get_runtime_config('regime_target6_7c_loop_risk:current') is None
        restarted = RegimeLiveLedger(repo, profile=PROFILE)
        empty = LoopLedgerSnapshot('current', True, (), (), (), ())
        with patch.object(restarted, '_snapshot_conn', AsyncMock(return_value=empty)):
            assert await restarted.check_risk('current', S+900000, S+1024000) == (False, 't69_loop_mdd_3.5')
            await repo.set_runtime_config(key, {**guard, 'fingerprint': 'wrong'})
            assert await restarted.check_risk('current', S+900000, S+1024000) == (False, 't69_loop_risk_state_invalid')
    finally:
        await repo.close()


class PostClaimDiagnosticsTests(critical.LedgerFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.repo._execute('UPDATE prediction_loops SET strategy_profile=?', (PROFILE,))
        self.ledger = RegimeLiveLedger(self.repo, profile=PROFILE)

    async def test_adverse_price_has_durable_reason_but_no_post_retry_or_hs_reset(self):
        def fault(worker):
            bad = SimpleNamespace(**vars(worker.ready))
            bad.reason = 't69_book_price_changed'
            bad.execution = SimpleNamespace(worst_ask_limit=D('.36'), expires_at_ms=critical.START+127000)
            worker.bridge.check_signal.return_value = bad
        worker, campaign = await critical.WorkerBoundaryTests.run_entry(self, fault)
        row = (await self.repo._fetchall('SELECT * FROM prediction_order_intents'))[0]
        details = json.loads(row['payload_json'])
        self.assertEqual(details['reason'], 'post_claim_admission_rejected')
        self.assertEqual(details['admission_reason'], 't69_book_price_changed')
        self.assertEqual(details['denial_categories'], ['worst_ask_exceeds_intent_limit'])
        telemetry = [payload for event, _, payload in worker._observability_events if event == 'entry_finished'][-1]
        self.assertEqual(telemetry['denial_categories'], details['denial_categories'])
        self.assertEqual(telemetry['admission_reason'], details['admission_reason'])
        self.assertEqual(worker.sent, [])
        self.assertIsNone(row['submission_at_ms'])
        self.assertEqual(row['status'], 'REJECTED')
        self.assertFalse(campaign.pending_unknown)
        self.assertEqual(campaign.initial_attempts, 1)
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')), 1)
        self.assertFalse(worker._hard_stop_latched)

    async def test_risk_hs_category_is_distinct_from_book_and_stays_latched(self):
        reserve = self.ledger.reserve_c180_intent
        async def latch(**kwargs):
            result = await reserve(**kwargs)
            await self.repo.set_runtime_config('prediction_risk_state', {'hard_stop_latched': True})
            return result
        self.ledger.reserve_c180_intent = latch
        worker, _ = await critical.WorkerBoundaryTests.run_entry(self)
        row = (await self.repo._fetchall('SELECT * FROM prediction_order_intents'))[0]
        details = json.loads(row['payload_json'])
        self.assertEqual(details['denial_categories'], ['risk_hard_stop'])
        self.assertEqual(details['admission_reason'], 'ready')
        self.assertEqual(worker.sent, [])
        self.assertTrue((await self.repo.get_runtime_config('prediction_risk_state'))['hard_stop_latched'])
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')), 1)


@pytest.mark.parametrize('change,expected', [
    ({'live_capability': False}, ('live_capability_disabled',)),
    ({'allow_new_buys': False}, ('new_buys_disabled',)),
    ({'hard_stop_latched': True}, ('worker_hard_stop',)),
    ({'bound': None}, ('loop_missing',)),
    ({'bound': {'state': 'DONE'}}, ('loop_not_running',)),
    ({'bound': {'state': 'RUNNING', 'new_entries_stopped': True}}, ('loop_entries_stopped',)),
    ({'bound': {'state': 'RUNNING', 'hard_stop_latched': True}}, ('loop_hard_stop',)),
    ({'now_ms': 136000}, ('execution_deadline_reached',)),
])
def test_post_claim_controls_are_individually_diagnosable(change, expected):
    signal = object()
    ready = SimpleNamespace(signal=signal, execution=SimpleNamespace(expires_at_ms=136000))
    checked = SimpleNamespace(allowed=True, signal=signal,
                              execution=SimpleNamespace(worst_ask_limit=D('.5')))
    arguments = dict(checked=checked, ready=ready, intent=SimpleNamespace(limit_price=D('.5')),
                     bound={'state': 'RUNNING'}, risk_state={}, live_capability=True,
                     allow_new_buys=True, hard_stop_latched=False, now_ms=135999)
    assert PredictionWorker._post_claim_admission_denials(**arguments) == ()
    arguments.update(change)
    assert PredictionWorker._post_claim_admission_denials(**arguments) == expected


def test_t69_live_composition_keeps_parent_and_adds_only_flat():
    from src.gridbot.prediction.regime_t67c_policy import LIVE_BRANCHES as retained
    from src.gridbot.prediction.regime_t69_policy import LIVE_BRANCHES, SHADOW_BRANCHES
    assert LIVE_BRANCHES == retained + ('flat_favorite', 'reference_180_mid')
    assert SHADOW_BRANCHES == ('external_lead_lag', 'reference_value', 'flat_quiet_favorite', 'flat_cheap_prior', 'flat_hold_180')
    assert len(LIVE_BRANCHES) == 9 and len(SHADOW_BRANCHES) == 5


@pytest.mark.parametrize('profile,offset,attempted,pending,expected', [
    (PROFILE, 178999, False, False, .1),
    (PROFILE, 179000, False, False, .1),
    (PROFILE, 181000, False, False, .1),
    (PROFILE, 183500, False, False, 1),
    (PROFILE, 181000, True, False, 1),
    (PROFILE, 181000, False, True, 1),
    ('regime_target6_7c_v1', 181000, False, False, 1),
])
def test_late_selection_scheduling_is_bounded_and_never_retries_attempted_markets(profile, offset, attempted, pending, expected):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    campaign = Campaign('current', MarketInfo('topic', 'up', 'test', S, S+300000))
    campaign.initial_attempts = int(attempted)
    campaign.pending_unknown = pending
    worker = object.__new__(PredictionWorker)
    worker.settings = SimpleNamespace(poll_interval_seconds=1)
    worker._selected_strategy_profile = profile
    worker._active_campaigns = {'current': campaign}
    worker._now_ms = lambda: S+offset
    assert worker._entry_tick_delay() == expected


class LateReferenceWorkerTests(critical.LedgerFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.repo._execute('UPDATE prediction_loops SET strategy_profile=?', (PROFILE,))
        self.ledger = RegimeLiveLedger(self.repo, profile=PROFILE)

    def late_worker(self, at=180000):
        worker = critical.EntryHarness(self.repo, self.ledger)
        worker.clock = critical.START+at
        worker.ready.execution.expires_at_ms = critical.START+182000
        worker.ready.book_at_ms = critical.START+179999
        worker.bridge.check_signal.return_value.book_at_ms = critical.START+180000
        worker.bridge.register_market = AsyncMock(return_value=SimpleNamespace(allowed=True))
        worker.bridge.prepare_market = AsyncMock(return_value=SimpleNamespace(allowed=True))
        return worker

    async def test_late_reference_goes_through_decision_claim_and_single_http_path(self):
        from src.gridbot.prediction.models import ActionType, Campaign
        worker = self.late_worker()
        campaign = Campaign('c1', self.market)
        decision = await worker._c180_decide(campaign, worker.clock)
        self.assertEqual(decision.action, ActionType.BUY_INITIAL)
        self.assertEqual(decision.ttl_ms, 2000)
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=worker.clock):
            await worker._handle_decision(campaign, decision)
        self.assertEqual(len(worker.sent), 1, worker._observability_events)
        intent = await self.repo.get_intent(campaign.pending_intent_id)
        self.assertEqual(intent['tier'], TIER)
        self.assertEqual(intent['status'], 'SUBMITTED')
        self.assertEqual(intent['ttl_deadline_ms'], critical.START+182000)
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')), 1)
        # Existing initial_attempts/pending order barrier survives every later tick.
        later = await worker._c180_decide(campaign, worker.clock)
        self.assertEqual(later.action, ActionType.HOLD)
        self.assertEqual(len(worker.sent), 1)

    async def test_real_reference_bridge_reaches_single_post_with_180s_signal_and_frozen_expiry(self):
        from contextlib import closing
        from pathlib import Path
        from src.gridbot.prediction.models import ActionType, Campaign
        from src.gridbot.prediction.regime_feature_service import connect
        from src.gridbot.prediction import regime_t69_bridge as late
        from test_t63 import feature
        from test_t67 import snap, tape

        base = critical.START
        delta = base-S
        def book(offset, up='.4', down='.6'):
            value = snap(offset, up=up, down=down)
            for key in ('market_start_ms', 'reference_received_ms', 'book_at_ms',
                        'received_at', 'received_at_ms', 'captured_at_ms'):
                value[key] += delta
            return value
        def public(_path, _start, at_ms):
            offset = at_ms-base
            spots = tape(offset)
            for value in spots:
                value['event_ms'] += delta
                value['received_ms'] += delta
            return [book(offset)], spots
        path = Path(self.tmp.name)/'features.sqlite3'
        features = feature(2, -1, -2)
        features.update(market_start_ms=base, cutoff_ms=base+120000, received_at_ms=base+120500)
        with closing(connect(path)) as db, db:
            db.execute('INSERT INTO features VALUES(?,?)', (base, json.dumps(features)))
        bridge = regime_worker_bridge.RegimeWorkerBridge(self.repo, Path(self.tmp.name)/'signals.sqlite3',
            feature_db=path, profile=PROFILE, exposure_checker=AsyncMock(return_value=True))
        bridge._registered_loop_id = 'loop1'
        initial = book(124000, up='.8', down='.2')
        with patch.object(bridge, '_first_book', return_value=initial), \
                patch.object(regime_worker_bridge, 'read_c180_book', return_value=initial), \
                patch.object(regime_worker_bridge, 'read_c180_signal', return_value=None):
            early = bridge.check_signal(market=self.market, unit_usdt=D(1),
                                        at_ms=base+124000, last_seen_book_at_ms=0)
        self.assertFalse(early.allowed)
        self.assertEqual(early.reason, 't69_wait_reference_checkpoint')
        worker = critical.EntryHarness(self.repo, self.ledger)
        worker.clock = base+180500
        worker._c180_bridge_for_worker = Mock(return_value=bridge)
        call = worker._call_api
        async def fresh_reads(method, *arguments, **options):
            if method in ('get_quote', 'query_payment_option_balances'):
                worker.clock += 100
            return await call(method, *arguments, **options)
        worker._call_api = fresh_reads
        campaign = Campaign('c1', self.market)
        with patch.object(late, 'read_inputs', side_effect=public), \
                patch('src.gridbot.prediction.regime_live_ledger._now_ms', side_effect=lambda: worker.clock):
            decision = await worker._c180_decide(campaign, worker.clock)
            self.assertEqual(decision.action, ActionType.BUY_INITIAL, decision.reason)
            ready = worker._c180_ready['c1']
            self.assertEqual(ready.signal.cutoff_ms, base+180000)
            self.assertEqual(ready.execution.expires_at_ms, base+182500)
            await worker._handle_decision(campaign, decision)
        self.assertEqual(len(worker.sent), 1, worker._observability_events)
        self.assertEqual(worker.sent[0]['_entry_deadline_ms'], base+182500)
        self.assertEqual(worker.sent[0]['_entry_profile'], PROFILE)
        row = await self.repo.get_intent(campaign.pending_intent_id)
        self.assertEqual(row['status'], 'SUBMITTED')
        self.assertEqual(row['ttl_deadline_ms'], base+182500)
        with sqlite3.connect(path) as db:
            frozen = json.loads(db.execute('SELECT payload FROM t69_decisions').fetchone()[0])
        self.assertEqual(frozen['branch'], 'reference_180_mid')
        self.assertTrue(frozen['selected'])
        self.assertNotIn('reference_execution_denied', frozen)
        self.assertEqual(len(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims')), 1)

    async def test_gap_between_core_and_reference_never_consults_or_sends_late_signal(self):
        from src.gridbot.prediction.models import ActionType, Campaign
        worker = self.late_worker(at=170000)
        decision = await worker._c180_decide(Campaign('c1', self.market), worker.clock)
        self.assertEqual(decision.action, ActionType.HOLD)
        self.assertEqual(decision.reason, 'c180 outside execution window')
        worker.bridge.check_signal.assert_not_called()
        self.assertEqual(worker.sent, [])

    async def test_reference_ready_after_global_deadline_never_reaches_bridge(self):
        from src.gridbot.prediction.models import ActionType, Campaign
        worker = self.late_worker(at=183500)
        decision = await worker._c180_decide(Campaign('c1', self.market), worker.clock)
        self.assertEqual(decision.action, ActionType.HOLD)
        worker.bridge.check_signal.assert_not_called()
        self.assertEqual(worker.sent, [])

    async def test_expiry_during_claim_risk_cannot_extend_late_window(self):
        from src.gridbot.prediction.models import Campaign
        worker = self.late_worker(at=181499)
        worker.ready.execution.expires_at_ms = critical.START+183499
        worker.bridge.check_signal.return_value.execution.expires_at_ms = critical.START+183499
        worker.bridge.check_signal.return_value.book_at_ms = critical.START+181499
        decision = await worker._c180_decide(Campaign('c1', self.market), worker.clock)
        risk = self.ledger._risk_conn
        async def cross(*arguments):
            result = await risk(*arguments)
            worker.clock = critical.START+183499
            return result
        with patch.object(self.ledger, '_risk_conn', side_effect=cross), \
                patch('src.gridbot.prediction.regime_live_ledger._now_ms', side_effect=lambda: worker.clock):
            await worker._handle_decision(Campaign('c1', self.market), decision)
        self.assertEqual(worker.sent, [])
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_regime_entry_claims'), [])
        self.assertEqual(await self.repo._fetchall('SELECT * FROM prediction_order_intents'), [])


@pytest.mark.asyncio
@pytest.mark.parametrize('prior', ['claim', 'rejected_intent'])
async def test_any_old_loop_attempt_blocks_late_reference_even_without_fill(tmp_path, prior):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
        await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
        await repo.start_loop('old', 100, mode='LIVE', strategy_profile='regime_target6_7c_v1')
        await repo.start_loop('current', 100, mode='LIVE', strategy_profile=PROFILE)
        market = MarketInfo('topic', 'up', 'test', S, S+300000, up_market_id='up', down_market_id='down')
        await repo.save_campaign(Campaign('old_campaign', market), loop_id='old')
        await repo.save_campaign(Campaign('new_campaign', market), loop_id='current')
        ledger = RegimeLiveLedger(repo, profile=PROFILE)
        await ledger.seed_schedule(loop_id='current', first_market_start_ms=S)
        await ledger.verify_market(loop_id='current', market_start_ms=S,
                                   market_topic_id='topic', market_id='up', verified_at_ms=S+120000)
        if prior == 'claim':
            # Damaged historical claim without its intent must still block;
            # do not turn missing reconciliation evidence into a new entry.
            await repo._execute('PRAGMA foreign_keys=OFF')
            await repo._execute('INSERT INTO prediction_regime_entry_claims '
                '(loop_id,market_start_ms,campaign_id,intent_id,unit_usdt,claimed_at_ms) VALUES(?,?,?,?,?,?)',
                ('old', S, 'old_campaign', 'old_intent', '1', S+125000))
            await repo._execute('PRAGMA foreign_keys=ON')
        else:
            await repo._execute("INSERT INTO prediction_order_intents "
                "(intent_id,campaign_id,action,outcome,order_side,amount,limit_price,created_at_ms,ttl_ms,attempt,status,unknown,payload_json) "
                "VALUES('old_intent','old_campaign','BUY_INITIAL','UP','BUY','1','.5',?,1000,1,'REJECTED',0,'{}')", (S+125000,))
        intent = dict(intent_id='late', campaign_id='new_campaign', action='BUY_INITIAL', outcome='UP',
                      order_side='BUY', amount='1', limit_price='.5', created_at_ms=S+180000,
                      ttl_ms=2000, attempt=1, status='PENDING', tier=TIER, payload={})
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=S+180000):
            result = await ledger.reserve_c180_intent(loop_id='current', market_start_ms=S,
                campaign_id='new_campaign', intent=intent, decision_at_ms=S+180000,
                wallet_reconciled_at_ms=S+180000, expires_at_ms=S+182000)
        assert not result.claimed and result.reason == 'market_buy_already_claimed'
        assert not await repo._fetchall("SELECT * FROM prediction_order_intents WHERE intent_id='late'")
    finally:
        await repo.close()


@pytest.mark.parametrize('profile,expected', [(PROFILE, .2), ('regime_target6_7c_v1', 5)])
def test_long_poll_wakes_before_reference_selection_start(profile, expected):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    campaign = Campaign('current', MarketInfo('topic', 'up', 'test', S, S+300000))
    worker = object.__new__(PredictionWorker)
    worker.settings = SimpleNamespace(poll_interval_seconds=5)
    worker._selected_strategy_profile = profile
    worker._active_campaigns = {'current': campaign}
    worker._now_ms = lambda: S+178800
    assert worker._entry_tick_delay() == expected
