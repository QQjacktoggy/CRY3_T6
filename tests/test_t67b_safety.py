"""T6.7b execution boundaries and inherited durable risk regressions."""
import sqlite3
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.gridbot.prediction.c180_gate_runtime import LiveSettlement, LoopLedgerSnapshot
from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
from src.gridbot.prediction.models import Campaign, MarketInfo
from src.gridbot.prediction.regime_lane import FINGERPRINT as RISK_FP
from src.gridbot.prediction.regime_live_ledger import RISK_PROFILES, RegimeLiveLedger
from src.gridbot.prediction.regime_t67b_policy import FINGERPRINT, PROFILE, TIER
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.telegram import _regime_risk_text, selectable_lanes_for_market
from src.gridbot.prediction.worker import PredictionWorker
from test_t63 import S, book, feature
from test_t67b import setup, state


@pytest.mark.parametrize('first,last,prior,initial_up,next_up,branch', [
    (2, -1, 2, '.30', '.29', 'core_first_up'),
    (2, -1, -2, '.60', '.61', 'shallow_retracement'),
    (-1, 3, 2, '.70', '.69', 'c_mirror_up_prior'),
])
def test_new_book_refreshes_execution_but_preserves_selected_signal_across_restart(
        tmp_path, first, last, prior, initial_up, next_up, branch):
    from src.gridbot.prediction import regime_worker_bridge as b
    from src.gridbot.prediction.regime_lane import walk

    bridge, check = setup(tmp_path, feature(first, last, prior), book(initial_up, '.40', 124000))
    ready = check()
    assert ready.allowed, ready.reason
    assert state(bridge)['branch'] == branch
    frozen_json = state(bridge)['signal']
    newer = book(next_up, '.40', 124400)
    refreshed = check(newer, seen=ready.book_at_ms)
    assert refreshed.allowed, refreshed.reason
    # The worker compares these immutable signals before reserving its BUY.
    assert refreshed.signal == ready.signal
    assert refreshed.execution.expected_shares != ready.execution.expected_shares
    assert refreshed.signal.entry.expected_shares == ready.execution.expected_shares
    assert refreshed.execution.expected_shares == walk(
        newer['quote']['UP']['ask_levels'], newer['fee_bps'],
        cap=refreshed.execution.worst_ask_limit, amount=D(1))['net_shares']
    assert state(bridge)['signal'] == frozen_json

    restarted = b.RegimeWorkerBridge(None, bridge.signal_db, feature_db=bridge.feature_db, profile=PROFILE)
    restarted._registered_loop_id = bridge._registered_loop_id
    market = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up')
    with patch.object(b, 'read_c180_book', return_value=newer):
        after_restart = restarted.check_signal(market=market, unit_usdt=D(1),
            at_ms=S+124400, last_seen_book_at_ms=ready.book_at_ms)
    assert after_restart.allowed, after_restart.reason
    assert after_restart.signal == ready.signal
    assert after_restart.execution == refreshed.execution


def test_same_book_requests_worker_retry_but_stale_book_does_not(tmp_path):
    bridge, check = setup(tmp_path, feature(2, -1, 2), book('.30', '.70', 124000))
    ready = check()
    assert ready.allowed, ready.reason
    same = check(at=S+124100, seen=ready.book_at_ms)
    assert not same.allowed and same.reason == 'quote_not_new_after_ready'
    stale = check(at=S+125100, seen=ready.book_at_ms)
    assert not stale.allowed and stale.reason == 't67b_fresh_book_required'
    assert check(book('.29', '.71', 125200), seen=ready.book_at_ms).allowed


@pytest.mark.parametrize('failure', ['cap', 'depth', 'original_ev'])
def test_frozen_signal_does_not_authorize_adverse_fresh_execution(tmp_path, failure):
    from src.gridbot.prediction.c180_favorite import C180EntryDecision
    from src.gridbot.prediction.c180_signal_service import C180Signal

    original = None
    f = feature(2, -1, 2)
    initial = book('.30', '.70', 124000)
    newer = book('.31', '.69', 124400)
    if failure == 'depth':
        newer = book('.29', '.71', 124400)
        newer['quote']['UP']['ask_levels'] = [['.29', '.01']]
    elif failure == 'original_ev':
        f = feature(2, 1, 2)
        initial = book('.80', '.20', 124000)
        initial['quote']['DOWN']['ask_levels'] = [['.15', '1'], ['.20', '100']]
        entry = C180EntryDecision('DOWN', 'original', 'DOWN', D(1), D(5), None)
        original = C180Signal(S, 'topic', 'up', S+120000, S+120500,
            'entry_positive_cost_after_ev', entry, D('.795'), None, None, D(200))
        newer = book('.80', '.20', 124400)
    bridge, check = setup(tmp_path, f, initial, original)
    ready = check()
    assert ready.allowed, ready.reason
    frozen = state(bridge)['signal']
    if failure == 'original_ev':
        from src.gridbot.prediction.regime_lane import walk
        assert D(newer['quote']['DOWN']['ask_levels'][0][0]) <= D(state(bridge)['cap'])
        execution = walk(newer['quote']['DOWN']['ask_levels'], newer['fee_bps'], D(state(bridge)['cap']))
        assert (1-ready.signal.original_p_up)*execution['net_shares']-execution['cash'] <= D('.005')
    rejected = check(newer, seen=ready.book_at_ms)
    assert not rejected.allowed
    assert state(bridge)['signal'] == frozen
    assert state(bridge)['side'] == ready.signal.entry.side


@pytest.mark.parametrize('failure', ['missing', 'topic', 'id', 'unit', 'fee'])
def test_existing_selection_requires_valid_stored_signal_without_rebuilding(tmp_path, failure):
    import json

    bridge, check = setup(tmp_path, feature(2, -1, 2), book('.30', '.70', 124000))
    ready = check()
    assert ready.allowed, ready.reason
    d = state(bridge)
    if failure == 'missing':
        del d['signal']
    else:
        signal = json.loads(d['signal'])
        if failure == 'topic':
            signal['market_topic'] = 'different'
        elif failure == 'id':
            signal['market_id'] = 'different'
        elif failure == 'unit':
            signal['entry']['stake_usdt'] = '2'
        elif failure == 'fee':
            signal['frozen_fee_bps'] = '300'
        d['signal'] = json.dumps(signal)
    with sqlite3.connect(bridge.feature_db) as db:
        db.execute('UPDATE t67b_decisions SET payload=? WHERE start=?', (json.dumps(d), S))
    rejected = check(book('.29', '.71', 124400), seen=ready.book_at_ms)
    assert not rejected.allowed
    assert rejected.reason == ('t67b_frozen_signal_missing' if failure == 'missing'
                               else 't67b_frozen_signal_identity_unit_fee_mismatch')
    persisted = state(bridge)
    assert persisted['selected'] and persisted['selected_at_ms'] == S+124000
    assert persisted.get('signal') == d.get('signal')


def test_t67b_profile_keeps_fixed_units_risk_and_suppresses_sibling_lanes():
    assert PROFILE in PredictionWorker._selectable_strategy_profiles()
    assert PROFILE in dict(selectable_lanes_for_market('BTCUSDT'))
    assert PROFILE not in dict(selectable_lanes_for_market('ETHUSDT'))
    cfg = StrategyConfig.for_profile(PROFILE)
    assert cfg.provenance_payload['regime_policy_fingerprint'] == FINGERPRINT
    assert cfg.entry_start_seconds >= 120 and cfg.entry_end_seconds == 136
    assert cfg.max_initial_attempts == 1
    assert cfg.max_scale_in_attempts == cfg.max_hedge_attempts == 0
    assert not cfg.protective_exit_enabled and not cfg.profit_lock_enabled
    assert RegimeLiveLedger(None, profile=PROFILE).tier == TIER == 'REGIME_T67B'
    worker = object.__new__(PredictionWorker)
    worker._selected_strategy_profile = PROFILE
    worker._fav_p3_arm_override = 'live'
    worker._shadow_lane_strategies = {'legacy': object()}
    assert not worker._fav_p3_live_orders_enabled()
    assert not worker._shadow_lane_experiment_enabled()
    for unit in (D(1), D(2), D(3)):
        sized = PredictionWorker._sized_strategy_config(PROFILE, unit)
        assert sized.max_buy_usdt == sized.max_market_buy_usdt == unit
        assert '本輪MDD' in _regime_risk_text(PROFILE, unit)


@pytest.mark.parametrize('profile,called', [(PROFILE, True), ('regime_target6_7_v1', False)])
def test_original_cutoff_signal_restored_only_for_t67b(tmp_path, profile, called):
    path = tmp_path/'prediction.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE prediction_runtime_config(config_key TEXT PRIMARY KEY,config_value_json TEXT)')
        db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',
                   ('prediction_selected_strategy', '{"profile":"'+profile+'"}'))
    runtime = object.__new__(C180SignalRuntime)
    runtime.prediction_db = path
    runtime.signals = SimpleNamespace(on_frozen=Mock())
    runtime._t67_profile_checked_ms = 0
    runtime._t67_selected = False
    runtime._t67_last_error_ms = 0
    with patch('src.gridbot.prediction.c180_signal_runtime._now_ms', return_value=S+120000):
        assert runtime._t67_active(S+120000) is True  # Both profiles retain the public evidence collector.
        runtime._on_frozen({'frozen': 'cutoff evidence'})
    assert runtime.signals.on_frozen.called is called


def test_unknown_selection_does_not_reuse_cached_t67b_for_paid_original():
    runtime = object.__new__(C180SignalRuntime)
    runtime.prediction_db = 'unused'
    runtime.signals = SimpleNamespace(on_frozen=Mock())
    runtime._t67_profile_checked_ms = 0
    runtime._t67_selected = True
    runtime._t67_selected_profile = PROFILE
    runtime._t67_last_error_ms = 0
    with patch('src.gridbot.prediction.c180_signal_runtime._now_ms', return_value=S+120000), \
            patch('src.gridbot.prediction.regime_t67_evidence.selected_profile', side_effect=OSError):
        runtime._on_frozen({'frozen':'cutoff evidence'})
    runtime.signals.on_frozen.assert_not_called()


@pytest.mark.parametrize('first,last,prior,up,down,branch', [
    (2, -1, 2, '.30', '.71', 'core_first_up'),
    (-2, 1, -2, '.71', '.30', 'core_first_down'),
    (2, '.2', 2, '.61', '.40', 'core_stall_down'),
])
def test_structural_core_retains_old_initial_age_and_ignores_opposite_thin_depth_and_late_original(
        first, last, prior, up, down, branch):
    from src.gridbot.prediction.c180_signal_service import C180Signal
    from src.gridbot.prediction.regime_t67b_bridge import freeze_core
    from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
    initial = book(up, down, 124000)
    initial['book_at_ms'] -= 1500  # Valid original initial-book age; current execution stays <=1s.
    opposite = 'DOWN' if branch == 'core_first_up' else 'UP'
    initial['quote'][opposite]['ask_levels'][0][1] = '.01'
    old_signal = C180Signal(S, 'topic', 'up', S+120000, S+123001,
                            'model_timeout', None, None, None, None, D(200))
    bridge = SimpleNamespace(signal_db='unused', _first_book=Mock(return_value=initial),
                             _book=RegimeWorkerBridge._book)
    market = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up')
    with patch('src.gridbot.prediction.regime_worker_bridge.read_c180_signal', return_value=old_signal):
        guard = freeze_core(bridge, market, feature(first, last, prior), S+124000, D(1))
    assert guard['verified'] and not guard['empty']
    assert [choice['branch'] for choice in guard['candidates']] == [branch]


@pytest.mark.asyncio
@pytest.mark.parametrize('offset,halt,price,allowed', [
    (123999, False, '.40', False),
    (124000, False, '.40', True),
    (135999, False, '.75', True),
    (136000, False, '.40', False),
    (240000, False, '.40', False),
    (124000, True, '.40', False),
    (124000, False, '.7501', False),
])
async def test_t67b_atomic_window_hs_cap_and_duplicate_buy(tmp_path, offset, halt, price, allowed):
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
        await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
        await repo.start_loop('current', 100, mode='LIVE', strategy_profile=PROFILE)
        market = MarketInfo('topic', 'up', 'test', S, S+300000,
                            up_market_id='up', down_market_id='down')
        await repo.save_campaign(Campaign('campaign', market), loop_id='current')
        ledger = RegimeLiveLedger(repo, profile=PROFILE)
        await ledger.seed_schedule(loop_id='current', first_market_start_ms=S)
        await ledger.verify_market(loop_id='current', market_start_ms=S,
                                   market_topic_id='topic', market_id='up', verified_at_ms=S+120000)
        if halt:
            await repo._execute("UPDATE prediction_loops SET hard_stop_latched=1 WHERE loop_id='current'")
        intent = dict(intent_id='intent', campaign_id='campaign', action='BUY_INITIAL', outcome='UP',
                      order_side='BUY', amount='2', limit_price=price, created_at_ms=S+offset,
                      ttl_ms=1000, attempt=1, status='PENDING', tier=TIER, payload={})
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=S+offset):
            claim = await ledger.reserve_c180_intent(loop_id='current', market_start_ms=S,
                campaign_id='campaign', intent=intent, decision_at_ms=S+offset,
                wallet_reconciled_at_ms=S+offset)
            assert claim.claimed is allowed, claim.reason
            if allowed:
                duplicate = await ledger.reserve_c180_intent(loop_id='current', market_start_ms=S,
                    campaign_id='campaign', intent={**intent, 'intent_id':'second'},
                    decision_at_ms=S+offset, wallet_reconciled_at_ms=S+offset)
                assert not duplicate.claimed and duplicate.reason == 'market_buy_already_claimed'
                rows = await repo._fetchall('SELECT unit_usdt FROM prediction_regime_entry_claims')
                assert [D(row['unit_usdt']) for row in rows] == [D(2)]
            else:
                assert not await repo._fetchall('SELECT 1 FROM prediction_regime_entry_claims')
                assert not await repo._fetchall('SELECT 1 FROM prediction_order_intents')
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_t67b_risk_includes_every_old_profile_and_preserves_epoch(tmp_path):
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        assert len(RISK_PROFILES) == len(set(RISK_PROFILES)) == 11
        assert PROFILE in RISK_PROFILES
        await repo.start_loop('oldest', 100, mode='LIVE', strategy_profile=RISK_PROFILES[0])
        oldest = RegimeLiveLedger(repo, profile=RISK_PROFILES[0])
        await oldest.seed_schedule(loop_id='oldest', first_market_start_ms=S)
        assert (await oldest.check_risk('oldest', S, S+124000))[0]
        loops = {'oldest': RISK_PROFILES[0]}
        for index, profile in enumerate(RISK_PROFILES[1:], 1):
            name = 'current' if profile == PROFILE else 'historical-'+str(index)
            await repo.start_loop(name, 100, mode='LIVE', strategy_profile=profile)
            loops[name] = profile
        losses = {}
        for index, profile in enumerate((RISK_PROFILES[0], 'regime_target6_5_v1', 'regime_target6_7_v1')):
            name = next(name for name, value in loops.items() if value == profile)
            start = S+index*20*300000
            losses[name] = (LiveSettlement('loss-'+str(index), start, D('-2'), start+300000, D(1)),)

        async def snapshot(_conn, loop, _now):
            return LoopLedgerSnapshot(loop, True, (), (), losses.get(loop, ()), ())

        ledger = RegimeLiveLedger(repo, profile=PROFILE)
        current = S+60*300000
        await ledger.seed_schedule(loop_id='current', first_market_start_ms=current)
        with patch.object(ledger, '_snapshot_conn', AsyncMock(side_effect=snapshot)) as snapshots:
            assert await ledger.check_risk('current', current, current+124000) == (False, 'cumulative_loss_6')
            assert {call.args[1] for call in snapshots.await_args_list} == set(loops)
        state = await repo.get_runtime_config('regime_target6_risk_v1')
        assert state['first_market_start_ms'] == S and state['fingerprint'] == RISK_FP
        assert D(state['risk_equity_1u']) == -6
    finally:
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('old_profile', ['regime_target6_5_v1', 'regime_target6_7_v1', PROFILE])
async def test_old_unknown_exposure_blocks_t67b_without_reset(tmp_path, old_profile):
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo.start_loop('old', 100, mode='LIVE', strategy_profile=old_profile)
        old = RegimeLiveLedger(repo, profile=old_profile)
        await old.seed_schedule(loop_id='old', first_market_start_ms=S)
        assert (await old.check_risk('old', S, S+124000))[0]
        await repo.save_campaign(Campaign('unknown', MarketInfo('topic', 'up', 'test', S, S+300000)), loop_id='old')
        await repo._execute("UPDATE prediction_campaigns SET pending_unknown=1 WHERE campaign_id='unknown'")
        await repo.start_loop('current', 100, mode='LIVE', strategy_profile=PROFILE)
        ledger = RegimeLiveLedger(repo, profile=PROFILE)
        await ledger.seed_schedule(loop_id='current', first_market_start_ms=S+300000)
        result = await ledger.check_risk('current', S+300000, S+424000)
        assert result == (False, 'unknown_order_reconciliation_required')
        assert (await repo.get_runtime_config('regime_target6_risk_v1'))['first_market_start_ms'] == S
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_t67b_loop_mdd_latch_survives_restart_and_tampered_fingerprint(tmp_path):
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
            assert await ledger.check_risk('current', S+600000, S+724000) == (False, 't67b_loop_mdd_3.5')
        key = 'regime_target6_7b_loop_risk:current'
        guard = await repo.get_runtime_config(key)
        assert guard['fingerprint'] == FINGERPRINT and D(guard['mdd_1u']) == D('3.5')
        restarted = RegimeLiveLedger(repo, profile=PROFILE)
        empty = LoopLedgerSnapshot('current', True, (), (), (), ())
        with patch.object(restarted, '_snapshot_conn', AsyncMock(return_value=empty)):
            assert await restarted.check_risk('current', S+900000, S+1024000) == (False, 't67b_loop_mdd_3.5')
            await repo.set_runtime_config(key, {**guard, 'fingerprint':'wrong'})
            assert await restarted.check_risk('current', S+900000, S+1024000) == (False, 't67b_loop_risk_state_invalid')
    finally:
        await repo.close()
