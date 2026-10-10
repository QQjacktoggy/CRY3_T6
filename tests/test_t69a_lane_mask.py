"""T6.9a per-loop lane mask: chosen before a loop, bound immutably, applied like DISABLED_BRANCHES."""
import asyncio
import hashlib
import json
import sqlite3
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from src.gridbot.prediction import regime_t69a_lane_mask as lm
from src.gridbot.prediction.loop_market import (T69A_PROFILE, PROFILE as T67C_PROFILE, T69_PROFILE,
    SYMBOLS, execution_fingerprint, binding_fingerprint, report_lane_mask)
from src.gridbot.prediction.loop_market_worker import LoopMarketWorker
from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT, LIVE_BRANCHES, POLICY
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.settings import PredictionSettings, RuntimeMode
from test_t63 import S, feature, book
from test_t69a import SCENARIOS, setup, state, original

MASKS = ['ALL', 'C_DOWN_OFF', 'DOWN_OFF', 'UP_OFF', 'shallow_retracement:DOWN',
         'core_first_down:DOWN,shallow_retracement:UP']


# ---------------------------------------------------------------- mask values

def test_tokens_cover_exactly_the_live_lanes_and_sides():
    branches = {t.split(':')[0] for t in lm.LANE_TOKENS}
    assert branches == set(LIVE_BRANCHES)
    assert 'core_continuation_original' not in branches  # permanently off, not maskable
    assert lm.DOWN_TOKENS + lm.UP_TOKENS == tuple(t for t in lm.LANE_TOKENS if t.endswith(':DOWN')) + \
        tuple(t for t in lm.LANE_TOKENS if t.endswith(':UP'))
    assert set(lm.PRESETS['DOWN_OFF']) | set(lm.PRESETS['UP_OFF']) == set(lm.LANE_TOKENS)


def test_mask_stays_out_of_policy_so_fingerprint_is_unchanged():
    assert 'lane_mask' not in json.dumps(POLICY)
    assert FINGERPRINT == hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()


@pytest.mark.parametrize('value,expected', [
    (None, ()), ('', ()), ('ALL', ()), ('all', ()), ([], ()),
    ('C_DOWN_OFF', ('core_c_down:DOWN',)),
    ('DOWN_OFF', tuple(sorted(lm.DOWN_TOKENS))),
    ('UP_OFF', tuple(sorted(lm.UP_TOKENS))),
    ('shallow_retracement:DOWN, core_c_down:DOWN', ('core_c_down:DOWN', 'shallow_retracement:DOWN')),
    ('["core_c_down:DOWN","core_c_down:DOWN"]', ('core_c_down:DOWN',)),
    (('core_first_up:UP',), ('core_first_up:UP',)),
])
def test_normalize_is_canonical(value, expected):
    assert lm.normalize(value) == expected
    assert lm.normalize(lm.to_text(value)) == expected
    assert lm.to_text(value) == (json.dumps(list(expected), separators=(',', ':')) if expected else '')


@pytest.mark.parametrize('value', [
    'core_continuation_original:DOWN',  # permanently disabled branch
    'core_c_down:UP',                    # wrong side
    'reference_180_mid:UP', 'nonsense', 'core_c_down', '[1]', '{"a":1}', '[', 5,
    ','.join(lm.LANE_TOKENS),             # every lane off
])
def test_normalize_rejects_unknown_or_everything_off(value):
    with pytest.raises(ValueError):
        lm.normalize(value)


def test_labels():
    assert lm.describe(()) == '全開'
    assert lm.describe('C_DOWN_OFF') == '只關原 C DOWN'
    assert lm.preset_of(lm.UP_TOKENS) == 'UP_OFF'
    assert lm.describe('shallow_retracement:DOWN,core_first_up:UP') == '自訂：關 first UP、淺回撤 DOWN'
    assert lm.side_closed('DOWN_OFF', 'DOWN') and not lm.side_closed('DOWN_OFF', 'UP')
    assert not lm.side_closed('C_DOWN_OFF', 'DOWN')


# ---------------------------------------------------------------- loop identity

def _old_execution_fingerprint(asset, profile):
    # The formula before masks existed; every stored binding was made with it.
    from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT as T69A
    from src.gridbot.prediction.regime_t69_policy import FINGERPRINT as T69
    from src.gridbot.prediction.regime_t67c_policy import FINGERPRINT as T67C
    parent, routing = {T69A_PROFILE: (T69A, 't69a_seven_branches'), T69_PROFILE: (T69, 't69_nine_branches'),
                       T67C_PROFILE: (T67C, 't67c_seven_branches')}[profile]
    return hashlib.sha256(json.dumps(dict(version=1, symbol=asset, parent=parent, routing=routing,
        isolation='asset_database_v1'), sort_keys=True).encode()).hexdigest()


@pytest.mark.parametrize('asset', SYMBOLS)
@pytest.mark.parametrize('profile', [T69A_PROFILE, T69_PROFILE, T67C_PROFILE])
def test_empty_mask_fingerprint_is_byte_identical(asset, profile):
    assert execution_fingerprint(asset, profile) == _old_execution_fingerprint(asset, profile)
    assert execution_fingerprint(asset, profile, '') == _old_execution_fingerprint(asset, profile)


def test_mask_changes_only_that_loop_fingerprint():
    base = execution_fingerprint('BTCUSDT', T69A_PROFILE)
    down = execution_fingerprint('BTCUSDT', T69A_PROFILE, 'DOWN_OFF')
    assert down != base and down != execution_fingerprint('BTCUSDT', T69A_PROFILE, 'UP_OFF')
    assert down == execution_fingerprint('BTCUSDT', T69A_PROFILE, lm.to_text('DOWN_OFF'))
    for profile in (T69_PROFILE, T67C_PROFILE):
        with pytest.raises(ValueError, match='only supported'):
            execution_fingerprint('BTCUSDT', profile, 'DOWN_OFF')


@pytest_asyncio.fixture
async def repo(tmp_path):
    r = PredictionRepository(tmp_path/'prediction/data/prediction.sqlite3')
    await r.initialize()
    try:
        yield r
    finally:
        await r.close()


async def start(repo, name='loop', asset='BTCUSDT', count=100, mask='', profile=T69A_PROFILE):
    return await repo.start_loop(name, count, mode='LIVE', strategy_profile=profile,
                                 market_symbol=asset, market_unit='1', lane_mask=mask)


@pytest.mark.asyncio
async def test_mask_is_bound_immutably_with_the_loop(repo):
    await start(repo, mask='DOWN_OFF')
    binding = await repo.get_loop_market_binding('loop')
    assert binding['lane_mask'] == lm.to_text('DOWN_OFF')
    assert binding['execution_fingerprint'] == execution_fingerprint('BTCUSDT', T69A_PROFILE, 'DOWN_OFF')
    assert binding_fingerprint(binding) == binding['execution_fingerprint']
    # Resume with the same mask is fine; a different or missing mask is not.
    await start(repo, mask='DOWN_OFF')
    for other in ('', 'UP_OFF', 'C_DOWN_OFF'):
        with pytest.raises(ValueError, match='immutable'):
            await start(repo, mask=other)
    # The legacy start path resumes with the bound mask.
    await repo.start_loop('loop', 100, mode='LIVE', strategy_profile=T69A_PROFILE)
    for sql in ("UPDATE prediction_loop_lane_masks SET lane_mask='[]'", 'DELETE FROM prediction_loop_lane_masks'):
        with pytest.raises(sqlite3.IntegrityError, match='immutable'):
            await repo._execute(sql)
    assert await repo.get_loop_market_binding('loop') == binding


@pytest.mark.asyncio
async def test_unmasked_binding_matches_pre_mask_rows(repo):
    await start(repo)
    binding = await repo.get_loop_market_binding('loop')
    assert binding['lane_mask'] == ''
    assert binding['execution_fingerprint'] == _old_execution_fingerprint('BTCUSDT', T69A_PROFILE)
    assert await repo._fetchall('SELECT * FROM prediction_loop_lane_masks') == []


@pytest.mark.asyncio
async def test_previous_release_can_still_bind_loops_after_migration(repo):
    # Rollback safety: the pre-mask release writes bindings with a positional
    # 7-value INSERT. Migration 029 adds a side table, not a binding column.
    await repo._execute("INSERT INTO prediction_loops(loop_id,target,completed,state,mode,strategy_profile,created_at_ms,updated_at_ms)"
                        " VALUES('old',20,0,'DONE','LIVE',?,1,1)", (T69A_PROFILE,))
    await repo._execute("INSERT INTO prediction_loop_market_bindings VALUES(?,?,?,?,?,?,?)",
                        ('old', 'BTCUSDT', T69A_PROFILE, _old_execution_fingerprint('BTCUSDT', T69A_PROFILE), '1', 20, 1))
    assert (await repo.get_loop_market_binding('old'))['lane_mask'] == ''


@pytest.mark.asyncio
@pytest.mark.parametrize('profile,mask', [(T67C_PROFILE, 'DOWN_OFF'), (T69_PROFILE, 'UP_OFF'),
                                          (T69A_PROFILE, 'core_continuation_original:DOWN'),
                                          (T69A_PROFILE, ','.join(lm.LANE_TOKENS))])
async def test_invalid_mask_creates_no_loop(repo, profile, mask):
    with pytest.raises(ValueError):
        await start(repo, mask=mask, profile=profile)
    assert await repo.get_active_loop() is None
    assert await repo._fetchall('SELECT * FROM prediction_loop_market_bindings') == []


@pytest.mark.asyncio
async def test_mask_requires_bound_loop(repo):
    with pytest.raises(ValueError, match='bound'):
        await repo.start_loop('x', 20, mode='LIVE', strategy_profile=T69A_PROFILE, lane_mask='UP_OFF')


@pytest.mark.asyncio
async def test_migration_keeps_existing_rows_unmasked(tmp_path):
    # A database made before 029: the column is added with '' for every row.
    path = tmp_path/'prediction/data/prediction.sqlite3'
    r = PredictionRepository(path)
    await r.initialize()
    try:
        await start(r)
        assert (await r._fetchall("SELECT filename FROM prediction_migrations WHERE filename='029_loop_lane_mask.sql'"))
        assert report_lane_mask(tmp_path, 'loop') == ()
    finally:
        await r.close()


# ---------------------------------------------------------------- the decision gate

def _token(d):
    return f"{d['branch']}:{d['side']}"


@pytest.mark.parametrize('mask', MASKS)
@pytest.mark.parametrize('first,last,prior,up,down', SCENARIOS)
def test_mask_only_removes_masked_lanes(tmp_path, mask, first, last, prior, up, down):
    f = feature(first, last, prior)
    plain_bridge, plain_check = setup(tmp_path/'plain', f, book(up, down, 124000), orig=original())
    masked_bridge, masked_check = setup(tmp_path/'masked', f, book(up, down, 124000), orig=original())
    masked_bridge._registered_lane_mask = lm.normalize(mask)
    old, new = plain_check(), masked_check()
    d0, d = state(plain_bridge), state(masked_bridge)
    tokens = lm.normalize(mask)
    assert ('lane_mask' in d) == bool(tokens) and 'lane_mask' not in d0
    if tokens:
        assert d['lane_mask'] == list(tokens)
    if not old.allowed:
        # Masking never creates an entry.
        assert not new.allowed
        return
    if _token(d0) not in tokens:
        assert new.allowed and new.reason == old.reason
        assert new.signal == old.signal and new.execution == old.execution
        return
    # The lane the plain loop would have entered is masked.
    record = next(r for r in d['rejected_branches'] if r['reason'] == 'loop_lane_masked')
    assert (record['branch'], record['side']) == (d0['branch'], d0['side'])
    assert record.get('would_cash') == str(old.execution.expected_cash_usdt)
    if not d0['core_guard']['empty']:
        # Masked core keeps the slot: no addition takes the market.
        assert not new.allowed and new.reason == 't69a_loop_lane_masked'
        assert d['selected'] is False and d['eligible_branches'] == []
    elif new.allowed:
        assert d['selected'] is True and _token(d) not in tokens
    else:
        assert d['selected'] is False


def test_masked_market_is_frozen_and_not_reselected_later(tmp_path):
    # C DOWN scenario with DOWN_OFF: later checks inside the window still refuse.
    f = feature(2, -4, -2)
    bridge, check = setup(tmp_path, f, book('.3', '.7', 124000), orig=original())
    bridge._registered_lane_mask = lm.normalize('DOWN_OFF')
    first = check()
    assert not first.allowed and first.reason == 't69a_loop_lane_masked'
    later = book('.3', '.7', 130000)
    assert not check(later, at=S+130000).allowed
    assert state(bridge)['selected'] is False


def test_changed_mask_after_freeze_fails_closed(tmp_path):
    f = feature(2, -1, 12)  # First UP
    bridge, check = setup(tmp_path, f, book('.3', '.7', 124000), orig=original())
    bridge._registered_lane_mask = lm.normalize('C_DOWN_OFF')
    assert check().allowed
    bridge._registered_lane_mask = ()
    refused = check(book('.3', '.7', 125000), at=S+125000)
    assert not refused.allowed and 'frozen_identity' in refused.reason


def test_unloaded_mask_fails_closed(tmp_path):
    bridge, check = setup(tmp_path, feature(2, -1, 12), book('.3', '.7', 124000), orig=original())
    del bridge._registered_lane_mask
    refused = check()
    assert not refused.allowed and refused.reason.endswith('loop_lane_mask_unavailable')


# ---------------------------------------------------------------- registration and claim

def _btc_market():
    from src.gridbot.prediction.models import MarketInfo
    raw = {'symbol': 'BTCUSDT', 'variantData': {'priceFeedSymbol': 'BTCUSDT'}}
    return MarketInfo('topic', 'up', 'slug', S, S+300000, up_market_id='up', down_market_id='down', raw=raw)


@pytest.mark.asyncio
@pytest.mark.parametrize('registered', [True, False])
async def test_registration_loads_mask_from_binding(repo, registered):
    from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
    await start(repo, mask='UP_OFF')
    bridge = RegimeWorkerBridge(repo, 'unused.db', profile=T69A_PROFILE, symbol='BTCUSDT')
    bridge._registered_lane_mask = ('stale',)
    with patch.object(bridge.ledger, 'market_is_registered', AsyncMock(return_value=registered)), \
            patch.object(bridge.ledger, 'seed_schedule', AsyncMock()), \
            patch.object(bridge.ledger, 'verify_market', AsyncMock()), \
            patch.object(bridge.ledger, 'check_risk', AsyncMock()):
        result = await bridge.register_market(loop_id='loop', market=_btc_market(), now_ms=S, unit_usdt=D(1))
    assert result.allowed, result.reason
    assert bridge._registered_lane_mask == lm.normalize('UP_OFF')


@pytest.mark.asyncio
async def test_registration_refuses_tampered_binding(repo):
    from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
    await start(repo, mask='UP_OFF')
    await repo._execute('DROP TRIGGER loop_lane_mask_no_delete')
    await repo._execute('DELETE FROM prediction_loop_lane_masks')
    bridge = RegimeWorkerBridge(repo, 'unused.db', profile=T69A_PROFILE, symbol='BTCUSDT')
    bridge._registered_lane_mask = ()
    with patch.object(bridge.ledger, 'market_is_registered', AsyncMock(return_value=True)):
        result = await bridge.register_market(loop_id='loop', market=_btc_market(), now_ms=S, unit_usdt=D(1))
    assert not result.allowed and result.reason == 'loop_market_binding_mismatch'
    assert bridge._registered_lane_mask is None


@pytest.mark.asyncio
@pytest.mark.parametrize('mask,side,allowed', [
    ('', 'DOWN', True), ('DOWN_OFF', 'DOWN', False), ('DOWN_OFF', 'UP', True),
    ('UP_OFF', 'UP', False), ('C_DOWN_OFF', 'DOWN', True),
])
async def test_claim_refuses_a_side_whose_every_lane_is_masked(repo, mask, side, allowed):
    from src.gridbot.prediction.models import Campaign
    from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
    from src.gridbot.prediction.regime_t69a_policy import TIER
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
    await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
    await start(repo, mask=mask)
    await repo.save_campaign(Campaign('campaign', _btc_market()), loop_id='loop')
    ledger = RegimeLiveLedger(repo, profile=T69A_PROFILE)
    await ledger.seed_schedule(loop_id='loop', first_market_start_ms=S)
    await ledger.verify_market(loop_id='loop', market_start_ms=S, market_topic_id='topic', market_id='up',
                               verified_at_ms=S+120000)
    intent = dict(intent_id='intent', campaign_id='campaign', action='BUY_INITIAL', outcome=side, order_side='BUY',
                  amount='1', limit_price='.4', created_at_ms=S+124000, ttl_ms=1000, attempt=1, status='PENDING',
                  tier=TIER, payload={})
    with patch('src.gridbot.prediction.regime_live_ledger._now_ms', return_value=S+124000):
        result = await ledger.reserve_c180_intent(loop_id='loop', market_start_ms=S, campaign_id='campaign',
            intent=intent, decision_at_ms=S+124000, wallet_reconciled_at_ms=S+124000)
    assert result.claimed is allowed, result.reason
    if not allowed:
        assert result.reason == 'loop_lane_masked'
        assert await repo._fetchall('SELECT 1 FROM prediction_order_intents') == []


# ---------------------------------------------------------------- operator flow

class Harness(LoopMarketWorker):
    def __init__(self, repo, profile=T69A_PROFILE, pending=None):
        self.repository = repo
        self.settings = PredictionSettings(market_symbol='BTCUSDT')
        self._selected_strategy_profile = profile
        self._selected_order_unit_usdt = D(1)
        self._effective_mode = RuntimeMode.LIVE
        self._lock = asyncio.Lock()
        self._task = None
        self._c180_recovery_exposure_clear = AsyncMock(return_value=True)
        self.restore_order_unit = AsyncMock()
        self.restore_selected_strategy = AsyncMock()
        self._load_pending_strategy = AsyncMock(return_value=pending)

    def _status(self):
        return {'market_symbol': self.settings.market_symbol}


@pytest.mark.asyncio
async def test_selection_queues_for_next_loop_and_never_touches_running_one(repo):
    await start(repo, mask='C_DOWN_OFF')
    w = Harness(repo)
    await w.restore_loop_market()  # bound masked loop restores
    result = await w.select_lane_mask('UP_OFF')
    assert result['lane_mask_queued'] and result['next_lane_mask'] == list(lm.normalize('UP_OFF'))
    assert result['lane_mask'] == ['core_c_down:DOWN'] and result['lane_mask_label'] == '只關原 C DOWN'
    assert (await repo.get_loop_market_binding('loop'))['lane_mask'] == lm.to_text('C_DOWN_OFF')
    # The running loop is checked against its own mask, not the queued one.
    assert await w._loop_market_start_guard(100) is None
    assert await w._active_lane_mask() == lm.normalize('C_DOWN_OFF')
    assert await w._pending_lane_mask() == lm.normalize('UP_OFF')


@pytest.mark.asyncio
async def test_new_loop_binds_and_clears_the_queued_mask_atomically(repo):
    w = Harness(repo)
    await w.select_lane_mask('DOWN_OFF')
    mask = lm.to_text(await w._pending_lane_mask())
    assert w._loop_market_start_kwargs(mask) == dict(market_symbol='BTCUSDT', market_unit='1', lane_mask=mask)
    assert w._loop_market_start_kwargs('') == dict(market_symbol='BTCUSDT', market_unit='1')
    # Compare-and-clear: a queue that no longer matches what is being bound aborts the start.
    with pytest.raises(ValueError, match='queued lane mask changed'):
        await repo.start_loop('x', 100, mode='LIVE', strategy_profile=T69A_PROFILE,
                              consume_pending_lane_mask=True, **w._loop_market_start_kwargs(lm.to_text('UP_OFF')))
    assert await repo.get_active_loop() is None and await w._pending_lane_mask() == lm.normalize('DOWN_OFF')
    await repo.start_loop('loop', 100, mode='LIVE', strategy_profile=T69A_PROFILE,
                          consume_pending_lane_mask=True, **w._loop_market_start_kwargs(mask))
    assert await w._pending_lane_mask() == ()
    assert (await repo.get_loop_market_binding('loop'))['lane_mask'] == mask
    # A resume never consumes the queue.
    await w.select_lane_mask('UP_OFF')
    await repo.start_loop('loop', 100, mode='LIVE', strategy_profile=T69A_PROFILE, **w._loop_market_start_kwargs(mask))
    assert await w._pending_lane_mask() == lm.normalize('UP_OFF')


@pytest.mark.asyncio
async def test_mask_is_only_for_t69a(repo):
    w = Harness(repo, profile=T67C_PROFILE)
    denied = await w.select_lane_mask('DOWN_OFF')
    assert denied['action_denied'] and 'T6.9b' in denied['reason']
    assert (await w.select_lane_mask('ALL')).get('action_denied') is None
    # Picked for T6.9b, then the next profile became T6.7c: the start is refused.
    await repo.set_runtime_config('prediction_pending_lane_mask', {'mask': ['core_c_down:DOWN']})
    assert 'only for T6.9b' in await w._loop_market_start_guard(20)
    # A queued T6.9b switch makes the choice acceptable before the switch applies.
    queued = Harness(repo, profile=T67C_PROFILE, pending=T69A_PROFILE)
    assert not (await queued.select_lane_mask('DOWN_OFF')).get('action_denied')


@pytest.mark.asyncio
async def test_invalid_selection_and_corrupt_queue_are_refused(repo):
    w = Harness(repo)
    denied = await w.select_lane_mask('core_continuation_original:DOWN')
    assert denied['action_denied']
    assert await w._pending_lane_mask() == ()
    await repo.set_runtime_config('prediction_pending_lane_mask', {'mask': ['bogus']})
    assert 'invalid' in await w._loop_market_start_guard(100)


# ---------------------------------------------------------------- Telegram and report

@pytest.mark.asyncio
async def test_telegram_routes_presets_and_custom_tokens():
    import re
    from src.gridbot.prediction.telegram import (PredictionTelegramService, build_prediction_handlers,
                                                 LANE_MASK_CALLBACK_PREFIX, format_runtime_result)
    svc = PredictionTelegramService(object(), 1)
    svc._deny_if_unauthorized = AsyncMock(return_value=False)
    svc._call_and_reply = AsyncMock()
    await svc.cmd_predict_lanemask(None, SimpleNamespace(args=['core_c_down:DOWN,', 'shallow_retracement:DOWN']))
    assert svc._call_and_reply.await_args.args[2] == ('select_lane_mask',)
    assert lm.normalize(svc._call_and_reply.await_args.args[3]) == ('core_c_down:DOWN', 'shallow_retracement:DOWN')
    query = SimpleNamespace(data=LANE_MASK_CALLBACK_PREFIX+'DOWN_OFF', answer=AsyncMock())
    await svc.handle_callback(SimpleNamespace(callback_query=query), None)
    assert svc._call_and_reply.await_args.args[2:] == (('select_lane_mask',), 'DOWN_OFF')
    callback = [h for h in build_prediction_handlers(svc) if type(h).__name__ == 'CallbackQueryHandler'][0]
    assert callback.pattern.match(LANE_MASK_CALLBACK_PREFIX+'UP_OFF')
    assert any(getattr(h, 'commands', None) == frozenset({'predict_lanemask'}) for h in build_prediction_handlers(svc))
    text = format_runtime_result('T6.9b Lane 遮罩', {'lane_mask_label': '全開', 'next_lane_mask_label': '關全部 DOWN',
                                                    'lane_mask_queued': True})
    assert '下一輪：關全部 DOWN' in text and '執行中的 Loop 不變' in text


@pytest.mark.asyncio
async def test_report_reads_the_loop_binding_mask(repo, tmp_path):
    await start(repo, mask='shallow_retracement:DOWN')
    assert report_lane_mask(tmp_path, 'loop') == ('shallow_retracement:DOWN',)
    assert report_lane_mask(tmp_path, 'missing') == ()


def test_masked_addition_lets_an_unmasked_addition_take_the_market(tmp_path):
    # Same semantics as DISABLED_BRANCHES for additions. In real features C-UP
    # mirror (reversal) and shallow (|first| >= 2|last|) never both match.
    from src.gridbot.prediction import regime_t69a_bridge as t69a
    mirror = dict(branch='c_mirror_up_prior', side='UP', action='c_mirror_up_prior', probability=None,
                  lower='0.65', cap='0.70', upper='0.70')
    shallow = dict(branch='shallow_retracement', side='UP', action='shallow_retracement', probability=None,
                   lower='.10', cap='.75', upper='.75')
    f = feature('.1', '.1', -6)
    for mask, branch in (((), 'c_mirror_up_prior'), (('c_mirror_up_prior:UP',), 'shallow_retracement'),
                         (('c_mirror_up_prior:UP', 'shallow_retracement:UP'), None)):
        bridge, check = setup(tmp_path/str(len(mask)), f, book('.68', '.32', 124000), orig=original())
        bridge._registered_lane_mask = mask
        with patch.object(t69a, '_additions', return_value=[dict(mirror), dict(shallow)]):
            result = check()
        d = state(bridge)
        assert d['core_guard']['empty'] is True
        if branch is None:
            assert not result.allowed and result.reason == 't69a_loop_lane_masked'
        else:
            assert result.allowed and d['branch'] == branch
        assert [r['branch'] for r in d['rejected_branches'] if r['reason'] == 'loop_lane_masked'] == \
            [t.split(':')[0] for t in mask]


# ---------------------------------------------------------------- stop, choose a mask, start

def _worker(repo, reconcile=None):
    from unittest.mock import Mock
    from src.gridbot.prediction.worker import PredictionWorker, WorkerHeartbeat
    w = object.__new__(PredictionWorker)
    w.repository = repo
    w.settings = PredictionSettings(market_symbol='BTCUSDT')
    w._selected_strategy_profile = T69A_PROFILE
    w._selected_order_unit_usdt = D(1)
    w._effective_mode = RuntimeMode.LIVE
    w._lock = asyncio.Lock()
    w._task = None
    w._hard_stop_latched = False
    w._accept_new_markets = w._allow_new_orders = w._allow_new_buys = w._allow_reductions = True
    w._active_campaigns = {}
    w.heartbeat = WorkerHeartbeat(0)
    w._risk_snapshot = AsyncMock(return_value=SimpleNamespace(hard_stop_latched=False))
    w.restore_order_unit = AsyncMock()
    w.restore_selected_strategy = AsyncMock()
    w._activate_pending_strategy_if_idle = AsyncMock()
    w._activate_pending_order_unit_if_idle = AsyncMock()
    w._refresh_live_prerequisites = AsyncMock(return_value=[])
    w._c180_recovery_exposure_clear = AsyncMock(return_value=True)
    w._load_pending_strategy = AsyncMock(return_value=None)
    w._shadow_lane_experiment_enabled = Mock(return_value=False)
    w.reconcile = AsyncMock(return_value=reconcile or {'known': True, 'orders': 0})
    w._status = Mock(return_value={})
    w.status = AsyncMock(return_value={})
    w._run_loop = AsyncMock()
    w._now_ms = lambda: int(__import__('time').time() * 1000)
    return w


@pytest.mark.asyncio
async def test_new_mask_after_drained_stop_ends_the_loop_and_starts_a_masked_one(repo):
    await start(repo, 'A')
    w = _worker(repo)
    await w.stop_loop()
    chosen = await w.select_lane_mask('DOWN_OFF')
    assert chosen['previous_loop_closed'] and chosen['previous_loop_id'] == 'A'
    assert not chosen['lane_mask_queued']
    assert (await repo._fetchall("SELECT state FROM prediction_loops WHERE loop_id='A'"))[0]['state'] == 'STOPPED'
    started = await w.start_loop(100)
    assert not started.get('action_denied'), started
    loop = await repo.get_active_loop()
    assert loop['loop_id'] != 'A'
    assert (await repo.get_loop_market_binding(loop['loop_id']))['lane_mask'] == lm.to_text('DOWN_OFF')
    assert await w._pending_lane_mask() == ()


@pytest.mark.asyncio
async def test_stopped_loop_never_resumes_under_a_different_queued_mask(repo):
    await start(repo, 'A')
    w = _worker(repo, reconcile={'known': True, 'orders': 1})  # not drained yet
    await w.stop_loop()
    queued = await w.select_lane_mask('DOWN_OFF')
    assert queued['lane_mask_queued'] and not queued.get('previous_loop_closed')
    w.reconcile.return_value = {'known': True, 'orders': 0}  # drained after the choice
    for attempt in (w.start_loop(100), w.resume()):
        denied = await attempt
        assert denied['action_denied'] and '新 Loop' in denied['reason']
    loop = await repo.get_active_loop()
    assert loop['loop_id'] == 'A' and loop['new_entries_stopped'] == 1
    # Choosing the loop's own lanes again lets it resume unchanged.
    await w.select_lane_mask('ALL')
    resumed = await w.start_loop(100)
    assert not resumed.get('action_denied'), resumed
    assert (await repo.get_active_loop())['new_entries_stopped'] == 0
    assert (await repo.get_loop_market_binding('A'))['lane_mask'] == ''


@pytest.mark.asyncio
async def test_denied_selection_reports_real_labels(repo):
    await start(repo, mask='C_DOWN_OFF')
    w = Harness(repo)
    await w.select_lane_mask('UP_OFF')
    denied = await w.select_lane_mask('bogus:UP')
    assert denied['action_denied']
    assert denied['lane_mask_label'] == '只關原 C DOWN' and denied['next_lane_mask_label'] == '關全部 UP'


def test_menu_lists_the_lane_mask_command():
    from predict_main import prediction_bot_commands
    assert any(c.command == 'predict_lanemask' for c in prediction_bot_commands())


@pytest.mark.asyncio
async def test_explicit_all_is_a_choice_not_an_empty_queue(repo):
    await start(repo, 'A', mask='DOWN_OFF')
    w = _worker(repo, reconcile={'known': True, 'orders': 1})  # not drained yet
    await w.stop_loop()
    queued = await w.select_lane_mask('ALL')
    assert queued['lane_mask_queued'] and not queued.get('previous_loop_closed')
    w.reconcile.return_value = {'known': True, 'orders': 0}
    for attempt in (w.start_loop(100), w.resume()):
        denied = await attempt
        assert denied['action_denied'] and '新 Loop' in denied['reason']
    # Choosing 全開 again after the drain ends A; the next start is a new, unmasked loop.
    closed = await w.select_lane_mask('ALL')
    assert closed['previous_loop_closed']
    assert not (await w.start_loop(100)).get('action_denied')
    loop = await repo.get_active_loop()
    assert loop['loop_id'] != 'A' and (await repo.get_loop_market_binding(loop['loop_id']))['lane_mask'] == ''


@pytest.mark.asyncio
async def test_paused_running_loop_resumes_with_a_mask_queued_for_the_next_loop(repo):
    await start(repo, 'A')
    w = _worker(repo)
    queued = await w.select_lane_mask('DOWN_OFF')
    assert queued['lane_mask_queued']
    await w.pause()
    resumed = await w.resume()
    assert '新 Loop' not in str(resumed.get('reason', ''))
    assert (await repo.get_active_loop())['loop_id'] == 'A'
    assert await w._pending_lane_mask() == lm.normalize('DOWN_OFF')


@pytest.mark.parametrize('first_down', ['.7', '.8'])
def test_masked_quote_keeps_the_first_executable_tick(tmp_path, first_down):
    # C DOWN masked: the paper quote is the first in-band one, as an unmasked loop would buy.
    bridge, check = setup(tmp_path, feature(2, -4, -2), book('.3', first_down, 124000), orig=original())
    bridge._registered_lane_mask = lm.normalize('C_DOWN_OFF')
    assert not check().allowed

    def masked():
        return next(r for r in state(bridge)['rejected_branches'] if r['reason'] == 'loop_lane_masked')

    if first_down == '.8':  # out of the .65-.75 band: nothing executable yet
        assert masked() == dict(branch='core_c_down', reason='loop_lane_masked', side='DOWN',
                                would_execution='unavailable')
        assert not check(book('.3', '.7', 126000), at=S+126000).allowed
        first, at = masked(), S+126000
    else:
        first, at = masked(), S+124000
    assert first['quoted_at_ms'] == at and D(first['would_cash']) == D('.7')*D('1.42')
    # Later ticks at another price, or out of band again, keep that first quote.
    for later_down, t in (('.72', 128000), ('.8', 130000)):
        assert not check(book('.3', later_down, t), at=S+t).allowed
        assert masked() == first
