"""T6.9a = T6.7c seven Live lanes + the T6.8a First UP 5bp floor, no 180s backfill."""
import json
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT, POLICY, PROFILE, LIVE_BRANCHES
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction.c180_favorite import C180EntryDecision
from test_t63 import S, feature, book


def original(p='.2'):
    entry = C180EntryDecision('DOWN', 'original', 'DOWN', D(1), D(5), None)
    return C180Signal(S, 'topic', 'up', S+120000, S+120500, 'entry_positive_cost_after_ev',
                      entry, D(p), None, None, D(200))

T67C = 'regime_target6_7c_v1'


def setup(tmp_path, f, initial, profile=PROFILE, orig=None):
    path = tmp_path/profile/'features'
    path.parent.mkdir(parents=True)
    with closing(connect(path)) as db, db:
        db.execute('INSERT INTO features VALUES(?,?)', (S, json.dumps(f)))
    bridge = b.RegimeWorkerBridge(None, tmp_path/profile/'signals', feature_db=path, profile=profile)
    bridge._registered_loop_id = 'testloop'
    bridge._registered_lane_mask = ()
    market = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up')
    def check(current=initial, at=None, unit=D(1), seen=0):
        with patch.object(bridge, '_first_book', return_value=initial), \
                patch.object(b, 'read_c180_book', return_value=current), \
                patch.object(b, 'read_c180_signal', return_value=orig):
            return bridge.check_signal(market=market, unit_usdt=unit,
                                       at_ms=at or current['captured_at_ms'], last_seen_book_at_ms=seen)
    return bridge, check


def state(bridge, table='t69a_decisions'):
    with closing(connect(bridge.feature_db)) as db:
        return json.loads(db.execute('SELECT payload FROM '+table).fetchone()[0])


def test_policy_is_t67c_lanes_plus_first_up_floor_on_t69_packaging():
    from src.gridbot.prediction.regime_t67c_policy import POLICY as t67c, FINGERPRINT as t67c_fp
    from src.gridbot.prediction.regime_t69_policy import POLICY as t69, FINGERPRINT as t69_fp
    # T6.7c lanes minus continuation Original, which is switched off but still reserves core.
    assert POLICY['disabled'] == ('core_continuation_original',)
    assert LIVE_BRANCHES == tuple(b for b in t67c['live'] if b != 'core_continuation_original')
    assert 'reference_180_mid' not in LIVE_BRANCHES
    assert POLICY['first_up_prior_min_bp'] == '5' == t69['first_up_prior_min_bp']
    assert POLICY['live_base_fingerprint'] == t67c_fp and POLICY['parent_fingerprint'] == t69_fp
    assert not any(k.startswith(('reference_backfill', 'reference_entry', 'reference_last')) for k in POLICY)
    # Everything else that decides an early-window entry matches T6.7c.
    for key in ('routing', 'new_priority', 'decision_ms', 'last_selection_ms', 'entry_ms',
                'quote_ttl_ms', 'core_expiry_ms', 'original_input_ms', 'book_max_age_ms', 'units',
                'risk_state_key', 'loop_mdd_1u'):
        assert POLICY[key] == t67c[key], key
    # Shallow retracement keeps the T6.7c rule plus the counter-trend prior floor.
    shallow = dict(POLICY['shallow_retracement'])
    assert shallow.pop('prior_against_min_bp') == '5'
    assert shallow == t67c['shallow_retracement']
    # C-UP mirror keeps the T6.7c rule but its price cap is .70 instead of .75.
    mirror, base = dict(POLICY['c_mirror_up_prior']), dict(t67c['c_mirror_up_prior'])
    assert mirror.pop('price_band') == ['0.65', '0.70'] and base.pop('price_band') == ['0.65', '0.75']
    assert mirror == base
    # Flat F1-F4 Shadow is retired too.
    assert POLICY['shadow_branches'] == ()
    assert POLICY['shadow_retired'] == ('external_lead_lag', 'reference_value') + t69['shadow_branches'][2:]
    assert POLICY['markets'] == ('BTCUSDT', 'ETHUSDT', 'BNBUSDT')
    assert FINGERPRINT not in (t67c_fp, t69_fp)


SCENARIOS = [
    # first, last, prior, up ask, down ask, initial-book offset
    (2, -1, 2, '.3', '.7'),        # First UP with a weak prior: T6.9a blocks it
    (2, -1, '4.999999', '.3', '.7'),
    (2, -1, 5, '.3', '.7'),        # First UP at the 5bp floor: allowed
    (2, -1, 12, '.3', '.7'),
    (-2, 1, -2, '.7', '.3'),       # First DOWN has no floor
    (2, '.2', 2, '.6', '.4'),      # stall DOWN
    (2, -4, -2, '.3', '.7'),       # C DOWN
    (-1, 3, 2, '.7', '.3'),        # C-UP prior mirror
    (2, -1, -2, '.6', '.4'),       # shallow UP, prior not against by 5bp: T6.9a skips it
    (2, -1, '-4.999999', '.6', '.4'),
    (2, -1, -5, '.6', '.4'),       # shallow UP at the counter-trend floor: allowed
    ('-1.5', '.7', 7, '.4', '.6'),  # shallow DOWN after a rising prior: allowed
    ('-1.5', '.7', 3, '.4', '.6'),  # shallow DOWN, prior rise under 5bp: skipped
    (2, 1, 2, '.8', '.2'),         # continuation Original
    ('.1', '.1', 0, '.5', '.5'),   # flat: no Live lane
]


@pytest.mark.parametrize('first,last,prior,up,down', SCENARIOS)
def test_decisions_match_t67c_except_weak_first_up(tmp_path, first, last, prior, up, down):
    f = feature(first, last, prior)
    _, t67c_check = setup(tmp_path, f, book(up, down, 124000), profile=T67C, orig=original())
    t69a_bridge, t69a_check = setup(tmp_path, f, book(up, down, 124000), orig=original())
    old, new = t67c_check(), t69a_check()
    d = state(t69a_bridge)
    first = (d['core_guard'].get('candidates') or [{}])[0].get('branch')
    weak_first_up = first == 'core_first_up' and D(str(prior)) < 5
    if weak_first_up:
        assert old.allowed and not new.allowed
        assert new.reason == 't69a_first_up_prior_below_5bp'
        assert d['rejected_branches'][0]['branch'] == 'core_first_up'
        return
    if old.allowed and old.reason.endswith(':shallow_retracement'):
        side = old.signal.entry.side
        against = D(str(prior)) <= -5 if side == 'UP' else D(str(prior)) >= 5
        if not against:
            assert not new.allowed and new.reason == 't69a_shallow_prior_not_against_5bp'
            assert d['selected'] is False
            assert d['rejected_branches'] == [dict(branch='shallow_retracement', reason='shallow_prior_not_against_5bp',
                                                   prior_bp=str(d['core_guard']['features']['prior_bp']), side=side)]
            return
    if old.allowed and old.reason.endswith(':core_continuation_original'):
        # Switched off: the market is skipped and no addition takes the core slot.
        assert not new.allowed and new.reason == 't69a_branch_disabled'
        assert d['selected'] is False and d['eligible_branches'] == []
        assert d['rejected_branches'] == [dict(branch='core_continuation_original', reason='branch_disabled')]
        return
    assert new.allowed == old.allowed, (old.reason, new.reason)
    if old.allowed:
        assert new.signal.entry.side == old.signal.entry.side
        assert new.signal.entry == old.signal.entry
        for key in ('expires_at_ms', 'worst_ask_limit', 'expected_cash_usdt', 'expected_shares'):
            if key == 'worst_ask_limit' and new.reason.endswith(':c_mirror_up_prior'):
                assert new.execution.worst_ask_limit == D('0.70')  # T6.9a cap; T6.7c uses .75
                continue
            assert getattr(new.execution, key) == getattr(old.execution, key), key
        assert new.reason.split(':', 1)[1] == old.reason.split(':', 1)[1]  # same branch
        assert new.signal.original_input_sha256 == FINGERPRINT


@pytest.mark.parametrize('up,allowed', [('.65', True), ('.70', True), ('.71', False), ('.75', False)])
@pytest.mark.parametrize('first,last', [(-1, 3), (3, -1)])  # (3, -1) also matches shallow UP
def test_c_mirror_cap_is_070(tmp_path, up, allowed, first, last):
    f = feature(first, last, 2)
    initial = book(up, str(1-D(up)), 124000)
    _, t67c_check = setup(tmp_path, f, initial, profile=T67C, orig=original())
    t69a_bridge, t69a_check = setup(tmp_path, f, initial, orig=original())
    old, new = t67c_check(), t69a_check()
    assert old.allowed and old.reason.endswith(':c_mirror_up_prior')
    assert new.allowed is allowed, new.reason
    d = state(t69a_bridge)
    if allowed:
        assert new.reason == 't69a_ready:c_mirror_up_prior' and d['cap'] == '0.70'
        assert new.execution.worst_ask_limit <= D('.70')
    else:
        # Skipped outright: shallow retracement must not take the same UP entry.
        assert d['selected'] is False and d['eligible_branches'] == []
        assert new.reason == 't69a_no_live_candidate:core_verified_empty'


@pytest.mark.parametrize('offset', [123999, 136000, 180000, 181500, 183000])
def test_no_entry_outside_the_initial_window_including_180s(tmp_path, offset):
    _, check = setup(tmp_path, feature('.1', '.1', 0), book('.5', '.5', 124000), orig=original())
    result = check(book('.5', '.5', offset))
    assert not result.allowed and result.reason == 't69a_unit_or_execution_window'


def test_verified_empty_core_with_no_addition_does_not_wait_for_reference(tmp_path):
    bridge, check = setup(tmp_path, feature('.1', '.1', 0), book('.5', '.5', 124000), orig=original('.5'))
    result = check()
    assert not result.allowed
    assert result.reason.startswith('t69a_no_live_candidate:')
    assert 'reference' not in result.reason


def test_entry_window_and_menu_wiring():
    from src.gridbot.prediction.strategy import StrategyConfig
    from src.gridbot.prediction.telegram import SELECTABLE_LANES, selectable_lanes_for_market
    from src.gridbot.prediction.loop_market import execution_fingerprint, SYMBOLS
    from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
    from src.gridbot.prediction.worker import PredictionWorker
    cfg = StrategyConfig.for_profile(PROFILE)
    assert (cfg.entry_start_seconds, cfg.entry_end_seconds) == (120, 136)
    assert cfg.provenance_payload['t69a_policy']['profile'] == PROFILE
    assert SELECTABLE_LANES[0][0] == PROFILE
    for asset in SYMBOLS:
        assert PROFILE in dict(selectable_lanes_for_market(asset))
        assert execution_fingerprint(asset, PROFILE) not in (
            execution_fingerprint(asset, T67C), execution_fingerprint(asset, 'regime_target6_9_v1'))
    assert PROFILE in PredictionWorker._selectable_strategy_profiles()
    assert RegimeLiveLedger(None, profile=PROFILE).tier == 'REGIME_T69A'


def test_empty_report_lists_live_lanes_and_no_flat_shadow_routes():
    from src.gridbot.prediction.regime_t69a_report import empty_report, LIVE_LABELS, SHADOW_LABELS
    text = empty_report(S)
    assert text.startswith('📊 T6.9b Report')
    assert tuple(b for b in LIVE_LABELS if b != 'core_continuation_original') == LIVE_BRANCHES
    assert 'continuation Original（已停用）' in text
    assert 'F1 Flat' not in text and 'F4 180s' not in text
    assert '外部先行' not in text and 'Reference 校正' not in text
    assert 'Reference 180s' not in text and '檢查點' not in text
    assert sum(label in text for label in LIVE_LABELS.values()) == 7


def test_shadow_observer_follows_its_own_profile_and_asset(tmp_path, monkeypatch):
    import sqlite3
    from src.gridbot.prediction.regime_t69a_shadow import observe, schema
    # The observer is retired; re-enable it here only to keep its gates covered.
    monkeypatch.setitem(POLICY, 'shadow_branches', ('flat_favorite',))
    pred = tmp_path/'prediction.sqlite3'
    with sqlite3.connect(pred) as db:
        db.executescript("""
            CREATE TABLE prediction_loops(loop_id TEXT,strategy_profile TEXT,mode TEXT,state TEXT);
            CREATE TABLE prediction_regime_slots(loop_id TEXT,market_start_ms INTEGER,market_topic_id TEXT,market_id TEXT,verified_at_ms INTEGER);
            CREATE TABLE prediction_loop_market_bindings(loop_id TEXT,symbol TEXT);
            CREATE TABLE prediction_runtime_config(config_key TEXT,config_value_json TEXT);
        """)
        db.execute("INSERT INTO prediction_loops VALUES('loop','regime_target6_9_v1','LIVE','RUNNING')")
        db.execute("INSERT INTO prediction_regime_slots VALUES('loop',?,'t','u',?)", (S, S+1))
    feature_db = connect(tmp_path/'features.sqlite3')
    schema(feature_db)
    # A T6.9 loop is not observed by the T6.9a Shadow.
    assert observe(feature_db, pred, tmp_path/'signals.sqlite3', S+70000, 'BTCUSDT') == 'no_active_verified_loop'
    with sqlite3.connect(pred) as db:
        db.execute("UPDATE prediction_loops SET strategy_profile=?", (PROFILE,))
        db.execute("INSERT INTO prediction_loop_market_bindings VALUES('loop','ETHUSDT')")
    assert observe(feature_db, pred, tmp_path/'signals.sqlite3', S+70000, 'BTCUSDT') == 'other_asset_loop'
    assert observe(feature_db, pred, tmp_path/'signals.sqlite3', S+70000, 'ETHUSDT') == 'shadow_unit_missing'


def test_flat_shadow_is_retired(tmp_path):
    from src.gridbot.prediction import regime_t69a_shadow
    # Retired: returns before reading any database (the paths do not exist).
    assert regime_t69a_shadow.observe(None, tmp_path/'none.sqlite3', tmp_path/'none.sqlite3', S+70000) == 'flat_shadow_retired'


BOOK_FAILURES = ['missing', 'receipt_future', 'receipt_stale', 'source_future', 'freshness']


def broken_book(failure, current, at):
    if failure == 'missing': current = None
    elif failure == 'receipt_future': current['received_at_ms'] += 1
    elif failure == 'receipt_stale': at += 2001
    elif failure == 'source_future': current['book_at_ms'] = current['received_at'] = at+1
    elif failure == 'freshness': at += 1001
    return current, at


@pytest.mark.parametrize('selected', [False, True])
@pytest.mark.parametrize('failure', BOOK_FAILURES)
def test_book_refusals_match_t67c_and_share_its_retry_set(tmp_path, failure, selected):
    from src.gridbot.prediction.regime_t67c_bridge import TRANSIENT_BOOK_REASONS as T67C_TRANSIENT
    from src.gridbot.prediction.regime_t69a_bridge import TRANSIENT_BOOK_REASONS
    assert {r.replace('t67c_', 't69a_') for r in T67C_TRANSIENT} == TRANSIENT_BOOK_REASONS
    reasons = {}
    for profile, prefix in ((T67C, 't67c_'), (PROFILE, 't69a_')):
        bridge, check = setup(tmp_path, feature(2, -1, 5), book('.30', '.70', 124000),
                              profile=profile, orig=original())
        seen = 0
        if selected:
            first = check()
            assert first.allowed, first.reason
            seen = first.book_at_ms
        current, at = broken_book(failure, book('.29', '.71', 125000), S+125000)
        result = check(current, at=at, seen=seen)
        assert not result.allowed
        reasons[prefix] = result.reason
        assert result.reason.startswith(prefix), result.reason
        assert (result.reason in (T67C_TRANSIENT | TRANSIENT_BOOK_REASONS)) == (failure != 'receipt_future' and failure != 'source_future')
    assert reasons['t69a_'] == reasons['t67c_'].replace('t67c_', 't69a_')


def worker_harness(clock):
    from src.gridbot.prediction.worker import PredictionWorker
    worker = PredictionWorker.__new__(PredictionWorker)
    worker.clock = clock
    worker._now_ms = lambda: worker.clock
    worker._selected_strategy_profile = PROFILE
    worker._selected_order_unit_usdt = 1
    campaign = SimpleNamespace(market=SimpleNamespace(start_time_ms=S,
        market_topic_id='topic', up_market_id='up'))
    return worker, campaign


@pytest.mark.asyncio
async def test_worker_retries_a_missing_initial_book_inside_the_window(tmp_path):
    worker, campaign = worker_harness(S+124998)
    initial = book('.30', '.70', 125183)
    bridge, _ = setup(tmp_path, feature(2, -1, 5), initial, orig=original())
    times = []
    def read(*args):
        times.append(worker.clock)
        return initial if worker.clock >= S+125183 else None
    async def sleep(seconds): worker.clock += round(seconds*1000)
    with patch.object(b, 'read_c180_book', side_effect=read), patch.object(
            bridge, '_first_book', return_value=initial), patch.object(
            b, 'read_c180_signal', return_value=original()), patch(
            'src.gridbot.prediction.worker.asyncio.sleep', side_effect=sleep):
        ready = await worker._entry_signal_within_window(bridge, campaign, S)
    assert ready.allowed and ready.signal.entry.side == 'UP'
    assert times == [S+124998, S+125098, S+125198]


@pytest.mark.asyncio
async def test_worker_refreshes_a_stale_book_after_selection(tmp_path):
    worker, campaign = worker_harness(S+125100)
    bridge, check = setup(tmp_path, feature(2, -1, 5), book('.30', '.70', 124000), orig=original())
    ready = check()
    assert ready.allowed
    current = book('.29', '.71', 125183)
    def read(*args): return current if worker.clock >= S+125183 else book('.30', '.70', 124000)
    async def sleep(seconds): worker.clock += round(seconds*1000)
    with patch.object(b, 'read_c180_book', side_effect=read), patch(
            'src.gridbot.prediction.worker.asyncio.sleep', side_effect=sleep):
        fresh = await worker._entry_refresh_within_deadline(bridge, campaign, ready)
    assert fresh.allowed and fresh.signal == ready.signal
    assert fresh.execution.expires_at_ms == ready.execution.expires_at_ms
