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
    assert LIVE_BRANCHES == tuple(t67c['live']) and 'reference_180_mid' not in LIVE_BRANCHES
    assert POLICY['first_up_prior_min_bp'] == '5' == t69['first_up_prior_min_bp']
    assert POLICY['live_base_fingerprint'] == t67c_fp and POLICY['parent_fingerprint'] == t69_fp
    assert not any(k.startswith(('reference_backfill', 'reference_entry', 'reference_last')) for k in POLICY)
    # Everything else that decides an early-window entry matches T6.7c.
    for key in ('routing', 'new_priority', 'decision_ms', 'last_selection_ms', 'entry_ms',
                'quote_ttl_ms', 'core_expiry_ms', 'original_input_ms', 'book_max_age_ms', 'units',
                'c_mirror_up_prior', 'shallow_retracement', 'risk_state_key', 'loop_mdd_1u'):
        assert POLICY[key] == t67c[key], key
    assert POLICY['shadow_branches'] == t69['shadow_branches']
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
    (2, -1, -2, '.6', '.4'),       # shallow retracement
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
    assert new.allowed == old.allowed, (old.reason, new.reason)
    if old.allowed:
        assert new.signal.entry.side == old.signal.entry.side
        assert new.signal.entry == old.signal.entry
        for key in ('expires_at_ms', 'worst_ask_limit', 'expected_cash_usdt', 'expected_shares'):
            assert getattr(new.execution, key) == getattr(old.execution, key), key
        assert new.reason.split(':', 1)[1] == old.reason.split(':', 1)[1]  # same branch
        assert new.signal.original_input_sha256 == FINGERPRINT


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


def test_empty_report_lists_seven_live_lanes_and_six_shadow_routes():
    from src.gridbot.prediction.regime_t69a_report import empty_report, LIVE_LABELS, SHADOW_LABELS
    text = empty_report(S)
    assert text.startswith('📊 T6.9a Report')
    assert tuple(LIVE_LABELS) == LIVE_BRANCHES and len(SHADOW_LABELS) == 6
    assert 'Reference 180s' not in text and '檢查點' not in text
    assert sum(label in text for label in LIVE_LABELS.values()) == 7


def test_shadow_observer_follows_its_own_profile_and_asset(tmp_path):
    import sqlite3
    from src.gridbot.prediction.regime_t69a_shadow import observe, schema
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
