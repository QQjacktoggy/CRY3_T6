import json
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.gridbot.prediction import regime_t65_lane as lane, regime_worker_bridge as bridge
from src.gridbot.prediction.c180_gate_runtime import LiveSettlement, LoopLedgerSnapshot
from src.gridbot.prediction.live_report import _shadow_metrics, format_live_report
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction import regime_t65_shadow as shadow_service
from src.gridbot.prediction.regime_t65_shadow import collect_once
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.telegram import selectable_lanes_for_market, _regime_risk_text
from src.gridbot.prediction.worker import PredictionWorker
from test_t63 import S, feature, book, original


def freeze(tmp_path, f, snap, signal=None):
    path = tmp_path / 'features.sqlite3'
    db = connect(path)
    with db:
        db.execute('INSERT INTO features VALUES(?,?)', (S, json.dumps(f)))
    obj = bridge.RegimeWorkerBridge(None, 'unused', feature_db=path, profile=lane.PROFILE)
    market = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up')
    with patch.object(obj, '_first_book', return_value=snap), patch.object(bridge, 'read_c180_signal', return_value=signal), patch.object(bridge, 'read_c180_book', return_value=snap):
        ready = obj.check_signal(market=market, unit_usdt=D(1), at_ms=S+125000, last_seen_book_at_ms=0)
    return db, obj, ready


def test_live_filter_and_parent_suppression():
    live, shadows = lane.candidates(feature(-2, 4), original(), book(), D(1))
    assert not live
    assert shadows['shadow_a']['branch'] == 'A_jev_conflict'
    assert shadows['shadow_fallback']['branch'] == 'fallback'
    # flat/original must not be promoted to another Live candidate.
    approved = dict(original('.7'), status='entry_positive_cost_after_ev', entry={'side': 'UP'})
    live, shadows = lane.candidates(feature(0, 0), approved, book('.5', '.5'), D(1))
    assert not live
    assert shadows['shadow_flat']['candidate']['action'] == 'original'
    # First/UP stays live; B stays paper and C stays live.
    live, _ = lane.candidates(feature(2, -2), None, book('.3', '.7'), D(1))
    assert live[0]['side'] == 'UP' and live[0]['action'] == 'first'
    live, shadow = lane.candidates(feature('.1', 2), None, book('.6', '.4'), D(1))
    assert not live and shadow['shadow_b']
    live, _ = lane.candidates(feature(2, -4, -2), None, book('.3', '.7'), D(1))
    assert live[0]['branch'] == 'C_reversal_netdown'


def test_frozen_new_rules_and_boundary():
    _, shadows = lane.candidates(feature(2, -1), None, book('.3', '.7'), D(1))
    assert shadows['shadow_m4']['candidate']['side'] == 'UP'
    assert shadows['shadow_m6']['candidate']['side'] == 'UP'  # compounded net is 0.9998 bp
    _, shadows = lane.candidates(feature(2, '-.4999'), None, book(), D(1))
    assert 'shadow_m4' not in shadows
    _, shadows = lane.candidates(feature(0, 0), None, book('.3', '.3'), D(1))
    assert shadows['shadow_m6']['candidate']['side'] == 'DOWN'


def test_shadow_after_live_selection_fresh_window_and_no_overwrite(tmp_path):
    db, obj, ready = freeze(tmp_path, feature(2, -1), book('.3', '.7'))
    try:
        assert ready.allowed and ready.reason.startswith('t65_ready:')
        frozen = db.execute('SELECT payload FROM decisions').fetchone()[0]
        # Initial quote cannot be called an M4 shadow entry.
        with patch.object(shadow_service, '_window_books', return_value=[book('.3', '.7', 127999)]):
            collect_once(db, S, 'unused', S+127999)
        row = json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0])
        assert row['quote'] is None
        for invalid in (book('.3', '.7', 126000), {**book('.3', '.7', 128000), 'market_id': 'wrong'},
                        {**book('.3', '.7', 128000), 'fee_bps': 201}):
            with patch.object(shadow_service, '_window_books', return_value=[invalid]):
                collect_once(db, S, 'unused', S+128000)
            assert json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0])['quote'] is None
        with patch.object(shadow_service, '_window_books', return_value=[book('.3', '.7', 128000)]):
            collect_once(db, S, 'unused', S+128000)
        row = json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0])
        assert row['fill_status'] == 'PAPER_QUOTE_ONLY'
        assert row['quoted_at_ms'] == S+128000 and D('.99') < D(row['quote']['cash']) <= 1
        with patch.object(shadow_service, '_window_books', return_value=[book('.5', '.5', 130000)]):
            collect_once(db, S, 'unused', S+130000)
        assert json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0]) == row
        assert db.execute('SELECT payload FROM decisions').fetchone()[0] == frozen
    finally:
        db.close()


@pytest.mark.parametrize('observed, status', [(True, 'NO_EXECUTABLE_WINDOW_QUOTE'), (False, 'UNOBSERVED_WINDOW')])
def test_shadow_missing_depth_and_missing_observation_distinct(tmp_path, observed, status):
    db, _, ready = freeze(tmp_path, feature(0, 0), book('.7', '.3'))
    try:
        assert not ready.allowed  # Shadow alone cannot authorize a Live BUY.
        if observed:
            thin = book('.7', '.3', 128000)
            thin['quote']['DOWN']['ask_levels'] = [['.3', '1']]
            with patch.object(shadow_service, '_window_books', return_value=[thin]):
                collect_once(db, S, 'unused', S+128000)
        collect_once(db, S, 'unused', S+134501)
        row = json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0])
        assert row['fill_status'] == status and row['quote'] is None
    finally:
        db.close()


def test_profile_wiring_and_amounts():
    assert lane.PROFILE in PredictionWorker._selectable_strategy_profiles()
    assert lane.PROFILE in dict(selectable_lanes_for_market('BTCUSDT'))
    assert lane.PROFILE not in dict(selectable_lanes_for_market('ETHUSDT'))
    assert StrategyConfig.for_profile(lane.PROFILE).provenance_payload['regime_policy_fingerprint'] == lane.FINGERPRINT
    assert RegimeLiveLedger(None, profile=lane.PROFILE).tier == 'REGIME_T65'
    for unit in (D(1), D(2), D(3)):
        assert PredictionWorker._sized_strategy_config(lane.PROFILE, unit).max_buy_usdt == unit
        assert '本輪MDD' in _regime_risk_text(lane.PROFILE, unit)


def test_shadow_report_official_outcomes_and_live_separation(tmp_path):
    import sqlite3
    from test_live_report import SCHEMA
    directory = tmp_path/'prediction/data'
    features = directory/'regime-target6'
    features.mkdir(parents=True)
    db, _, ready = freeze(features, feature(2, -1), book('.3', '.7'))
    try:
        assert ready.allowed
        with patch.object(shadow_service, '_window_books', return_value=[book('.3', '.7', 128000)]):
            collect_once(db, S, 'unused', S+128000)
        with sqlite3.connect(directory/'prediction.sqlite3') as main:
            main.executescript(SCHEMA)
            main.execute('ALTER TABLE prediction_settlements ADD COLUMN winner TEXT')
            main.execute('CREATE TABLE prediction_shadow_observer_markets(market_topic_id TEXT,market_id TEXT,start_time_ms INTEGER,winner TEXT,state TEXT)')
            main.execute("INSERT INTO prediction_shadow_observer_markets VALUES('topic','up',?,'UP','SETTLED')", (S,))
            main.execute("INSERT INTO prediction_loops VALUES('new',?,'LIVE','RUNNING',100,1,1,0,0)", (lane.PROFILE,))
            main.execute("INSERT INTO prediction_campaigns VALUES('c','new',?,0)", (S,))
            main.execute("INSERT INTO prediction_regime_slots VALUES('new',?,1,?,?)", (S, S, S+300000))
        slots = [{'market_start_ms': S}]
        campaigns = [dict(loop_id='new', start_time_ms=S, campaign_id='c')]
        metrics = _shadow_metrics(tmp_path, 'new', slots, campaigns, [], profile=lane.PROFILE,
                                  key='shadow_m4', branch='M4_first_pullback')
        assert metrics['known'] == metrics['wins'] == metrics['quoted'] == 1
        assert metrics['pnl'] > 2
        text = format_live_report(tmp_path, now_ms=S+600000)
        assert 'T6.5 Live Report' in text and 'Live fill rate 0.0%' in text
        assert '本輪已知淨 PnL +0.0000 USDT' in text
        assert 'M4 Shadow 已知WR 100.0%' in text
        # Missing or contradictory official results must never be made a loss/zero.
        with sqlite3.connect(directory/'prediction.sqlite3') as main:
            main.execute('UPDATE prediction_shadow_observer_markets SET winner=NULL')
        metrics = _shadow_metrics(tmp_path, 'new', slots, campaigns, [], profile=lane.PROFILE,
                                  key='shadow_m4', branch='M4_first_pullback')
        assert metrics['known'] == 0 and metrics['unknown'] == 1
        with sqlite3.connect(directory/'prediction.sqlite3') as main:
            main.execute("UPDATE prediction_shadow_observer_markets SET winner='UP'")
        conflicting = [dict(status='SETTLED', campaign_id='c', winner='DOWN')]
        with pytest.raises(ValueError, match='conflicting'):
            _shadow_metrics(tmp_path, 'new', slots, campaigns, conflicting, profile=lane.PROFILE,
                            key='shadow_m4', branch='M4_first_pullback')
    finally:
        db.close()


@pytest.mark.asyncio
async def test_risk_shared_history_and_loop_latch_survive_restart(tmp_path):
    from src.gridbot.prediction.regime_lane import FINGERPRINT as RISK_FP
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo.start_loop('t65', 100, mode='LIVE', strategy_profile=lane.PROFILE)
        ledger = RegimeLiveLedger(repo, profile=lane.PROFILE)
        await ledger.seed_schedule(loop_id='t65', first_market_start_ms=S)
        assert (await ledger.check_risk('t65', S, S+124000))[0]
        rows = (LiveSettlement('profit', S, D(3), S+300000, D(1)),
                LiveSettlement('loss', S+300000, D('-3.5'), S+600000, D(1)))
        snap = LoopLedgerSnapshot('t65', True, (), (), rows, ())
        with patch.object(ledger, '_snapshot_conn', AsyncMock(return_value=snap)):
            assert await ledger.check_risk('t65', S+600000, S+724000) == (False, 't65_loop_mdd_3.5')
        guard = await repo.get_runtime_config('regime_target6_5_loop_risk:t65')
        assert guard['fingerprint'] == lane.FINGERPRINT and guard['mdd_1u'] == '3.5'
        assert (await repo.get_runtime_config('regime_target6_risk_v1'))['fingerprint'] == RISK_FP
        # A fresh ledger instance cannot erase the stored per-loop latch.
        ledger = RegimeLiveLedger(repo, profile=lane.PROFILE)
        empty = LoopLedgerSnapshot('t65', True, (), (), (), ())
        with patch.object(ledger, '_snapshot_conn', AsyncMock(return_value=empty)):
            assert await ledger.check_risk('t65', S+900000, S+1024000) == (False, 't65_loop_mdd_3.5')
        await repo.set_runtime_config('regime_target6_5_loop_risk:t65', {**guard, 'fingerprint': 'wrong'})
        with patch.object(ledger, '_snapshot_conn', AsyncMock(return_value=empty)):
            assert (await ledger.check_risk('t65', S+900000, S+1024000))[1] == 't65_loop_risk_state_invalid'
    finally:
        await repo.close()
