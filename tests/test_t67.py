"""T6.7 causal evidence, durable routing and shared Live risk regressions."""
import json
import sqlite3
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.gridbot.prediction import regime_t67_lane as lane
from src.gridbot.prediction.regime_t67_policy import PROFILE, FINGERPRINT, BRANCHES
from src.gridbot.prediction.regime_t67_evidence import EvidenceStore, evidence_path, read_inputs
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.c180_gate_runtime import LiveSettlement, LoopLedgerSnapshot
from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
from src.gridbot.prediction.strategy import StrategyConfig
from src.gridbot.prediction.telegram import selectable_lanes_for_market, _regime_risk_text
from src.gridbot.prediction.worker import PredictionWorker
from src.gridbot.prediction.live_report import format_live_report
from test_t63 import S, book, feature
from test_live_report import SCHEMA


def snap(t=60000, up='.3', down='.7'):
    return {**book(up, down, t), 'reference': '100', 'reference_received_ms': S-60000}


def spot(t, price='100', generation=1, received=None):
    return dict(source='binance_spot', generation=generation, event_ms=S+t,
                received_ms=S+t if received is None else received, price=str(price))


def tape(t=60000, price='100.1'):
    return [spot(k) for k in range(-60000, t, 1000)]+[spot(t, price)]


def decision(tmp_path, snapshot, spots=(), f=None, at=None, unit=D(1), bridge=None):
    path = tmp_path/'features.sqlite3'
    with closing(connect(path)) as db, db:
        if f is not None:
            db.execute('INSERT OR IGNORE INTO features VALUES(?,?)', (S, json.dumps(f)))
    obj = bridge or RegimeWorkerBridge(None, tmp_path/'signals.sqlite3', feature_db=path, profile=PROFILE)
    with closing(EvidenceStore(evidence_path(obj.signal_db))) as store:
        store.book(snapshot)
        with store.db:
            store.db.executemany('INSERT OR IGNORE INTO spot VALUES(?,?,?,?,?)',
                                [(s['source'], s['generation'], s['event_ms'], s['received_ms'], s['price']) for s in spots])
    market = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up', reference_price=D(100))
    ready = obj.check_signal(market=market, unit_usdt=unit,
                            at_ms=at or snapshot['captured_at_ms'], last_seen_book_at_ms=0)
    return obj, ready


def test_profile_selectable_original_risk_and_no_sibling_orders():
    assert PROFILE in PredictionWorker._selectable_strategy_profiles()
    assert PROFILE in dict(selectable_lanes_for_market('BTCUSDT'))
    assert PROFILE not in dict(selectable_lanes_for_market('ETHUSDT'))
    cfg = StrategyConfig.for_profile(PROFILE)
    assert cfg.provenance_payload['regime_policy_fingerprint'] == FINGERPRINT
    assert RegimeLiveLedger(None, profile=PROFILE).tier == 'REGIME_T67'
    worker = object.__new__(PredictionWorker)
    worker._selected_strategy_profile = PROFILE
    worker._fav_p3_arm_override = 'live'
    worker.settings = SimpleNamespace(shadow_lane_experiment_enabled=True)
    assert not worker._fav_p3_live_orders_enabled()
    assert not worker._shadow_lane_experiment_enabled()
    for unit in (D(1), D(2), D(3)):
        assert PredictionWorker._sized_strategy_config(PROFILE, unit).max_buy_usdt == unit
        assert '本輪MDD' in _regime_risk_text(PROFILE, unit)


@pytest.mark.parametrize('change', ['anchor', 'restart', 'stale', 'future', 'history', 'reference'])
def test_model_rejects_missing_or_noncausal_evidence(change):
    quotes, spots = snap(), tape()
    if change == 'anchor':
        spots = [s for s in spots if not S <= s['event_ms'] <= S+1500]
    elif change == 'restart':
        spots[-1]['generation'] = 2
    elif change == 'stale':
        spots = [s for s in spots if s['event_ms'] < S+58000]
    elif change == 'future':
        spots = [s for s in spots if s['event_ms'] < S+58000]
        spots.append(spot(60001, '100.1', received=S+60000))
    elif change == 'history':
        spots = [spot(0), spot(60000, '100.1')]
    else:
        quotes['reference_received_ms'] = S+60001
    with pytest.raises(ValueError):
        lane.probability(quotes, spots, S+60000)


def test_model_opening_basis_and_future_winner_cannot_change_probability():
    quotes, spots = snap(), tape()
    p, _, model = lane.probability(quotes, spots, S+60000)
    assert D('.5') < p < 1
    assert model['basis_assumption'] == 'constant_proxy_vs_settlement_basis'
    quotes['reference'] = '101'  # Constant basis correction keeps opening proxy displacement.
    quotes['winner'] = 'DOWN'
    spots += [spot(60001, '1'), spot(61000, '9999')]
    assert lane.probability(quotes, spots, S+60000)[0] == p


def test_value_checkpoints_are_once_and_require_fee_and_stress_ev():
    state = {}
    choices = lane.candidates(snap(), tape(), None, state, S+60000, D(1))
    assert choices[0]['branch'] == 'reference_value' and choices[0]['side'] == 'UP'
    assert not lane.candidates(snap(61000), tape(61000), None, state, S+61000, D(1))
    for quotes in (snap(up='.76'), snap(up='.3', down='.7')):
        if quotes['quote']['UP']['ask_levels'][0][0] == '.3':
            quotes['fee_bps'] = 10000
        assert not lane.candidates(quotes, tape(), None, {}, S+60000, D(1))
    # Actual EV passes but a two-cent worsening has no edge.
    with pytest.raises(ValueError):
        lane.model_candidate('reference_value', snap(up='.46'), 'UP', D('.48'), D(1), {})


def test_lead_lag_requires_distinct_public_300_and_1000_samples():
    t = 70000
    spots = tape(t, '100.1')
    spots[-2]['price'] = '100.07'
    state = {}
    assert not lane.candidates(snap(t), spots, None, state, S+t, D(1), [snap(t-1000)])
    assert state['lead_lag']['status'] == 'PENDING'
    # A worker waking at one second can reconstruct both confirmations.
    spots += [spot(t+300, '100.1'), spot(t+1000, '100.1')]
    choices = lane.candidates(snap(t+1000), spots, None, state, S+t+1000, D(1), [snap(t+300)])
    assert choices[0]['branch'] == 'external_lead_lag'
    assert set(state['lead_lag']['confirmations']) == {'300', '1000'}


@pytest.mark.parametrize('bad', ['missing_300', 'caught_up', 'reversed', 'restart', 'late'])
def test_lead_lag_never_fabricates_confirmations_or_retries_trigger(bad):
    t = 70000
    spots = tape(t, '100.1')
    spots[-2]['price'] = '100.07'
    state = {}
    lane.candidates(snap(t), spots, None, state, S+t, D(1), [snap(t-1000)])
    old = snap(t+300)
    current = snap(t+1000)
    spots += [spot(t+300, '100.1'), spot(t+1000, '100.1')]
    if bad == 'missing_300':
        old = snap(t+1000)
    elif bad == 'caught_up':
        old = snap(t+300, up='.31')
    elif bad == 'reversed':
        spots[-2]['price'] = '100.09'
    elif bad == 'restart':
        spots[-1]['generation'] = 2
    else:
        current = snap(t+2001)
        spots += [spot(t+2001, '100.1')]
    assert not lane.candidates(current, spots, None, state, current['captured_at_ms'], D(1), [old])
    assert state['lead_lag']['status'] == 'REJECTED'
    lane.candidates(snap(t+3000), tape(t+3000), None, state, S+t+3000, D(1), [snap(t+2000)])
    assert state['lead_lag']['at_ms'] == S+t


@pytest.mark.parametrize('a,b,side', [(2,-1,'UP'), (-2,1,'DOWN'), (1,-.5,'UP')])
def test_retracement_uses_compounded_direction_without_model(a, b, side):
    choices = lane.candidates(snap(124000), [], feature(a,b), {}, S+124000, D(1))
    assert choices[0]['branch'] == 'shallow_retracement' and choices[0]['side'] == side
    assert not lane.candidates(snap(134501), [], feature(a,b), {}, S+134501, D(1))
    assert not lane.candidates(snap(124000), [], feature(a,a), {}, S+124000, D(1))


def test_durable_first_choice_expiry_no_shadow_and_restart(tmp_path):
    obj, ready = decision(tmp_path, snap(), tape())
    assert ready.allowed, ready.reason
    assert ready.execution.expires_at_ms == S+62000
    with closing(connect(obj.feature_db)) as db:
        before = db.execute('SELECT payload FROM t67_decisions').fetchone()[0]
        assert not db.execute('SELECT 1 FROM decisions').fetchone()
        assert not db.execute('SELECT 1 FROM t65_shadow_quotes').fetchone()
    obj2 = RegimeWorkerBridge(None, obj.signal_db, feature_db=obj.feature_db, profile=PROFILE)
    _, ready = decision(tmp_path, snap(61000, up='.29'), tape(61000), bridge=obj2)
    assert ready.allowed
    _, expired = decision(tmp_path, snap(62000), tape(62000), bridge=obj2)
    assert not expired.allowed and 'expired' in expired.reason
    _, mismatch = decision(tmp_path, snap(61001), tape(61001), bridge=obj2, unit=D(2))
    assert not mismatch.allowed and 'mismatch' in mismatch.reason
    with closing(connect(obj.feature_db)) as db:
        assert db.execute('SELECT payload FROM t67_decisions').fetchone()[0] == before


@pytest.mark.parametrize('bad', ['id', 'stale', 'reference', 'fee', 'thin', 'future'])
def test_bridge_quote_and_metadata_fail_closed(tmp_path, bad):
    quotes = snap()
    if bad == 'id':
        quotes['market_id'] = 'wrong'
    elif bad == 'stale':
        quotes['book_at_ms'] -= 1001
    elif bad == 'reference':
        quotes['reference'] = '99'
    elif bad == 'fee':
        quotes['fee_bps'] = 'NaN'
    elif bad == 'thin':
        quotes['quote']['UP']['ask_levels'] = [['.3', '1']]
    else:
        quotes['received_at_ms'] += 1
    _, ready = decision(tmp_path, quotes, tape())
    assert not ready.allowed, ready.reason


def test_feature_provenance_is_required_only_for_retracement(tmp_path):
    f = feature(2,-1)
    f['fingerprint'] = 'bad'
    _, ready = decision(tmp_path, snap(124000), [], f=f)
    assert not ready.allowed


def test_evidence_inputs_are_causal_bounded_and_opening_anchor_preserved(tmp_path):
    signal = tmp_path/'signals.sqlite3'
    with closing(EvidenceStore(evidence_path(signal))) as store:
        for t in (10000, 58000, 60000, 60001):
            store.book(snap(t))
        with store.db:
            store.db.executemany('INSERT INTO spot VALUES(?,?,?,?,?)',
                [(s['source'], s['generation'], s['event_ms'], s['received_ms'], s['price']) for s in tape()+[spot(60001)]])
    books, spots = read_inputs(signal, S, S+60000)
    assert [b['captured_at_ms'] for b in books] == [S+58000, S+60000]
    assert all(s['received_ms'] <= S+60000 for s in spots)
    assert any(s['event_ms'] == S for s in spots)


def test_t67_skips_original_paid_signals_and_paper_recovery(tmp_path):
    runtime = object.__new__(C180SignalRuntime)
    runtime.prediction_db = tmp_path/"prediction.sqlite3"
    with sqlite3.connect(runtime.prediction_db):
        pass
    runtime.signals = SimpleNamespace(on_frozen=Mock())
    with patch.object(runtime, '_t67_active', return_value=True):
        runtime._on_frozen({})
    runtime.signals.on_frozen.assert_not_called()
    with patch.object(runtime, '_t67_active', return_value=False):
        runtime._on_frozen({})
    runtime.signals.on_frozen.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('t,halt,cap,allowed', [(60000,False,'.4',True), (269999,False,'.75',True),
    (59999,False,'.4',False), (270000,False,'.4',False), (60000,True,'.4',False), (60000,False,'.751',False)])
async def test_atomic_claim_window_hard_stop_price_cap_and_one_market_buy(tmp_path,t,halt,cap,allowed):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        # Model the deployed VM schema; the source-only bootstrap migrations
        # predate the existing client-order-id column used by all T6 claims.
        await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN client_order_id TEXT')
        await repo._execute('ALTER TABLE prediction_order_intents ADD COLUMN tier TEXT')
        await repo.start_loop('new',100,mode='LIVE',strategy_profile=PROFILE)
        await repo.save_campaign(Campaign('c',MarketInfo('topic','up','test',S,S+300000,
                                 up_market_id='up',down_market_id='down')),loop_id='new')
        ledger = RegimeLiveLedger(repo,profile=PROFILE)
        await ledger.seed_schedule(loop_id='new',first_market_start_ms=S)
        await ledger.verify_market(loop_id='new',market_start_ms=S,market_topic_id='topic',
                                   market_id='up',verified_at_ms=S+50000)
        if halt:
            await repo._execute("UPDATE prediction_loops SET hard_stop_latched=1 WHERE loop_id='new'")
        intent = dict(intent_id='i',campaign_id='c',action='BUY_INITIAL',outcome='UP',order_side='BUY',
            amount='2',limit_price=cap,created_at_ms=S+t,ttl_ms=1000,attempt=1,status='PENDING',
            tier='REGIME_T67',payload={})
        with patch('src.gridbot.prediction.regime_live_ledger._now_ms',return_value=S+t):
            claim = await ledger.reserve_c180_intent(loop_id='new',market_start_ms=S,campaign_id='c',
                intent=intent,decision_at_ms=S+t,wallet_reconciled_at_ms=S+t)
            assert claim.claimed == allowed, claim.reason
            if allowed:
                again = await ledger.reserve_c180_intent(loop_id='new',market_start_ms=S,campaign_id='c',
                    intent={**intent,'intent_id':'second'},decision_at_ms=S+t,wallet_reconciled_at_ms=S+t)
                assert not again.claimed
                rows = await repo._fetchall('SELECT unit_usdt FROM prediction_regime_entry_claims')
                assert [D(r['unit_usdt']) for r in rows] == [D(2)]
            else:
                assert not await repo._fetchall('SELECT 1 FROM prediction_regime_entry_claims')
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_existing_t65_unknown_exposure_blocks_t67_and_preserves_epoch(tmp_path):
    from src.gridbot.prediction.models import Campaign, MarketInfo
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo.start_loop('old',100,mode='LIVE',strategy_profile='regime_target6_5_v1')
        old = RegimeLiveLedger(repo,profile='regime_target6_5_v1')
        await old.seed_schedule(loop_id='old',first_market_start_ms=S)
        await old.check_risk('old',S,S+60000)
        await repo.save_campaign(Campaign('old-c',MarketInfo('topic','up','test',S,S+300000)),loop_id='old')
        await repo._execute("UPDATE prediction_campaigns SET pending_unknown=1 WHERE campaign_id='old-c'")
        await repo.start_loop('new',100,mode='LIVE',strategy_profile=PROFILE)
        ledger = RegimeLiveLedger(repo,profile=PROFILE)
        await ledger.seed_schedule(loop_id='new',first_market_start_ms=S+300000)
        allowed, reason = await ledger.check_risk('new',S+300000,S+360000)
        assert not allowed and reason == 'unknown_order_reconciliation_required'
        state = await repo.get_runtime_config('regime_target6_risk_v1')
        assert state['first_market_start_ms'] == S
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_risk_shared_epoch_unknown_and_per_loop_latch_survive_restart(tmp_path):
    from src.gridbot.prediction.regime_lane import FINGERPRINT as RISK_FP
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo.start_loop('new',100,mode='LIVE',strategy_profile=PROFILE)
        ledger = RegimeLiveLedger(repo,profile=PROFILE)
        await ledger.seed_schedule(loop_id='new',first_market_start_ms=S)
        assert (await ledger.check_risk('new',S,S+60000))[0]
        gate = await repo.get_runtime_config('regime_target6_risk_v1')
        assert gate['fingerprint'] == RISK_FP
        rows = (LiveSettlement('win',S,D(3),S+300000,D(1)),
                LiveSettlement('loss',S+300000,D('-3.5'),S+600000,D(1)))
        snap = LoopLedgerSnapshot('new',True,(),(),rows,())
        with patch.object(ledger,'_snapshot_conn',AsyncMock(return_value=snap)):
            assert await ledger.check_risk('new',S+600000,S+660000) == (False,'t67_loop_mdd_3.5')
        guard = await repo.get_runtime_config('regime_target6_7_loop_risk:new')
        assert guard['fingerprint'] == FINGERPRINT and guard['mdd_1u'] == '3.5'
        ledger = RegimeLiveLedger(repo,profile=PROFILE)
        empty = LoopLedgerSnapshot('new',True,(),(),(),())
        with patch.object(ledger,'_snapshot_conn',AsyncMock(return_value=empty)):
            assert await ledger.check_risk('new',S+900000,S+960000) == (False,'t67_loop_mdd_3.5')
    finally:
        await repo.close()


def test_report_attributes_only_actual_fills_and_preserves_official_pnl(tmp_path):
    directory = tmp_path/'prediction/data'
    directory.mkdir(parents=True)
    with closing(connect(directory/'regime-target6/features.sqlite3')) as db, db:
        db.execute('CREATE TABLE t67_decisions(start INTEGER PRIMARY KEY,payload TEXT)')
        db.execute('INSERT INTO t67_decisions VALUES(?,?)',(S,json.dumps(dict(
            selected=True,fingerprint=FINGERPRINT,branch=BRANCHES[0],market_topic='topic',market_id='up',market_start_ms=S,market_end_ms=S+300000))))
    with closing(sqlite3.connect(directory/'prediction.sqlite3')) as db, db:
        db.executescript(SCHEMA)
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN market_topic_id TEXT')
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN market_id TEXT')
        db.execute('ALTER TABLE prediction_regime_slots ADD COLUMN market_topic_id TEXT')
        db.execute('ALTER TABLE prediction_regime_slots ADD COLUMN market_id TEXT')
        db.execute("INSERT INTO prediction_loops VALUES('new',?,'LIVE','RUNNING',100,1,1,0,0)",(PROFILE,))
        db.execute("INSERT INTO prediction_campaigns VALUES('c','new',?,0,'topic','')",(S,))
        db.execute("INSERT INTO prediction_regime_slots VALUES('new',?,1,?,NULL,'topic','up')",(S,S))
        db.execute("INSERT INTO prediction_regime_entry_claims VALUES('new',?,'c','i','1')",(S,))
        db.execute("INSERT INTO prediction_order_intents VALUES('i','c','FILLED','o',0,?)",(S+60000,))
        db.execute("INSERT INTO prediction_fills VALUES('c','BUY')")
        db.execute("INSERT INTO prediction_settlements VALUES('s','c','SETTLED','1.234')")
        db.execute("INSERT INTO prediction_regime_settlement_observations VALUES('s','c','1.234',?)",(S+300000,))
    text = format_live_report(tmp_path,now_ms=S+600000)
    assert 'T6.7 Live Report' in text and 'Live fill rate 100.0%' in text
    assert '外部先行｜成交 1｜已知WR 100.0%｜已知PnL +1.2340' in text
    assert '本輪已知淨 PnL +1.2340' in text and 'Shadow' not in text
    with closing(sqlite3.connect(directory/'prediction.sqlite3')) as db, db:
        db.execute("UPDATE prediction_campaigns SET market_id='wrong'")
    text = format_live_report(tmp_path,now_ms=S+600000)
    assert '子策略歸因待核對 1' in text and '本輪已知淨 PnL +1.2340' in text
