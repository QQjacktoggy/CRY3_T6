"""Regression coverage for the independent review's recovery/accounting findings.

All exchange responses and installer effects are local fixtures or mocks.
"""
import asyncio
import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
from src.gridbot.prediction.live_report import _reported_hard_stop
from src.gridbot.prediction.models import Campaign, CampaignState, Fill, MarketInfo, OrderSide, OutcomeSide
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t67a_shadow import schema
from src.gridbot.prediction.repository import PredictionRepository
from src.gridbot.prediction.settings import RuntimeMode
from src.gridbot.prediction.worker import PredictionWorker
from test_loop_cancel_recovery import assert_admission_closed, cleanup, setup_worker
from test_t63 import S
from test_t65_observation_regressions import official
from test_t67a_report import FINGERPRINT, NOW, main_database, render

ROOT = Path(__file__).resolve().parents[1]


async def buy(repo, campaign):
    campaign.buy_count = 1
    campaign.position.up_cost = D(1)
    campaign.position.up_shares = D(2)
    await repo.save_campaign(campaign)
    await repo.record_fill(Fill('buy', 'up', OrderSide.BUY, OutcomeSide.UP,
                               D(2), D('.5'), D(1), event_time_ms=S+200000),
                           campaign_id=campaign.campaign_id)


def settlement_worker(worker):
    worker._ensure_shadow_window = AsyncMock()
    worker._shadow_config_hash = 'review-config'
    worker._shadow_window = {'window_start_ms': S, 'window_end_ms': S+86400000}
    worker._shadow_campaign_ids = {}
    worker._configured_shadow_lanes = Mock(return_value=[])
    worker._official_resolution = Mock(return_value=OutcomeSide.UP)
    worker.client = SimpleNamespace(batch_redeem=AsyncMock())
    worker.settle_campaign = PredictionWorker.settle_campaign.__get__(worker)
    worker.reconcile = PredictionWorker.reconcile.__get__(worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('entry', ['reconcile', 'cancel', 'shadow_finalizer'])
async def test_live_fill_survives_restart_or_live_off_in_shadow(tmp_path, entry):
    repo, worker, campaign, clock, _ = await setup_worker(tmp_path)
    settlement_worker(worker)
    worker._loop_id = None
    worker._effective_mode = RuntimeMode.SHADOW
    clock[0] = S+310000
    try:
        await buy(repo, campaign)
        if entry == 'reconcile':
            result = await worker.reconcile()
            assert result['settled'] == 0
        elif entry == 'cancel':
            result = await worker.cancel_loop()
            assert result['action_denied']
            assert worker._task is not None
        else:
            result = await worker._settle_shadow_campaign(campaign)
            assert result['status'] == 'SETTLEMENT_PROVENANCE_UNKNOWN'
        stored = await repo.load_campaign(campaign.campaign_id)
        assert stored.state == CampaignState.FINAL_HOLD
        assert stored.position.up_shares == 2 and stored.position.up_cost == 1
        assert await repo.get_campaign_execution_mode(campaign.campaign_id) == 'LIVE'
        assert not await repo._fetchall('SELECT 1 FROM prediction_shadow_settlements')
        assert not await repo._fetchall('SELECT 1 FROM prediction_settlements')
        assert len(await repo.load_active_campaigns()) == 1
        worker._ensure_shadow_window.assert_not_awaited()
        worker.client.batch_redeem.assert_not_awaited()
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_shadow_campaign_stays_paper_when_process_is_live(tmp_path):
    repo, worker, campaign, clock, _ = await setup_worker(tmp_path)
    settlement_worker(worker)
    worker._effective_mode = RuntimeMode.LIVE
    worker._loop_id = None
    clock[0] = S+310000
    try:
        await repo._execute("UPDATE prediction_loops SET mode='SHADOW' WHERE loop_id='existing'")
        campaign.position.up_cost = D(1)
        await repo.save_campaign(campaign)
        result = await worker.reconcile()
        assert result['settled'] == 1
        assert (await repo.load_campaign(campaign.campaign_id)).state == CampaignState.DONE
        assert len(await repo._fetchall('SELECT 1 FROM prediction_shadow_settlements')) == 1
        assert not await repo._fetchall('SELECT 1 FROM prediction_settlements')
        worker.client.batch_redeem.assert_not_awaited()
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('evidence', ['fill', 'intent', 'order', 'settlement'])
async def test_legacy_shadow_loop_with_live_execution_never_becomes_paper(tmp_path, evidence):
    repo, worker, campaign, _, _ = await setup_worker(tmp_path)
    try:
        await repo._execute("UPDATE prediction_loops SET mode='SHADOW' WHERE loop_id='existing'")
        if evidence == 'fill':
            await buy(repo, campaign)
        elif evidence == 'intent':
            await repo._execute("INSERT INTO prediction_order_intents "
                "(intent_id,campaign_id,action,outcome,order_side,amount,limit_price,created_at_ms,ttl_ms,status,payload_json) "
                "VALUES('i','held','BUY_INITIAL','UP','BUY','1','.5',?,1000,'UNKNOWN','{}')", (S+124000,))
        elif evidence == 'order':
            await repo.save_order({"order_id": "order", "campaign_id": "held", "status": "FILLED"})
        else:
            await repo.finalize_settlement(campaign, {'status': 'SETTLED', 'net_pnl': '1'})
        assert await repo.get_campaign_execution_mode('held') == 'LIVE'
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_missing_execution_provenance_defers_without_signed_calls(tmp_path):
    repo, worker, campaign, _, _ = await setup_worker(tmp_path)
    settlement_worker(worker)
    worker._effective_mode = RuntimeMode.LIVE
    try:
        await repo._execute("UPDATE prediction_campaigns SET loop_id=NULL WHERE campaign_id='held'")
        assert await repo.get_campaign_execution_mode('missing') is None
        assert await repo.get_campaign_execution_mode('held') is None
        result = await worker.settle_campaign(campaign)
        assert result['status'] == 'SETTLEMENT_PROVENANCE_UNKNOWN'
        assert (await repo.load_campaign('held')).state == CampaignState.FINAL_HOLD
        worker._ensure_shadow_window.assert_not_awaited()
        worker.client.batch_redeem.assert_not_awaited()
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
async def test_authorized_live_resumes_existing_terminal_settlement_observation(tmp_path):
    repo, worker, campaign, _, _ = await setup_worker(tmp_path)
    settlement_worker(worker)
    worker._effective_mode = RuntimeMode.LIVE
    worker._finalize_settlement_with_c180 = AsyncMock()
    try:
        await buy(repo, campaign)
        await repo.finalize_settlement(campaign, {'status': 'SETTLED', 'net_pnl': '1', 'winner': 'UP'})
        campaign.state = CampaignState.FINAL_HOLD
        result = await worker.settle_campaign(campaign)
        assert result['status'] == 'SETTLED' and campaign.state == CampaignState.DONE
        worker._finalize_settlement_with_c180.assert_awaited_once()
        assert not await repo._fetchall('SELECT 1 FROM prediction_shadow_settlements')
        worker.client.batch_redeem.assert_not_awaited()
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('reduced', [False, True])
async def test_denied_cancel_recovers_durable_settlement_without_buy(tmp_path, restart, reduced):
    repo, worker, campaign, clock, original = await setup_worker(tmp_path)
    worker.reconcile = PredictionWorker.reconcile.__get__(worker)
    if restart:
        worker._loop_id = None
        worker._target_markets = 1
        worker._loop_created_at_ms = None
        worker._active_campaigns = {}
    try:
        await buy(repo, campaign)
        if reduced:
            campaign.position.up_shares = D(0)
            campaign.position.realized_cash = D('.8')
            await repo.save_campaign(campaign)
            await repo.record_fill(Fill('sell', 'up', OrderSide.SELL, OutcomeSide.UP,
                D(2), D('.4'), D('.8'), event_time_ms=S+210000), campaign_id='held')
            worker._call_api = AsyncMock(return_value=[])
        result = await worker.cancel_loop()
        assert result['action_denied'] and worker._task is not None
        assert worker._loop_id == original['loop_id']
        assert worker._target_markets == original['target']
        assert worker._loop_created_at_ms == original['created_at_ms']
        assert worker._settlement_only_recovery
        assert_admission_closed(worker)
        # A pre-expiry recovery tick cannot evaluate strategy or submit a new order.
        assert not await worker.manage_campaign(worker._active_campaigns['held'])
        clock[0] = S+310000
        await asyncio.wait_for(worker._task, 1)
        worker.settle_campaign.assert_awaited_once()
        assert not worker._active_campaigns
        assert (await repo.get_loop('existing'))['state'] == 'HARD_STOP'
        assert len(await repo._fetchall('SELECT 1 FROM prediction_loops')) == 1
        assert not await repo._fetchall('SELECT 1 FROM prediction_order_intents')
        assert_admission_closed(worker)
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('mismatch', ['local_loop', 'campaign_owner'])
async def test_recovery_does_not_adopt_mismatched_durable_identity(tmp_path, mismatch):
    repo, worker, _, _, _ = await setup_worker(tmp_path)
    worker._run_loop = AsyncMock()
    try:
        if mismatch == 'local_loop':
            worker._loop_id = 'different'
        else:
            await repo._execute("UPDATE prediction_campaigns SET loop_id='different' WHERE campaign_id='held'")
        result = await worker.cancel_loop()
        assert result['action_denied']
        assert worker._task is None
        worker._run_loop.assert_not_called()
    finally:
        await cleanup(repo, worker)


@pytest.mark.asyncio
@pytest.mark.parametrize('unit', [1, 2, 3])
@pytest.mark.parametrize('edge', ['daily', 'consecutive'])
async def test_t67a_settlement_uses_shared_risk_not_legacy_thresholds(tmp_path, unit, edge):
    repo = PredictionRepository(tmp_path/'db')
    await repo.initialize()
    try:
        await repo.start_loop('current', 100, mode='LIVE', strategy_profile='regime_target6_7a_v1')
        await repo.set_runtime_config('prediction_selected_strategy', {'profile': 'regime_target6_7a_v1'})
        losses = [D(-unit)]*(2 if unit == 1 else 1) if edge == 'daily' else [D('-.1')]*3
        now = __import__('time').time_ns()//1000000
        for index, pnl in enumerate(losses):
            c = Campaign(str(index), MarketInfo('topic'+str(index), 'up', 'test', S+index*300000, S+(index+1)*300000))
            await repo.save_campaign(c, loop_id='current')
            await repo.finalize_settlement(c, {'status': 'SETTLED', 'net_pnl': str(pnl), 'settled_at_ms': now+index})
        state = await repo.get_runtime_config('prediction_risk_state')
        assert not state['hard_stop_latched']
        assert D(state['daily_net_pnl']) == sum(losses)
        assert state['consecutive_losses'] == len(losses)
        await repo.set_runtime_config('prediction_risk_state', {**state, 'hard_stop_latched': True})
        await repo.finalize_settlement(c, {'status': 'SETTLED', 'net_pnl': str(losses[-1]), 'settled_at_ms': now+index})
        assert (await repo.get_runtime_config('prediction_risk_state'))['hard_stop_latched']
    finally:
        await repo.close()


@pytest.mark.parametrize('state,legacy,expected', [
    ({'hard_stop_latched': True}, False, '已鎖定'),
    ({'hard_stop_latched': False}, False, '未鎖定'),
    ({'hard_stop_latched': False}, True, '已鎖定'),
    (None, None, '全域狀態待核對'),
])
def test_t67a_report_includes_authoritative_and_legacy_hs(tmp_path, state, legacy, expected):
    with closing(main_database(tmp_path)) as db, db:
        db.execute("DELETE FROM prediction_runtime_config WHERE config_key='prediction_hard_stop_latched'")
        if legacy is not None:
            db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',
                ('prediction_hard_stop_latched', json.dumps({'latched': legacy})))
        if state is not None:
            db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',
                ('prediction_risk_state', json.dumps(state)))
    assert 'HS：'+expected in render(tmp_path)


def test_report_matches_worker_day_boundary_and_does_not_guess_invalid_hs():
    assert _reported_hard_stop({'prediction_risk_state': {'day': '2000-01-01', 'hard_stop_latched': True}}, NOW) == {'latched': False}
    assert _reported_hard_stop({'prediction_risk_state': {'hard_stop_latched': 'false'}}, NOW) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('profile', ['regime_target6_7a_v1', 'regime_target6_7_v1', 'regime_target6_5_v1'])
async def test_stored_t67a_paper_outcomes_resolve_after_profile_switch(tmp_path, profile):
    feature = tmp_path/'features.sqlite3'
    with closing(connect(feature)) as db:
        schema(db)
        quote = dict(fingerprint=FINGERPRINT, loop_id='old-t67a', market_topic='topic', market_id='up',
                     market_start_ms=S, market_end_ms=S+300000, end_ms=S+300000)
        with db:
            db.execute('INSERT INTO t67a_shadow_quotes VALUES(?,?,?)', (S, 'reference_value', json.dumps(quote)))
    worker = object.__new__(C180SignalRuntime)
    worker.feature_db = feature
    worker._last_t65_shadow_scan_ms = 0
    worker._t67_active = Mock(return_value=profile in ('regime_target6_7_v1', 'regime_target6_7a_v1'))
    worker._t67_selected_profile = profile
    worker._detail = AsyncMock(return_value=official())
    with patch('src.gridbot.prediction.c180_signal_runtime._now_ms', return_value=S+310000):
        await worker.t65_shadow_scan_once()
        await worker.t65_shadow_scan_once()  # Cadence excludes a duplicate poll.
    with closing(sqlite3.connect(feature)) as db:
        outcome = json.loads(db.execute('SELECT payload FROM t67a_shadow_outcomes').fetchone()[0])
    assert outcome['complete'] and outcome['winner'] == 'DOWN'
    assert outcome['loop_id'] == 'old-t67a'
    worker._detail.assert_awaited_once_with('topic')
    with patch('src.gridbot.prediction.c180_signal_runtime._now_ms', return_value=S+340000):
        await worker.t65_shadow_scan_once()
    assert worker._detail.await_count == 1  # Confirmed outcomes remain immutable.


@pytest.mark.asyncio
@pytest.mark.parametrize('offset', [119500, 124000, 137500])
async def test_paper_backlog_never_polls_in_live_entry_window(tmp_path, offset):
    feature = tmp_path/'features.sqlite3'
    feature.touch()
    worker = object.__new__(C180SignalRuntime)
    worker.feature_db = feature
    worker._last_t65_shadow_scan_ms = 0
    worker._detail = AsyncMock()
    with patch('src.gridbot.prediction.c180_signal_runtime._now_ms', return_value=S+offset):
        await worker.t65_shadow_scan_once()
    worker._detail.assert_not_awaited()
    assert worker._last_t65_shadow_scan_ms == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('optimized', [False, True])
@pytest.mark.parametrize('unsafe', ['target', 'running', 'order', 'intent', 'campaign'])
async def test_installer_snapshot_safety_survives_python_optimization(tmp_path, optimized, unsafe):
    repo = PredictionRepository(tmp_path/'prediction/data/prediction.sqlite3')
    await repo.initialize()
    await repo.start_loop('target', 100, mode='LIVE', strategy_profile='regime_target6_7a_v1')
    await repo._execute("UPDATE prediction_loops SET state='DONE',completed=100 WHERE loop_id='target'")
    if unsafe == 'target':
        await repo._execute("UPDATE prediction_loops SET state='RUNNING',completed=0 WHERE loop_id='target'")
    elif unsafe == 'running':
        await repo.start_loop('other', 100, mode='LIVE', strategy_profile='regime_target6_7a_v1')
    elif unsafe == 'order':
        await repo.save_campaign(Campaign("order-c", MarketInfo("topic", "up", "test", S, S+300000)), loop_id="target")
        await repo.save_order({"order_id": "o", "campaign_id": "order-c", "status": "UNKNOWN"})
    else:
        c = Campaign('c', MarketInfo('topic', 'up', 'test', S, S+300000))
        c.buy_count = int(unsafe == 'campaign')
        await repo.save_campaign(c, loop_id='target')
        if unsafe == 'intent':
            await repo._execute("INSERT INTO prediction_order_intents "
                "(intent_id,campaign_id,action,outcome,order_side,amount,limit_price,created_at_ms,ttl_ms,status,unknown,payload_json) "
                "VALUES('i','c','BUY_INITIAL','UP','BUY','1','.5',?,1000,'UNKNOWN',1,'{}')", (S+124000,))
    await repo.close()
    # Only call snapshot. Never run installer main, official_clear or service.
    code = """import runpy,sys
from pathlib import Path
f=runpy.run_path(sys.argv[1])['snapshot']
f.__globals__['ROOT']=Path(sys.argv[2])
try:
    f('target')
except RuntimeError as e:
    print(e)
    sys.exit(0)
sys.exit(1)
"""
    command = [sys.executable]+(['-O'] if optimized else [])+['-c', code, str(ROOT/'deploy/t67a_manual_install.py'), str(tmp_path)]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr or result.stdout
    message = {'target': 'Target loop', 'running': 'Another loop', 'order': 'Nonterminal order',
               'intent': 'Nonterminal/UNKNOWN intent', 'campaign': 'Unsettled campaign'}[unsafe]
    assert message in result.stdout


@pytest.mark.asyncio
async def test_real_manual_hard_stop_is_visible_in_t67a_report(tmp_path):
    from src.gridbot.prediction.live_report import format_live_report
    from src.gridbot.prediction.regime_live_ledger import RegimeLiveLedger
    repo = PredictionRepository(tmp_path/'prediction/data/prediction.sqlite3')
    await repo.initialize()
    try:
        await repo.start_loop('current', 100, mode='LIVE', strategy_profile='regime_target6_7a_v1')
        ledger = RegimeLiveLedger(repo, profile='regime_target6_7a_v1')
        await ledger.seed_schedule(loop_id='current', first_market_start_ms=S)
        await repo.set_runtime_config('prediction_hard_stop_latched', {'latched': False})
        worker = object.__new__(PredictionWorker)
        worker.repository = repo
        worker._now_ms = lambda: __import__('time').time_ns()//1000000
        worker._status = Mock(return_value={})
        await worker.hard_stop('manual regression')
        assert (await repo.get_runtime_config('prediction_risk_state'))['hard_stop_latched']
        text = format_live_report(tmp_path, profile_filter='regime_target6_7a_v1', now_ms=worker._now_ms())
        assert 'HS：已鎖定' in text
        assert not (await repo.get_loop('current'))['hard_stop_latched']
    finally:
        await repo.close()


@pytest.mark.parametrize('optimized', [0, 2])
@pytest.mark.parametrize('unsafe', [None, 'parent', 'pin', 'source', 'guard'])
def test_installer_preflight_checks_run_even_when_asserts_are_optimized(tmp_path, optimized, unsafe, monkeypatch):
    import hashlib
    installer = ROOT/'deploy/t67a_manual_install.py'
    namespace = {'__name__': 'offline_installer_test', '__file__': str(installer)}
    exec(compile(installer.read_text(), str(installer), 'exec', optimize=optimized), namespace)
    root, stage = tmp_path/'root', tmp_path/'stage'
    fingerprints = {}
    for base in (root, stage):
        release_path = 'src/gridbot/prediction/release.py'
        contents = {'source.py': 'old' if base == root else 'new',
                    release_path: f'_REQUIRED_FIXED_RELEASE_PATHS = ({release_path!r}, "source.py")\n'}
        entries = []
        for relative, content in sorted(contents.items()):
            path = base/relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            entries.append({'path': relative, 'sha256': hashlib.sha256(content.encode()).hexdigest()})
        canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        fingerprints[base] = hashlib.sha256(canonical.encode()).hexdigest()
        (base/'prediction').mkdir(parents=True)
        (base/'prediction/release-manifest.json').write_text(json.dumps({
            'schema': 'prediction-release-v1', 'files': entries,
            'release_fingerprint': fingerprints[base]}))
        (base/'prediction/release-pin.env').write_text(fingerprints[base])
    digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
    (stage/'candidate.json').write_text(json.dumps({'parent': 'changed' if unsafe == 'parent' else fingerprints[root],
        'files': [{'path': 'source.py', 'before': digest('old'), 'after': digest('new')}]}))
    (stage/'validation.json').write_text(json.dumps({'status': 'STAGED_VERIFIED_NOT_DEPLOYED',
                                                  'parent': fingerprints[root], 'fingerprint': fingerprints[stage]}))
    if unsafe == 'pin':
        (stage/'prediction/release-pin.env').write_text('wrong pin')
    (root/'prediction/hs-recovery-startup.env').write_text(
        'PREDICTION_LIVE_ARM_ON_START='+('true' if unsafe == 'guard' else 'false')+'\nPREDICTION_AUTO_START_LOOP=false\n')
    if unsafe == 'source':
        (root/'source.py').write_text('unexpected source')
    snapshot, official_read, service = Mock(return_value={'stable': True}), Mock(), Mock(return_value='active')
    namespace.update(ROOT=root, STAGE=stage, snapshot=snapshot, official_clear=official_read, service=service)
    verifier = Mock(wraps=namespace['verify_release'])
    namespace['verify_release'] = verifier
    monkeypatch.setattr(namespace['os'], 'getuid', lambda: 1000)
    monkeypatch.setattr(namespace['sys'], 'argv', ['installer'])
    if unsafe:
        with pytest.raises(RuntimeError):
            namespace['main']()
        official_read.assert_not_called()
        service.assert_not_called()
    else:
        namespace['main']()
        official_read.assert_called_once()
        assert all(call.args[0] == 'is-active' for call in service.call_args_list)
        assert verifier.call_count == 2
    assert not list((root/'prediction').glob('t67a-rollback-*'))
    assert (root/'source.py').read_text() == ('unexpected source' if unsafe == 'source' else 'old')
