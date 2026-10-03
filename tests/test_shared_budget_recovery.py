"""Temporary metadata errors recover; real/unknown bans retain authority."""
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.gridbot.prediction import rate_limit as rate
from src.gridbot.prediction.settings import RuntimeMode
from src.gridbot.prediction.worker import PredictionRateLimitDeferred, PredictionWorker
from scripts.resume_prediction_existing_loop import validate_authorization
from test_loop_cancel_recovery import setup_worker, cleanup


@pytest.mark.parametrize('status', [200, 418, 429])
def test_sqlite_response_lock_does_not_permanently_disable_budget(tmp_path, status):
    clock = [1000]
    budget = rate.SharedRequestBudget(tmp_path/'budget.db', clock_ms=lambda: clock[0])
    token = budget.begin_request()
    with patch.object(budget, '_connect', side_effect=sqlite3.OperationalError('database is locked')):
        budget.note_response(status, {'Retry-After': '10'}, token=token)
    assert not budget._metadata_unavailable
    assert budget.health()['last_metadata_fault']['code'] == 'response_metadata_db_unavailable'
    assert budget.acquire() is (status == 200)
    restarted = rate.SharedRequestBudget(tmp_path/'budget.db', clock_ms=lambda: clock[0])
    assert restarted.acquire() is (status == 200)
    clock[0] = 11000
    assert budget.acquire() and restarted.acquire()


def test_peer_removing_expired_record_during_scan_is_normal(tmp_path, monkeypatch):
    budget = rate.SharedRequestBudget(tmp_path/'budget.db', clock_ms=lambda: 1000)
    token = budget._cooldowns.begin_record()
    budget._cooldowns.finish(token, 999)
    real_open = rate.os.open
    def raced(path, *args, **kwargs):
        if str(path).split('/')[-1].startswith('request-'):
            from pathlib import Path
            Path(path).unlink(missing_ok=True)
        return real_open(path, *args, **kwargs)
    with monkeypatch.context() as m:
        m.setattr(rate.os, 'open', raced)
        assert budget.can_send_prepaid()
    assert not budget._metadata_unavailable and budget.acquire()


@pytest.mark.parametrize('record', ['pending', 'corrupt', 'missing_ready'])
def test_unknown_journal_state_stays_blocked(tmp_path, record):
    budget = rate.SharedRequestBudget(tmp_path/'budget.db', clock_ms=lambda: 1000)
    if record == 'missing_ready':
        (budget._cooldowns.path/'ready').unlink()
    else:
        (budget._cooldowns.path/'request-unknown').write_text(record)
    assert not budget.acquire() and not budget.can_send_prepaid()
    assert budget.begin_request() is None


def test_journal_response_failure_is_still_fail_closed(tmp_path):
    budget = rate.SharedRequestBudget(tmp_path/'budget.db', clock_ms=lambda: 1000)
    token = budget.begin_request()
    with patch.object(budget._cooldowns, 'finish', side_effect=OSError('failed persistence')):
        budget.note_response(429, {'Retry-After': '10'}, token=token)
    token[0].close()
    assert budget._metadata_unavailable
    assert not budget.acquire()
    restarted = rate.SharedRequestBudget(tmp_path/'budget.db', clock_ms=lambda: 100000)
    assert not restarted.acquire()


@pytest.mark.asyncio
async def test_discovery_deferred_is_durable_and_throttled():
    worker = object.__new__(PredictionWorker)
    clock = [100000]
    worker._now_ms = lambda: clock[0]
    worker._loop_id = 'existing'
    worker.repository = SimpleNamespace(record_risk_event=AsyncMock(), set_runtime_config=AsyncMock())
    exc = PredictionRateLimitDeferred('list_prediction_markets', {'error': 'shared_budget_unavailable'})
    await worker._record_discovery_deferred(exc)
    await worker._record_discovery_deferred(exc)
    assert worker.repository.record_risk_event.await_count == 1
    assert worker.repository.set_runtime_config.call_args.args[1]['budget_error'] == 'shared_budget_unavailable'
    clock[0] += 60000
    await worker._record_discovery_deferred(exc)
    assert worker.repository.record_risk_event.await_count == 2


def proof():
    return dict(loop_id='existing', profile='regime_target6_8a_v1', unit='1', fingerprint='approved',
                target=100, completed=74, issued_at_ms=1000, expires_at_ms=11000)


@pytest.mark.parametrize('change', ['loop_id', 'target', 'completed', 'profile', 'unit', 'fingerprint', 'expired', 'stopped'])
def test_one_use_authority_rejects_changed_scope(change):
    p = proof()
    loop = dict(loop_id='existing', mode='LIVE', state='RUNNING', target=100, completed=74,
                strategy_profile=p['profile'])
    args = dict(now_ms=2000, profile=p['profile'], unit='1', fingerprint='approved')
    if change in ('loop_id', 'target', 'completed'):
        loop[change] = 'another' if change == 'loop_id' else 101
    elif change == 'stopped':
        loop['new_entries_stopped'] = True
    elif change == 'expired':
        args['now_ms'] = 12000
    else:
        args[change] = 'another'
    with pytest.raises(ValueError):
        validate_authorization(p, loop, **args)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['missing', 'wrong_id', 'target', 'profile', 'stopped', 'none'])
async def test_expected_loop_guard_never_creates_or_extends_loop(tmp_path, change):
    repo, worker, _, _, _ = await setup_worker(tmp_path, shares='0')
    try:
        worker.settings = SimpleNamespace(max_loop_limit=100, is_live_requested=False)
        worker._effective_mode = RuntimeMode.LIVE
        worker._risk_snapshot = AsyncMock(return_value=SimpleNamespace(hard_stop_latched=False))
        for name in ('restore_order_unit','restore_selected_strategy','_activate_pending_strategy_if_idle',
                     '_activate_pending_order_unit_if_idle'):
            setattr(worker, name, AsyncMock())
        worker._run_loop = AsyncMock()
        await repo._execute("UPDATE prediction_campaigns SET state='DONE'")
        await repo._execute('UPDATE prediction_loops SET completed=74')
        if change == 'missing':
            await repo._execute("UPDATE prediction_loops SET state='DONE'")
        elif change == 'profile':
            await repo._execute("UPDATE prediction_loops SET strategy_profile='another'")
        elif change == 'stopped':
            await repo._execute('UPDATE prediction_loops SET new_entries_stopped=1')
        rows_before = await repo._fetchall('SELECT loop_id,completed,target,state,strategy_profile FROM prediction_loops')
        result = await worker.start_loop(99 if change=='target' else 100,
                                       expected_loop_id='another' if change=='wrong_id' else 'existing')
        assert bool(result.get('action_denied')) is (change != 'none')
        assert await repo._fetchall('SELECT loop_id,completed,target,state,strategy_profile FROM prediction_loops') == rows_before
        if change != 'none':
            worker._run_loop.assert_not_awaited()
            assert worker._task is None
    finally:
        await cleanup(repo, worker)
