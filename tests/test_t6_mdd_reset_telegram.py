"""Telegram button for the audited shared T6 20-run MDD reset."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from src.gridbot.prediction.telegram import PredictionTelegramService

HALTED = {'regime_lane_risk': {'halt_reason': 'scheduled20_mdd_3.5'}}


def _service(risk):
    service = PredictionTelegramService(object(), 1, token_factory=lambda: 'tok')
    service._deny_if_unauthorized = AsyncMock(return_value=False)
    service._read_risk = AsyncMock(return_value=(risk, None))
    service._reply = AsyncMock()
    return service


def _buttons(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row] if markup else []


def _cb(data):
    return SimpleNamespace(callback_query=SimpleNamespace(data=data, answer=AsyncMock()))


def test_button_only_for_scheduled20_mdd_halt():
    service = PredictionTelegramService(object(), 1)
    assert 'predict_t6reset:request' in _buttons(service._status_markup(HALTED))
    for halt in (None, 'cumulative_loss_6', 'unknown_order_reconciliation_required'):
        assert 'predict_t6reset:request' not in _buttons(
            service._status_markup({'regime_lane_risk': {'halt_reason': halt}}))


@pytest.mark.asyncio
async def test_confirm_flow_invokes_worker_reset_once():
    service = _service(HALTED)
    service._invoke = AsyncMock(return_value={'regime_risk_reset': {
        'reset': True, 'risk_epoch_start_ms': 1791350400000}})
    await service.handle_callback(_cb('predict_t6reset:request'), None)
    assert 'predict_t6reset:confirm:tok' in _buttons(service._reply.await_args.kwargs['reply_markup'])
    await service.handle_callback(_cb('predict_t6reset:confirm:tok'), None)
    assert service._invoke.await_args.args[0] == ('reset_regime_risk',)
    assert '已解除' in service._reply.await_args.args[1]
    await service.handle_callback(_cb('predict_t6reset:confirm:tok'), None)
    assert service._invoke.await_count == 1
    assert '無效' in service._reply.await_args.args[1]


@pytest.mark.asyncio
async def test_no_reset_when_not_halted_or_token_wrong():
    service = _service({'regime_lane_risk': {'halt_reason': 'cumulative_loss_6'}})
    service._invoke = AsyncMock()
    await service.handle_callback(_cb('predict_t6reset:request'), None)
    assert '不需要解除' in service._reply.await_args.args[1]
    service = _service(HALTED)
    service._invoke = AsyncMock()
    await service.handle_callback(_cb('predict_t6reset:request'), None)
    await service.handle_callback(_cb('predict_t6reset:confirm:bad'), None)
    service._invoke.assert_not_awaited()


def _worker(orders=0, unresolved=()):
    import asyncio
    repo = SimpleNamespace(load_unresolved_intents=AsyncMock(return_value=list(unresolved)),
                           get_active_loop=AsyncMock(return_value={'loop_id': 'L1'}),
                           request_operator_stop=AsyncMock(), record_risk_event=AsyncMock())
    return SimpleNamespace(_lock=asyncio.Lock(), repository=repo, _active_campaigns={},
                           reconcile=AsyncMock(return_value={'known': True, 'orders': orders}),
                           settings=SimpleNamespace(wallet_address=''), _status=lambda: {},
                           _now_ms=lambda: 123)


@pytest.mark.asyncio
async def test_worker_reset_requires_zero_exposure_then_stops_loop(monkeypatch):
    from src.gridbot.prediction import regime_live_ledger
    from src.gridbot.prediction.worker import PredictionWorker
    calls = []

    class FakeLedger:
        def __init__(self, repo):
            pass

        async def reset_shared_risk(self, **kw):
            # The old loop must already be stopped when the halt clears.
            w.repository.request_operator_stop.assert_awaited_once_with('L1')
            calls.append(kw)
            return {'reset': True, 'risk_epoch_start_ms': 300000}
    monkeypatch.setattr(regime_live_ledger, 'RegimeLiveLedger', FakeLedger)
    w = _worker(orders=1)
    assert (await PredictionWorker.reset_regime_risk(w))['action_denied']
    assert calls == []
    w = _worker()
    result = await PredictionWorker.reset_regime_risk(w)
    assert result['regime_risk_reset']['reset'] and calls[0]['now_ms'] == 123
    w.repository.request_operator_stop.assert_awaited_once_with('L1')
    assert w.repository.record_risk_event.await_args.args[0] == 'REGIME_T6_MDD_RESET'


@pytest.mark.asyncio
async def test_worker_reset_reports_success_even_if_event_write_fails(monkeypatch):
    from src.gridbot.prediction import regime_live_ledger
    from src.gridbot.prediction.worker import PredictionWorker

    class FakeLedger:
        def __init__(self, repo):
            pass

        async def reset_shared_risk(self, **kw):
            return {'reset': True, 'risk_epoch_start_ms': 300000}
    monkeypatch.setattr(regime_live_ledger, 'RegimeLiveLedger', FakeLedger)
    w = _worker()
    w.repository.record_risk_event.side_effect = RuntimeError('db')
    result = await PredictionWorker.reset_regime_risk(w)
    assert result['regime_risk_reset']['reset']
    assert result['regime_risk_reset']['risk_event_recorded'] is False
