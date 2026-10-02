"""Timestamp recovery is bounded to reads; writes and existing stops stay closed."""
import hashlib
import hmac
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from src.gridbot.prediction.client import (
    BinancePredictionClient, PREDICTION_PREFIX, PredictionAPIError,
    PredictionReadTimestampError, PredictionTransportError, TransportResponse,
)
from src.gridbot.prediction.rate_limit import REQUEST_PREPAID, SharedBudgetDeferred
from src.gridbot.prediction.worker import PredictionWorker


def rejection(code=-1021, status=400):
    return TransportResponse(status, {"code": code, "msg": "Timestamp outside recvWindow"})


class Transport:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        result = next(self.responses)
        if isinstance(result, Exception):
            raise result
        return result


class Budget:
    def __init__(self, clock, allowed=(True, True)):
        self.clock = clock
        self.allowed = iter(allowed)
        self.reservations = []
        self.prepaid_checks = 0

    def acquire(self, weight, *, priority):
        self.reservations.append(weight)
        self.clock[0] += 6000  # Budget work must happen before signing.
        return next(self.allowed)

    def can_send_prepaid(self):
        self.prepaid_checks += 1
        return True

    def begin_request(self):
        return object()

    def transport_failed(self, token):
        pass

    def note_response(self, status, headers, *, token=None):
        pass

    def health(self):
        return {"deferred": True}


def client(responses):
    clock = [100000]
    transport = Transport(responses)
    sdk = BinancePredictionClient("test-key", "test-secret", transport=transport,
                                  clock_ms=lambda: clock[0])
    sdk.request_budget = Budget(clock)
    return sdk, transport, clock


def signed_fields(call):
    method, url, kwargs = call
    canonical = urlsplit(url).query if method == "GET" else kwargs["body"].decode()
    unsigned, signature = canonical.rsplit("&signature=", 1)
    expected = hmac.new(b"test-secret", unsigned.encode(), hashlib.sha256).hexdigest()
    assert signature == expected
    fields = parse_qs(canonical)
    assert len(fields["timestamp"]) == len(fields["signature"]) == 1
    return fields


@pytest.mark.parametrize("code,status", [(-1021, 400), ("-1021", 200)])
def test_read_resigns_after_each_budget_reservation(code, status, caplog):
    sdk, transport, _ = client([rejection(code, status), TransportResponse(200, {"data": []})])
    with caplog.at_level(logging.INFO):
        result = sdk._request(PREDICTION_PREFIX + "/market/list", params={"recvWindow": 4500})
    assert result == {"data": []}
    first, second = map(signed_fields, transport.calls)
    assert first["timestamp"] == ["106000"]
    assert second["timestamp"] == ["112000"]
    assert first["signature"] != second["signature"]
    assert first["recvWindow"] == second["recvWindow"] == ["4500"]
    assert sdk.request_budget.reservations == [1, 1]
    assert "timestamp_recovered" in caplog.text


def test_exhausted_read_is_typed_bounded_and_logs_only_safe_metadata(caplog):
    sdk, transport, _ = client([rejection(), rejection(), TransportResponse(200, {})])
    with pytest.raises(PredictionReadTimestampError) as caught:
        sdk._request(PREDICTION_PREFIX + "/order/list", params={"walletAddress": "private-wallet"})
    exc = caught.value
    assert len(transport.calls) == exc.attempts == 2
    assert [item["attempt"] for item in exc.timings] == [1, 2]
    assert all(set(item) == {"attempt", "signed_at_ms", "sent_at_ms", "received_at_ms", "duration_ms"}
               for item in exc.timings)
    assert all(signed_fields(call)["recvWindow"] == ["5000"] for call in transport.calls)
    for secret in ("test-key", "test-secret", "private-wallet", "signature=", "https://"):
        assert secret not in caplog.text


@pytest.mark.parametrize("suffix,method,signed,error", [
    ("/market/list", "GET", False, rejection()),
    ("/future-unknown", "GET", True, rejection()),
    ("/market/list", "GET", True, rejection(-1022)),
    ("/market/list", "GET", True, rejection(status=429)),
    ("/market/list", "GET", True, rejection(status=500)),
    ("/trade/place-order", "POST", True, rejection()),
    ("/trade/quote", "POST", True, rejection()),
    ("/redeem", "POST", True, rejection()),
])
def test_other_requests_and_errors_are_never_automatically_retried(suffix, method, signed, error):
    sdk, transport, _ = client([error, TransportResponse(200, {})])
    with pytest.raises(PredictionAPIError) as caught:
        sdk._request(PREDICTION_PREFIX + suffix, method, signed=signed)
    assert type(caught.value) is PredictionAPIError
    assert len(transport.calls) == 1
    if signed:
        assert signed_fields(transport.calls[0])["timestamp"] == ["106000"]


def test_batch_cancel_preserves_raw_bracket_signature_and_sends_once():
    sdk, transport, _ = client([rejection(), TransportResponse(200, {})])
    with pytest.raises(PredictionAPIError) as caught:
        sdk.batch_cancel_orders(wallet_address="wallet", wallet_id="id", order_ids=["a", "b"])
    assert type(caught.value) is PredictionAPIError
    assert len(transport.calls) == 1
    fields = signed_fields(transport.calls[0])
    assert fields["timestamp"] == ["106000"]
    assert fields["cancelInfoList[1].orderId"] == ["b"]
    assert b"cancelInfoList[0].orderId=" in transport.calls[0][2]["body"]


@pytest.mark.parametrize("operation", ["place_order", "get_quote", "batch_redeem"])
def test_public_execution_methods_do_not_retry(operation):
    sdk, transport, _ = client([rejection(), TransportResponse(200, {})])
    params = {
        "place_order": dict(wallet_address="wallet", wallet_id="id", quote_id="q",
                            account_type="SPOT", order_type="LIMIT", time_in_force="GTC", slippage_bps=1),
        "get_quote": dict(wallet_address="wallet", token_id="t", side="BUY", amount_in="1",
                          order_type="LIMIT", slippage_bps=1),
        "batch_redeem": dict(wallet_address="wallet", wallet_id="id", token_ids=["a", "b"]),
    }
    with pytest.raises(PredictionAPIError) as caught:
        getattr(sdk, operation)(**params[operation])
    assert type(caught.value) is PredictionAPIError
    assert len(transport.calls) == 1
    fields = signed_fields(transport.calls[0])
    assert fields["timestamp"] == ["106000"]
    if operation == "batch_redeem":
        assert fields["tokenIds"] == ["a", "b"]


def test_read_retry_cannot_reuse_prepaid_bundle():
    sdk, transport, _ = client([rejection(), TransportResponse(200, {})])
    token = REQUEST_PREPAID.set(True)
    try:
        sdk._request(PREDICTION_PREFIX + "/market/list")
    finally:
        REQUEST_PREPAID.reset(token)
    assert sdk.request_budget.prepaid_checks == 1
    assert sdk.request_budget.reservations == [1]
    assert len(transport.calls) == 2


def test_retry_budget_exhaustion_and_transport_ambiguity_do_not_send_again():
    sdk, transport, _ = client([rejection(), TransportResponse(200, {})])
    sdk.request_budget.allowed = iter([True, False])
    with pytest.raises(SharedBudgetDeferred):
        sdk._request(PREDICTION_PREFIX + "/market/list")
    assert len(transport.calls) == 1
    sdk, transport, _ = client([PredictionTransportError("ambiguous"), TransportResponse(200, {})])
    with pytest.raises(PredictionTransportError):
        sdk._request(PREDICTION_PREFIX + "/market/list")
    assert len(transport.calls) == 1


def worker():
    instance = object.__new__(PredictionWorker)
    instance._accept_new_markets = True
    instance._active_campaigns = {}
    instance._cancel_requested = instance._hard_stop_latched = False
    instance._target_markets = 10
    instance._loop_id = "loop"
    instance.heartbeat = SimpleNamespace(markets_completed=0, last_loop_at_ms=0, last_error=None)
    instance.settings = SimpleNamespace(poll_interval_seconds=0)
    instance._now_ms = lambda: 100000
    instance._begin_shadow_tick = lambda: None
    instance._check_adaptive_jump_stop = AsyncMock()
    instance._check_loop_loss_guard = AsyncMock()
    instance._manage_active_campaigns = AsyncMock()
    instance._persist_heartbeat = AsyncMock()
    instance.repository = SimpleNamespace(
        get_loop=AsyncMock(return_value={"state": "RUNNING", "completed": 0}),
        record_risk_event=AsyncMock(), set_runtime_config=AsyncMock(),
        get_runtime_config=AsyncMock(return_value={"unknown_execution": True}),
        _execute=AsyncMock(), stop_loop=AsyncMock(),
    )
    return instance


@pytest.mark.asyncio
async def test_worker_defers_read_tick_rechecks_gates_and_preserves_hs_and_unknown(monkeypatch):
    instance = worker()
    error = PredictionReadTimestampError("timestamp", status_code=400, payload={"code": -1021},
                                         path=PREDICTION_PREFIX + "/market/list", attempts=2, timings=[])
    calls = []

    async def market():
        calls.append("read")
        if len(calls) == 1:
            raise error
        instance._accept_new_markets = False

    instance._run_market_once = market
    sleep = AsyncMock()
    monkeypatch.setattr("src.gridbot.prediction.worker.asyncio.sleep", sleep)
    await instance._run_loop()
    sleep.assert_awaited_once_with(5.0)
    assert calls == ["read", "read"]
    assert instance._check_loop_loss_guard.await_count == 4
    assert instance._manage_active_campaigns.await_count == 2
    event = instance.repository.record_risk_event.call_args
    assert event.args[:2] == ("READ_TIMESTAMP_DEFERRED", "WARN")
    assert event.kwargs["payload"]["read_only"] is True
    assert [call.args[0] for call in instance.repository.set_runtime_config.call_args_list] == [
        "prediction_read_timestamp_deferred"]
    instance.repository._execute.assert_not_awaited()
    instance.repository.get_runtime_config.assert_not_awaited()
    assert instance._hard_stop_latched is False


@pytest.mark.asyncio
async def test_untyped_write_timestamp_error_still_latches_hard_stop():
    instance = worker()
    instance._run_market_once = AsyncMock(side_effect=PredictionAPIError(
        "write timestamp", status_code=400, payload={"code": -1021}))
    await instance._run_loop()
    assert instance._hard_stop_latched is True
    states = {call.args[0]: call.args[1] for call in instance.repository.set_runtime_config.call_args_list}
    assert states["prediction_hard_stop_latched"]["latched"] is True
    assert states["prediction_risk_state"]["hard_stop_latched"] is True
    assert states["prediction_risk_state"]["unknown_execution"] is True
    instance.repository._execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_existing_hard_stop_never_admits_another_market():
    instance = worker()
    instance._hard_stop_latched = True
    instance._run_market_once = AsyncMock()
    await instance._run_loop()
    instance._run_market_once.assert_not_awaited()
    instance.repository.stop_loop.assert_awaited_once_with("loop", state="HARD_STOP")
    assert instance._hard_stop_latched is True
    instance.repository.set_runtime_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_history_timestamp_exhaustion_does_not_fallback_or_discard_intent():
    from src.gridbot.prediction.models import OrderSide
    instance = worker()
    instance.settings.wallet_address = "wallet"
    intent = SimpleNamespace(created_at_ms=100000, order_side=OrderSide.BUY, status="UNKNOWN")
    error = PredictionReadTimestampError("timestamp", status_code=400, payload={"code": -1021},
                                         path=PREDICTION_PREFIX + "/order/history", attempts=2, timings=[])
    instance._call_api = AsyncMock(side_effect=error)
    with pytest.raises(PredictionReadTimestampError):
        await instance._history_rows(intent)
    instance._call_api.assert_awaited_once()
    assert intent.status == "UNKNOWN"
