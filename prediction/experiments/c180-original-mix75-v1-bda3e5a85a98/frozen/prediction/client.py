"""Small, dependency-light REST client for Binance Web3 Prediction markets.

The existing project has a Futures client whose request semantics are not
compatible with the Web3 Prediction SAPI.  This module intentionally keeps a
separate client and accepts the API key/secret as constructor arguments; it
never reads ``.env`` and never logs credentials.

The client is synchronous by design.  Prediction orchestration can run these
calls in its existing worker thread (or with ``asyncio.to_thread``), while
tests can inject a deterministic transport without opening a socket.

Binance's Prediction endpoints use the ``/sapi/v1/w3w/wallet/prediction``
namespace.  Signed requests are HMAC-SHA256 over the exact form-encoded
parameter string.  ``batch_cancel_orders`` uses a low-level/raw form body so
the indexed ``cancelInfoList[0].orderId`` keys are not rewritten to
``%5B``/``%5D`` before signature verification.
"""

from __future__ import annotations

import hashlib
import os
from .rate_limit import SharedRequestBudget, SharedBudgetDeferred, REQUEST_PRIORITY, REQUEST_PREPAID
import hmac
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Protocol, Sequence


PREDICTION_PREFIX = "/sapi/v1/w3w/wallet/prediction"
DEFAULT_BASE_URL = "https://api.binance.com"
USDT_WEI = Decimal("1000000000000000000")
# Prediction execution defaults to one USDT. The reviewed runtime units
# are 1, 2 and 3 USDT; each client instance enforces its selected unit.
ORDER_UNIT_USDT = Decimal("1")
ALLOWED_ORDER_UNITS_USDT = frozenset({Decimal("1"), Decimal("2"), Decimal("3")})


class PredictionClientError(RuntimeError):
    """Base class for client and transport failures."""


class PredictionTransportError(PredictionClientError):
    """Raised when the HTTP transport cannot obtain a response."""


class PredictionAPIError(PredictionClientError):
    """Raised for non-2xx responses or Binance error envelopes."""

    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


@dataclass(frozen=True)
class TransportResponse:
    """Minimal response shape required by :class:`BinancePredictionClient`."""

    status_code: int
    body: bytes | str | Mapping[str, Any] | Sequence[Any] | None = None
    headers: Mapping[str, str] | None = None

    def json(self) -> Any:
        if isinstance(self.body, (Mapping, list, tuple)):
            return self.body
        if self.body is None or self.body == b"":
            return None
        text = self.body.decode("utf-8") if isinstance(self.body, bytes) else self.body
        try:
            return json.loads(text)
        except (TypeError, ValueError) as exc:
            raise PredictionTransportError("Binance returned a non-JSON response") from exc


class PredictionTransport(Protocol):
    """Transport seam used by tests and by production HTTP implementations."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None = None,
        timeout: float = 10.0,
    ) -> TransportResponse:
        ...


class UrllibTransport:
    """stdlib transport; no third-party HTTP package is required."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None = None,
        timeout: float = 10.0,
    ) -> TransportResponse:
        request = urllib.request.Request(url, data=body, headers=dict(headers), method=method.upper())
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return TransportResponse(
                    status_code=int(response.status),
                    body=response.read(),
                    headers=dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            # Preserve the Binance JSON error envelope for useful diagnostics.
            return TransportResponse(
                status_code=int(exc.code),
                body=exc.read(),
                headers=dict(exc.headers.items()) if exc.headers else {},
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise PredictionTransportError(f"Prediction HTTP request failed: {exc}") from exc


class RequestsTransport:
    """Optional requests-backed transport.

    Importing ``requests`` is delayed until construction so importing this
    client never adds a hard dependency to the project.
    """

    def __init__(self, session: Any = None) -> None:
        if session is None:
            try:
                import requests
            except ImportError as exc:  # pragma: no cover - environment-specific
                raise PredictionTransportError("requests is not installed") from exc
            session = requests.Session()
        self.session = session

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None = None,
        timeout: float = 10.0,
    ) -> TransportResponse:
        try:
            response = self.session.request(method.upper(), url, headers=dict(headers), data=body, timeout=timeout)
        except Exception as exc:  # requests exposes several transport exception classes
            raise PredictionTransportError(f"Prediction HTTP request failed: {exc}") from exc
        return TransportResponse(
            status_code=int(response.status_code),
            body=response.content,
            headers=dict(response.headers),
        )


def _stringify(value: Any) -> str:
    """Format a scalar in the form Binance expects without float surprises."""

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        raise TypeError("list parameters must be flattened before encoding")
    return str(value)


def _form_encode(pairs: Iterable[tuple[str, Any]], *, raw_brackets: bool = False) -> str:
    """Encode ordered pairs using RFC3986-compatible query encoding.

    ``raw_brackets`` is only used for the batch-cancel endpoint.  It keeps
    square brackets in *keys* while still encoding values and other key
    characters.  This is the exact workaround documented by Binance for the
    SAPI bracket-signature incompatibility.
    """

    encoded: list[str] = []
    for key, value in pairs:
        key_safe = "-_.~[]" if raw_brackets else "-_.~"
        encoded.append(
            f"{urllib.parse.quote(_stringify(key), safe=key_safe)}="
            f"{urllib.parse.quote(_stringify(value), safe='-_.~')}"
        )
    return "&".join(encoded)


def hmac_sha256_signature(secret: str, canonical_payload: str) -> str:
    """Return the lowercase HMAC-SHA256 SAPI signature."""

    if not secret:
        raise ValueError("api_secret is required for a signed Prediction request")
    return hmac.new(secret.encode("utf-8"), canonical_payload.encode("utf-8"), hashlib.sha256).hexdigest()


def usdt_to_wei(amount: Decimal | str | int) -> str:
    """Encode a USDT display amount as the integer ``amountIn`` field."""

    try:
        value = Decimal(str(amount))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid USDT amount: {amount!r}") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError("USDT amount must be finite and positive")
    wei = value * USDT_WEI
    if wei != wei.to_integral_value():
        raise ValueError("USDT amount has more than 18 decimal places")
    return str(int(wei))


def normalize_amount_in(value: Decimal | str | int) -> str:
    """Accept display USDT while preserving an already-encoded wei integer."""

    text = str(value).strip()
    if text.isdigit() and len(text) >= 16:
        return text
    return usdt_to_wei(value)


def available_balance_display(payload: Any, *, account_type: str = "SPOT") -> Decimal | None:
    """Extract one exact enabled account from the official ``items[]`` list.

    Binance returns multiple payment accounts from this endpoint.  A missing
    requested account is *not* equivalent to a different account being
    available. Never fall back to the first item (or to a single mapping
    response): the payment source must be selected explicitly.
    """

    data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
    items = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(items, list):
        return None
    wanted = str(account_type).strip().upper()
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("accountType", "")).strip().upper() != wanted:
            continue
        # The official boolean is required.  Missing/false is not proof that
        # this account can be used for a live order.
        if item.get("enabled") is not True:
            continue
        value = item.get("availableBalanceDisplay")
        if value is None:
            continue
        try:
            result = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError("invalid availableBalanceDisplay") from exc
        if not result.is_finite() or result < 0:
            raise ValueError("availableBalanceDisplay must be finite and non-negative")
        return result
    return None


def payment_option_balance_summary(payload: Any) -> list[dict[str, Any]]:
    """Return safe display fields for every official payment option."""

    data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
    items = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(items, list):
        return []
    result: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        value = item.get("availableBalanceDisplay")
        result.append(
            {
                "account_type": str(item.get("accountType") or "UNKNOWN"),
                "enabled": item.get("enabled") is True,
                "available_balance_display": str(value) if value is not None else None,
            }
        )
    return result


def _official_wallets(payload: Any) -> list[Mapping[str, Any]] | None:
    """Return only the documented top-level/data ``wallets[]`` collection."""

    data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
    wallets = data.get("wallets") if isinstance(data, Mapping) else None
    if wallets is None and isinstance(payload, Mapping):
        wallets = payload.get("wallets")
    if not isinstance(wallets, list):
        return None
    return [item for item in wallets if isinstance(item, Mapping)]


def _remaining_daily_limit(payload: Any) -> Decimal:
    data = payload.get("data", payload) if isinstance(payload, Mapping) else None
    if not isinstance(data, Mapping) or "remainingDailyLimit" not in data:
        raise ValueError("official quota response is missing remainingDailyLimit")
    try:
        value = Decimal(str(data["remainingDailyLimit"]))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("official remainingDailyLimit is invalid") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError("official remainingDailyLimit is exhausted")
    return value


def _normalize_order_unit_usdt(value: Decimal | str | int) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Prediction order unit must be exactly 1, 2 or 3 USDT") from exc
    if not amount.is_finite() or amount not in ALLOWED_ORDER_UNITS_USDT:
        raise ValueError("Prediction order unit must be exactly 1, 2 or 3 USDT")
    return amount


def _require_prediction_order_unit(
    amount_in: Decimal | str | int,
    *,
    side: str = "BUY",
    max_buy_usdt: Decimal | str | int = ORDER_UNIT_USDT,
) -> str:
    """Normalize an order amount with a side-aware canary guard.

    Binance's ``amountIn`` is a wei-scaled input amount.  BUY is a USDT
    notional and is capped at the selected 1/2/3-USDT unit. SELL is a token-share
    quantity, so applying a USDT cap to it would either reject a valid
    reduction or silently confuse shares with cash.  The worker separately
    bounds SELL to the position it is reducing.
    """

    normalized = normalize_amount_in(amount_in)
    try:
        amount = Decimal(normalized) / USDT_WEI
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - defensive
        raise ValueError("invalid normalized Prediction order amount") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("Prediction order amount must be positive")
    selected_limit = _normalize_order_unit_usdt(max_buy_usdt)
    if str(side or "BUY").strip().upper() == "BUY" and amount > selected_limit:
        raise ValueError(f"Prediction BUY amount must be no greater than {selected_limit} USDT")
    return normalized


def _pairs(values: Mapping[str, Any] | None) -> list[tuple[str, Any]]:
    if not values:
        return []
    return [(str(key), value) for key, value in values.items() if value is not None]


class BinancePredictionClient:
    """REST client for Binance Web3 Wallet Prediction Trading.

    Parameters are deliberately explicit instead of loading the repository's
    large Futures ``Settings`` object.  The caller passes the dedicated
    Prediction credentials explicitly; this client never reads credential environment variables and cannot
    fall back to legacy ``BINANCE_*`` credentials. A non-secret shared budget
    path may be explicitly configured through the service environment.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        transport: PredictionTransport | None = None,
        timeout: float = 10.0,
        recv_window: int | None = 5_000,
        clock_ms: Callable[[], int] | None = None,
        sign_market_data: bool = True,
        order_unit_usdt: Decimal | str | int = ORDER_UNIT_USDT,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.transport = transport or UrllibTransport()
        self.timeout = float(timeout)
        self.recv_window = recv_window
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.sign_market_data = bool(sign_market_data)
        self.order_unit_usdt = _normalize_order_unit_usdt(order_unit_usdt)
        # This opt-in path contains no credentials. Both VM services use it.
        budget_path = os.environ.get("PREDICTION_SHARED_WEIGHT_DB", "")
        self.request_budget = SharedRequestBudget(budget_path) if budget_path else None
        self.last_response_budget_headers = {}

    def set_order_unit_usdt(self, value: Decimal | str | int) -> Decimal:
        """Update the HTTP BUY ceiling to one reviewed runtime unit."""

        selected = _normalize_order_unit_usdt(value)
        self.order_unit_usdt = selected
        return selected

    def _request(
        self,
        path: str,
        method: str = "GET",
        *,
        params: Mapping[str, Any] | None = None,
        signed: bool = True,
        raw_brackets: bool = False,
    ) -> Any:
        pairs = _pairs(params)
        if signed:
            pairs.append(("timestamp", int(self.clock_ms())))
            if self.recv_window is not None and not any(key == "recvWindow" for key, _ in pairs):
                pairs.append(("recvWindow", self.recv_window))
            signing_payload = _form_encode(pairs, raw_brackets=raw_brackets)
            pairs.append(("signature", hmac_sha256_signature(self.api_secret, signing_payload)))
            canonical = _form_encode(pairs, raw_brackets=raw_brackets)
        else:
            canonical = _form_encode(pairs, raw_brackets=raw_brackets)

        url = f"{self.base_url}{path}"
        upper_method = method.upper()
        body: bytes | None = None
        if upper_method == "GET":
            if canonical:
                url = f"{url}?{canonical}"
        else:
            body = canonical.encode("utf-8")

        headers: MutableMapping[str, str] = {
            "Accept": "application/json",
            "User-Agent": "cry3-prediction/1.0",
        }
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        if body is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        self._before_http()
        response = self.transport.request(upper_method, url, headers=headers, body=body, timeout=self.timeout)
        self._after_http(response)
        try:
            payload = response.json()
        except PredictionTransportError:
            payload = response.body
        if not 200 <= int(response.status_code) < 300:
            raise PredictionAPIError(
                self._error_message(payload, response.status_code),
                status_code=int(response.status_code),
                payload=payload,
            )
        if isinstance(payload, Mapping) and ("code" in payload and payload.get("code") not in (0, "0", None)):
            raise PredictionAPIError(
                self._error_message(payload, int(response.status_code)),
                status_code=int(response.status_code),
                payload=payload,
            )
        return payload

    def _before_http(self):
        # Prediction catalog endpoints audited 2026-09-15 use IP weight 1.
        # Worker execution paths reserve the complete HTTP bundle beforehand.
        if self.request_budget:
            allowed = (self.request_budget.can_send_prepaid() if REQUEST_PREPAID.get()
                       else self.request_budget.acquire(1, priority=REQUEST_PRIORITY.get()))
            if not allowed:
                raise SharedBudgetDeferred(self.request_budget.health())

    def _after_http(self, response):
        self.last_response_budget_headers = {
            str(k).lower(): str(v) for k, v in (response.headers or {}).items()
            if str(k).lower() == "retry-after" or str(k).lower().startswith(
                ("x-mbx-used-weight", "x-sapi-used-ip-weight", "x-sapi-used-uid-weight"))
        }
        if self.request_budget:
            self.request_budget.note_response(response.status_code, response.headers)

    @staticmethod
    def _error_message(payload: Any, status_code: int | None) -> str:
        if isinstance(payload, Mapping):
            code = payload.get("code")
            msg = payload.get("msg") or payload.get("message")
            if code is not None or msg:
                return f"Binance Prediction API error ({status_code}): {code}: {msg}"
        return f"Binance Prediction API HTTP error ({status_code})"

    @staticmethod
    def _with_wallet(params: Mapping[str, Any] | None, wallet_address: str | None) -> dict[str, Any]:
        result = dict(params or {})
        if wallet_address is not None:
            result["walletAddress"] = wallet_address
        return result

    # ---- Market data -------------------------------------------------

    def list_prediction_markets(
        self,
        *,
        l1_category: str | None = None,
        l2_category: str | None = None,
        sort_by: str | None = None,
        order_by: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/market/list",
            params={
                "l1Category": l1_category,
                "l2Category": l2_category,
                "sortBy": sort_by,
                "orderBy": order_by,
                "offset": offset,
                "limit": limit,
            },
            signed=self.sign_market_data,
        )

    # Friendly alias used by the domain runtime.
    list_markets = list_prediction_markets
    market_list = list_prediction_markets

    def get_market_detail(self, market_topic_id: str | int) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/market/detail",
            params={"marketTopicId": market_topic_id},
            signed=self.sign_market_data,
        )

    market_detail = get_market_detail

    def query_order_book(self, market_id: str | int, token_id: str, *, vendor: str = "predict_fun") -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/order-book",
            params={"vendor": vendor, "marketId": market_id, "tokenId": token_id},
            signed=self.sign_market_data,
        )

    get_order_book = query_order_book
    order_book = query_order_book

    # ---- Trade -------------------------------------------------------

    def get_quote(
        self,
        *,
        wallet_address: str,
        token_id: str,
        side: str,
        amount_in: str | int,
        order_type: str,
        slippage_bps: int,
        chain_id: str | int = "56",
        fee_rate_bps: int | None = None,
        funding_source: str | None = None,
        fund_transfer_amount: str | int | None = None,
        price_limit: str | None = None,
    ) -> Any:
        if str(order_type).upper() != "LIMIT":
            raise ValueError("Prediction orders are restricted to LIMIT")
        normalized_side = str(side or "").strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("Prediction order side must be BUY or SELL")
        amount_in = _require_prediction_order_unit(
            amount_in,
            side=normalized_side,
            max_buy_usdt=self.order_unit_usdt,
        )
        return self._request(
            f"{PREDICTION_PREFIX}/trade/get-quote",
            "POST",
            params={
                "walletAddress": wallet_address,
                "tokenId": token_id,
                "side": side,
                "amountIn": amount_in,
                "orderType": order_type,
                "slippageBps": slippage_bps,
                "chainId": chain_id,
                "feeRateBps": fee_rate_bps,
                "fundingSource": funding_source,
                "fundTransferAmount": fund_transfer_amount,
                "priceLimit": price_limit,
            },
        )

    def place_order(
        self,
        *,
        wallet_address: str,
        wallet_id: str,
        quote_id: str,
        account_type: str,
        order_type: str,
        time_in_force: str,
        slippage_bps: int,
        price_limit: str | None = None,
        funding_source: str | None = None,
        fund_transfer_amount: str | int | None = None,
    ) -> Any:
        if str(account_type).upper() not in {"SPOT", "FUNDING"}:
            raise ValueError("Prediction orders require a SPOT or FUNDING account")
        if str(order_type).upper() != "LIMIT":
            raise ValueError("Prediction orders are restricted to LIMIT")
        if str(time_in_force).upper() != "GTC":
            raise ValueError("Prediction orders are restricted to GTC")
        return self._request(
            f"{PREDICTION_PREFIX}/trade/place-order-bundle",
            "POST",
            params={
                "walletAddress": wallet_address,
                "walletId": wallet_id,
                "quoteId": quote_id,
                "accountType": account_type,
                "orderType": order_type,
                "timeInForce": time_in_force,
                "slippageBps": slippage_bps,
                "priceLimit": price_limit,
                "fundingSource": funding_source,
                "fundTransferAmount": fund_transfer_amount,
            },
        )

    quote = get_quote
    place_prediction_order = place_order

    def query_active_orders(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/order/list",
            params=self._with_wallet(filters, wallet_address),
        )

    active_orders = query_active_orders
    active = query_active_orders

    def query_order_history(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/order/history",
            params=self._with_wallet(filters, wallet_address),
        )

    order_history = query_order_history
    history = query_order_history

    def batch_cancel_orders(
        self,
        *,
        wallet_address: str,
        wallet_id: str,
        order_ids: Sequence[str] | None = None,
        cancel_info_list: Sequence[Mapping[str, Any]] | None = None,
    ) -> Any:
        """Cancel active orders using Binance's raw indexed-bracket encoding.

        ``order_ids`` is a convenience form.  ``cancel_info_list`` permits
        callers to pass any additional documented fields.  Each item is
        encoded as ``cancelInfoList[i].<field>``; the list itself is never
        JSON-encoded into one form value.
        """

        if cancel_info_list is None:
            if not order_ids:
                raise ValueError("order_ids or cancel_info_list is required")
            cancel_info_list = [{"orderId": order_id} for order_id in order_ids]
        if not cancel_info_list:
            raise ValueError("cancel_info_list cannot be empty")
        params: list[tuple[str, Any]] = [
            ("walletAddress", wallet_address),
            ("walletId", wallet_id),
        ]
        for index, item in enumerate(cancel_info_list):
            if not isinstance(item, Mapping):
                raise TypeError("cancel_info_list items must be mappings")
            for field, value in item.items():
                if value is not None:
                    params.append((f"cancelInfoList[{index}].{field}", value))
        return self._request_pairs(
            f"{PREDICTION_PREFIX}/trade/batch-cancel",
            "POST",
            params,
            raw_brackets=True,
        )

    def _request_pairs(
        self,
        path: str,
        method: str,
        params: Sequence[tuple[str, Any]],
        *,
        signed: bool = True,
        raw_brackets: bool = False,
    ) -> Any:
        # `_request` accepts mappings for ordinary calls.  Keeping this small
        # pair-specific path avoids losing the intentional nested-key order.
        ordered: list[tuple[str, Any]] = list(params)
        if signed:
            ordered.append(("timestamp", int(self.clock_ms())))
            if self.recv_window is not None and not any(key == "recvWindow" for key, _ in ordered):
                ordered.append(("recvWindow", self.recv_window))
            signing_payload = _form_encode(ordered, raw_brackets=raw_brackets)
            ordered.append(("signature", hmac_sha256_signature(self.api_secret, signing_payload)))
            canonical = _form_encode(ordered, raw_brackets=raw_brackets)
        else:
            canonical = _form_encode(ordered, raw_brackets=raw_brackets)
        headers: MutableMapping[str, str] = {
            "Accept": "application/json",
            "User-Agent": "cry3-prediction/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        self._before_http()
        response = self.transport.request(
            method.upper(),
            f"{self.base_url}{path}",
            headers=headers,
            body=canonical.encode("utf-8"),
            timeout=self.timeout,
        )
        self._after_http(response)
        try:
            payload = response.json()
        except PredictionTransportError:
            payload = response.body
        if not 200 <= int(response.status_code) < 300:
            raise PredictionAPIError(
                self._error_message(payload, response.status_code), status_code=int(response.status_code), payload=payload
            )
        if isinstance(payload, Mapping) and ("code" in payload and payload.get("code") not in (0, "0", None)):
            raise PredictionAPIError(
                self._error_message(payload, response.status_code), status_code=int(response.status_code), payload=payload
            )
        return payload

    # ---- Wallet, account and position --------------------------------

    def list_prediction_wallets(self, *, recv_window: int | None = None) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/wallet/list",
            params={"recvWindow": recv_window},
        )

    list_wallets = list_prediction_wallets
    wallets = list_prediction_wallets

    def get_quota_status(self, *, recv_window: int | None = None) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/quota/limit/status",
            params={"recvWindow": recv_window},
        )

    quota_status = get_quota_status

    def query_payment_option_balances(self, *, recv_window: int | None = None) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/balance/payment-options",
            params={"recvWindow": recv_window},
        )

    payment_option_balances = query_payment_option_balances
    balance = query_payment_option_balances

    def authoritative_preflight(
        self,
        *,
        wallet_address: str,
        wallet_id: str,
        required_balance_usdt: Decimal | str | int = Decimal("0"),
        account_type: str = "SPOT",
        funding_source: str | None = None,
        recv_window: int | None = None,
    ) -> dict[str, Any]:
        """Prove read-only live capability using official required responses.

        Binance's documented Wallet endpoints use signed
        ``PREDICTION_TRADE`` reads.  They prove wallet identity, quota, and
        payment-source balance, but they do not prove the SAS requirement on
        the mutating ``placeOrder`` endpoint; that separate operator gate is
        enforced by the controller.
        """

        reasons: list[str] = []
        details: dict[str, Any] = {
            "capability_source": "signed_PREDICTION_TRADE_read_only_wallet_quota_payment",
            "security_type": "PREDICTION_TRADE",
            "signed": True,
            "endpoints": [
                f"{PREDICTION_PREFIX}/wallet/list",
                f"{PREDICTION_PREFIX}/quota/limit/status",
                f"{PREDICTION_PREFIX}/balance/payment-options",
            ],
            "wallet_match": False,
        }
        normalized_account_type = str(account_type or "SPOT").strip().upper()
        # Keep direct callers from the original SPOT-only interface
        # compatible: an omitted funding source means the legacy SPOT balance
        # check. PredictionSettings always supplies MPC/CEX explicitly.
        normalized_funding_source = str(funding_source or "CEX").strip().upper()
        if normalized_account_type not in {"SPOT", "FUNDING"}:
            reasons.append("prediction payment account type must be SPOT or FUNDING")
        if normalized_funding_source not in {"MPC", "CEX"}:
            reasons.append("prediction funding source must be MPC or CEX")
        # With MPC funding the balance is drawn from the CeDeFi/Prediction
        # wallet. With CEX funding it is drawn from the selected SPOT/FUNDING
        # payment account. The order API still receives SPOT/FUNDING as its
        # accountType; CeDeFi is not a valid accountType value.
        balance_account_type = "CeDeFi" if normalized_funding_source == "MPC" else normalized_account_type
        details["account_type"] = normalized_account_type
        details["funding_source"] = normalized_funding_source
        details["balance_account_type"] = balance_account_type
        configured_address = str(wallet_address or "").strip().lower()
        configured_id = str(wallet_id or "").strip()
        wallets = _official_wallets(self.list_prediction_wallets(recv_window=recv_window))
        if wallets is None:
            reasons.append("official wallet response does not contain wallets[]")
        elif not any(
            str(item.get("walletAddress", item.get("address", ""))).strip().lower() == configured_address
            and str(item.get("walletId", item.get("id", ""))).strip() == configured_id
            for item in wallets
        ):
            reasons.append("configured wallet address/id is not an exact official wallet match")
        else:
            details["wallet_match"] = True

        try:
            remaining = _remaining_daily_limit(self.get_quota_status(recv_window=recv_window))
            details["remaining_daily_limit"] = str(remaining)
        except Exception as exc:  # noqa: BLE001 - report the official response failure
            reasons.append(str(exc))

        try:
            balance = available_balance_display(
                self.query_payment_option_balances(recv_window=recv_window), account_type=balance_account_type
            )
            if balance is None:
                reasons.append(f"enabled {balance_account_type} availableBalanceDisplay is missing; no account fallback is allowed")
            else:
                required = Decimal(str(required_balance_usdt))
                if balance < required:
                    reasons.append(f"enabled {balance_account_type} availableBalanceDisplay {balance} < required {required}")
                else:
                    details["available_balance_display"] = str(balance)
        except Exception as exc:  # noqa: BLE001 - report the official response failure
            reasons.append(str(exc))

        details["permission_verified"] = not reasons
        details["permission_capability"] = {
            "verified": not reasons,
            "security_type": "PREDICTION_TRADE",
            "signed": True,
            "account_type": normalized_account_type,
            "funding_source": normalized_funding_source,
            "endpoints": list(details["endpoints"]),
        }
        return {
            "checked": True,
            "passed": not reasons,
            "reasons": list(dict.fromkeys(reasons)),
            **details,
        }

    preflight_authoritative = authoritative_preflight

    def get_portfolio(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/pnl/portfolio",
            params=self._with_wallet(filters, wallet_address),
        )

    def get_position_by_token(self, *, wallet_address: str, token_id: str, recv_window: int | None = None) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/position/token",
            params={"walletAddress": wallet_address, "tokenId": token_id, "recvWindow": recv_window},
        )

    position_by_token = get_position_by_token

    def query_positions(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/position/list",
            params=self._with_wallet(filters, wallet_address),
        )

    positions = query_positions

    def query_positions_by_filter(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/position/filter",
            params=self._with_wallet(filters, wallet_address),
        )

    def query_pnl(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/pnl/query",
            params=self._with_wallet(filters, wallet_address),
        )

    pnl = query_pnl

    def query_settled_position_history(self, *, wallet_address: str | None = None, **filters: Any) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/position/settled-history",
            params=self._with_wallet(filters, wallet_address),
        )

    settled_position_history = query_settled_position_history
    settled = query_settled_position_history

    # ---- Settlement / redeem ----------------------------------------

    def batch_redeem(
        self,
        *,
        wallet_address: str,
        wallet_id: str,
        token_ids: Sequence[str],
        chain_id: str | int = "56",
    ) -> Any:
        if not token_ids:
            raise ValueError("token_ids cannot be empty")
        params: list[tuple[str, Any]] = [
            ("walletAddress", wallet_address),
            ("walletId", wallet_id),
            *(('tokenIds', token_id) for token_id in token_ids),
            ("chainId", chain_id),
        ]
        return self._request_pairs(f"{PREDICTION_PREFIX}/batch-redeem", "POST", params)

    redeem = batch_redeem

    def get_redeem_status(self, *, wallet_address: str, tx_hash: str, recv_window: int | None = None) -> Any:
        return self._request(
            f"{PREDICTION_PREFIX}/redeem/status",
            params={"walletAddress": wallet_address, "txHash": tx_hash, "recvWindow": recv_window},
        )

    redeem_status = get_redeem_status


__all__ = [
    "BinancePredictionClient",
    "DEFAULT_BASE_URL",
    "PREDICTION_PREFIX",
    "USDT_WEI",
    "ORDER_UNIT_USDT",
    "PredictionAPIError",
    "PredictionClientError",
    "PredictionTransport",
    "PredictionTransportError",
    "RequestsTransport",
    "TransportResponse",
    "UrllibTransport",
    "available_balance_display",
    "payment_option_balance_summary",
    "_official_wallets",
    "_remaining_daily_limit",
    "hmac_sha256_signature",
    "normalize_amount_in",
    "usdt_to_wei",
]
