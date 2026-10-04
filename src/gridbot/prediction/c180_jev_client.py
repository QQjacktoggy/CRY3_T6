"""Original JEV probability client for the frozen C180 favorite policy.

The caller provides a T+120 state already built with the frozen shadow
``Tape.packet`` and ``model_state(packet, 'Original')`` contract. This module
does not collect market data, read credentials, place orders, or persist state.
It never includes the injected API key or provider response body in results.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Literal, Mapping

import aiohttp
from .http_bounds import JEV_BODY_BYTES, read_bounded_async

from src.gridbot.prediction.c180_favorite import MODEL_ID, parse_original_jev_p_up


ENDPOINT = "https://openrouter.ai/api/alpha/decisions"
MAX_INPUT_BYTES = 30_000
JEV_DEADLINE_MS = 3_000
MARKET_WINDOW_MS = 300_000
ENTRY_OFFSET_MS = 120_000

# Byte-for-byte instruction/criteria text from the immutable C180 shadow's
# original_logic.QUESTIONS. Original uses only this question; A/B/Meta are not
# part of the C180_FAVORITE_HOLD signal.
ORIGINAL_QUESTIONS: dict[str, dict[str, Any]] = {
    "direction": {
        "type": "choice",
        "instructions": (
            "Estimate the FINAL official BTC five-minute settlement direction relative to the contract reference. "
            "Use only supplied evidence received by observed_at; lookback windows may include pre-open trades. "
            "Spot and futures are proxies, not the official Chainlink settlement. Quotes are market evidence. "
            "Return UP/DOWN probabilities conditional on non-tie settlement. Express uncertainty near 0.5. "
            "Never invent missing information; do not recommend SKIP or make an exit decision."
        ),
        "criteria": {
            "UP": "Official end price strictly above starting reference",
            "DOWN": "Official end price strictly below starting reference",
        },
    },
}

JEVStatus = Literal[
    "ok", "invalid_state", "missing_key", "deadline_expired", "http_error",
    "invalid_response", "transport_error", "late",
]


@dataclass(frozen=True)
class OriginalJEVResult:
    status: JEVStatus
    p_up: Decimal | None
    completed_at_ms: int | None
    input_sha256: str | None
    provider_cost_usdt: Decimal | None = None
    http_status: int | None = None


def questions_for_symbol(symbol):
    from .loop_market import symbol as valid_symbol
    asset = valid_symbol(symbol).removesuffix("USDT")
    questions = json.loads(json.dumps(ORIGINAL_QUESTIONS))
    questions["direction"]["instructions"] = questions["direction"]["instructions"].replace("BTC five-minute", asset+" five-minute")
    return questions


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _dumps(value: Any) -> str:
    # Match the frozen shadow's canonical serialization and input-size check.
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _frozen_state(state: Mapping[str, Any], *, symbol="BTCUSDT") -> tuple[dict[str, Any], int, str]:
    """Copy and check the state contract without changing model evidence."""

    copied = json.loads(_dumps(state))
    if not isinstance(copied, dict):
        raise ValueError("state")
    contract = copied.get("contract")
    if not isinstance(contract, dict):
        raise ValueError("contract")
    if symbol != "BTCUSDT" and contract.get("symbol") != symbol:
        raise ValueError("Original contract asset mismatch")
    start = contract.get("start")
    end = contract.get("end")
    observed = copied.get("observed_at")
    if (type(start) is not int or type(end) is not int or type(observed) is not int
            or end != start + MARKET_WINDOW_MS
            or observed != start + ENTRY_OFFSET_MS):
        raise ValueError("cutoff")
    if not {"contract", "observed_at", "remaining_seconds", "features", "quote",
            "stake_usdt", "fee_bps_cash_sensitivity", "data_warnings",
            "basis_bps"} <= set(copied):
        raise ValueError("missing Original field")
    try:
        reference = Decimal(str(contract.get("reference")))
    except (ValueError, TypeError, InvalidOperation):
        raise ValueError("reference") from None
    if (not contract.get("topic") or not reference.is_finite() or reference <= 0
            or type(copied.get("remaining_seconds")) not in (int, float)
            or copied["remaining_seconds"] != 180
            or type(copied.get("stake_usdt")) not in (int, float)
            or copied["stake_usdt"] != 1):
        raise ValueError("frozen fields")
    if any(name in copied for name in ("holdings", "holding", "evidence_v2")):
        raise ValueError("non-Original fields")
    features, quote = copied.get("features"), copied.get("quote")
    if not isinstance(features, dict) or not {"spot", "futures"} <= set(features):
        raise ValueError("features")
    if quote is not None:
        if not isinstance(quote, dict) or not {"UP", "DOWN"} <= set(quote):
            raise ValueError("quote")
        for side in ("UP", "DOWN"):
            side_quote = quote[side]
            if not isinstance(side_quote, dict):
                raise ValueError("quote side")
            for key in ("ask_levels", "bid_levels"):
                levels = side_quote.get(key)
                if not isinstance(levels, list) or len(levels) > 3:
                    raise ValueError("quote top depth")
    serialized = _dumps(copied)
    if len(serialized.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("input too large")
    envelope = {"model": MODEL_ID, "questions": questions_for_symbol(symbol),
                "state": copied}
    input_sha256 = hashlib.sha256(_dumps(envelope).encode("utf-8")).hexdigest()
    return copied, observed, input_sha256


def _reported_cost(usage: Any) -> Decimal | None:
    if not isinstance(usage, Mapping):
        return None
    raw = usage.get("cost")
    if isinstance(raw, bool) or raw is None:
        return None
    try:
        cost = Decimal(str(raw))
    except (ValueError, TypeError, InvalidOperation):
        return None
    return cost if cost.is_finite() and cost >= 0 else None


async def infer_original_jev_p_up(
    session: aiohttp.ClientSession,
    *,
    api_key: str,
    frozen_state: Mapping[str, Any],
    now_ms: Callable[[], int] = _now_ms,
    user_label: str = "cry3-c180-favorite-hold-live",
    symbol: str = "BTCUSDT",
) -> OriginalJEVResult:
    """Call Original JEV once and return only a validated probability or state.

    The hard deadline is the market's T+123s, including response parsing.
    Cancellation propagates to the caller for normal service shutdown.
    """

    try:
        state, cutoff_ms, input_sha256 = _frozen_state(frozen_state, **({"symbol": symbol} if symbol != "BTCUSDT" else {}))
    except (ValueError, TypeError, OverflowError):
        return OriginalJEVResult("invalid_state", None, None, None)
    if not api_key:
        return OriginalJEVResult("missing_key", None, None, input_sha256)
    deadline_ms = cutoff_ms + JEV_DEADLINE_MS
    remaining_ms = deadline_ms - now_ms()
    if remaining_ms <= 0:
        return OriginalJEVResult("deadline_expired", None, None, input_sha256)
    payload = {"model": MODEL_ID, "questions": questions_for_symbol(symbol),
               "state": state, "user": user_label}
    try:
        async with asyncio.timeout(remaining_ms / 1000):
            async with session.post(
                ENDPOINT,
                headers={"Authorization": "Bearer " + api_key,
                         "X-OpenRouter-Title": "cry3 C180 favorite hold live"},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=remaining_ms / 1000),
                auto_decompress=False,
            ) as response:
                if response.status != 200:
                    return OriginalJEVResult("http_error", None, now_ms(),
                                             input_sha256, http_status=response.status)
                body = json.loads(await read_bounded_async(response.content, response.headers, JEV_BODY_BYTES))
        completed = now_ms()
    except TimeoutError:
        return OriginalJEVResult("deadline_expired", None, now_ms(), input_sha256)
    except (aiohttp.ClientError, ValueError, TypeError):
        return OriginalJEVResult("transport_error", None, now_ms(), input_sha256)
    if completed > deadline_ms:
        return OriginalJEVResult("late", None, completed, input_sha256)
    if not isinstance(body, Mapping):
        return OriginalJEVResult("invalid_response", None, completed, input_sha256)
    cost = _reported_cost(body.get("usage"))
    try:
        p_up = parse_original_jev_p_up(body)
    except (ValueError, TypeError, KeyError):
        return OriginalJEVResult("invalid_response", None, completed,
                                 input_sha256, provider_cost_usdt=cost)
    return OriginalJEVResult("ok", p_up, completed, input_sha256,
                             provider_cost_usdt=cost)
