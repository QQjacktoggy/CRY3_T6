"""Pure C180_FAVORITE_HOLD entry decision from the frozen paper candidate.

The caller freezes the BTC five-minute market evidence at T+120 seconds and
supplies the validated Original JEV probability and normalized order-book
depth. Upstream normalization must check market identity, all three source
timestamps, valid prices/sizes, uncrossed book, and UP/DOWN orientation as in
the frozen shadow ``normalize_book``. This module makes no model, exchange,
storage, or clock calls. A caller
must separately execute and reconcile an order, then hold filled shares until
official settlement. The paper candidate's simulated fill is not a live fill.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal, Mapping


MODEL_ID = "typesafe/jev-1.13-20260917"
ENTRY_OFFSET_MS = 120_000
EXECUTION_READY_OFFSET_MS = 124_000
EXECUTION_EXPIRES_OFFSET_MS = 136_000
MARKET_WINDOW_MS = 300_000
MAX_BOOK_AGE_MS = 2_000
MAX_MODEL_LATENCY_MS = 3_000
MAX_DECISION_LATENCY_MS = 4_000
ENTRY_BUFFER_PER_USDT = Decimal("0.005")
V11_MAX_BUY_PRICE = Decimal("0.90")
_CASH_TOLERANCE = Decimal("0.000000001")

Side = Literal["UP", "DOWN"]
Action = Literal["UP", "DOWN", "SKIP", "UNKNOWN"]


@dataclass(frozen=True)
class PriceLevel:
    price: Decimal
    shares: Decimal


@dataclass(frozen=True)
class C180EntryInput:
    market_start_ms: int
    cutoff_ms: int
    decision_at_ms: int
    market_id: str | None
    book_market_id: str | None
    market_identified_at_ms: int | None
    reference_price: Decimal | None
    fee_bps: Decimal | None
    original_model_id: str | None
    original_p_up: Decimal | None
    model_completed_at_ms: int | None
    book_at_ms: int | None
    book_received_at_ms: int | None
    book_received_at_ms_secondary: int | None
    up_ask_levels: tuple[PriceLevel, ...]
    down_ask_levels: tuple[PriceLevel, ...]
    stake_usdt: Decimal = Decimal("1")
    recovery_gap: bool = False
    missed_cutoff: bool = False


@dataclass(frozen=True)
class C180EntryDecision:
    action: Action
    reason: str
    side: Side | None
    stake_usdt: Decimal
    expected_shares: Decimal | None = None
    cost_after_ev_usdt: Decimal | None = None


@dataclass(frozen=True)
class C180ExecutionInput:
    """Fresh, full-depth observation for the previously chosen C180 entry."""

    entry_decision: C180EntryDecision
    market_start_ms: int
    at_ms: int
    frozen_market_topic: str | None
    current_market_topic: str | None
    frozen_market_id: str | None
    current_market_id: str | None
    book_market_id: str | None
    frozen_fee_bps: Decimal | None
    current_fee_bps: Decimal | None
    original_p_up: Decimal | None
    book_at_ms: int | None
    book_received_at_ms: int | None
    book_received_at_ms_secondary: int | None
    last_seen_book_at_ms: int | None
    selected_ask_levels: tuple[PriceLevel, ...] | None
    full_depth: bool


@dataclass(frozen=True)
class C180ExecutionRecheck:
    permitted: bool
    reason: str
    ready_at_ms: int
    expires_at_ms: int
    worst_ask_limit: Decimal | None = None
    expected_cash_usdt: Decimal | None = None
    expected_shares: Decimal | None = None
    cost_after_ev_usdt: Decimal | None = None


def _valid_levels(levels: tuple[PriceLevel, ...]) -> bool:
    return all(
        level.price.is_finite()
        and Decimal("0") < level.price < Decimal("1")
        and level.shares.is_finite()
        and level.shares > 0
        for level in levels
    )


def _walk_ask(levels: tuple[PriceLevel, ...], stake: Decimal) -> tuple[Decimal, Decimal]:
    cash = Decimal("0")
    shares = Decimal("0")
    for level in levels:
        take = min(level.shares, (stake - cash) / level.price)
        cash += take * level.price
        shares += take
        if stake - cash < Decimal("0.0000000001"):
            break
    return cash, shares


def _walk_ask_with_limit(
    levels: tuple[PriceLevel, ...], stake: Decimal,
) -> tuple[Decimal, Decimal, Decimal | None]:
    cash = shares = Decimal("0")
    worst_ask = None
    for level in levels:
        take = min(level.shares, (stake - cash) / level.price)
        if take <= 0:
            break
        cash += take * level.price
        shares += take
        worst_ask = level.price
        if stake - cash < Decimal("0.0000000001"):
            break
    return cash, shares, worst_ask


def parse_original_jev_p_up(response: Mapping[str, Any]) -> Decimal:
    """Validate the frozen Original ``direction`` answer and return P(UP).

    This mirrors ``logic.parse_response`` for the one-question Original lane.
    The caller checks request status and completion time separately.
    """

    if response.get("model") != MODEL_ID:
        raise ValueError("model")
    answers = response.get("answers")
    if not isinstance(answers, Mapping):
        raise ValueError("answers")
    direction = answers.get("direction")
    if not isinstance(direction, Mapping) or direction.get("type") != "choice":
        raise ValueError("direction")
    probabilities = direction.get("probabilities")
    if not isinstance(probabilities, Mapping) or set(probabilities) != {"UP", "DOWN"}:
        raise ValueError("probabilities")
    if direction.get("choice") not in ("UP", "DOWN"):
        raise ValueError("choice")
    raw_up, raw_down = probabilities["UP"], probabilities["DOWN"]
    raw_confidence = direction.get("confidence")
    if any(type(value) not in (int, float) for value in
           (raw_up, raw_down, raw_confidence)):
        raise ValueError("numeric type")
    up, down, confidence = (Decimal(str(value)) for value in
                            (raw_up, raw_down, raw_confidence))
    if any(not value.is_finite() or not Decimal("0") <= value <= Decimal("1")
           for value in (up, down, confidence)):
        raise ValueError("numeric range")
    if abs(up + down - Decimal("1")) > Decimal("0.000001"):
        raise ValueError("distribution")
    chosen = up if direction["choice"] == "UP" else down
    if chosen < max(up, down) - Decimal("0.000001"):
        raise ValueError("choice probability")
    return up


def decide_c180_favorite_entry(inp: C180EntryInput) -> C180EntryDecision:
    """Apply the Original JEV favorite and fee/depth EV rule at T+120 seconds.

    EV threshold scales with stake: 0.005 USDT for each 1 USDT of intended
    stake. The ask depth is walked anew for 2U or 3U; its average price and EV
    can change with size. Output ``expected_shares`` is a depth estimate, not a
    fill. A live order needs a fresh book and EV recheck before submission.
    """

    def result(action: Action, reason: str, *, side: Side | None = None,
               shares: Decimal | None = None, ev: Decimal | None = None) -> C180EntryDecision:
        return C180EntryDecision(action, reason, side, inp.stake_usdt, shares, ev)

    if not inp.stake_usdt.is_finite() or inp.stake_usdt <= 0:
        return result("UNKNOWN", "invalid_stake")
    if inp.cutoff_ms != inp.market_start_ms + ENTRY_OFFSET_MS:
        return result("UNKNOWN", "not_c180_cutoff")
    if (inp.recovery_gap or inp.missed_cutoff or inp.decision_at_ms < inp.cutoff_ms
            or inp.decision_at_ms > inp.cutoff_ms + MAX_DECISION_LATENCY_MS):
        return result("UNKNOWN", "missed_cutoff")
    if (not inp.market_id or inp.market_id == "missing"
            or inp.reference_price is None or not inp.reference_price.is_finite()
            or inp.reference_price <= 0):
        return result("UNKNOWN", "missing_market_identity_or_reference")
    if inp.market_identified_at_ms is None or inp.market_identified_at_ms > inp.cutoff_ms:
        return result("UNKNOWN", "market_identity_not_known_at_cutoff")
    if inp.fee_bps is None or not inp.fee_bps.is_finite() or inp.fee_bps < 0:
        return result("UNKNOWN", "fee_unknown")
    if inp.original_model_id != MODEL_ID or inp.original_p_up is None:
        return result("UNKNOWN", "original_jev_unavailable")
    if not inp.original_p_up.is_finite() or not Decimal("0") <= inp.original_p_up <= Decimal("1"):
        return result("UNKNOWN", "invalid_original_probability")
    if (inp.model_completed_at_ms is None
            or inp.model_completed_at_ms < inp.cutoff_ms
            or inp.model_completed_at_ms > inp.cutoff_ms + MAX_MODEL_LATENCY_MS):
        return result("UNKNOWN", "original_jev_late")
    if inp.book_market_id != inp.market_id:
        return result("UNKNOWN", "book_market_mismatch")
    if any(
        stamp is None or not 0 <= inp.cutoff_ms - stamp <= MAX_BOOK_AGE_MS
        for stamp in (inp.book_at_ms, inp.book_received_at_ms,
                      inp.book_received_at_ms_secondary)
    ):
        return result("UNKNOWN", "quote_missing_or_stale")
    if not _valid_levels(inp.up_ask_levels) or not _valid_levels(inp.down_ask_levels):
        return result("UNKNOWN", "invalid_quote_depth")

    options: list[tuple[Decimal, Side, Decimal, Decimal | None]] = []
    fee_rate = inp.fee_bps / Decimal("10000")
    for side, probability, levels in (
        ("UP", inp.original_p_up, inp.up_ask_levels),
        ("DOWN", Decimal("1") - inp.original_p_up, inp.down_ask_levels),
    ):
        cash, shares, worst_ask = _walk_ask_with_limit(levels, inp.stake_usdt)
        if cash < inp.stake_usdt - _CASH_TOLERANCE:
            continue
        ev = shares * probability - cash * (Decimal("1") + fee_rate)
        options.append((ev, side, shares, worst_ask))
    if not options:
        return result("SKIP", "nonpositive_cost_after_ev")
    ev, side, shares, worst_ask = max(options)
    if ev <= ENTRY_BUFFER_PER_USDT * inp.stake_usdt:
        return result("SKIP", "nonpositive_cost_after_ev", ev=ev)
    forecast_favorite: Side | None = (
        "UP" if inp.original_p_up > Decimal("0.5") else
        "DOWN" if inp.original_p_up < Decimal("0.5") else None
    )
    if side != forecast_favorite:
        return result("SKIP", "not_forecast_favorite", ev=ev)
    if worst_ask is None or worst_ask > V11_MAX_BUY_PRICE:
        return result("SKIP", "price_cap_exceeded", ev=ev)
    return result(side, "positive_cost_after_ev", side=side, shares=shares, ev=ev)


def recheck_c180_execution(inp: C180ExecutionInput) -> C180ExecutionRecheck:
    """Recheck the chosen side on a *new* book from T+124s through T+136s.

    Frozen shadow ``engine.order`` sets ``ready=cutoff+4000`` and
    ``expires=cutoff+16000``. Its ``logic.advance`` requires a fresh book
    observed after ready and recomputes fee/depth EV before hypothetical fill.
    This function only authorizes a bounded limit; the caller must recheck the
    book again at the real submit boundary and reconcile actual fill/fees.
    """

    ready = inp.market_start_ms + EXECUTION_READY_OFFSET_MS
    expires = min(inp.market_start_ms + EXECUTION_EXPIRES_OFFSET_MS,
                  inp.market_start_ms + MARKET_WINDOW_MS - 1)

    def reject(reason: str) -> C180ExecutionRecheck:
        return C180ExecutionRecheck(False, reason, ready, expires)

    decision = inp.entry_decision
    if decision.action not in ("UP", "DOWN") or decision.side != decision.action:
        return reject("no_approved_entry")
    if decision.stake_usdt not in (Decimal("1"), Decimal("2"), Decimal("3")):
        return reject("unsupported_stake")
    if inp.at_ms < ready:
        return reject("not_ready")
    if inp.at_ms > expires:
        return reject("expired")
    if (not inp.frozen_market_topic or inp.current_market_topic != inp.frozen_market_topic
            or not inp.frozen_market_id or inp.current_market_id != inp.frozen_market_id
            or inp.book_market_id != inp.frozen_market_id):
        return reject("market_identity_changed")
    if (inp.frozen_fee_bps is None or not inp.frozen_fee_bps.is_finite()
            or inp.frozen_fee_bps < 0 or inp.current_fee_bps is None
            or not inp.current_fee_bps.is_finite()
            or inp.current_fee_bps != inp.frozen_fee_bps):
        return reject("fee_changed_or_unknown")
    if (inp.original_p_up is None or not inp.original_p_up.is_finite()
            or not Decimal("0") <= inp.original_p_up <= Decimal("1")):
        return reject("invalid_original_probability")
    favorite = "UP" if inp.original_p_up > Decimal("0.5") else (
        "DOWN" if inp.original_p_up < Decimal("0.5") else None
    )
    if decision.side != favorite:
        return reject("not_forecast_favorite")
    if not inp.full_depth:
        return reject("full_depth_unavailable")
    if any(
        stamp is None or not 0 <= inp.at_ms - stamp <= MAX_BOOK_AGE_MS
        for stamp in (inp.book_at_ms, inp.book_received_at_ms,
                      inp.book_received_at_ms_secondary)
    ):
        return reject("quote_missing_or_stale")
    # The shadow checks book_at_ms and received_at against ready, then accepts
    # only a book newer than the order's last observed snapshot.
    if (inp.last_seen_book_at_ms is None
            or min(inp.book_at_ms, inp.book_received_at_ms) < ready
            or inp.book_at_ms <= inp.last_seen_book_at_ms):
        return reject("quote_not_new_after_ready")
    levels = inp.selected_ask_levels
    if (not levels or not isinstance(levels, tuple) or not _valid_levels(levels)
            or any(a.price > b.price for a, b in zip(levels, levels[1:]))):
        return reject("invalid_or_empty_depth")
    cash, shares, worst_ask = _walk_ask_with_limit(levels, decision.stake_usdt)
    if cash < decision.stake_usdt - _CASH_TOLERANCE:
        return reject("insufficient_full_depth")
    if worst_ask is None or worst_ask > V11_MAX_BUY_PRICE:
        return reject("price_cap_exceeded")
    p_side = inp.original_p_up if decision.side == "UP" else Decimal("1") - inp.original_p_up
    ev = shares * p_side - cash * (
        Decimal("1") + inp.frozen_fee_bps / Decimal("10000")
    )
    if ev <= ENTRY_BUFFER_PER_USDT * decision.stake_usdt:
        return reject("value_gone")
    return C180ExecutionRecheck(
        True, "fee_depth_ev_pass", ready, expires, worst_ask,
        cash, shares, ev,
    )
