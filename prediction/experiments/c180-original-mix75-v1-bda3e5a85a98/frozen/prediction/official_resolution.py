"""Pure parsing for official Prediction market resolution payloads."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .models import OutcomeSide, as_decimal


def _data(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return payload.get("data", payload)
    return payload


def official_market_topic_id(payload: Any) -> str:
    """Return only an explicit official market-topic identity.

    Generic ``id`` fields are intentionally rejected: nested market and
    outcome payloads also expose identifiers, and treating one of those as a
    topic identity could bind a resolution to the wrong five-minute market.
    """

    data = _data(payload)
    if not isinstance(data, Mapping):
        return ""
    return str(
        data.get("marketTopicId")
        or data.get("market_topic_id")
        or data.get("topicId")
        or ""
    ).strip()


def _side(value: Any) -> OutcomeSide | None:
    if isinstance(value, Mapping):
        value = value.get("name") or value.get("outcome") or value.get("winner") or value.get("side") or value.get("title")
    text = str(value or "").strip().upper()
    return OutcomeSide(text) if text in {OutcomeSide.UP.value, OutcomeSide.DOWN.value} else None


def _is_half(value: Any) -> bool:
    try:
        return as_decimal(value) == Decimal("0.5")
    except (InvalidOperation, TypeError, ValueError, ArithmeticError):
        return False


def _exact_tie(outcomes: list[Mapping[str, Any]]) -> bool:
    """Require official 0.5/0.5 pricing evidence for a dual winner."""

    if len(outcomes) != 2:
        return False
    for outcome in outcomes:
        value = next(
            (
                outcome.get(key)
                for key in ("chance", "price", "probability", "odds")
                if outcome.get(key) is not None
            ),
            None,
        )
        if not _is_half(value):
            return False
    return True


@dataclass(frozen=True)
class OfficialResolution:
    winners: tuple[OutcomeSide, ...] = ()
    terminal: bool = False
    exact_dual_tie: bool = False
    ambiguous: bool = False
    reason: str = ""


def parse_official_resolution(payload: Any) -> OfficialResolution:
    """Parse official winners without collapsing a dual winner to one side."""

    data = _data(payload)
    if not isinstance(data, Mapping):
        return OfficialResolution(reason="official resolution payload is not a mapping")
    status = str(data.get("status") or data.get("marketStatus") or data.get("tradingStatus") or "").upper()
    terminal = status in {"CLOSED", "SETTLED", "RESOLVED", "EXPIRED"}

    for key in ("resolvedOutcome", "finalOutcome", "resolvedSide", "result"):
        value = data.get(key)
        if isinstance(value, (list, tuple)):
            winners = tuple(side for side in (_side(item) for item in value) if side is not None)
            if len(winners) == 2:
                return OfficialResolution(winners=winners, terminal=True, ambiguous=True, reason="dual winner lacks nested tie proof")
        side = _side(value)
        if side is not None:
            return OfficialResolution(winners=(side,), terminal=True)

    direct_winner = data.get("winner") or data.get("outcome")
    if not isinstance(direct_winner, (list, tuple, Mapping)):
        side = _side(direct_winner)
        if side is not None:
            return OfficialResolution(winners=(side,), terminal=True)

    markets = data.get("markets")
    if not isinstance(markets, list):
        return OfficialResolution(terminal=terminal)
    nested_winners: list[OutcomeSide] = []
    winning_records: list[Mapping[str, Any]] = []
    for market in markets:
        if not isinstance(market, Mapping):
            continue
        outcomes = market.get("outcomes")
        if not isinstance(outcomes, list):
            continue
        for outcome in outcomes:
            if not isinstance(outcome, Mapping):
                continue
            if outcome.get("winner") is True or outcome.get("isWinner") is True:
                side = _side(outcome)
                if side is None:
                    return OfficialResolution(terminal=True, ambiguous=True, reason="official winner side is unknown")
                nested_winners.append(side)
                winning_records.append(outcome)
    if nested_winners:
        unique = tuple(dict.fromkeys(nested_winners))
        if len(unique) == 1 and len(nested_winners) == 1:
            return OfficialResolution(winners=unique, terminal=True)
        if set(unique) == {OutcomeSide.UP, OutcomeSide.DOWN} and _exact_tie(winning_records):
            return OfficialResolution(
                winners=(OutcomeSide.UP, OutcomeSide.DOWN),
                terminal=True,
                exact_dual_tie=True,
                reason="official exact-tie dual winner",
            )
        return OfficialResolution(terminal=True, ambiguous=True, reason="official resolution has multiple winners")

    legacy_winners: list[OutcomeSide] = []
    for item in markets:
        if not isinstance(item, Mapping):
            continue
        if item.get("isWinner") is True or item.get("winner") is True:
            side = _side(item)
            if side is None:
                return OfficialResolution(terminal=True, ambiguous=True, reason="official winner side is unknown")
            legacy_winners.append(side)
    if len(legacy_winners) == 1:
        return OfficialResolution(winners=(legacy_winners[0],), terminal=True)
    if legacy_winners:
        return OfficialResolution(terminal=True, ambiguous=True, reason="official resolution has multiple legacy winners")
    return OfficialResolution(terminal=terminal)


__all__ = [
    "OfficialResolution",
    "official_market_topic_id",
    "parse_official_resolution",
]
