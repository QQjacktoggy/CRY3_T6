"""Pure domain models for Prediction markets.

All monetary and share quantities use :class:`~decimal.Decimal`.  Binance
responses are intentionally kept as dictionaries by the REST client because
the API adds fields periodically; these models cover only fields required by
the strategy and ledger.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import re
from typing import Any, Mapping


ZERO = Decimal("0")


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class OutcomeSide(_StringEnum):
    UP = "UP"
    DOWN = "DOWN"

    @property
    def opposite(self) -> "OutcomeSide":
        return OutcomeSide.DOWN if self is OutcomeSide.UP else OutcomeSide.UP


class OrderSide(_StringEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(_StringEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderStatus(_StringEnum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"
    CLOSED = "CLOSED"


class CampaignState(_StringEnum):
    BOOTSTRAP = "BOOTSTRAP"
    RECOVER = "RECOVER"
    OBSERVE = "OBSERVE"
    INITIAL_PENDING = "INITIAL_PENDING"
    INITIAL_POSITION = "INITIAL_POSITION"
    PROFIT_LOCK = "PROFIT_LOCK"
    HEDGE_PENDING = "HEDGE_PENDING"
    HEDGED = "HEDGED"
    WAIT_CONFIRM = "WAIT_CONFIRM"
    UNWIND_LOSER = "UNWIND_LOSER"
    FINAL_HOLD = "FINAL_HOLD"
    SETTLEMENT = "SETTLEMENT"
    PAUSED = "PAUSED"
    SOFT_COOLDOWN = "SOFT_COOLDOWN"
    HARD_STOP = "HARD_STOP"
    DONE = "DONE"
    CANCELLED = "CANCELLED"


class ActionType(_StringEnum):
    HOLD = "HOLD"
    BUY_INITIAL = "BUY_INITIAL"
    BUY_ADD = "BUY_ADD"
    BUY_HEDGE = "BUY_HEDGE"
    SELL_PROFIT_LOCK = "SELL_PROFIT_LOCK"
    SELL_LOSER = "SELL_LOSER"
    SELL_PROTECTIVE = "SELL_PROTECTIVE"
    CANCEL = "CANCEL"
    RECONCILE = "RECONCILE"
    SETTLE = "SETTLE"
    PAUSE = "PAUSE"


def as_decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    """Convert API numbers safely without inheriting binary float error."""

    if value is None or value == "":
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception as exc:  # noqa: BLE001 - malformed exchange data is fatal to caller
        raise ValueError(f"invalid decimal value: {value!r}") from exc


def _enum(enum_type: type[Enum], value: Any, default: Enum) -> Enum:
    try:
        return enum_type(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class MarketInfo:
    """Canonical market metadata obtained from market detail."""

    market_topic_id: str
    market_id: str
    slug: str
    start_time_ms: int
    end_time_ms: int
    reference_price: Decimal | None = None
    up_token_id: str | None = None
    down_token_id: str | None = None
    vendor: str = "predict_fun"
    chain_id: str = "56"
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)
    # Prediction detail returns separate market IDs for the UP and DOWN
    # contracts.  ``market_id`` remains the first/legacy ID for compatibility;
    # callers placing an order must use ``market_id_for``.
    up_market_id: str | None = None
    down_market_id: str | None = None
    status: str = "OPEN"

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.end_time_ms - self.start_time_ms) / 1000.0)

    def elapsed_seconds(self, now_ms: int) -> float:
        return (int(now_ms) - self.start_time_ms) / 1000.0

    def remaining_seconds(self, now_ms: int) -> float:
        return (self.end_time_ms - int(now_ms)) / 1000.0

    def market_id_for(self, side: OutcomeSide) -> str:
        selected = self.up_market_id if side is OutcomeSide.UP else self.down_market_id
        return str(selected or self.market_id)

    @classmethod
    def from_api(cls, payload: Mapping[str, Any]) -> "MarketInfo":
        """Best-effort parser for the documented and legacy field spellings."""

        root = payload.get("data", payload)
        data: Mapping[str, Any] = root if isinstance(root, Mapping) else {}
        if isinstance(data.get("market"), Mapping):
            data = data["market"]
        topics = data.get("marketTopics")
        if isinstance(topics, list):
            topic = next((item for item in topics if isinstance(item, Mapping)), {})
            data = topic or data

        market_nodes = data.get("markets") if isinstance(data, Mapping) else None
        nodes: list[Mapping[str, Any]] = []
        if isinstance(market_nodes, list):
            nodes.extend(item for item in market_nodes if isinstance(item, Mapping))
        if isinstance(data, Mapping):
            # Some detail responses put the side market directly at the topic
            # level; include it without duplicating a normal ``markets`` node.
            if data.get("marketId") is not None or data.get("title") is not None:
                nodes.insert(0, data)
        market_node = nodes[0] if nodes else {}
        # Topic-level timing/slug metadata must win over a side-market node;
        # the node is only a fallback for flat legacy responses.
        merged = {**dict(market_node), **dict(data)}

        token_ids: dict[str, str] = {}
        market_ids: dict[str, str] = {}

        def nonempty(value: Any) -> str | None:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        def first_nonempty(item: Mapping[str, Any], *keys: str) -> str | None:
            for key in keys:
                value = nonempty(item.get(key))
                if value is not None:
                    return value
            return None

        def side_from_text(value: Any) -> str:
            """Return a side only when the text names exactly one outcome."""

            label = nonempty(value)
            if not label:
                return ""
            upper = label.upper()
            if upper in {"UP", "DOWN"}:
                return upper
            # The topic title for the official binary schema contains both
            # words ("Bitcoin Up or Down").  Treat that as descriptive, not
            # as a side.  Word boundaries also avoid classifying "UPPER".
            words = set(re.findall(r"[A-Z]+", upper))
            sides = words & {"UP", "DOWN"}
            return next(iter(sides)) if len(sides) == 1 else ""

        def side_label(item: Mapping[str, Any]) -> str:
            # Explicit outcome fields always win over a descriptive title.
            # Binance's official binary outcome schema uses ``name``.  When
            # present it is authoritative, including when invalid; legacy
            # aliases are considered only when ``name`` is absent/empty.
            for key in ("name", "side", "outcome", "direction", "label"):
                if key in item and nonempty(item.get(key)) is not None:
                    # A populated explicit field is authoritative even when
                    # it is invalid/ambiguous.  Never reinterpret a conflicting
                    # title as a valid contract side.
                    return side_from_text(item.get(key))
            return side_from_text(item.get("title"))

        invalid_side_mapping = False

        def set_token(side: str, token: Any) -> None:
            nonlocal invalid_side_mapping
            value = nonempty(token)
            if side in {"UP", "DOWN"} and value is not None:
                if side in token_ids and token_ids[side] != value:
                    invalid_side_mapping = True
                    return
                token_ids[side] = value

        def set_market_id(side: str, market_id: Any) -> None:
            nonlocal invalid_side_mapping
            value = nonempty(market_id)
            if side in {"UP", "DOWN"} and value is not None:
                if side in market_ids and market_ids[side] != value:
                    invalid_side_mapping = True
                    return
                market_ids[side] = value

        for node in nodes:
            node_side = side_label(node)
            node_market_id = first_nonempty(node, "marketId", "market_id", "id")
            outcomes = node.get("outcomes") or node.get("variants") or []
            outcome_sides: set[str] = set()
            if isinstance(outcomes, list):
                for item in outcomes:
                    if not isinstance(item, Mapping):
                        continue
                    # Official Binance detail uses one market node with two
                    # outcome objects.  Parse each outcome's name before
                    # considering the node title or side.
                    outcome_side = side_label(item)
                    token = first_nonempty(item, "tokenId", "token_id", "id")
                    if outcome_side in {"UP", "DOWN"}:
                        if outcome_side in outcome_sides:
                            invalid_side_mapping = True
                        outcome_sides.add(outcome_side)
                        set_token(outcome_side, token)
                    elif node_side in {"UP", "DOWN"}:
                        # Legacy two-node responses often omit outcome names.
                        set_token(node_side, token)

            # A shared market node ID is valid for both contracts when the
            # outcome objects establish both sides.  Legacy side nodes keep
            # their individual IDs.
            mapped_sides = outcome_sides or ({node_side} if node_side in {"UP", "DOWN"} else set())
            for side in mapped_sides:
                set_market_id(side, node_market_id)

            token = first_nonempty(node, "tokenId", "token_id")
            if node_side in {"UP", "DOWN"}:
                set_token(node_side, token)

        # Preserve support for the older flat outcomes/variants schema.  A
        # flat node market ID is shared when both outcome names are present.
        outcomes = merged.get("outcomes") or merged.get("variants") or []
        flat_sides: set[str] = set()
        if isinstance(outcomes, list):
            for item in outcomes:
                if not isinstance(item, Mapping):
                    continue
                label = side_label(item)
                token = first_nonempty(item, "tokenId", "token_id", "id")
                if label in {"UP", "DOWN"}:
                    flat_sides.add(label)
                    set_token(label, token)
        flat_market_id = first_nonempty(merged, "marketId", "market_id", "id")
        for side in flat_sides:
            set_market_id(side, flat_market_id)

        if invalid_side_mapping:
            token_ids.clear()
            market_ids.clear()

        def first(*keys: str, default: Any = None) -> Any:
            for key in keys:
                if merged.get(key) is not None:
                    return merged[key]
            return default

        def timestamp(value: Any) -> int:
            if value is None or value == "":
                return 0
            if isinstance(value, (int, float)) or str(value).replace(".", "", 1).isdigit():
                # Legacy prediction endpoints expose epoch milliseconds.  The
                # current endpoint uses ISO-8601 startDate/endDate strings;
                # do not silently reinterpret numeric legacy values as secs.
                return int(float(value))
            text = str(value).replace("Z", "+00:00")
            try:
                return int(datetime.fromisoformat(text).replace(tzinfo=datetime.fromisoformat(text).tzinfo or timezone.utc).timestamp() * 1000)
            except ValueError:
                return 0

        variant_data = merged.get("variantData") if isinstance(merged.get("variantData"), Mapping) else {}

        return cls(
            market_topic_id=str(first("marketTopicId", "market_topic_id", "topicId", "id", default="")),
            market_id=nonempty(first("marketId", "market_id", "id", default="")) or "",
            slug=str(first("slug", "marketSlug", default="")),
            start_time_ms=timestamp(first("startTime", "startTimeMs", "start_time_ms", "openTime", "startDate", default=0)),
            end_time_ms=timestamp(first("endTime", "endTimeMs", "end_time_ms", "closeTime", "endDate", default=0)),
            reference_price=(as_decimal(first("referencePrice", "reference_price", "openPrice", default=variant_data.get("startPrice")))
                            if first("referencePrice", "reference_price", "openPrice", default=variant_data.get("startPrice")) is not None else None),
            up_token_id=token_ids.get("UP"),
            down_token_id=token_ids.get("DOWN"),
            vendor=str(first("vendor", default="predict_fun")),
            chain_id=str(first("chainId", "chain_id", default="56")),
            raw=dict(root) if isinstance(root, Mapping) else {},
            up_market_id=market_ids.get("UP") or nonempty(merged.get("upMarketId") or merged.get("up_market_id")),
            down_market_id=market_ids.get("DOWN") or nonempty(merged.get("downMarketId") or merged.get("down_market_id")),
            status=str(first("status", "marketStatus", default="OPEN")).upper(),
        )


@dataclass(frozen=True)
class QuoteSnapshot:
    """One coherent quote used for a strategy decision."""

    observed_at_ms: int
    up_bid: Decimal | None = None
    up_ask: Decimal | None = None
    down_bid: Decimal | None = None
    down_ask: Decimal | None = None
    leader: OutcomeSide | None = None
    btc_spot: Decimal | None = None
    reference_price: Decimal | None = None
    feed_ok: bool = True
    flip_confirmed: bool = False
    btc_crossed_reference: bool = False
    reference_recross: bool = False
    leader_duration_ms: int = 0
    stable_final: bool = False
    # ``spot_observed_at_ms`` is kept separately from the quote timestamp:
    # the book and the public BTC feed are independent sources.  Persisting
    # this value lets a restart make a freshness decision without trusting an
    # in-memory clock.
    spot_observed_at_ms: int = 0
    book_up_observed_at_ms: int = 0
    book_down_observed_at_ms: int = 0
    history_complete: bool = False
    fee_rate_bps: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    def bid(self, side: OutcomeSide) -> Decimal | None:
        return self.up_bid if side is OutcomeSide.UP else self.down_bid

    def ask(self, side: OutcomeSide) -> Decimal | None:
        return self.up_ask if side is OutcomeSide.UP else self.down_ask

    def price(self, side: OutcomeSide) -> Decimal | None:
        return self.bid(side)

    def age_ms(self, now_ms: int) -> int:
        return max(0, int(now_ms) - self.observed_at_ms)

    @classmethod
    def from_books(
        cls,
        *,
        observed_at_ms: int,
        up_bid: Any = None,
        up_ask: Any = None,
        down_bid: Any = None,
        down_ask: Any = None,
        leader: OutcomeSide | str | None = None,
        **kwargs: Any,
    ) -> "QuoteSnapshot":
        return cls(
            observed_at_ms=int(observed_at_ms),
            up_bid=as_decimal(up_bid) if up_bid is not None else None,
            up_ask=as_decimal(up_ask) if up_ask is not None else None,
            down_bid=as_decimal(down_bid) if down_bid is not None else None,
            down_ask=as_decimal(down_ask) if down_ask is not None else None,
            leader=(OutcomeSide(str(leader).upper()) if leader is not None else None),
            **kwargs,
        )


@dataclass
class Position:
    """Campaign ledger for both outcome-token legs."""

    up_shares: Decimal = ZERO
    down_shares: Decimal = ZERO
    up_cost: Decimal = ZERO
    down_cost: Decimal = ZERO
    realized_cash: Decimal = ZERO
    fees: Decimal = ZERO
    up_initial_shares: Decimal = ZERO
    down_initial_shares: Decimal = ZERO

    @property
    def total_buy_cost(self) -> Decimal:
        return self.up_cost + self.down_cost

    @property
    def has_any(self) -> bool:
        return self.up_shares > ZERO or self.down_shares > ZERO

    @property
    def total_shares(self) -> Decimal:
        return self.up_shares + self.down_shares

    def shares(self, side: OutcomeSide) -> Decimal:
        return self.up_shares if side is OutcomeSide.UP else self.down_shares

    def initial_shares(self, side: OutcomeSide) -> Decimal:
        return self.up_initial_shares if side is OutcomeSide.UP else self.down_initial_shares

    def cost(self, side: OutcomeSide) -> Decimal:
        return self.up_cost if side is OutcomeSide.UP else self.down_cost

    def add_buy(self, side: OutcomeSide, shares: Any, cost: Any, fee: Any = ZERO, *, initial: bool = False) -> None:
        shares_d, cost_d, fee_d = as_decimal(shares), as_decimal(cost), as_decimal(fee)
        if shares_d <= ZERO or cost_d < ZERO or fee_d < ZERO:
            raise ValueError("buy fill must have positive shares and non-negative cost/fee")
        if side is OutcomeSide.UP:
            self.up_shares += shares_d
            self.up_cost += cost_d
            if initial:
                self.up_initial_shares += shares_d
        else:
            self.down_shares += shares_d
            self.down_cost += cost_d
            if initial:
                self.down_initial_shares += shares_d
        self.fees += fee_d

    def add_sell(self, side: OutcomeSide, shares: Any, proceeds: Any, fee: Any = ZERO) -> None:
        shares_d, proceeds_d, fee_d = as_decimal(shares), as_decimal(proceeds), as_decimal(fee)
        if shares_d <= ZERO or proceeds_d < ZERO or fee_d < ZERO:
            raise ValueError("sell fill must have positive shares and non-negative proceeds/fee")
        if shares_d > self.shares(side):
            raise ValueError("sell exceeds known position")
        if side is OutcomeSide.UP:
            self.up_shares -= shares_d
        else:
            self.down_shares -= shares_d
        self.realized_cash += proceeds_d
        self.fees += fee_d


@dataclass(frozen=True)
class Fill:
    order_id: str
    token_id: str
    side: OrderSide
    outcome: OutcomeSide
    shares: Decimal
    price: Decimal
    gross_amount: Decimal
    fee: Decimal = ZERO
    event_time_ms: int = 0
    trade_id: str | None = None

    @classmethod
    def from_api(cls, payload: Mapping[str, Any], *, outcome: OutcomeSide | None = None) -> "Fill":
        data = payload.get("data", payload)
        side = OrderSide(str(data.get("side", "BUY")).upper())
        shares = as_decimal(
            data.get("filledShareQty")
            or data.get("filledShares")
            or data.get("shares")
            or data.get("quantity")
            or data.get("executedQty")
        )
        price = as_decimal(data.get("price") or data.get("avgPrice"))
        gross = as_decimal(
            data.get("filledUsdtAmount")
            or data.get("grossAmount")
            or data.get("amount")
            or (shares * price)
        )
        provider_fee = as_decimal(data.get("marketProviderFee") or data.get("providerFee"))
        network_fee = as_decimal(data.get("networkFee"))
        fee_value = as_decimal(data.get("fee") or data.get("feeAmount")) + provider_fee + network_fee
        if price == ZERO and shares > ZERO and gross > ZERO:
            price = gross / shares
        return cls(
            order_id=str(data.get("orderId") or data.get("order_id") or ""),
            token_id=str(data.get("tokenId") or data.get("token_id") or ""),
            side=side,
            outcome=outcome or OutcomeSide(str(data.get("outcome") or "UP").upper()),
            shares=shares,
            price=price,
            gross_amount=gross,
            fee=fee_value,
            event_time_ms=int(as_decimal(data.get("eventTime") or data.get("time") or 0)),
            trade_id=(str(data["tradeId"]) if data.get("tradeId") is not None else None),
        )


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    campaign_id: str
    action: ActionType
    outcome: OutcomeSide
    order_side: OrderSide
    amount: Decimal
    limit_price: Decimal
    created_at_ms: int
    ttl_ms: int
    attempt: int = 1
    order_id: str | None = None
    unknown: bool = False
    # Durable strategy attribution in the existing intent tier column.
    tier: str | None = None


@dataclass
class Campaign:
    campaign_id: str
    market: MarketInfo
    state: CampaignState = CampaignState.OBSERVE
    position: Position = field(default_factory=Position)
    initial_outcome: OutcomeSide | None = None
    hedge_used: bool = False
    profit_lock_used: bool = False
    loser_unwind_count: int = 0
    loser_unwind_shares: Decimal = ZERO
    buy_count: int = 0
    order_attempts: int = 0
    initial_attempts: int = 0
    scale_in_attempts: int = 0
    hedge_attempts: int = 0
    pending_intent_id: str | None = None
    pending_unknown: bool = False
    last_leader: OutcomeSide | None = None
    leader_flip_count: int = 0
    hedged_at_ms: int | None = None
    initial_filled_at_ms: int | None = None
    pnl_btc_peak_bps: Decimal | None = None
    fee_rate_bps: Decimal | None = None
    fee_per_share: Decimal | None = None
    last_error: str | None = None
    # Campaign-scoped market continuity.  These fields deliberately live on
    # the domain object as well as in ``prediction_market_state`` so a loaded
    # campaign can be evaluated deterministically before the next DB write.
    prior_spot: Decimal | None = None
    last_spot: Decimal | None = None
    last_spot_at_ms: int | None = None
    prior_leader: OutcomeSide | None = None
    leader_since_ms: int = 0
    leader_quotes: int = 0
    reference_cross_count: int = 0
    last_quote_at_ms: int = 0

    @property
    def total_invested(self) -> Decimal:
        return self.position.total_buy_cost

    def remaining_seconds(self, now_ms: int) -> float:
        return self.market.remaining_seconds(now_ms)

    def elapsed_seconds(self, now_ms: int) -> float:
        return self.market.elapsed_seconds(now_ms)

    @property
    def winner_candidate(self) -> OutcomeSide | None:
        if self.position.up_shares <= ZERO and self.position.down_shares <= ZERO:
            return None
        if self.position.up_shares >= self.position.down_shares:
            return OutcomeSide.UP
        return OutcomeSide.DOWN

    def update_leader(self, leader: OutcomeSide | None) -> None:
        if leader is not None and self.last_leader is not None and leader is not self.last_leader:
            self.leader_flip_count += 1
        if leader is not None:
            self.last_leader = leader


@dataclass(frozen=True)
class OrderRecord:
    order_id: str
    status: OrderStatus
    side: OrderSide
    token_id: str
    filled_shares: Decimal = ZERO
    avg_price: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)
    filled_usdt_amount: Decimal = ZERO

    @classmethod
    def from_api(cls, payload: Mapping[str, Any]) -> "OrderRecord":
        """Parse the Prediction history schema, including CLOSED fills."""

        data = payload.get("data", payload)
        if not isinstance(data, Mapping):
            raise ValueError("order payload must be a mapping")
        raw_status = str(data.get("status") or "UNKNOWN").upper()
        try:
            status = OrderStatus(raw_status)
        except ValueError:
            status = OrderStatus.UNKNOWN
        side = OrderSide(str(data.get("side") or data.get("orderSide") or "BUY").upper())
        shares = as_decimal(data.get("filledShareQty") or data.get("filledShares") or data.get("executedQty"))
        gross = as_decimal(data.get("filledUsdtAmount") or data.get("filledAmount") or data.get("grossAmount"))
        avg = as_decimal(data.get("avgPrice") or data.get("price"))
        if avg == ZERO and shares > ZERO and gross > ZERO:
            avg = gross / shares
        return cls(
            order_id=str(data.get("orderId") or data.get("order_id") or data.get("id") or ""),
            status=status,
            side=side,
            token_id=str(data.get("tokenId") or data.get("token_id") or ""),
            filled_shares=shares,
            avg_price=avg if avg > ZERO else None,
            raw=dict(data),
            filled_usdt_amount=gross,
        )
