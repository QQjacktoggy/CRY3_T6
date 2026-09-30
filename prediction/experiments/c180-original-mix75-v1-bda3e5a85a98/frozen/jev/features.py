from __future__ import annotations

import math
import statistics
import time
from typing import Any, Iterable

from .models import Trade


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def _price_at_or_before(trades: list[Trade], target: float) -> float | None:
    for trade in reversed(trades):
        if trade.ts <= target:
            return trade.price
    return None


def _return_bps(trades: list[Trade], now: float, seconds: float) -> float | None:
    if not trades:
        return None
    old = _price_at_or_before(trades, now - seconds)
    latest = trades[-1].price
    if old is None or old <= 0:
        return None
    return (latest / old - 1.0) * 10_000.0


def _trade_window(trades: list[Trade], now: float, seconds: float) -> list[Trade]:
    cutoff = now - seconds
    return [trade for trade in trades if trade.ts >= cutoff]


def _flow_metrics(trades: list[Trade], now: float, seconds: float) -> dict[str, Any]:
    window = _trade_window(trades, now, seconds)
    total_qty = sum(t.qty for t in window)
    buy_qty = sum(t.qty for t in window if t.taker_buy)
    sell_qty = max(0.0, total_qty - buy_qty)
    return {
        "volume": _round(total_qty, 8),
        "taker_buy_ratio": _round(buy_qty / total_qty, 4) if total_qty > 0 else None,
        "signed_volume_ratio": _round((buy_qty - sell_qty) / total_qty, 4) if total_qty > 0 else None,
        "trade_count": len(window),
    }


def _realized_vol_bps(trades: list[Trade], now: float, seconds: float) -> float | None:
    window = _trade_window(trades, now, seconds)
    if len(window) < 3:
        return None
    second_close: dict[int, float] = {}
    for trade in window:
        second_close[int(trade.ts)] = trade.price
    prices = [second_close[key] for key in sorted(second_close)]
    if len(prices) < 3:
        return None
    returns = [math.log(prices[i] / prices[i - 1]) * 10_000.0 for i in range(1, len(prices)) if prices[i - 1] > 0]
    return statistics.pstdev(returns) if len(returns) >= 2 else None


def _book_metrics(quote: dict[str, float]) -> dict[str, float | None]:
    bid = float(quote.get("bid") or 0.0)
    ask = float(quote.get("ask") or 0.0)
    bid_qty = float(quote.get("bid_qty") or 0.0)
    ask_qty = float(quote.get("ask_qty") or 0.0)
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
    total_qty = bid_qty + ask_qty
    return {
        "bid": _round(bid, 8),
        "ask": _round(ask, 8),
        "mid": _round(mid, 8) if mid else None,
        "spread_bps": _round((ask - bid) / mid * 10_000.0, 4) if mid else None,
        "bid_share": _round(bid_qty / total_qty, 4) if total_qty > 0 else None,
    }


def _oi_change(oi_history: list[tuple[float, float]], now: float, seconds: float) -> float | None:
    if not oi_history:
        return None
    latest = oi_history[-1][1]
    old: float | None = None
    target = now - seconds
    for ts, value in reversed(oi_history):
        if ts <= target:
            old = value
            break
    if old is None or old <= 0:
        return None
    return (latest / old - 1.0) * 10_000.0


def _bar_metrics(kline: dict[str, Any], now: float) -> dict[str, Any]:
    open_price = float(kline.get("open") or 0.0)
    high = float(kline.get("high") or 0.0)
    low = float(kline.get("low") or 0.0)
    close = float(kline.get("close") or 0.0)
    close_time = float(kline.get("close_time") or 0.0)
    volume = float(kline.get("volume") or 0.0)
    taker_buy_volume = float(kline.get("taker_buy_volume") or 0.0)
    result: dict[str, Any] = {
        "open": _round(open_price, 8) if open_price else None,
        "high": _round(high, 8) if high else None,
        "low": _round(low, 8) if low else None,
        "close": _round(close, 8) if close else None,
        "return_from_open_bps": _round((close / open_price - 1.0) * 10_000.0, 4) if open_price and close else None,
        "range_bps": _round((high - low) / open_price * 10_000.0, 4) if open_price and high >= low else None,
        "position_in_range": _round((close - low) / (high - low), 4) if high > low and close else None,
        "volume": _round(volume, 8),
        "taker_buy_ratio": _round(taker_buy_volume / volume, 4) if volume > 0 else None,
        "trade_count": int(kline.get("trade_count") or 0),
        "seconds_to_close": _round(max(0.0, close_time - now), 3) if close_time else _round(300.0 - (now % 300.0), 3),
    }
    return result


def build_feature_state(
    snapshot: dict[str, Any],
    *,
    now: float | None = None,
    strategy_context: dict[str, Any] | None = None,
    context_age_ms: int | None = None,
) -> dict[str, Any]:
    now = now or time.time()
    futures_trades: list[Trade] = snapshot["futures_trades"]
    spot_trades: list[Trade] = snapshot["spot_trades"]
    mark = snapshot["mark"]

    futures_price = futures_trades[-1].price if futures_trades else float(mark.get("mark_price") or 0.0)
    spot_price = spot_trades[-1].price if spot_trades else 0.0
    history_seconds = (now - futures_trades[0].ts) if futures_trades else 0.0
    futures_age_ms = int(max(0.0, now - float(snapshot.get("last_futures_trade_at") or 0.0)) * 1000) if snapshot.get("last_futures_trade_at") else None
    spot_age_ms = int(max(0.0, now - float(snapshot.get("last_spot_trade_at") or 0.0)) * 1000) if snapshot.get("last_spot_trade_at") else None

    futures_book = _book_metrics(snapshot["futures_quote"])
    spot_book = _book_metrics(snapshot["spot_quote"])
    bar = _bar_metrics(snapshot["futures_kline"], now)

    missing: list[str] = []
    if not futures_trades:
        missing.append("futures_trades")
    if futures_book["mid"] is None:
        missing.append("futures_book")
    if not float(snapshot["futures_kline"].get("open") or 0.0):
        missing.append("futures_kline")
    if not spot_trades:
        missing.append("spot_trades")
    if not snapshot["oi_history"]:
        missing.append("open_interest")

    state: dict[str, Any] = {
        "schema_version": 2,
        "task": "Predict the binary direction at the current five-minute interval close. UP means the settlement/reference price at interval close is above the interval reference price; DOWN means it is at or below the reference price. Do not make a trade decision.",
        "prediction_target": {
            "horizon": "current_5m_interval_close",
            "default_reference": "current_futures_5m_open",
            "reference_price": _round(float(snapshot["futures_kline"].get("open") or 0.0), 8) or None,
            "up_definition": "close_price > reference_price",
            "down_definition": "close_price <= reference_price",
        },
        "symbol": snapshot["symbol"],
        "observed_at_unix": _round(now, 3),
        "market": {
            "futures_price": _round(futures_price, 8) if futures_price else None,
            "spot_price": _round(spot_price, 8) if spot_price else None,
            "spot_futures_basis_bps": _round((futures_price / spot_price - 1.0) * 10_000.0, 4) if futures_price and spot_price else None,
            "mark_price": _round(float(mark.get("mark_price") or 0.0), 8) or None,
            "index_price": _round(float(mark.get("index_price") or 0.0), 8) or None,
            "mark_premium_bps": _round(
                (float(mark.get("mark_price") or 0.0) / float(mark.get("index_price") or 0.0) - 1.0) * 10_000.0,
                4,
            ) if float(mark.get("mark_price") or 0.0) and float(mark.get("index_price") or 0.0) else None,
            "funding_rate_bps": _round(float(mark.get("funding_rate") or 0.0) * 10_000.0, 5),
            "seconds_to_close": bar["seconds_to_close"],
        },
        "five_minute_bar": bar,
        "momentum_bps": {
            f"{window}s": _round(_return_bps(futures_trades, now, window), 4)
            for window in (1, 5, 15, 30, 60, 180)
        },
        "spot_momentum_bps": {
            f"{window}s": _round(_return_bps(spot_trades, now, window), 4)
            for window in (5, 15, 30, 60)
        },
        "futures_flow": {
            f"{window}s": _flow_metrics(futures_trades, now, window)
            for window in (5, 15, 30, 60)
        },
        "spot_flow": {
            f"{window}s": _flow_metrics(spot_trades, now, window)
            for window in (15, 60)
        },
        "microstructure": {
            "futures": futures_book,
            "spot": spot_book,
            "realized_vol_bps_30s": _round(_realized_vol_bps(futures_trades, now, 30), 4),
            "realized_vol_bps_60s": _round(_realized_vol_bps(futures_trades, now, 60), 4),
        },
        "derivatives": {
            "open_interest": _round(snapshot["oi_history"][-1][1], 6) if snapshot["oi_history"] else None,
            "oi_change_bps_15s": _round(_oi_change(snapshot["oi_history"], now, 15), 4),
            "oi_change_bps_30s": _round(_oi_change(snapshot["oi_history"], now, 30), 4),
            "oi_change_bps_60s": _round(_oi_change(snapshot["oi_history"], now, 60), 4),
        },
        "data_quality": {
            "history_seconds": _round(history_seconds, 3),
            "futures_trade_age_ms": futures_age_ms,
            "spot_trade_age_ms": spot_age_ms,
            "missing": missing,
        },
    }
    if strategy_context is not None:
        state["strategy_context"] = {
            "age_ms": context_age_ms,
            "payload": strategy_context,
        }
        # If CRY3 knows the exact binary-market reference/strike, expose it to Jev.
        # This keeps the continuous predictor useful before an entry signal while
        # allowing a live market reference to override the default 5m candle open.
        ref = strategy_context.get("prediction_reference_price")
        try:
            ref_value = float(ref) if ref is not None else None
        except (TypeError, ValueError):
            ref_value = None
        if ref_value is not None and math.isfinite(ref_value) and ref_value > 0:
            state["prediction_target"]["reference_price"] = _round(ref_value, 8)
            state["prediction_target"]["default_reference"] = "strategy_context.prediction_reference_price"
    return state


def is_ready(state: dict[str, Any], *, min_history_seconds: float) -> tuple[bool, str | None]:
    quality = state.get("data_quality") or {}
    if float(quality.get("history_seconds") or 0.0) < min_history_seconds:
        return False, "insufficient_history"
    if quality.get("futures_trade_age_ms") is None or int(quality["futures_trade_age_ms"]) > 5_000:
        return False, "stale_futures_market_data"
    target = state.get("prediction_target") or {}
    if not float(target.get("reference_price") or 0.0):
        return False, "missing_prediction_reference"
    market = state.get("market") or {}
    if not float(market.get("futures_price") or 0.0):
        return False, "missing_futures_price"
    return True, None
