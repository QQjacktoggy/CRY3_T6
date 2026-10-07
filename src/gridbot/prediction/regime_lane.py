"""Frozen regime-target6-robust-v1 policy. No exchange or outcome inputs."""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_DOWN

PROFILE = "regime_target6_v1"
STATE_KEY = "regime_target6_risk_v1"
SLOT_MS = 300_000
RULES = {
    "continuation": ("original", "opposed", "both", "0.15", "0.25"),
    "reversal": ("first", "aligned", "both", "0.15", "0.45"),
    "late_move": ("skip", "any", "both", "0", "0"),
    "stall": ("fade_latest", "opposed", "DOWN", "0.25", "0.65"),
    "flat": ("original", "any", "both", "0.45", "0.65"),
}
POLICY = {"profile": PROFILE, "rules": RULES, "epsilon_bp": "0.5",
          "prior_minutes": 15, "prior_neutral_bp": "1", "unit": "1",
          "decision_ms": [124000, 126000], "expiry_ms": 136000,
          "cumulative_loss": "-6", "block_mdd": "3.5", "block_slots": 20,
          "automatic_recovery": False, "version": 1}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True,
                           separators=(",", ":")).encode()).hexdigest()


def dec(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("nonfinite input")
    return result


def state_of(first, last):
    first, last = dec(first), dec(last)
    a, b = abs(first) >= Decimal("0.5"), abs(last) >= Decimal("0.5")
    return ("continuation" if first * last > 0 else "reversal") if a and b else (
        "late_move" if b else "stall" if a else "flat")


def freeze_features(start, candles, received_at_ms, *, symbol="BTCUSDT"):
    """Require all 17 contiguous closed candles received before T+123."""
    from .loop_market import symbol as valid_symbol
    symbol = valid_symbol(symbol)
    if start <= 0 or start % SLOT_MS or not start + 120000 <= received_at_ms <= start + 123000:
        raise ValueError("feature cutoff missed")
    if len(candles) != 17:
        raise ValueError("17 completed candles required")
    for i, candle in enumerate(candles):
        opening = start - 900000 + i * 60000
        if int(candle[0]) != opening or int(candle[6]) != opening + 59999:
            raise ValueError("candle identity or continuity mismatch")
        if min(dec(candle[1]), dec(candle[4])) <= 0:
            raise ValueError("invalid spot price")
    bp = lambda opening, closing: str((dec(closing) / dec(opening) - 1) * 10000)
    return {"market_start_ms": start, "received_at_ms": received_at_ms,
            "cutoff_ms": start + 120000,
            "first_bp": bp(candles[15][1], candles[15][4]),
            "last_bp": bp(candles[16][1], candles[16][4]),
            "net_bp": bp(candles[15][1], candles[16][4]),
            "prior_bp": bp(candles[0][1], candles[14][4]),
            "source": f"Binance Spot {symbol} 1m", "symbol": symbol, "fingerprint": FINGERPRINT}


def select_side(features, original):
    state = state_of(features["first_bp"], features["last_bp"])
    action, macro, scope, lower, upper = RULES[state]
    result = {"state": state, "action": action, "allowed": False,
              "reason": "state_disabled", "lower": lower, "upper": upper}
    if action == "skip":
        return result
    if action == "original":
        start = int(features["market_start_ms"])
        if (not original or original.get("status") != "entry_positive_cost_after_ev"
                or not start + 120000 <= int(original["completed_at_ms"]) <= start + 123000
                or int(original["cutoff_ms"]) != start + 120000
                or int(original["market_start_ms"]) != start):
            return {**result, "reason": "original_not_approved"}
        side = original["entry"]["side"]
        p_up = dec(original["original_p_up"])
        if side not in {"UP", "DOWN"} or not 0 <= p_up <= 1:
            raise ValueError("invalid original probability/side")
        result["probability"] = str(p_up if side == "UP" else 1-p_up)
    else:
        move = dec(features["first_bp"] if action == "first" else features["last_bp"])
        if move == 0:
            return {**result, "reason": "zero_displacement"}
        side = "UP" if move > 0 else "DOWN"
        if action == "fade_latest":
            side = "DOWN" if side == "UP" else "UP"
    result["side"] = side
    if scope != "both" and side != scope:
        return {**result, "reason": "side_scope"}
    if macro != "any":
        prior = dec(features["prior_bp"])
        direction = "UP" if prior >= 1 else "DOWN" if prior <= -1 else "FLAT"
        if direction == "FLAT" or ((macro == "aligned") != (side == direction)):
            return {**result, "reason": "prior_trend_filter"}
    return {**result, "allowed": True, "reason": "side_selected"}


def walk(levels, fee_bps, cap=Decimal("0.90"), amount=Decimal("1")):
    """Walk the full requested cash depth; fee is removed from shares once."""
    fee = dec(fee_bps) / 10000
    amount = dec(amount)
    if amount not in (Decimal("1"), Decimal("2"), Decimal("3")):
        raise ValueError("unsupported regime order amount")
    if not 0 <= fee <= 1:
        raise ValueError("invalid fee")
    parsed = [(dec(x[0]), dec(x[1])) for x in levels]
    if not parsed or any(not 0 < p < 1 or q <= 0 for p, q in parsed):
        raise ValueError("invalid ask depth")
    if parsed != sorted(parsed):
        raise ValueError("unsorted ask depth")
    cash = gross = net = Decimal(0)
    worst = None
    for p, q in parsed:
        if p > cap:
            break
        take = min(q, (amount-cash)/p)
        cash += take*p
        gross += take
        net += take * (1 - fee*min(p, 1-p)/p)
        worst = p
        if cash >= amount-Decimal("0.0000000001"):
            break
    if cash < amount-Decimal("0.0000000001"):
        raise ValueError("insufficient requested depth")
    # Exact executable 0.01-share floor, preserving the depth walk.
    remaining = gross.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    cash = net = Decimal(0)
    for p, q in parsed:
        take = min(q, remaining)
        cash += take*p
        net += take*(1-fee*min(p, 1-p)/p)
        remaining -= take
        if remaining <= 0:
            break
    return {"cash": cash, "net_shares": net, "limit": worst}


def risk_result(state, settlements, start, now, *, unresolved=False, unknown=False):
    """All lane loops; triggers persist until an audited operator reset moves the risk epoch."""
    if state.get("fingerprint") != FINGERPRINT:
        return False, "policy_fingerprint_mismatch"
    anchor = int(state["first_market_start_ms"])
    if start < anchor or (start-anchor) % SLOT_MS:
        return False, "market_time_discontinuous"
    # An audited operator reset moves only the risk-counting start; the
    # 20-run block grid stays on the original anchor.
    epoch = int(state.get("risk_epoch_start_ms") or anchor)
    if epoch < anchor or (epoch-anchor) % SLOT_MS:
        return False, "risk_epoch_invalid"
    if start < epoch:
        return False, "market_before_risk_epoch"
    if unknown:
        state["halt_reason"] = state.get("halt_reason") or "unknown_order_reconciliation_required"
    equity = normalized_equity = Decimal(0)
    blocks, seen = {}, set()
    for row in sorted(settlements, key=lambda r: (r.known_at_ms, r.settlement_id)):
        if row.settlement_id in seen or row.known_at_ms > now or row.market_start_ms >= start:
            return False, "invalid_settlement_provenance"
        seen.add(row.settlement_id)
        delta = row.market_start_ms-anchor
        unit = dec(row.unit_usdt)
        if delta < 0 or delta % SLOT_MS or unit not in (1, 2, 3):
            return False, "mixed_lane_provenance"
        pnl = dec(row.net_pnl_usdt)
        equity += pnl
        if row.market_start_ms < epoch:
            continue
        normalized = pnl / unit
        normalized_equity += normalized
        block = delta // SLOT_MS // 20
        e, peak = blocks.get(block, (Decimal(0), Decimal(0)))
        e += normalized
        peak = max(peak, e)
        blocks[block] = (e, peak)
        if peak-e >= Decimal("3.5"):
            state["halt_reason"] = state.get("halt_reason") or "scheduled20_mdd_3.5"
        if normalized_equity <= -6:
            state["halt_reason"] = state.get("halt_reason") or "cumulative_loss_6"
    state["net_pnl_usdt"] = str(equity)
    state["risk_equity_1u"] = str(normalized_equity)
    state["risk_accounting"] = "pnl_per_claimed_unit_v1"
    if state.get("halt_reason"):
        return False, str(state["halt_reason"])
    if unresolved:
        return False, "prior_exposure_or_settlement_pending"
    return True, "persistent_risk_pass"
