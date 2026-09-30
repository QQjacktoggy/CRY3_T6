"""T6.1 additive entry rules; T6 remains the first candidate in every market.

This is a retrospective research candidate.  The durable risk epoch and
one-USDT execution are intentionally shared with T6.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from .regime_lane import FINGERPRINT as T6_FINGERPRINT, dec, select_side

PROFILE = "regime_target6_1_v1"
TIER = "REGIME_T61"
FALLBACK = {
    "continuation": ("fade_latest", "any", "DOWN", "0.35", "0.45"),
    "reversal": ("net_up", "any", "UP", "0.65", "0.75"),
    "late_move": ("skip", "any", "both", "0", "0"),
    "stall": ("original", "aligned", "both", "0.25", "0.45"),
    "flat": ("fade_latest", "any", "both", "0.45", "0.75"),
}
POLICY = {"profile": PROFILE, "parent_fingerprint": T6_FINGERPRINT,
          "fallback": FALLBACK, "unit_usdt": "1", "decision_ms": [124000, 126000],
          "expiry_ms": 136000, "risk_state_key": "regime_target6_risk_v1",
          "version": 1}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True,
                            separators=(",", ":")).encode()).hexdigest()


def select_fallback(features, original, base_result):
    """Select only after T6 has made a verified pre-order SKIP.

    Missing/malformed features, original identity errors, and unavailable
    depth are handled by the bridge as fail-closed input errors.
    """
    if base_result.get("allowed"):
        raise ValueError("T6 did not skip")
    state = base_result["state"]
    action, macro, scope, lower, upper = FALLBACK[state]
    result = {"state": state, "action": action, "branch": "fallback",
              "allowed": False, "reason": "state_disabled", "lower": lower,
              "upper": upper, "parent_skip_reason": base_result["reason"]}
    if action == "skip":
        return result
    if action == "original":
        # Reuse every T6 original-signal provenance and probability check.
        reference = select_side({**features, "first_bp": "0", "last_bp": "0"}, original)
        # The synthetic flat state calls T6's original action. It also validates
        # the exact T+120..123 cutoff and side/probability without placing an order.
        if not reference["allowed"]:
            return {**result, "reason": reference["reason"]}
        side = reference["side"]
        result["probability"] = reference["probability"]
    elif action == "net_up":
        move = dec(features["net_bp"])
        if move < Decimal("1"):
            return {**result, "reason": "net_below_1bp"}
        side = "UP"
    else:
        move = dec(features["last_bp"])
        if move == 0:
            return {**result, "reason": "zero_displacement"}
        side = "DOWN" if move > 0 else "UP"
    result["side"] = side
    if scope != "both" and side != scope:
        return {**result, "reason": "side_scope"}
    if macro == "aligned":
        prior = dec(features["prior_bp"])
        direction = "UP" if prior >= 1 else "DOWN" if prior <= -1 else "FLAT"
        if side != direction:
            return {**result, "reason": "prior_trend_filter"}
    return {**result, "allowed": True, "reason": "side_selected"}
