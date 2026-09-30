"""T6.2: retain T6.1 routing, cap primary flat entries at 0.60."""
from __future__ import annotations

import hashlib
import json

from .regime_lane import select_side
from .regime_t61_lane import FINGERPRINT as PARENT_FINGERPRINT

PROFILE = "regime_target6_2_v1"
TIER = "REGIME_T62"
FLAT_PRIMARY_CAP = "0.60"
POLICY = {
    "profile": PROFILE,
    "parent_fingerprint": PARENT_FINGERPRINT,
    "primary_flat_upper": FLAT_PRIMARY_CAP,
    "fallback": "unchanged_t61",
    "allowed_units_usdt": ["1", "2", "3"],
    "risk_accounting": "pnl_per_claimed_unit_v1",
    "risk_state_key": "regime_target6_risk_v1",
    "version": 1,
}
FINGERPRINT = hashlib.sha256(json.dumps(
    POLICY, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def select_primary(features, original):
    result = select_side(features, original)
    if result["state"] == "flat":
        result["upper"] = FLAT_PRIMARY_CAP
    return result
