"""T6.6 prospective observations. This module cannot authorize a Live order."""
import hashlib
import json
from decimal import Decimal

from .regime_lane import dec, state_of
from .regime_t65_lane import FINGERPRINT as CORE_FINGERPRINT, candidates as core_candidates

PROFILE = "regime_t66_observe_v1"
RETIRED_KEYS = ("shadow_a", "shadow_flat", "shadow_b", "shadow_fallback")
BRANCHES = {
    "M6a": "M6a 趨勢一致",
    "M8_UP": "M8 UP 停滯順勢",
    "M4a": "M4a 趨勢一致",
    "M7_DOWN": "M7 DOWN 延續",
    "M4_control": "M4 原規則對照",
    "M6_control": "M6 原規則對照",
    "M7_UP_control": "M7 UP 方向對照",
    "M8_DOWN_control": "M8 DOWN 方向對照",
}
POLICY = {
    "profile": PROFILE, "core_fingerprint": CORE_FINGERPRINT,
    "live": "unchanged_T65_no_new_live_branch", "unit": "1",
    "retired_shadow_keys": RETIRED_KEYS,
    "M6a": ["abs_compounded_net<1", "initial_cheaper_tie_down", "aligned_prior>=1", ".25", ".40"],
    "M4a": ["opposite_sign", "abs_first>=1", ".5<=abs_last<=.5*abs_first", "aligned_prior>=1", ".15", ".55"],
    "M8_UP": ["first>=.5", "abs_last<.5", "prior>=1", ".55", ".75"],
    "M7_DOWN": ["first<=-.5", "last<=-.5", "net<=-1", "prior<=-1", ".55", ".75"],
    "controls": ["M4", "M6", "M7_UP", "M8_DOWN"],
    "new_branch_gate": "all_frozen_core_candidates_empty",
    "freeze_ms": [124000, 126000], "quote_ms": [128000, 134500],
    "book_max_age_ms": 1000, "latency_ms": [300, 1000], "delay_deadline_ms": 135500,
    "priority": ["M6a", "M4a", "M8_UP", "M7_DOWN"],
    "prospective_target_markets": 500, "promotion": "manual_only_minimum_30_resolved_quotes_not_sufficient_alone",
    "version": 1,
}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()


def candidates(features, original, initial):
    """Return frozen core plus observations; never append observations to core."""
    core, controls = core_candidates(features, original, initial, Decimal(1))
    first, last, prior = (dec(features[k]) for k in ("first_bp", "last_bp", "prior_bp"))
    net = ((1 + first / 10000) * (1 + last / 10000) - 1) * 10000
    state = state_of(first, last)
    result = {}

    def add(name, side, lower, upper, control=False):
        result[name] = {
            "branch": name, "side": side, "action": name, "state": state,
            "lower": lower, "upper": upper, "cap": upper,
            "control": control, "core_overlap": bool(core),
            "incremental_eligible": not core,
            "gate": "core_candidate_present" if core else "core_empty",
        }

    for key, name in (("shadow_m4", "M4_control"), ("shadow_m6", "M6_control")):
        shadow = controls.get(key)
        if shadow:
            c = shadow["candidate"]
            add(name, c["side"], c["lower"], c["upper"], True)
            aligned = (c["side"] == "UP" and prior >= 1) or (c["side"] == "DOWN" and prior <= -1)
            if aligned:
                add("M4a" if key == "shadow_m4" else "M6a", c["side"], c["lower"], c["upper"])
    if state == "stall" and abs(prior) >= 1 and first * prior > 0:
        side = "UP" if first > 0 else "DOWN"
        add("M8_UP" if side == "UP" else "M8_DOWN_control", side, ".55", ".75", side == "DOWN")
    if state == "continuation" and abs(net) >= 1 and abs(prior) >= 1 and net * prior > 0:
        side = "UP" if net > 0 else "DOWN"
        add("M7_DOWN" if side == "DOWN" else "M7_UP_control", side, ".55", ".75", side == "UP")
    return core, result
