"""T6.5: filtered T6/C live; frozen T6.4 additions remain quote-only shadow."""
import hashlib
import json
from decimal import Decimal

from .regime_lane import dec, state_of
from .regime_t63b_lane import FINGERPRINT as PARENT, _paper, candidates as parent_candidates

PROFILE = "regime_target6_5_v1"
TIER = "REGIME_T65"
POLICY = {
    "profile": PROFILE, "parent_fingerprint": PARENT,
    "live": "T6_except_flat_original_and_C; preserve_parent_priority",
    "shadow": ["A_jev_conflict", "T6_flat_original", "B_late_momentum", "fallback",
               "M4_first_pullback", "M6_neutral_cheap"],
    "M4": ["opposite_minute_sign", "abs_first>=1", "0.5<=abs_last<=0.5*abs_first",
           "first_side", "0.15", "0.55"],
    "M6": ["abs_compounded_net<1", "initial_cheaper_side_tie_down", "0.25", "0.40"],
    "shadow_pricing": "no_assumed_fill; M4_M6_first_fresh_executable_quote_from_128s",
    "decision_ms": [124000, 126000], "shadow_new_branch_ms": 128000,
    "last_selection_ms": 134500, "expiry_ms": 136000, "book_max_age_ms": 1000,
    "units": [1, 2, 3], "recommended_initial_unit": 1,
    "loop_mdd_1u": "3.5", "risk_state_key": "regime_target6_risk_v1", "version": 1,
}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()


def candidates(features, original, snapshot, amount):
    choices, fallback, shadow_b = parent_candidates(features, original, snapshot, amount)
    live, shadow = [], {"shadow_fallback": fallback, "shadow_b": shadow_b}
    for choice in choices:
        if choice["branch"] == "A_jev_conflict":
            shadow["shadow_a"] = _paper(choice, snapshot, amount)
        elif choice["branch"] == "T6" and choice["state"] == "flat" and choice["action"] == "original":
            shadow["shadow_flat"] = _paper(choice, snapshot, amount)
        else:
            live.append(choice)
    first, last = (dec(features[k]) for k in ("first_bp", "last_bp"))
    net = ((1 + first / 10000) * (1 + last / 10000) - 1) * 10000

    def add(key, branch, side, lower, upper):
        candidate = dict(branch=branch, action=branch, side=side, lower=lower, upper=upper,
                         cap=upper, state=state_of(first, last), allowed=True, reason="t65_shadow_candidate")
        shadow[key] = dict(branch=branch, candidate=candidate, quote=None,
                           fill_status="AWAITING_SHADOW_WINDOW")

    if first * last < 0 and abs(first) >= 1 and Decimal(".5") <= abs(last) <= Decimal(".5") * abs(first):
        add("shadow_m4", "M4_first_pullback", "UP" if first > 0 else "DOWN", ".15", ".55")
    if abs(net) < 1:
        up, down = (snapshot["quote"][s]["ask_levels"] for s in ("UP", "DOWN"))
        if up and down:
            add("shadow_m6", "M6_neutral_cheap", "UP" if dec(up[0][0]) < dec(down[0][0]) else "DOWN", ".25", ".40")
    return live, shadow
