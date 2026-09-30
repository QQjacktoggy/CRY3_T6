"""T6.3b: T6/A/C Live, B and fallback frozen as paper observations."""
import hashlib
import json

from .regime_t63_lane import eligible_execution
from .regime_t63a_lane import FINGERPRINT as PARENT, candidates as parent_candidates

PROFILE = "regime_target6_3b_v1"
TIER = "REGIME_T63B"
POLICY = {
    "profile": PROFILE,
    "parent_fingerprint": PARENT,
    "live_branches": ["T6", "A_jev_conflict", "C_reversal_netdown"],
    "shadow_branches": ["fallback", "B_late_momentum"],
    "shadow_pricing": "initial_executable_book_only_no_assumed_fill",
    "loop_mdd_1u": "3.5",
    "loop_mdd_accounting": "official_fee_net_pnl_per_claimed_unit_at_observation",
    "risk_state_key": "regime_target6_risk_v1",
    "version": 1,
}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()


def _paper(candidate, snapshot, amount):
    try:
        execution = eligible_execution(candidate, snapshot, amount)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return {"branch": candidate["branch"], "candidate": dict(candidate),
                "quote": None, "fill_status": "NO_EXECUTABLE_INITIAL_QUOTE"}
    return {"branch": candidate["branch"], "candidate": dict(candidate),
            "quote": {"cash": str(execution["cash"]),
                      "net_shares": str(execution["net_shares"]),
                      "limit": str(execution["limit"]),
                      "fee_bps": str(snapshot["fee_bps"])},
            "fill_status": "PAPER_QUOTE_ONLY"}


def candidates(features, original, snapshot, amount):
    """Retain T6.3a priority; excluded branches do not promote replacements."""
    live, fallback = parent_candidates(features, original, snapshot, amount)
    retained = []
    shadow_b = None
    for choice in live:
        if choice["branch"] == "B_late_momentum":
            shadow_b = _paper(choice, snapshot, amount)
        else:
            retained.append(choice)
    return retained, fallback, shadow_b
