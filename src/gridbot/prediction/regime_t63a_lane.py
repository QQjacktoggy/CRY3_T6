"""T6.3a: execute T6/A/B/C; retain eligible T6.1 fallback as paper evidence."""
import hashlib
import json

from .regime_t63_lane import FINGERPRINT as PARENT, candidates as parent_candidates
from .regime_t63_lane import eligible_execution

PROFILE = "regime_target6_3a_v1"
TIER = "REGIME_T63A"
POLICY = {
    "profile": PROFILE,
    "parent_fingerprint": PARENT,
    "live_branches": ["T6", "A_jev_conflict", "B_late_momentum", "C_reversal_netdown"],
    "shadow_branch": "fallback",
    "shadow_pricing": "initial_executable_book_only_no_assumed_fill",
    "risk_state_key": "regime_target6_risk_v1",
    "version": 1,
}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()


def candidates(features, original, snapshot, amount):
    """Return unchanged T6.3 live choices except fallback, plus frozen paper quote.

    The parent decides eligibility and priority. An eligible fallback still
    suppresses B/C and can coexist with a higher-priority A. The paper record
    is a quoted opportunity, never a live order or fill.
    """
    choices = parent_candidates(features, original, snapshot, amount)
    live = []
    shadow = None
    for candidate in choices:
        if candidate["branch"] != "fallback":
            live.append(candidate)
            continue
        execution = eligible_execution(candidate, snapshot, amount)
        shadow = {
            "branch": "fallback",
            "candidate": dict(candidate),
            "quote": {
                "cash": str(execution["cash"]),
                "net_shares": str(execution["net_shares"]),
                "limit": str(execution["limit"]),
                "fee_bps": str(snapshot["fee_bps"]),
            },
            "fill_status": "PAPER_QUOTE_ONLY",
        }
    return live, shadow
