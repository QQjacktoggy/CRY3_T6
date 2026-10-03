"""T6.7d preserves T6.5 execution; additions require verified core-empty."""
import hashlib
import json

from .regime_t67c_policy import FINGERPRINT as PARENT_FINGERPRINT
from .regime_t65_lane import FINGERPRINT as CORE_FINGERPRINT
from .regime_t67_policy import FINGERPRINT as RESEARCH_FINGERPRINT

PROFILE = 'regime_target6_7d_v1'
TIER = 'REGIME_T67D'
CORE_BRANCHES = ('core_first_down', 'core_first_up', 'core_stall_down',
                 'core_c_down', 'core_continuation_original')
NEW_BRANCHES = ('c_mirror_up_prior', 'shallow_retracement', 'flat_favorite')
LIVE_BRANCHES = CORE_BRANCHES + NEW_BRANCHES
SHADOW_BRANCHES = ('external_lead_lag', 'reference_value')
POLICY = dict(
    profile=PROFILE, version=1, parent_fingerprint=PARENT_FINGERPRINT,
    report_revision='pr12_verified_fixed_20run_v1', execution_revision='pr9_atomic_claim_selected_readonly_v1', core_fingerprint=CORE_FINGERPRINT,
    research_fingerprint=RESEARCH_FINGERPRINT, live=LIVE_BRANCHES,
    routing='freeze_T65_candidates_at_initial_book; reserve_nonempty_core; new_only_verified_empty',
    new_priority=NEW_BRANCHES, one_market_one_buy=True,
    retired=('flat_original', 'A_jev_conflict', 'B_late_momentum', 'fallback', 'legacy_M_observers'),
    decision_ms=[124000, 126000], last_selection_ms=134500,
    entry_ms=[124000, 136000], quote_ttl_ms=2000,
    core_expiry_ms=136000, original_input_ms=[120000, 123000],
    book_max_age_ms=1000, units=[1, 2, 3],
    c_mirror_up_prior=dict(state='reversal', minute_abs_min_bp='0.5',
                           compounded_net_min_bp='1', prior_min_bp='1',
                           side='UP', price_band=['0.65', '0.75']),
    shallow_retracement=dict(opposite_minute_sign=True, first_abs_min_bp='1',
                             first_abs_to_last_abs_min='2', side='compounded_net',
                             price_band=['0.10', '0.75']),
    flat_favorite=dict(first_abs_max_exclusive_bp='0.5', last_abs_max_exclusive_bp='0.5',
                       confirmation_ms=[128000, 129500], favorite='stable_actual_ask_no_ties',
                       checkpoint='first_fresh_valid_immutable; no_later_price_search',
                       price_band=['0.65', '0.80'], limit='frozen_requested_depth_limit',
                       core='verified_empty_only', ttl_ms=2000),
    shadow='independent_public_quote_only; no_claim_no_live_risk_writes',
    shadow_branches=SHADOW_BRANCHES,
    risk_state_key='regime_target6_risk_v1', loop_mdd_1u='3.5',
    validation_mode='new_branches_experimental_live; shadow_quotes_are_not_fills',
)
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()
