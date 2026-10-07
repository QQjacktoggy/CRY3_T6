"""T6.9a: T6.7c seven Live lanes with the T6.8a First UP 5bp floor and a shallow
retracement counter-trend floor (prior 15m against the bet by >=5bp); Flat F1-F4 Shadow."""
import hashlib
import json

from .regime_t69_policy import FINGERPRINT as PARENT_FINGERPRINT
from .regime_t67c_policy import FINGERPRINT as LIVE_BASE_FINGERPRINT
from .regime_t67d_policy import FINGERPRINT as FLAT_FINGERPRINT
from .regime_t65_lane import FINGERPRINT as CORE_FINGERPRINT
from .regime_t67_policy import FINGERPRINT as RESEARCH_FINGERPRINT

PROFILE = 'regime_target6_9a_v1'
TIER = 'REGIME_T69A'
CORE_BRANCHES = ('core_first_down', 'core_first_up', 'core_stall_down',
                 'core_c_down', 'core_continuation_original')
NEW_BRANCHES = ('c_mirror_up_prior', 'shallow_retracement')
LIVE_BRANCHES = CORE_BRANCHES + NEW_BRANCHES
MARKETS = ('BTCUSDT', 'ETHUSDT', 'BNBUSDT')
FLAT_SHADOW_BRANCHES = ('flat_favorite', 'flat_quiet_favorite', 'flat_cheap_prior', 'flat_hold_180')
SHADOW_BRANCHES = FLAT_SHADOW_BRANCHES
POLICY = dict(
    profile=PROFILE, version=1, parent_fingerprint=PARENT_FINGERPRINT,
    report_revision='pr12_verified_fixed_20run_v1', execution_revision='pr9_atomic_claim_selected_readonly_v1', core_fingerprint=CORE_FINGERPRINT,
    research_fingerprint=RESEARCH_FINGERPRINT, live=LIVE_BRANCHES,
    live_base_fingerprint=LIVE_BASE_FINGERPRINT, live_base='T6.7c_seven_lanes',
    routing='freeze_T65_candidates_at_initial_book; reserve_nonempty_core; new_only_verified_empty',
    first_up_prior_min_bp='5',
    first_up_filter='frozen_positive_prior; preserve_original_core_reservation',
    new_priority=NEW_BRANCHES, one_market_one_buy=True,
    retired=('flat_original', 'A_jev_conflict', 'B_late_momentum', 'fallback', 'legacy_M_observers'),
    decision_ms=[124000, 126000], last_selection_ms=134500,
    entry_ms=[124000, 136000], quote_ttl_ms=2000,
    core_expiry_ms=136000, original_input_ms=[120000, 123000],
    book_max_age_ms=1000, units=[1, 2, 3],
    c_mirror_up_prior=dict(state='reversal', minute_abs_min_bp='0.5',
                           compounded_net_min_bp='1', prior_min_bp='1',
                           side='UP', price_band=['0.65', '0.70']),
    shallow_retracement=dict(opposite_minute_sign=True, first_abs_min_bp='1',
                             first_abs_to_last_abs_min='2', side='compounded_net',
                             price_band=['0.10', '0.75'], prior_against_min_bp='5'),
    flat_source_fingerprint=FLAT_FINGERPRINT,
    markets=MARKETS, market_binding='immutable_loop_symbol; isolated_asset_data',
    shadow='independent_public_quote_only; no_claim_no_live_risk_writes',
    shadow_branches=SHADOW_BRANCHES,
    flat_shadow=dict(
        gate='verified_empty_core; first_observed_checkpoint_immutable; records_live_overlap',
        flat_favorite=dict(source='T6.7d_flat_favorite_rule', minute_abs_max_exclusive_bp='0.5',
                           initial_ms=[124000, 126000], confirmation_ms=[128000, 129500],
                           favorite='stable_actual_ask_no_ties', price_band=['0.65', '0.80'],
                           promotion='manual_after_regime_scoreboard_evidence'),
        flat_quiet_favorite=dict(minute_abs_max_exclusive_bp='1', net_abs_max_exclusive_bp='1',
                                 prior_abs_max_exclusive_bp='5', excludes='flat_favorite_state',
                                 initial_ms=[124000, 126000], confirmation_ms=[128000, 129500],
                                 favorite='stable_actual_ask_no_ties', price_band=['0.62', '0.80']),
        flat_cheap_prior=dict(net_abs_max_exclusive_bp='1', prior_abs_min_bp='1',
                              initial_ms=[124000, 126000], side='initial_cheaper_tie_down_aligned_prior',
                              quote_ms=[128000, 134500], quote='first_executable_observed',
                              price_band=['0.25', '0.40']),
        flat_hold_180=dict(minute_abs_max_exclusive_bp='0.5', third_minute='binance_spot_120s_to_180s',
                           spot_max_gap_ms=1500, initial_ms=[120000, 121500],
                           confirmation_ms=[180000, 181500], favorite='stable_actual_ask_no_ties',
                           price_band=['0.70', '0.85'], after='core_entry_window'),
    ),
    risk_state_key='regime_target6_risk_v1', loop_mdd_1u='3.5',
    shadow_retired=('external_lead_lag', 'reference_value'),
    validation_mode='t67c_seven_live; first_up_prior_5bp; shallow_prior_against_5bp; no_reference_180_backfill; flat_f1_f4_shadow_only; no_profit_or_fill_forecast',
)
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()
