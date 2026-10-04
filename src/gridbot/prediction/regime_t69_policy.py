"""T6.9 keeps T6.8a and adds the verified Flat favorite; three loop markets."""
import hashlib
import json

from .regime_t68a_policy import FINGERPRINT as PARENT_FINGERPRINT
from .regime_t67d_policy import FINGERPRINT as FLAT_FINGERPRINT
from .regime_t65_lane import FINGERPRINT as CORE_FINGERPRINT
from .regime_t67_policy import FINGERPRINT as RESEARCH_FINGERPRINT

PROFILE = 'regime_target6_9_v1'
TIER = 'REGIME_T69'
CORE_BRANCHES = ('core_first_down', 'core_first_up', 'core_stall_down',
                 'core_c_down', 'core_continuation_original')
NEW_BRANCHES = ('c_mirror_up_prior', 'shallow_retracement')
FLAT_BRANCHES = ('flat_favorite',)
LIVE_BRANCHES = CORE_BRANCHES + NEW_BRANCHES + FLAT_BRANCHES + ('reference_180_mid',)
MARKETS = ('BTCUSDT', 'ETHUSDT', 'BNBUSDT')
FLAT_SHADOW_BRANCHES = ('flat_quiet_favorite', 'flat_cheap_prior', 'flat_hold_180')
SHADOW_BRANCHES = ('external_lead_lag', 'reference_value') + FLAT_SHADOW_BRANCHES
POLICY = dict(
    profile=PROFILE, version=1, parent_fingerprint=PARENT_FINGERPRINT,
    report_revision='pr12_verified_fixed_20run_v1', execution_revision='pr9_atomic_claim_selected_readonly_v1', core_fingerprint=CORE_FINGERPRINT,
    research_fingerprint=RESEARCH_FINGERPRINT, live=LIVE_BRANCHES,
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
                           side='UP', price_band=['0.65', '0.75']),
    shallow_retracement=dict(opposite_minute_sign=True, first_abs_min_bp='1',
                             first_abs_to_last_abs_min='2', side='compounded_net',
                             price_band=['0.10', '0.75']),
    flat_source_fingerprint=FLAT_FINGERPRINT,
    flat_favorite=dict(first_abs_max_exclusive_bp='0.5', last_abs_max_exclusive_bp='0.5',
                       confirmation_ms=[128000, 129500], favorite='stable_actual_ask_no_ties',
                       checkpoint='first_fresh_valid_immutable; no_later_price_search',
                       price_band=['0.65', '0.80'], limit='frozen_requested_depth_limit',
                       core='verified_empty_only; after_additions', ttl_ms=2000,
                       reference_after='selected_flat_blocks_180s_backfill'),
    markets=MARKETS, market_binding='immutable_loop_symbol; isolated_asset_data',
    reference_checkpoints_ms=(60000, 120000, 180000, 240000),
    reference_checkpoint_grace_ms=1500,
    reference_backfill_cost_band=('.40', '.65'),
    reference_backfill_mode='live',
    reference_backfill_ms=180000,
    reference_last_selection_ms=181500,
    reference_entry_ms=[180000, 183500],
    reference_backfill_guard='verified_core_empty; no_selected_decision; no_claim_intent_position_unknown',
    checkpoint_model='T67_reference_probability_asof; complete_book; causal_input_summary',
    checkpoint_max_rows=20000, checkpoint_max_payload_bytes=32*1024*1024,
    shadow='independent_public_quote_only; no_claim_no_live_risk_writes',
    shadow_branches=SHADOW_BRANCHES,
    flat_shadow=dict(
        gate='verified_empty_core; first_observed_checkpoint_immutable; records_live_overlap',
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
                           price_band=['0.70', '0.85'], after='reference_180_mid'),
    ),
    risk_state_key='regime_target6_risk_v1', loop_mdd_1u='3.5',
    validation_mode='retained_seven_live; flat_favorite_live; reference_180_mid_live; flat_f2_f4_shadow_only; no_profit_or_fill_forecast',
)
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()
