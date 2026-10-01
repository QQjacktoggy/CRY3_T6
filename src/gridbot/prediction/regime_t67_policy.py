"""Frozen T6.7 experimental Live policy, sharing the existing T6 risk epoch."""
import hashlib
import json

PROFILE = 'regime_target6_7_v1'
TIER = 'REGIME_T67'
BRANCHES = ('external_lead_lag', 'reference_value', 'shallow_retracement', 'c_mirror_up_prior')
POLICY = {
    'profile': PROFILE, 'version': 2, 'branches': BRANCHES,
    'routing': 'first_eligible; simultaneous_priority_in_branch_order; one_market_one_buy',
    'shadow': False, 'units': [1, 2, 3],
    'entry_ms': [60000, 270000], 'quote_ttl_ms': 2000,
    'book_max_age_ms': 1000, 'spot_max_age_ms': 1500,
    'price_band': ['0.10', '0.75'],
    'model': 'official_reference_anchored_proxy; causal_same_generation_log_return_variance',
    'opening_proxy_ms': [0, 1500], 'history_ms': 900000,
    'minimum_returns': 20, 'minimum_history_span_ms': 60000,
    'sigma_floor_bp_sqrt_second': '0.35',
    'minimum_ev_per_unit': '0.03', 'stress_price_add': '0.02',
    'minimum_stress_ev_per_unit': '0.005',
    'reference_value_ms': [60000, 120000, 180000, 240000], 'slot_grace_ms': 1500,
    'lead_lag': {'lookback_ms': 1000, 'move_bp': '2', 'ask_move_max': '0.01',
                 'confirm_ms': [300, 1000], 'confirmation_grace_ms': 300, 'maximum_wait_ms': 2000,
                 'triggers_per_market': 1},
    'retracement': {'first_abs_min_bp': '1', 'first_abs_to_last_abs_min': '2',
                    'opposite_minute_sign': True, 'side': 'compounded_net',
                    'entry_ms': [124000, 134500]},
    'c_mirror_up_prior': {'state': 'reversal', 'minute_abs_min_bp': '0.5',
                          'compounded_net_min_bp': '1', 'prior_min_bp': '1',
                          'side': 'UP', 'price_band': ['0.65', '0.75'],
                          'initial_decision_ms': [124000, 126000],
                          'entry_ms': [124000, 134500], 'submission_deadline_ms': 136000,
                          'guard': 'freeze_T65_core_empty_at_initial_book; no_paid_signal_needed_for_reversal',
                          'priority': 'after_existing_T67_branches'},
    'risk_state_key': 'regime_target6_risk_v1', 'loop_mdd_1u': '3.5',
    'validation_mode': 'experimental_live; no_profit_or_fill_claim',
}
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()
