"""Experimental ETH paper policy; the BTC parent and frozen experiment stay intact."""
import hashlib
import json

from .regime_t67c_policy import FINGERPRINT as BTC_FINGERPRINT

PROFILE = 'eth_t67c_shadow_v1'
SYMBOL = 'ETHUSDT'
SLOT_MS = 300_000
BASELINE_COMMIT = 'a3cd39896437149775a9f3273f8df037e776d17b'
POLICY = dict(
    profile=PROFILE, symbol=SYMBOL, mode='SHADOW', experimental=True,
    btc_parent_fingerprint=BTC_FINGERPRINT, baseline_commit=BASELINE_COMMIT,
    branches=['core_first_down', 'core_first_up', 'core_stall_down', 'core_c_down',
              'c_mirror_up_prior', 'shallow_retracement'],
    unavailable_branches={'core_continuation_original': 'ETH original probability producer not validated',
                          'external_lead_lag': 'not included in phase one',
                          'reference_value': 'not included in phase one'},
    frozen_btc_thresholds='candidate hypotheses only; no ETH calibration claim',
    feature_ms=[120000, 123000], initial_ms=[124000, 126000],
    last_selection_ms=134500, expiry_ms=136000, book_max_age_ms=1000,
    additive_ttl_ms=2000, nominal_unit_usdt='1', share_step='0.01',
    execution='PAPER_QUOTE_ONLY; never fill, claim, intent, order, redeem or Live',
    requires='reviewed ETH market specification and matching official metadata',
    version=1,
)
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                    separators=(',', ':')).encode()).hexdigest()
