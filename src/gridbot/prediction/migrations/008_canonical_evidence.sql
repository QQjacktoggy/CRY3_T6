-- Canonical balanced_hold evidence is bound at collection time.  Empty
-- values preserve read-only access to old research rows but make them
-- ineligible for LIVE promotion; no migration may retrospectively stamp an
-- old lane with the current strategy or release.

ALTER TABLE prediction_shadow_windows ADD COLUMN lane TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_windows ADD COLUMN strategy_identity_json TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_windows ADD COLUMN execution_identity_json TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_windows ADD COLUMN collection_release_fingerprint TEXT NOT NULL DEFAULT '';

ALTER TABLE prediction_shadow_campaigns ADD COLUMN lane TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_campaigns ADD COLUMN strategy_identity_json TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_campaigns ADD COLUMN execution_identity_json TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_campaigns ADD COLUMN collection_release_fingerprint TEXT NOT NULL DEFAULT '';

ALTER TABLE prediction_shadow_evidence ADD COLUMN strategy_identity_json TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_evidence ADD COLUMN collection_release_fingerprint TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_prediction_shadow_windows_lane_identity
    ON prediction_shadow_windows(lane, collection_release_fingerprint, window_end_ms);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_evidence_canonical
    ON prediction_shadow_evidence(strategy_profile, collection_release_fingerprint, window_end_ms);
