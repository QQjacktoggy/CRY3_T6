-- Final Shadow evidence and official dual-winner/no-fill accounting.
-- These rows are append-only.  An exact-tie market has no single local
-- winner; it may be terminally accounted for only when the lane ledger proves
-- that it had no fills, actions, or cost.

ALTER TABLE prediction_shadow_evidence ADD COLUMN evidence_kind TEXT NOT NULL DEFAULT 'INTERIM';

CREATE TABLE IF NOT EXISTS prediction_shadow_exclusions (
    exclusion_id TEXT PRIMARY KEY,
    shadow_campaign_id TEXT NOT NULL UNIQUE REFERENCES prediction_shadow_campaigns(shadow_campaign_id),
    campaign_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    exclusion_type TEXT NOT NULL CHECK(exclusion_type = 'DUAL_WINNER_NO_FILL'),
    official_outcomes_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    simulated_fees TEXT NOT NULL DEFAULT '0',
    simulated_pnl TEXT NOT NULL DEFAULT '0',
    excluded_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    CHECK(window_start_ms <= window_end_ms)
);

CREATE INDEX IF NOT EXISTS idx_prediction_shadow_exclusions_window
    ON prediction_shadow_exclusions(mode, config_hash, window_start_ms, window_end_ms, excluded_at_ms);

CREATE TRIGGER IF NOT EXISTS prediction_shadow_exclusions_immutable_update
BEFORE UPDATE ON prediction_shadow_exclusions
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow exclusions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_exclusions_immutable_delete
BEFORE DELETE ON prediction_shadow_exclusions
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow exclusions are immutable');
END;
