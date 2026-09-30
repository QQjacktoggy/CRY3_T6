-- Immutable authority record for the balanced_hold live promotion.
-- This is trade-DB provenance only; Telegram commands remain in the separate
-- prediction control SQLite.
CREATE TABLE IF NOT EXISTS prediction_promotion_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    strategy_profile TEXT NOT NULL,
    mode TEXT NOT NULL CHECK(mode = 'SHADOW'),
    evidence_identity TEXT NOT NULL,
    strategy_config_hash TEXT NOT NULL,
    release_fingerprint TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    generated_at_ms INTEGER NOT NULL,
    eligible INTEGER NOT NULL CHECK(eligible IN (0, 1)),
    reasons_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prediction_promotion_snapshots_latest
    ON prediction_promotion_snapshots(strategy_profile, generated_at_ms DESC, created_at_ms DESC);

CREATE TRIGGER IF NOT EXISTS prediction_promotion_snapshots_immutable_update
BEFORE UPDATE ON prediction_promotion_snapshots
BEGIN
    SELECT RAISE(ABORT, 'prediction promotion snapshots are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_promotion_snapshots_immutable_delete
BEFORE DELETE ON prediction_promotion_snapshots
BEGIN
    SELECT RAISE(ABORT, 'prediction promotion snapshots are immutable');
END;
