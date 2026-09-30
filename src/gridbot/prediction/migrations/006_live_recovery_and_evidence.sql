-- Crash-safe LIVE execution and evidence provenance.
-- This migration is additive for databases created by migrations 001-005.

ALTER TABLE prediction_loops ADD COLUMN mode TEXT NOT NULL DEFAULT 'SHADOW';
ALTER TABLE prediction_loops ADD COLUMN batch_id TEXT;
ALTER TABLE prediction_loops ADD COLUMN terminal_reason TEXT;
ALTER TABLE prediction_loops ADD COLUMN new_entries_stopped INTEGER NOT NULL DEFAULT 0;

ALTER TABLE prediction_order_intents ADD COLUMN submission_at_ms INTEGER;
ALTER TABLE prediction_order_intents ADD COLUMN ttl_deadline_ms INTEGER;
ALTER TABLE prediction_order_intents ADD COLUMN cancel_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_order_intents ADD COLUMN cancel_in_flight INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_order_intents ADD COLUMN cancel_last_attempt_at_ms INTEGER;
ALTER TABLE prediction_order_intents ADD COLUMN cancel_accepted_at_ms INTEGER;
ALTER TABLE prediction_order_intents ADD COLUMN cancel_last_error TEXT;

ALTER TABLE prediction_shadow_evidence ADD COLUMN content_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_evidence ADD COLUMN strategy_profile TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_evidence ADD COLUMN execution_identity TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_shadow_fills ADD COLUMN action TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_prediction_loops_mode_state
    ON prediction_loops(mode, state, updated_at_ms);
CREATE INDEX IF NOT EXISTS idx_prediction_intents_submission
    ON prediction_order_intents(submission_at_ms, ttl_deadline_ms, cancel_requested);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_evidence_identity
    ON prediction_shadow_evidence(evidence_identity, config_hash, window_start_ms, window_end_ms);
