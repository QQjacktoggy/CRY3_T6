ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN feature_snapshot_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN input_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN source_watermark_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN state_before_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN state_after_digest TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS prediction_moe_router_state (
    config_hash TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    state_digest TEXT NOT NULL,
    last_decision_id TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prediction_moe_arm_entries (
    config_hash TEXT NOT NULL,
    market_id TEXT NOT NULL,
    expert TEXT NOT NULL,
    risk_policy TEXT NOT NULL,
    decision_id TEXT NOT NULL REFERENCES prediction_moe_shadow_decisions(decision_id),
    entered_at_ms INTEGER NOT NULL,
    PRIMARY KEY(config_hash,market_id,expert,risk_policy)
);

CREATE TABLE IF NOT EXISTS prediction_moe_selected_entries (
    config_hash TEXT NOT NULL,
    market_id TEXT NOT NULL,
    decision_id TEXT NOT NULL REFERENCES prediction_moe_shadow_decisions(decision_id),
    expert TEXT NOT NULL,
    risk_policy TEXT NOT NULL,
    entered_at_ms INTEGER NOT NULL,
    PRIMARY KEY(config_hash,market_id)
);
