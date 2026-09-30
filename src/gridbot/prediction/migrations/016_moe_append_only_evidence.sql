ALTER TABLE prediction_moe_sim_orders ADD COLUMN last_source_event_ms INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS prediction_moe_raw_events (
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    event_time_ms INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    release_fingerprint TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(source_type, source_id)
);

CREATE TABLE IF NOT EXISTS prediction_moe_official_resolutions (
    resolution_event_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    market_topic_id TEXT NOT NULL,
    resolution_status TEXT NOT NULL CHECK(resolution_status IN ('UP','DOWN','VOID','DISPUTED','PENDING')),
    source_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    supersedes_event_id TEXT REFERENCES prediction_moe_official_resolutions(resolution_event_id),
    UNIQUE(market_id, source_digest)
);
CREATE INDEX IF NOT EXISTS idx_moe_resolution_latest
ON prediction_moe_official_resolutions(market_id, observed_at_ms DESC);

CREATE TABLE IF NOT EXISTS prediction_moe_positions (
    config_hash TEXT NOT NULL,
    market_id TEXT NOT NULL,
    expert TEXT NOT NULL,
    risk_policy TEXT NOT NULL,
    side TEXT,
    filled_shares TEXT NOT NULL,
    gross_usdt TEXT NOT NULL,
    fees_usdt TEXT NOT NULL,
    status TEXT NOT NULL,
    resolution_event_id TEXT NOT NULL REFERENCES prediction_moe_official_resolutions(resolution_event_id),
    payload_json TEXT NOT NULL,
    PRIMARY KEY(config_hash,market_id,expert,risk_policy,resolution_event_id)
);

CREATE TABLE IF NOT EXISTS prediction_moe_reconciliations (
    reconciliation_id TEXT PRIMARY KEY,
    config_hash TEXT NOT NULL,
    market_id TEXT NOT NULL,
    resolution_event_id TEXT NOT NULL REFERENCES prediction_moe_official_resolutions(resolution_event_id),
    status TEXT NOT NULL CHECK(status IN ('MATCHED','MISMATCH')),
    details_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
