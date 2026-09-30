-- Immutable shadow-canary provenance.  These tables are deliberately
-- separate from the live/order ledger: a shadow observation is a
-- counterfactual, never an exchange order.  Every identity carries the
-- mode, configuration hash, and fixed canary window so an evidence query
-- cannot accidentally aggregate another run.

CREATE TABLE IF NOT EXISTS prediction_shadow_windows (
    window_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    database_path TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(mode, config_hash, window_start_ms, window_end_ms),
    CHECK(window_start_ms <= window_end_ms)
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_windows_latest
    ON prediction_shadow_windows(config_hash, window_end_ms, created_at_ms);

CREATE TABLE IF NOT EXISTS prediction_shadow_campaigns (
    shadow_campaign_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    market_topic_id TEXT NOT NULL DEFAULT '',
    market_id TEXT NOT NULL DEFAULT '',
    slug TEXT NOT NULL DEFAULT '',
    campaign_start_ms INTEGER NOT NULL,
    campaign_end_ms INTEGER NOT NULL,
    resolved_outcome TEXT NOT NULL CHECK(resolved_outcome IN ('UP','DOWN')),
    resolved_at_ms INTEGER NOT NULL,
    simulated_fees TEXT NOT NULL DEFAULT '0',
    simulated_pnl TEXT NOT NULL DEFAULT '0',
    expected_fill_count INTEGER NOT NULL DEFAULT 0,
    simulated_fill_count INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(campaign_id, config_hash, window_start_ms, window_end_ms),
    CHECK(window_start_ms <= window_end_ms),
    CHECK(campaign_start_ms <= campaign_end_ms)
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_campaign_window
    ON prediction_shadow_campaigns(mode, config_hash, window_start_ms, window_end_ms, resolved_at_ms);

CREATE TABLE IF NOT EXISTS prediction_shadow_fills (
    shadow_fill_id TEXT PRIMARY KEY,
    shadow_campaign_id TEXT NOT NULL REFERENCES prediction_shadow_campaigns(shadow_campaign_id),
    campaign_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    fill_identity TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('UP','DOWN')),
    order_side TEXT NOT NULL CHECK(order_side IN ('BUY','SELL')),
    shares TEXT NOT NULL,
    price TEXT NOT NULL,
    gross_amount TEXT NOT NULL,
    simulated_fee TEXT NOT NULL DEFAULT '0',
    event_time_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(shadow_campaign_id, fill_identity),
    UNIQUE(shadow_campaign_id, event_time_ms, outcome, order_side, shares, price)
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_fills_window
    ON prediction_shadow_fills(mode, config_hash, window_start_ms, window_end_ms, event_time_ms);

CREATE TABLE IF NOT EXISTS prediction_shadow_settlements (
    shadow_settlement_id TEXT PRIMARY KEY,
    shadow_campaign_id TEXT NOT NULL REFERENCES prediction_shadow_campaigns(shadow_campaign_id),
    campaign_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    resolved_outcome TEXT NOT NULL CHECK(resolved_outcome IN ('UP','DOWN')),
    status TEXT NOT NULL CHECK(status = 'SETTLED'),
    simulated_fees TEXT NOT NULL DEFAULT '0',
    simulated_pnl TEXT NOT NULL DEFAULT '0',
    settled_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(shadow_campaign_id)
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_settlements_window
    ON prediction_shadow_settlements(mode, config_hash, window_start_ms, window_end_ms, settled_at_ms);

CREATE TABLE IF NOT EXISTS prediction_shadow_evidence (
    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_identity TEXT NOT NULL UNIQUE,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    generated_at_ms INTEGER NOT NULL,
    repository_commit TEXT NOT NULL,
    database_path TEXT NOT NULL,
    unique_resolved_count INTEGER NOT NULL DEFAULT 0,
    campaign_count INTEGER NOT NULL DEFAULT 0,
    settled_count INTEGER NOT NULL DEFAULT 0,
    coverage TEXT NOT NULL DEFAULT '0',
    settlement_rate TEXT NOT NULL DEFAULT '0',
    expected_fill_count INTEGER NOT NULL DEFAULT 0,
    simulated_fill_count INTEGER NOT NULL DEFAULT 0,
    fill_rate TEXT NOT NULL DEFAULT '0',
    simulated_fees TEXT NOT NULL DEFAULT '0',
    after_fee_pnl TEXT NOT NULL DEFAULT '0',
    unresolved_intents INTEGER NOT NULL DEFAULT 0,
    unresolved_orders INTEGER NOT NULL DEFAULT 0,
    duplicate_violations INTEGER NOT NULL DEFAULT 0,
    overbuy_violations INTEGER NOT NULL DEFAULT 0,
    action_limit_violations INTEGER NOT NULL DEFAULT 0,
    invariant_violations INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_evidence_window
    ON prediction_shadow_evidence(mode, config_hash, window_start_ms, window_end_ms, generated_at_ms);

-- The evidence and counterfactual ledgers are append-only.  A retry may
-- insert the same identity with INSERT ... DO NOTHING, but a caller can
-- never rewrite its time, hash, window, outcome, fees, or PnL in place.
CREATE TRIGGER IF NOT EXISTS prediction_shadow_campaigns_immutable_update
BEFORE UPDATE ON prediction_shadow_campaigns
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow campaigns are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_campaigns_immutable_delete
BEFORE DELETE ON prediction_shadow_campaigns
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow campaigns are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_fills_immutable_update
BEFORE UPDATE ON prediction_shadow_fills
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow fills are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_fills_immutable_delete
BEFORE DELETE ON prediction_shadow_fills
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow fills are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_settlements_immutable_update
BEFORE UPDATE ON prediction_shadow_settlements
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow settlements are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_settlements_immutable_delete
BEFORE DELETE ON prediction_shadow_settlements
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow settlements are immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_evidence_immutable_update
BEFORE UPDATE ON prediction_shadow_evidence
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow evidence is immutable');
END;
CREATE TRIGGER IF NOT EXISTS prediction_shadow_evidence_immutable_delete
BEFORE DELETE ON prediction_shadow_evidence
BEGIN
    SELECT RAISE(ABORT, 'prediction shadow evidence is immutable');
END;
