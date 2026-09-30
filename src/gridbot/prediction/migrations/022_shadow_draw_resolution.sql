-- Support official equal-payout resolutions without inventing a direction.
-- Rebuild only the three constrained Shadow tables; preserve every value,
-- foreign key, index, immutable trigger and the authoritative rollup view.
-- Live tables and UP/DOWN fill-side constraints are deliberately unchanged.
PRAGMA foreign_keys=OFF;
BEGIN IMMEDIATE;
DROP VIEW IF EXISTS prediction_shadow_campaign_rollups;

CREATE TABLE prediction_shadow_campaigns_draw (
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
    resolved_outcome TEXT NOT NULL CHECK(resolved_outcome IN ('UP','DOWN','DRAW')),
    resolved_at_ms INTEGER NOT NULL,
    simulated_fees TEXT NOT NULL DEFAULT '0',
    simulated_pnl TEXT NOT NULL DEFAULT '0',
    expected_fill_count INTEGER NOT NULL DEFAULT 0,
    simulated_fill_count INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    lane TEXT NOT NULL DEFAULT '',
    strategy_identity_json TEXT NOT NULL DEFAULT '',
    execution_identity_json TEXT NOT NULL DEFAULT '',
    collection_release_fingerprint TEXT NOT NULL DEFAULT '',
    UNIQUE(campaign_id, config_hash, window_start_ms, window_end_ms),
    CHECK(window_start_ms <= window_end_ms),
    CHECK(campaign_start_ms <= campaign_end_ms)
);
INSERT INTO prediction_shadow_campaigns_draw SELECT * FROM prediction_shadow_campaigns;
DROP TABLE prediction_shadow_campaigns;
ALTER TABLE prediction_shadow_campaigns_draw RENAME TO prediction_shadow_campaigns;
CREATE INDEX idx_prediction_shadow_campaign_window ON prediction_shadow_campaigns(mode, config_hash, window_start_ms, window_end_ms, resolved_at_ms);
CREATE INDEX idx_prediction_shadow_campaign_lane_start ON prediction_shadow_campaigns(lane, campaign_start_ms, config_hash);
CREATE INDEX idx_prediction_shadow_campaign_start_config ON prediction_shadow_campaigns(campaign_start_ms, config_hash);
CREATE TRIGGER prediction_shadow_campaigns_immutable_update BEFORE UPDATE ON prediction_shadow_campaigns BEGIN SELECT RAISE(ABORT, 'prediction shadow campaigns are immutable'); END;
CREATE TRIGGER prediction_shadow_campaigns_immutable_delete BEFORE DELETE ON prediction_shadow_campaigns BEGIN SELECT RAISE(ABORT, 'prediction shadow campaigns are immutable'); END;

CREATE TABLE prediction_shadow_settlements_draw (
    shadow_settlement_id TEXT PRIMARY KEY,
    shadow_campaign_id TEXT NOT NULL REFERENCES prediction_shadow_campaigns(shadow_campaign_id),
    campaign_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'SHADOW' CHECK(mode = 'SHADOW'),
    config_hash TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    window_end_ms INTEGER NOT NULL,
    resolved_outcome TEXT NOT NULL CHECK(resolved_outcome IN ('UP','DOWN','DRAW')),
    status TEXT NOT NULL CHECK(status = 'SETTLED'),
    simulated_fees TEXT NOT NULL DEFAULT '0',
    simulated_pnl TEXT NOT NULL DEFAULT '0',
    settled_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(shadow_campaign_id)
);
INSERT INTO prediction_shadow_settlements_draw SELECT * FROM prediction_shadow_settlements;
DROP TABLE prediction_shadow_settlements;
ALTER TABLE prediction_shadow_settlements_draw RENAME TO prediction_shadow_settlements;
CREATE INDEX idx_prediction_shadow_settlements_window ON prediction_shadow_settlements(mode, config_hash, window_start_ms, window_end_ms, settled_at_ms);
CREATE TRIGGER prediction_shadow_settlements_immutable_update BEFORE UPDATE ON prediction_shadow_settlements BEGIN SELECT RAISE(ABORT, 'prediction shadow settlements are immutable'); END;
CREATE TRIGGER prediction_shadow_settlements_immutable_delete BEFORE DELETE ON prediction_shadow_settlements BEGIN SELECT RAISE(ABORT, 'prediction shadow settlements are immutable'); END;

CREATE TABLE prediction_shadow_observer_markets_draw (
    observer_campaign_id TEXT PRIMARY KEY,
    market_topic_id TEXT NOT NULL UNIQUE,
    market_id TEXT NOT NULL DEFAULT '',
    slug TEXT NOT NULL DEFAULT '',
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'ACTIVE',
    winner TEXT,
    last_quote_at_ms INTEGER NOT NULL DEFAULT 0,
    last_seen_at_ms INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK(state IN ('ACTIVE','SETTLED','PENDING_RESOLUTION','EXPIRED')),
    CHECK(winner IS NULL OR winner IN ('UP','DOWN','DRAW'))
);
INSERT INTO prediction_shadow_observer_markets_draw SELECT * FROM prediction_shadow_observer_markets;
DROP TABLE prediction_shadow_observer_markets;
ALTER TABLE prediction_shadow_observer_markets_draw RENAME TO prediction_shadow_observer_markets;
CREATE INDEX idx_prediction_shadow_observer_markets_state ON prediction_shadow_observer_markets(state, end_time_ms, updated_at_ms);

CREATE VIEW prediction_shadow_campaign_rollups AS
SELECT c.shadow_campaign_id, c.campaign_id,
    CASE WHEN trim(c.lane) <> '' THEN lower(trim(c.lane))
         WHEN instr(c.campaign_id, '::shadow::') > 0 THEN lower(substr(c.campaign_id, instr(c.campaign_id, '::shadow::') + length('::shadow::')))
         ELSE '' END AS lane,
    c.mode, c.config_hash, c.window_start_ms, c.window_end_ms,
    c.market_topic_id, c.market_id, c.slug, c.campaign_start_ms, c.campaign_end_ms,
    c.expected_fill_count, COALESCE(f.simulated_fill_count, 0) AS simulated_fill_count,
    COALESCE(s.resolved_outcome, c.resolved_outcome) AS resolved_outcome,
    COALESCE(s.settled_at_ms, c.resolved_at_ms) AS resolved_at_ms,
    COALESCE(s.simulated_fees, c.simulated_fees) AS simulated_fees,
    COALESCE(s.simulated_pnl, c.simulated_pnl) AS simulated_pnl,
    c.created_at_ms, c.payload_json
FROM prediction_shadow_campaigns c
LEFT JOIN (SELECT shadow_campaign_id, COUNT(*) AS simulated_fill_count FROM prediction_shadow_fills WHERE mode='SHADOW' GROUP BY shadow_campaign_id) f ON f.shadow_campaign_id=c.shadow_campaign_id
LEFT JOIN prediction_shadow_settlements s ON s.shadow_campaign_id=c.shadow_campaign_id AND s.mode='SHADOW'
WHERE c.mode='SHADOW';
COMMIT;
PRAGMA foreign_keys=ON;
