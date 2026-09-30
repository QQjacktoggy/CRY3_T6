-- Durable, read-only market observation state.  These tables are separate
-- from the Live campaign/order ledger so an observer restart cannot create a
-- Live campaign, order intent, fill, or settlement.
CREATE TABLE IF NOT EXISTS prediction_shadow_observer_markets (
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
    CHECK(winner IS NULL OR winner IN ('UP','DOWN'))
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_observer_markets_state
    ON prediction_shadow_observer_markets(state, end_time_ms, updated_at_ms);

CREATE TABLE IF NOT EXISTS prediction_shadow_observer_state (
    observer_campaign_id TEXT PRIMARY KEY
        REFERENCES prediction_shadow_observer_markets(observer_campaign_id) ON DELETE CASCADE,
    prior_spot TEXT,
    last_spot TEXT,
    last_spot_at_ms INTEGER,
    prior_leader TEXT,
    last_leader TEXT,
    leader_since_ms INTEGER NOT NULL DEFAULT 0,
    leader_quotes INTEGER NOT NULL DEFAULT 0,
    cross_count INTEGER NOT NULL DEFAULT 0,
    last_quote_at_ms INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prediction_shadow_observer_quotes (
    observer_quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observer_campaign_id TEXT NOT NULL
        REFERENCES prediction_shadow_observer_markets(observer_campaign_id) ON DELETE CASCADE,
    observed_at_ms INTEGER NOT NULL,
    up_bid TEXT,
    up_ask TEXT,
    down_bid TEXT,
    down_ask TEXT,
    leader TEXT,
    btc_spot TEXT,
    reference_price TEXT,
    feed_ok INTEGER NOT NULL DEFAULT 1,
    flip_confirmed INTEGER NOT NULL DEFAULT 0,
    btc_crossed_reference INTEGER NOT NULL DEFAULT 0,
    stable_final INTEGER NOT NULL DEFAULT 0,
    leader_duration_ms INTEGER NOT NULL DEFAULT 0,
    reference_recross INTEGER NOT NULL DEFAULT 0,
    spot_observed_at_ms INTEGER,
    payload_json TEXT NOT NULL,
    UNIQUE(observer_campaign_id, observed_at_ms)
);
CREATE INDEX IF NOT EXISTS idx_prediction_shadow_observer_quotes_time
    ON prediction_shadow_observer_quotes(observer_campaign_id, observed_at_ms);
