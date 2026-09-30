-- Prediction domain storage is intentionally separate from the legacy gridbot
-- database.  Monetary values are stored as TEXT so Decimal values retain
-- their exact representation across process restarts.

CREATE TABLE IF NOT EXISTS prediction_campaigns (
    campaign_id TEXT PRIMARY KEY,
    loop_id TEXT,
    market_topic_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    slug TEXT NOT NULL DEFAULT '',
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER NOT NULL,
    state TEXT NOT NULL,
    initial_outcome TEXT,
    hedge_used INTEGER NOT NULL DEFAULT 0,
    profit_lock_used INTEGER NOT NULL DEFAULT 0,
    loser_unwind_count INTEGER NOT NULL DEFAULT 0,
    loser_unwind_shares TEXT NOT NULL DEFAULT '0',
    buy_count INTEGER NOT NULL DEFAULT 0,
    order_attempts INTEGER NOT NULL DEFAULT 0,
    initial_attempts INTEGER NOT NULL DEFAULT 0,
    hedge_attempts INTEGER NOT NULL DEFAULT 0,
    pending_intent_id TEXT,
    pending_unknown INTEGER NOT NULL DEFAULT 0,
    hedged_at_ms INTEGER,
    last_error TEXT,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prediction_campaign_state
    ON prediction_campaigns(state, end_time_ms);
CREATE INDEX IF NOT EXISTS idx_prediction_campaign_loop
    ON prediction_campaigns(loop_id, updated_at_ms);

CREATE TABLE IF NOT EXISTS prediction_quotes (
    quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
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
    payload_json TEXT NOT NULL,
    UNIQUE(campaign_id, observed_at_ms)
);
CREATE INDEX IF NOT EXISTS idx_prediction_quotes_campaign_time
    ON prediction_quotes(campaign_id, observed_at_ms);

CREATE TABLE IF NOT EXISTS prediction_order_intents (
    intent_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL,
    order_side TEXT NOT NULL,
    amount TEXT NOT NULL,
    limit_price TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    order_id TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    unknown INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    UNIQUE(campaign_id, action, attempt)
);
CREATE INDEX IF NOT EXISTS idx_prediction_intents_campaign
    ON prediction_order_intents(campaign_id, created_at_ms);

CREATE TABLE IF NOT EXISTS prediction_orders (
    order_id TEXT PRIMARY KEY,
    intent_id TEXT UNIQUE,
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
    token_id TEXT,
    outcome TEXT,
    order_side TEXT,
    status TEXT NOT NULL,
    requested_amount TEXT,
    limit_price TEXT,
    filled_shares TEXT NOT NULL DEFAULT '0',
    avg_price TEXT,
    submitted_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prediction_orders_campaign_status
    ON prediction_orders(campaign_id, status, updated_at_ms);

CREATE TABLE IF NOT EXISTS prediction_fills (
    fill_id TEXT PRIMARY KEY,
    trade_id TEXT UNIQUE,
    order_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
    token_id TEXT,
    outcome TEXT NOT NULL,
    order_side TEXT NOT NULL,
    shares TEXT NOT NULL,
    price TEXT NOT NULL,
    gross_amount TEXT NOT NULL,
    fee TEXT NOT NULL DEFAULT '0',
    event_time_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prediction_fills_campaign_time
    ON prediction_fills(campaign_id, event_time_ms);

CREATE TABLE IF NOT EXISTS prediction_position_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
    captured_at_ms INTEGER NOT NULL,
    up_shares TEXT NOT NULL DEFAULT '0',
    down_shares TEXT NOT NULL DEFAULT '0',
    up_cost TEXT NOT NULL DEFAULT '0',
    down_cost TEXT NOT NULL DEFAULT '0',
    realized_cash TEXT NOT NULL DEFAULT '0',
    fees TEXT NOT NULL DEFAULT '0',
    payload_json TEXT NOT NULL,
    UNIQUE(campaign_id, captured_at_ms)
);

CREATE TABLE IF NOT EXISTS prediction_settlements (
    settlement_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
    loop_id TEXT,
    settled_at_ms INTEGER NOT NULL,
    winner TEXT,
    status TEXT NOT NULL DEFAULT 'SETTLED',
    gross_pnl TEXT NOT NULL DEFAULT '0',
    realized_pnl TEXT NOT NULL DEFAULT '0',
    net_pnl TEXT NOT NULL DEFAULT '0',
    fees TEXT NOT NULL DEFAULT '0',
    payload_json TEXT NOT NULL,
    UNIQUE(campaign_id, settled_at_ms)
);
CREATE INDEX IF NOT EXISTS idx_prediction_settlements_time
    ON prediction_settlements(settled_at_ms);
CREATE INDEX IF NOT EXISTS idx_prediction_settlements_loop
    ON prediction_settlements(loop_id, settled_at_ms);

CREATE TABLE IF NOT EXISTS prediction_risk_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT,
    event_time_ms INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prediction_risk_events_time
    ON prediction_risk_events(event_time_ms);

CREATE TABLE IF NOT EXISTS prediction_runtime_config (
    config_key TEXT PRIMARY KEY,
    config_value_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
