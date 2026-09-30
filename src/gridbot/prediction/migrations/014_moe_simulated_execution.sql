CREATE TABLE IF NOT EXISTS prediction_moe_sim_orders (
    order_id TEXT PRIMARY KEY,
    config_hash TEXT NOT NULL,
    market_id TEXT NOT NULL,
    expert TEXT NOT NULL,
    risk_policy TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),
    amount_usdt TEXT NOT NULL,
    candidate_price TEXT NOT NULL,
    submitted_at_ms INTEGER NOT NULL,
    latency_ms INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL,
    max_slippage_bps TEXT NOT NULL,
    fee_version TEXT,
    fee_rate TEXT,
    status TEXT NOT NULL,
    quarantine_reason TEXT,
    payload_json TEXT NOT NULL,
    UNIQUE(config_hash,market_id,expert,risk_policy)
);

CREATE TABLE IF NOT EXISTS prediction_moe_sim_fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES prediction_moe_sim_orders(order_id),
    event_id TEXT NOT NULL,
    source_event_ms INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),
    shares TEXT NOT NULL,
    price TEXT NOT NULL,
    gross_usdt TEXT NOT NULL,
    fee_usdt TEXT NOT NULL,
    fee_version TEXT NOT NULL,
    slippage_bps TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(order_id,event_id,fill_id)
);
CREATE INDEX IF NOT EXISTS idx_prediction_moe_orders_market ON prediction_moe_sim_orders(config_hash,market_id);
CREATE INDEX IF NOT EXISTS idx_prediction_moe_fills_order ON prediction_moe_sim_fills(order_id,source_event_ms);
