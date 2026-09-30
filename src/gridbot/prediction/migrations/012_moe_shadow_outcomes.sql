-- Official-settlement-derived outcomes for restart-safe MoE memory.
CREATE TABLE IF NOT EXISTS prediction_moe_shadow_outcomes (
    market_id TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    expert TEXT NOT NULL,
    risk_policy TEXT NOT NULL,
    official_outcome TEXT NOT NULL CHECK(official_outcome IN ('UP','DOWN')),
    action TEXT NOT NULL,
    entry_side TEXT,
    entry_price TEXT,
    amount_usdt TEXT NOT NULL,
    simulated_pnl_usdt TEXT NOT NULL,
    net_return REAL NOT NULL,
    settled_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (market_id, config_hash, expert, risk_policy)
);
CREATE INDEX IF NOT EXISTS idx_prediction_moe_outcomes_history
    ON prediction_moe_shadow_outcomes(config_hash, settled_at_ms, market_id);
