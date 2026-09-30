-- Migration 022: Independent Strategy Lane Attribution Ledger
-- Supports concurrent, side-by-side live execution for FAV_BASELINE and FAV_P3_LIVE

CREATE TABLE IF NOT EXISTS prediction_lane_attribution (
    attribution_id TEXT PRIMARY KEY,
    strategy_lane TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    market_id TEXT,
    window_id TEXT,
    intent_id TEXT,
    order_id TEXT,
    signal_ts INTEGER NOT NULL,
    direction TEXT,
    reference_price REAL,
    spot_price REAL,
    pre_cross_count INTEGER DEFAULT 0,
    same_side_seconds REAL DEFAULT 0.0,
    distance_bps REAL DEFAULT 0.0,
    ask_at_signal REAL,
    bid_at_signal REAL,
    order_submit_ts INTEGER,
    fill_ts INTEGER,
    fill_price REAL,
    fill_shares REAL,
    fill_latency_ms INTEGER,
    size_usdt REAL,
    status TEXT NOT NULL,
    reject_reason TEXT,
    final_result TEXT,
    realized_pnl REAL,
    payload_json TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lane_attribution_lane_time
    ON prediction_lane_attribution(strategy_lane, signal_ts);

CREATE INDEX IF NOT EXISTS idx_lane_attribution_campaign
    ON prediction_lane_attribution(campaign_id, strategy_lane);
