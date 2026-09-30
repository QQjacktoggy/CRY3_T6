-- Fourth-round Prediction state.  This migration is additive so an existing
-- shadow/live database can be opened and recovered without a destructive
-- rewrite.

ALTER TABLE prediction_orders ADD COLUMN cumulative_gross TEXT NOT NULL DEFAULT '0';
ALTER TABLE prediction_orders ADD COLUMN cumulative_fee TEXT NOT NULL DEFAULT '0';

ALTER TABLE prediction_order_intents ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_order_intents ADD COLUMN cancel_requested_at_ms INTEGER;

ALTER TABLE prediction_quotes ADD COLUMN leader_duration_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_quotes ADD COLUMN reference_recross INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_quotes ADD COLUMN spot_observed_at_ms INTEGER;

ALTER TABLE prediction_settlements ADD COLUMN tx_hash TEXT;
ALTER TABLE prediction_settlements ADD COLUMN batch_id TEXT;

ALTER TABLE prediction_loops ADD COLUMN net_pnl TEXT NOT NULL DEFAULT '0';
ALTER TABLE prediction_loops ADD COLUMN consecutive_losses INTEGER NOT NULL DEFAULT 0;
ALTER TABLE prediction_loops ADD COLUMN hard_stop_latched INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS prediction_market_state (
    campaign_id TEXT PRIMARY KEY REFERENCES prediction_campaigns(campaign_id) ON DELETE CASCADE,
    prior_spot TEXT,
    last_spot TEXT,
    last_spot_at_ms INTEGER,
    prior_leader TEXT,
    last_leader TEXT,
    leader_since_ms INTEGER,
    leader_quotes INTEGER NOT NULL DEFAULT 0,
    cross_count INTEGER NOT NULL DEFAULT 0,
    last_quote_at_ms INTEGER,
    payload_json TEXT NOT NULL DEFAULT '{}',
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prediction_market_state_quote
    ON prediction_market_state(last_quote_at_ms);
