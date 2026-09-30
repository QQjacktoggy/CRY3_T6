-- C180 live ordinal/evidence ownership. Existing Prediction tables stay intact.
CREATE TABLE IF NOT EXISTS prediction_c180_slots (
    loop_id TEXT NOT NULL REFERENCES prediction_loops(loop_id),
    market_start_ms INTEGER NOT NULL,
    run_ordinal INTEGER NOT NULL,
    market_topic_id TEXT,
    market_id TEXT,
    verified_at_ms INTEGER,
    empty_attested_at_ms INTEGER,
    PRIMARY KEY (loop_id, market_start_ms),
    UNIQUE (loop_id, run_ordinal)
);

CREATE TABLE IF NOT EXISTS prediction_c180_entry_claims (
    loop_id TEXT NOT NULL REFERENCES prediction_loops(loop_id),
    market_start_ms INTEGER NOT NULL,
    campaign_id TEXT NOT NULL UNIQUE REFERENCES prediction_campaigns(campaign_id),
    intent_id TEXT NOT NULL UNIQUE REFERENCES prediction_order_intents(intent_id),
    unit_usdt TEXT NOT NULL,
    claimed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (loop_id, market_start_ms),
    FOREIGN KEY (loop_id, market_start_ms)
      REFERENCES prediction_c180_slots(loop_id, market_start_ms)
);

CREATE INDEX IF NOT EXISTS idx_c180_claims_loop_time
    ON prediction_c180_entry_claims(loop_id, market_start_ms);

-- The existing settled_at_ms may be supplied by an upstream API and can be
-- backdated. The gate uses this first durable observation time instead.
CREATE TABLE IF NOT EXISTS prediction_c180_settlement_observations (
    settlement_id TEXT PRIMARY KEY REFERENCES prediction_settlements(settlement_id),
    campaign_id TEXT NOT NULL REFERENCES prediction_campaigns(campaign_id),
    net_pnl TEXT NOT NULL,
    known_at_ms INTEGER NOT NULL
);
