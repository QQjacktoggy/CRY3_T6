-- Preserve the complete historical BUY barrier, including rejected intents.
-- Drive both joins from official market identity instead of scanning history.
CREATE INDEX IF NOT EXISTS idx_prediction_campaigns_market_buy_lookup
ON prediction_campaigns(market_topic_id, start_time_ms, campaign_id);
