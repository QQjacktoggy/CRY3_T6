ALTER TABLE prediction_moe_shadow_outcomes ADD COLUMN official_source_digest TEXT NOT NULL DEFAULT '';
ALTER TABLE prediction_moe_shadow_outcomes ADD COLUMN official_market_topic_id TEXT NOT NULL DEFAULT '';
