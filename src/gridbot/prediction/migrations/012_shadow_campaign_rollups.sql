-- Campaign, fill, and settlement rows are immutable ledgers.  Expose their
-- authoritative aggregate without rewriting historical campaign records.
CREATE VIEW IF NOT EXISTS prediction_shadow_campaign_rollups AS
SELECT
    c.shadow_campaign_id,
    c.campaign_id,
    CASE
        WHEN trim(c.lane) <> '' THEN lower(trim(c.lane))
        WHEN instr(c.campaign_id, '::shadow::') > 0
            THEN lower(substr(
                c.campaign_id,
                instr(c.campaign_id, '::shadow::') + length('::shadow::')
            ))
        ELSE ''
    END AS lane,
    c.mode,
    c.config_hash,
    c.window_start_ms,
    c.window_end_ms,
    c.market_topic_id,
    c.market_id,
    c.slug,
    c.campaign_start_ms,
    c.campaign_end_ms,
    c.expected_fill_count,
    COALESCE(f.simulated_fill_count, 0) AS simulated_fill_count,
    COALESCE(s.resolved_outcome, c.resolved_outcome) AS resolved_outcome,
    COALESCE(s.settled_at_ms, c.resolved_at_ms) AS resolved_at_ms,
    COALESCE(s.simulated_fees, c.simulated_fees) AS simulated_fees,
    COALESCE(s.simulated_pnl, c.simulated_pnl) AS simulated_pnl,
    c.created_at_ms,
    c.payload_json
FROM prediction_shadow_campaigns c
LEFT JOIN (
    SELECT shadow_campaign_id, COUNT(*) AS simulated_fill_count
    FROM prediction_shadow_fills
    WHERE mode = 'SHADOW'
    GROUP BY shadow_campaign_id
) f ON f.shadow_campaign_id = c.shadow_campaign_id
LEFT JOIN prediction_shadow_settlements s
    ON s.shadow_campaign_id = c.shadow_campaign_id AND s.mode = 'SHADOW'
WHERE c.mode = 'SHADOW';

CREATE INDEX IF NOT EXISTS idx_prediction_shadow_campaign_lane_start
    ON prediction_shadow_campaigns(lane, campaign_start_ms, config_hash);

CREATE INDEX IF NOT EXISTS idx_prediction_shadow_campaign_start_config
    ON prediction_shadow_campaigns(campaign_start_ms, config_hash);
