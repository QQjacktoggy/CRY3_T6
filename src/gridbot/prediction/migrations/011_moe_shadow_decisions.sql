-- Append-only, Shadow-only audit records for the regime-aware MoE router.
CREATE TABLE IF NOT EXISTS prediction_moe_shadow_decisions (
    decision_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    mode TEXT NOT NULL CHECK(mode = 'SHADOW'),
    trade_allowed INTEGER NOT NULL CHECK(trade_allowed = 0),
    config_hash TEXT NOT NULL,
    candidate_regime TEXT NOT NULL,
    active_regime TEXT NOT NULL,
    selected_expert TEXT NOT NULL,
    selected_side TEXT,
    selected_risk_policy TEXT,
    selected_amount_usdt TEXT NOT NULL,
    reason TEXT NOT NULL,
    safety_reasons_json TEXT NOT NULL,
    arms_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prediction_moe_shadow_market
    ON prediction_moe_shadow_decisions(market_id, observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_prediction_moe_shadow_regime
    ON prediction_moe_shadow_decisions(active_regime, selected_expert, observed_at_ms);
