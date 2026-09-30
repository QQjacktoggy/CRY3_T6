ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN regime_scores_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN expert_scores_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE prediction_moe_shadow_decisions ADD COLUMN router_weights_json TEXT NOT NULL DEFAULT '{}';
