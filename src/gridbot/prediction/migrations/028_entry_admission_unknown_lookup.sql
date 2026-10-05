-- The pre-HTTP durable admission read must not scan every LIVE campaign and
-- intent. Both partial indexes stay empty unless an execution is unknown.
CREATE INDEX IF NOT EXISTS idx_prediction_campaigns_pending_unknown
ON prediction_campaigns(loop_id) WHERE pending_unknown=1;
CREATE INDEX IF NOT EXISTS idx_prediction_intents_unknown
ON prediction_order_intents(campaign_id) WHERE unknown=1;
