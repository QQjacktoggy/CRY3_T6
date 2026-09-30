ALTER TABLE prediction_moe_sim_orders ADD COLUMN max_book_age_ms INTEGER NOT NULL DEFAULT 1000;

CREATE TRIGGER IF NOT EXISTS prediction_moe_raw_events_no_update
BEFORE UPDATE ON prediction_moe_raw_events
BEGIN
    SELECT RAISE(ABORT, 'prediction_moe_raw_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prediction_moe_raw_events_no_delete
BEFORE DELETE ON prediction_moe_raw_events
BEGIN
    SELECT RAISE(ABORT, 'prediction_moe_raw_events is append-only');
END;
