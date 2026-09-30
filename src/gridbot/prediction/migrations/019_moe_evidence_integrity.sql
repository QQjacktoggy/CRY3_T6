-- Evidence rows are immutable. Simulated orders are mutable only while their
-- execution FSM is non-terminal; every transition is retained in an append-only
-- event ledger and terminal evidence can never be rewritten.
CREATE TABLE IF NOT EXISTS prediction_schema_artifacts (
    filename TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
    registered_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prediction_moe_sim_order_events (
    order_event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES prediction_moe_sim_orders(order_id),
    prior_status TEXT,
    next_status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TRIGGER IF NOT EXISTS prediction_moe_decisions_no_update
BEFORE UPDATE ON prediction_moe_shadow_decisions BEGIN SELECT RAISE(ABORT, 'immutable moe decision'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_decisions_no_delete
BEFORE DELETE ON prediction_moe_shadow_decisions BEGIN SELECT RAISE(ABORT, 'immutable moe decision'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_fills_no_update
BEFORE UPDATE ON prediction_moe_sim_fills BEGIN SELECT RAISE(ABORT, 'immutable moe fill'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_fills_no_delete
BEFORE DELETE ON prediction_moe_sim_fills BEGIN SELECT RAISE(ABORT, 'immutable moe fill'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_positions_no_update
BEFORE UPDATE ON prediction_moe_positions BEGIN SELECT RAISE(ABORT, 'immutable moe position'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_positions_no_delete
BEFORE DELETE ON prediction_moe_positions BEGIN SELECT RAISE(ABORT, 'immutable moe position'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_reconciliations_no_update
BEFORE UPDATE ON prediction_moe_reconciliations BEGIN SELECT RAISE(ABORT, 'immutable moe reconciliation'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_reconciliations_no_delete
BEFORE DELETE ON prediction_moe_reconciliations BEGIN SELECT RAISE(ABORT, 'immutable moe reconciliation'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_resolutions_no_update
BEFORE UPDATE ON prediction_moe_official_resolutions BEGIN SELECT RAISE(ABORT, 'append-only official resolution'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_resolutions_no_delete
BEFORE DELETE ON prediction_moe_official_resolutions BEGIN SELECT RAISE(ABORT, 'append-only official resolution'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_orders_no_delete
BEFORE DELETE ON prediction_moe_sim_orders BEGIN SELECT RAISE(ABORT, 'append-only moe order identity'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_terminal_orders_no_update
BEFORE UPDATE ON prediction_moe_sim_orders
WHEN OLD.status IN ('SETTLED','QUARANTINED')
BEGIN SELECT RAISE(ABORT, 'immutable terminal moe order'); END;
