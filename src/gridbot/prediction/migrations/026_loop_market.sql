-- Existing history is not relabelled. New bindings are immutable.
CREATE TABLE prediction_loop_market_bindings (
 loop_id TEXT PRIMARY KEY REFERENCES prediction_loops(loop_id),
 symbol TEXT NOT NULL CHECK(symbol IN ('BTCUSDT','ETHUSDT','BNBUSDT')),
 profile TEXT NOT NULL,
 execution_fingerprint TEXT NOT NULL,
 unit TEXT NOT NULL CHECK(unit IN ('1','2','3')),
 target INTEGER NOT NULL CHECK(target>0),
 selected_at_ms INTEGER NOT NULL
);
CREATE TRIGGER loop_market_binding_no_update BEFORE UPDATE ON prediction_loop_market_bindings
BEGIN SELECT RAISE(ABORT,'loop market binding immutable'); END;
CREATE TRIGGER loop_market_binding_no_delete BEFORE DELETE ON prediction_loop_market_bindings
BEGIN SELECT RAISE(ABORT,'loop market binding immutable'); END;
