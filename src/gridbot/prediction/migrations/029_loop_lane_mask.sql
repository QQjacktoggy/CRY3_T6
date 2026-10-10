-- T6.9a per-loop lane mask. A side table, not a new binding column, so the
-- previous release's positional binding INSERT still works after a rollback.
-- No row = no lane switched off (every existing loop). Rows are immutable.
CREATE TABLE prediction_loop_lane_masks (
 loop_id TEXT PRIMARY KEY REFERENCES prediction_loop_market_bindings(loop_id),
 lane_mask TEXT NOT NULL CHECK(lane_mask<>'')
);
CREATE TRIGGER loop_lane_mask_no_update BEFORE UPDATE ON prediction_loop_lane_masks
BEGIN SELECT RAISE(ABORT,'loop lane mask immutable'); END;
CREATE TRIGGER loop_lane_mask_no_delete BEFORE DELETE ON prediction_loop_lane_masks
BEGIN SELECT RAISE(ABORT,'loop lane mask immutable'); END;
