-- T6.9a per-loop lane mask. '' = no lane switched off (every existing loop).
-- ADD COLUMN is not an UPDATE, so the immutability triggers are unchanged.
ALTER TABLE prediction_loop_market_bindings ADD COLUMN lane_mask TEXT NOT NULL DEFAULT '';
