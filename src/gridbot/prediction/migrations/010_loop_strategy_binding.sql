-- Bind every loop to one immutable operator-selected strategy.
ALTER TABLE prediction_loops ADD COLUMN strategy_profile TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_prediction_loops_strategy_state
    ON prediction_loops(strategy_profile, state, updated_at_ms);
