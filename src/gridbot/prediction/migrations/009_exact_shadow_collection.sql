-- Bind one canonical balanced_hold Shadow window to one exact 200-market
-- collector.  The window primary key prevents a restart or second worker
-- from creating another loop for the same immutable evidence boundary.
CREATE TABLE IF NOT EXISTS prediction_shadow_collectors (
    window_id TEXT PRIMARY KEY REFERENCES prediction_shadow_windows(window_id) ON DELETE RESTRICT,
    loop_id TEXT NOT NULL UNIQUE REFERENCES prediction_loops(loop_id) ON DELETE RESTRICT,
    target INTEGER NOT NULL CHECK(target = 200),
    collection_release_fingerprint TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_shadow_collectors_release_window
    ON prediction_shadow_collectors(collection_release_fingerprint, window_id);
