CREATE TABLE IF NOT EXISTS prediction_moe_release_binding (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    config_hash TEXT NOT NULL,
    release_fingerprint TEXT NOT NULL CHECK(length(release_fingerprint)=64),
    service_identity TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS prediction_moe_release_binding_no_update
BEFORE UPDATE ON prediction_moe_release_binding BEGIN SELECT RAISE(ABORT,'immutable moe release binding'); END;
CREATE TRIGGER IF NOT EXISTS prediction_moe_release_binding_no_delete
BEFORE DELETE ON prediction_moe_release_binding BEGIN SELECT RAISE(ABORT,'immutable moe release binding'); END;
