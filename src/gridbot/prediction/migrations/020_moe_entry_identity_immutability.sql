-- Canonical per-arm and router-selected entry identities are outcome-selection
-- evidence. They must remain append-only after their first pre-outcome write.
CREATE TRIGGER IF NOT EXISTS prediction_moe_arm_entries_no_update
BEFORE UPDATE ON prediction_moe_arm_entries
BEGIN SELECT RAISE(ABORT, 'immutable moe arm entry identity'); END;

CREATE TRIGGER IF NOT EXISTS prediction_moe_arm_entries_no_delete
BEFORE DELETE ON prediction_moe_arm_entries
BEGIN SELECT RAISE(ABORT, 'immutable moe arm entry identity'); END;

CREATE TRIGGER IF NOT EXISTS prediction_moe_selected_entries_no_update
BEFORE UPDATE ON prediction_moe_selected_entries
BEGIN SELECT RAISE(ABORT, 'immutable moe selected entry identity'); END;

CREATE TRIGGER IF NOT EXISTS prediction_moe_selected_entries_no_delete
BEFORE DELETE ON prediction_moe_selected_entries
BEGIN SELECT RAISE(ABORT, 'immutable moe selected entry identity'); END;
