# Step 5: install. Backs up first, cold-restarts the 7 services, rolls back by itself on failure.
source "$(dirname "$0")/common.sh"
FP=$(stage_fp); [ "$FP" = "$EXPECTED_FP" ] || { echo "STAGE_FP_MISMATCH $FP"; exit 1; }
asj "$PY $INSTALLER --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP $FLAGS --apply"
