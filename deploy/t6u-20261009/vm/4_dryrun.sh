# Step 4: read-only preflight. Changes nothing.
source "$(dirname "$0")/common.sh"
FP=$(stage_fp); [ "$FP" = "$EXPECTED_FP" ] || { echo "STAGE_FP_MISMATCH $FP"; exit 1; }
asj "$PY $INSTALLER --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP $FLAGS"
