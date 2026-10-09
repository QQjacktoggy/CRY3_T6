# Rollback: bash rollback.sh <backup dir printed by step 5> [--apply]
# Without --apply it is a read-only dry run.
source "$(dirname "$0")/common.sh"
BACKUP=${1:?usage: rollback.sh <backup dir> [--apply]}
case "$BACKUP" in "$OPS"/t69d-20261007/runs/*) ;; *) echo "backup dir must be under $OPS/t69d-20261007/runs/"; exit 1;; esac
asj "$PY $ROLLBACK --backup $BACKUP $FLAGS ${2:-}"
