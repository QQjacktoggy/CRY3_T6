#!/bin/sh
# Isolated read-only producers. No bot, worker, arm or order entry point.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
ROLE=${1:?feature or signal required}
ASSET=${2:?ETHUSDT or BNBUSDT required}
case "$ASSET" in ETHUSDT|BNBUSDT) ;; *) exit 2 ;; esac
DATA="$ROOT/prediction/data/t67c-multimarket/$ASSET"
PYTHON="$ROOT/testnet/.venv/bin/python"
if [ ! -x "$PYTHON" ]; then PYTHON="$ROOT/.venv/bin/python"; fi
case "$ROLE" in
  feature)
    exec "$PYTHON" -B -m src.gridbot.prediction.regime_feature_service \
      --symbol "$ASSET" --db "$DATA/features.sqlite3" --signal-db "$DATA/signals.sqlite3" \
      --prediction-db "$ROOT/prediction/data/prediction.sqlite3" ;;
  signal)
    exec "$PYTHON" -B -m src.gridbot.prediction.c180_signal_runtime \
      --symbol "$ASSET" --feature-db "$DATA/features.sqlite3" --signal-db "$DATA/signals.sqlite3" \
      --prediction-db "$ROOT/prediction/data/prediction.sqlite3" \
      --shared-weight-db "$ROOT/prediction/data/request-weight.sqlite3" \
      --frozen-source "$ROOT/prediction/experiments/c180-original-mix75-v1-bda3e5a85a98" ;;
  *) exit 2 ;;
esac
