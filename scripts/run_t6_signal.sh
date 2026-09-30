#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
exec "$ROOT/.venv/bin/python" -m src.gridbot.prediction.c180_signal_runtime   --frozen-source "$ROOT/prediction/experiments/c180-original-mix75-v1-bda3e5a85a98"   --prediction-db "$ROOT/prediction/data/prediction.sqlite3"   --signal-db "$ROOT/prediction/data/c180-favorite-live/signals.sqlite3"   --shared-weight-db "$ROOT/prediction/data/request-weight.sqlite3"
