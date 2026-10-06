#!/bin/sh
# One-coin T6 operation on the VM: run only the selected coin's producers and
# no research observers. Run as jack_shih (user systemd units). It never arms
# Live, starts a loop, places orders or edits the trading database.
#
#   scripts/t6_coin.sh status
#   scripts/t6_coin.sh use BTC|ETH|BNB   # also turns the observers off
#   scripts/t6_coin.sh slim              # observers + observer Telegram off
set -eu
ROOT=${CRY3_ROOT:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}
DB="$ROOT/prediction/data/prediction.sqlite3"
PYTHON="$ROOT/testnet/.venv/bin/python"
if [ ! -x "$PYTHON" ]; then PYTHON="$ROOT/.venv/bin/python"; fi
if [ ! -x "$PYTHON" ]; then PYTHON=python3; fi
: "${XDG_RUNTIME_DIR:=/run/user/$(id -u)}"
export XDG_RUNTIME_DIR

BTC_UNITS="cry3-regime-feature.service cry3-c180-favorite-signal.service"
ETH_UNITS="cry3-t67c-ethusdt-feature.service cry3-t67c-ethusdt-signal.service"
BNB_UNITS="cry3-t67c-bnbusdt-feature.service cry3-t67c-bnbusdt-signal.service"
# Research observers and the observer Telegram sender (auto "最近20場" and
# fixed 20-slot observer messages). None of them trades.
OBSERVER_UNITS="cry3-first-observer-telegram.timer cry3-first-observer-telegram.service cry3-first-multimarket-observer.service cry3-t67c-multimarket-observer.service"
MAIN_UNIT="cry3-predict-user.service"

sc() { systemctl --user "$@"; }
exists() { [ -n "$(sc list-unit-files --no-legend "$1" 2>/dev/null)" ]; }

# Prints "<running loop id or -> <bound symbol or -> <selected market or ->".
loop_state() {
  "$PYTHON" -B - "$DB" <<'PY'
import json, sqlite3, sys
from pathlib import Path
db = sqlite3.connect(Path(sys.argv[1]).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
loop, bound = "-", "-"
if "prediction_loops" in names:
    if "prediction_loop_market_bindings" in names:
        row = db.execute("""SELECT l.loop_id, b.symbol FROM prediction_loops l
            LEFT JOIN prediction_loop_market_bindings b ON l.loop_id=b.loop_id
            WHERE l.state='RUNNING' ORDER BY l.created_at_ms DESC LIMIT 1""").fetchone()
    else:
        row = db.execute("SELECT loop_id, NULL FROM prediction_loops WHERE state='RUNNING' "
                         "ORDER BY created_at_ms DESC LIMIT 1").fetchone()
    if row:
        loop, bound = row[0], row[1] or "BTCUSDT"
selected = "-"
row = db.execute("SELECT config_value_json FROM prediction_runtime_config "
                 "WHERE config_key='prediction_selected_market'").fetchone()
if row:
    selected = (json.loads(row[0]) or {}).get("symbol") or "-"
print(loop, bound, selected)
PY
}

coin_units() {
  case "$1" in
    BTCUSDT) echo "$BTC_UNITS" ;;
    ETHUSDT) echo "$ETH_UNITS" ;;
    BNBUSDT) echo "$BNB_UNITS" ;;
  esac
}

show_units() {
  for unit in $MAIN_UNIT $BTC_UNITS $ETH_UNITS $BNB_UNITS $OBSERVER_UNITS; do
    if exists "$unit"; then
      printf '  %-46s %s\n' "$unit" "$(sc is-active "$unit" 2>/dev/null || true)"
    else
      printf '  %-46s %s\n' "$unit" "not-installed"
    fi
  done
}

warn_paid_original() {
  for unit in cry3-c180-favorite-signal.service cry3-t67c-ethusdt-signal.service cry3-t67c-bnbusdt-signal.service; do
    exists "$unit" || continue
    if sc show -p Environment "$unit" 2>/dev/null | grep -q 'PREDICTION_T67C_OBSERVER_ORIGINAL_ENABLED=1'; then
      echo "注意：$unit 仍設 PREDICTION_T67C_OBSERVER_ORIGINAL_ENABLED=1（三幣觀測用的付費 Original）。"
      echo "      觀測器已停，可在沒有 RUNNING loop 時移除該設定並重啟此 producer。"
    fi
  done
}

status() {
  set -- $(loop_state)
  echo "RUNNING loop：$1  綁定幣：$2  已選市場：$3"
  show_units
  free -m | sed -n '1,2p'
  warn_paid_original
}

slim() {
  for unit in $OBSERVER_UNITS; do
    if exists "$unit"; then
      sc disable --now "$unit" >/dev/null 2>&1 || sc stop "$unit" || true
      echo "已停用 $unit"
    fi
  done
}

use() {
  case "$(echo "${1:-}" | tr '[:lower:]' '[:upper:]')" in
    BTC|BTCUSDT) asset=BTCUSDT ;;
    ETH|ETHUSDT) asset=ETHUSDT ;;
    BNB|BNBUSDT) asset=BNBUSDT ;;
    *) echo "用法：$0 use BTC|ETH|BNB" >&2; exit 2 ;;
  esac
  set -- $(loop_state)
  if [ "$1" != "-" ] && [ "$2" != "$asset" ]; then
    echo "拒絕：loop $1 正在跑 $2，producer 不能中途切換。等本輪結束或取消後再換。" >&2
    exit 3
  fi
  slim
  # BTC producers stay up: they are the default market and the monitor's feed.
  for unit in $BTC_UNITS $(coin_units "$asset"); do
    exists "$unit" || { echo "缺少 $unit，無法切到 $asset" >&2; exit 4; }
  done
  sc start $BTC_UNITS
  if [ "$asset" != BTCUSDT ]; then sc enable --now $(coin_units "$asset") >/dev/null; fi
  for other in ETHUSDT BNBUSDT; do
    [ "$other" = "$asset" ] && continue
    for unit in $(coin_units "$other"); do
      if exists "$unit"; then
        sc disable --now "$unit" >/dev/null 2>&1 || sc stop "$unit" || true
      fi
    done
  done
  echo "已切到 $asset：只跑 BTC 基準與 $asset 的 producer，觀測器已停。"
  status
  short=${asset%USDT}
  echo
  echo "下一步（Telegram）：等 producer 跑滿約 10 分鐘（兩個市場）後"
  echo "  /predict_market $short  →  /predict_live on  →  /predict_loop 20"
}

case "${1:-status}" in
  status) status ;;
  slim) slim; status ;;
  use) shift; use "${1:-}" ;;
  *) echo "用法：$0 status | use BTC|ETH|BNB | slim" >&2; exit 2 ;;
esac
