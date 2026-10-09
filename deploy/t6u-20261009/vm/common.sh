# Shared settings for the t6u install steps (sourced by each step).
set -euo pipefail
PY=/home/jack_shih/cry3/testnet/.venv/bin/python
OPS=/mnt/disks/data/cry3/operators
DIR=$OPS/t6u-20261009
STAGE=t69-release-staged-t6u-v1-20261009
TGZ_SHA=956f43abeb78d547cba32c9cb4240ec951ef9f5d8b08d4f280e5e0a857edb230
SCRIPT_SHA=77476d6087c3658797786b796a4fcf8ee67d938f9fed689ea3e0239acb346e89
PARENT=8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa
EXPECTED_FP=e8a460465d8234c531e1053d9eff9ce4d85ef164dfa06d537577a5cd9a02e4ef
LOOP=loop:1791510510192
INSTALLER=$OPS/t69d-20261007/t69_manual_install.py
ROLLBACK=$OPS/t69d-20261007/t69_rollback.py
FLAGS="--allow-historical-closed-ledger --allow-shared-mdd-halt scheduled20_mdd_3.5"
SERVICES="cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal"
asj() { sudo -n -u jack_shih bash -c "export XDG_RUNTIME_DIR=/run/user/\$(id -u); cd /tmp; $1"; }
stage_fp() { asj "$PY -c 'import json;print(json.load(open(\"/home/jack_shih/cry3/prediction/$STAGE/candidate.json\"))[\"expected_fingerprint\"])'"; }
