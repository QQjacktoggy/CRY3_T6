# Shared settings for the t6t install steps (sourced by each step).
set -euo pipefail
PY=/home/jack_shih/cry3/testnet/.venv/bin/python
OPS=/mnt/disks/data/cry3/operators
DIR=$OPS/t6t-20261008
STAGE=t69-release-staged-t6t-v1-20261008
TGZ_SHA=8c5cca1a03015850132483731f7b73adb0d9210b609b551e29a97b724e418e93
SCRIPT_SHA=bf5200a958dadd2df2fddeda7cb7e7febc02bc4f5cdd222d494a317cf62c999a
PARENT=f1aed6580bd5ab2d1eedb7baec59d96306fb69ffea2ed4e789cf4166f67afdb8
EXPECTED_FP=8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa
LOOP=loop:1791412028086
INSTALLER=$OPS/t69d-20261007/t69_manual_install.py
ROLLBACK=$OPS/t69d-20261007/t69_rollback.py
FLAGS="--allow-cancelled-loop --allow-historical-closed-ledger --allow-shared-mdd-halt scheduled20_mdd_3.5"
SERVICES="cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal"
asj() { sudo -n -u jack_shih bash -c "export XDG_RUNTIME_DIR=/run/user/\$(id -u); cd /tmp; $1"; }
stage_fp() { asj "$PY -c 'import json;print(json.load(open(\"/home/jack_shih/cry3/prediction/$STAGE/candidate.json\"))[\"expected_fingerprint\"])'"; }
