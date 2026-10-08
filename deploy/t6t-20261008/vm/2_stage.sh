# Step 2: unpack the bundle and build the stage (writes only a new stage dir).
source "$(dirname "$0")/common.sh"
cd "$(dirname "$0")"
echo "$TGZ_SHA  t6t.tgz" | sha256sum -c
sudo install -d -o jack_shih -m 700 "$DIR"
sudo install -o jack_shih -m 600 t6t.tgz "$DIR/"
asj "cd $DIR && rm -rf overlay && mkdir overlay && tar -xzf t6t.tgz -C overlay && echo '$SCRIPT_SHA  overlay/deploy/t6t_stage_build.py' | sha256sum -c"
asj "$PY -B $DIR/overlay/deploy/t6t_stage_build.py --root /home/jack_shih/cry3 --overlay $DIR/overlay --stage $STAGE"
FP=$(stage_fp)
echo "STAGE_FP=$FP"
[ "$FP" = "$EXPECTED_FP" ] && echo STEP2_OK || { echo "STEP2_FP_MISMATCH expected $EXPECTED_FP"; exit 1; }
