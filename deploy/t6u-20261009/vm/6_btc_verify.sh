# Step 6: back to BTC only (stops ETH/BNB producers), then verify the new release.
source "$(dirname "$0")/common.sh"
asj "/home/jack_shih/cry3/scripts/t6_coin.sh use BTC"
PIN=$(asj "cat /home/jack_shih/cry3/prediction/release-pin.env")
echo "$PIN"
N=$(asj "grep -c loop_lane_masked /home/jack_shih/cry3/src/gridbot/prediction/regime_t69a_bridge.py")
M=$(asj "$PY -B $DIR/overlay/deploy/t6u_check.py")
echo "loop_lane_masked=$N migration_029/table=$M"
asj "systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal" | tee /tmp/t6u_btc.txt
[ "$PIN" = "PREDICTION_EXPECTED_RELEASE_FINGERPRINT=$EXPECTED_FP" ] && [ "$N" -ge 1 ] && [ "$M" = "1 1" ] && [ "$(grep -cx active /tmp/t6u_btc.txt)" = 3 ] \
  && echo STEP6_OK || { echo STEP6_CHECK_FAILED; exit 1; }
