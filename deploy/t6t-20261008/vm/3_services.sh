# Step 3: the installer preflight needs all 7 services active (starts the ETH/BNB producers).
source "$(dirname "$0")/common.sh"
asj "systemctl --user start cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal; sleep 5; systemctl --user is-active $SERVICES" | tee /tmp/t6t_services.txt
[ "$(grep -cx active /tmp/t6t_services.txt)" = 7 ] && echo STEP3_OK || { echo STEP3_NOT_ALL_ACTIVE; exit 1; }
