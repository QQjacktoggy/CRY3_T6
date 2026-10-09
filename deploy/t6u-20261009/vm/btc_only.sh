# After a rollback: back to BTC only and print the active release pin.
source "$(dirname "$0")/common.sh"
asj "/home/jack_shih/cry3/scripts/t6_coin.sh use BTC; cat /home/jack_shih/cry3/prediction/release-pin.env"
