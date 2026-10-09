# Step 5 (detached): runs 5_apply.sh in its own session, so a dropped SSH
# connection cannot hang it up halfway. Refuses to start a second time.
cd "$(dirname "$0")"
LOG="$HOME/t6u/apply.log"
if [ -e "$LOG" ]; then echo "apply.log already exists: step 5 was already started; do not start it again"; exit 1; fi
setsid nohup bash -c 'bash ./5_apply.sh; echo "EXIT=$?"' > "$LOG" 2>&1 < /dev/null &
sleep 2
echo STEP5_STARTED
