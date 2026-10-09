# Step 5 progress: prints the end of the install log and whether it finished.
LOG="$HOME/t6u/apply.log"
[ -e "$LOG" ] || { echo "STEP5_NOT_STARTED"; exit 1; }
tail -n 40 "$LOG"
if grep -q '^EXIT=' "$LOG"; then
  if grep -q CODE_INSTALLED_LIVE_NOT_ACTIVATED "$LOG" && grep -q '^EXIT=0$' "$LOG"; then echo STEP5_OK; else echo STEP5_FAILED; fi
else
  echo STEP5_RUNNING
fi
