# 給 Grok：T6.9b 每輪 Lane 遮罩部署（PR #48）完整指南 — 2026-10-09

## 你的任務
在 meihan 這台 Windows PC 上，用 **PowerShell** 把 PR #48 安裝到 GCP VM `cry3jack`。從第 0 步依序做到第 6 步；第 7 步由 jack 在 Telegram 操作。

**規則（一定要遵守）**
1. 每一步的輸出都要看到該步寫明的 OK 字樣，才能做下一步。看不到就**停下**，把完整輸出交給 jack，不要自己想辦法修。
2. 不要修改腳本，不要跳過檢查，不要動資料庫，也不要執行本文件以外的 VM 指令。
3. 第 5 步只能啟動**一次**。失敗或中斷時**不要重跑**，直接交給 jack。
4. 每完成一步，回報一行：步驟編號、看到的 OK 字樣。第 5 步另外附上它印出的備份路徑（`backup`）。
5. 第 4 到第 6 步進行期間，請 jack **不要在 Telegram 下任何指令**。bot 重啟時會處理積壓的指令，可能觸發自動回退。

## 這次安裝的內容
- 效果：開新 loop 前可以用 `/predict_lanemask` 選這一輪要關掉哪些 lane（全開、只關原 C DOWN、關全部 DOWN、關全部 UP、自訂），不需要再出新版本。說明在 repo 的 `docs/T6_9A_LANE_MASK.md`。
- 換 VM 上 12 個既有檔，新增 2 個檔（`regime_t69a_lane_mask.py`、`migrations/029_loop_lane_mask.sql`）。VM 自己的 `release.py` 只多兩行新檔路徑。
- 服務重啟時 migration 029 會建立一張新表 `prediction_loop_lane_masks`，不改任何既有的表。
- T6.9a policy fingerprint 不變，既有的決策、風控和 MDD 狀態全部延續。

| 項目 | 值 |
|---|---|
| VM | `cry3jack`，project `project-f7b56371-5bd7-47cc-ad6`，zone `asia-east1-a`，走 IAP |
| gcloud 帳號 | `pennyfamily9512f@gmail.com` |
| 程式來源 | PR #48，branch `claude/kind-darwin-hsx2mz`，程式 commit `5a792b4` |
| 安裝前版本 | `8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa`（PR #44） |
| 安裝後版本（預期） | `e8a460465d8234c531e1053d9eff9ce4d85ef164dfa06d537577a5cd9a02e4ef` |
| 安裝包 `t6u.tgz` sha256 | `19e8cb02e475241a499aeb69068708d8f044906bd7e61b0d8c5474381851256c` |
| 腳本清單 `SHA256SUMS` sha256 | `be62bac073d17aa0890d6b17cf6bd103dc4dd7a307474279a583ad50ef84fe2d` |
| VM 安裝工具（已在 VM 上） | `/mnt/disks/data/cry3/operators/t69d-20261007/`（t69_manual_install.py、t69_rollback.py） |
| 安裝用 loop-id | `loop:1791510510192`（10-09 09:48–18:10，DONE 100/100） |

---

## 0. 前置檢查（不改任何東西）
先請 jack 在 Telegram 打 `/predict_status`，確認**沒有 loop 在跑**。有在跑就停。

然後在 PowerShell 執行：
```powershell
gcloud config set account pennyfamily9512f@gmail.com
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="date -Is; free -m | head -2"
```
預期：印出 VM 時間和記憶體兩行。連不上就停。**OK 字樣：看到 `Mem:` 那一行。**

## 1. 核對 VM 上的安裝腳本
腳本已經放在 VM 上同一個帳號的 `~/t6u/vm/`。執行：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="cd ~/t6u/vm && sha256sum SHA256SUMS && sha256sum -c SHA256SUMS"
```
預期：
- 第一行 `be62bac073d17aa0890d6b17cf6bd103dc4dd7a307474279a583ad50ef84fe2d  SHA256SUMS`
- 接著 11 行，每行結尾都是 `OK`（2_stage.sh、3_services.sh、4_dryrun.sh、5_apply.sh、5_apply_bg.sh、5_status.sh、6_btc_verify.sh、btc_only.sh、common.sh、rollback.sh、t6u.tgz）

**OK 字樣：第一行雜湊相符，且 11 行都是 `OK`。**

若顯示 `No such file or directory` 或有任何一行 `FAILED`，改用以下方式從 GitHub 取得並上傳，再重做第 1 步：
```powershell
cd C:\Users\pipi\Desktop\cry3_t6
git fetch origin claude/kind-darwin-hsx2mz
Remove-Item -Recurse -Force $env:TEMP\t6u -ErrorAction SilentlyContinue
git archive FETCH_HEAD deploy/t6u-20261009/vm --format=zip -o $env:TEMP\t6u.zip
Expand-Archive $env:TEMP\t6u.zip -DestinationPath $env:TEMP\t6u
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="rm -rf ~/t6u; mkdir -p ~/t6u"
gcloud compute scp --recurse $env:TEMP\t6u\deploy\t6u-20261009\vm cry3jack:~/t6u/ --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap
```
（`git fetch`／`git archive` 不會改動 meihan 資料夾裡的檔案。）

---

以下第 2–6 步都用同一個格式，只換腳本名：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6u/vm/<腳本名>"
```

## 2. 建 stage（只寫一個新資料夾，不動服務）
腳本名：`2_stage.sh`
預期：兩行 `OK`（雜湊核對）；一段 JSON，其中 `"changed"` 剛好列出 14 個檔；`STAGE_FP=e8a460465d8234c531e1053d9eff9ce4d85ef164dfa06d537577a5cd9a02e4ef`。
**OK 字樣：最後一行 `STEP2_OK`。**

## 3. 啟動 7 個服務（安裝程式要求全部 active）
腳本名：`3_services.sh`
它會啟動 ETH/BNB 的 4 個 producer。平常只跑 BTC，這 4 個原本是 inactive，屬正常。
預期：7 行 `active`。**OK 字樣：最後一行 `STEP3_OK`。**

## 4. 試跑安裝（唯讀，不改任何東西）
腳本名：`4_dryrun.sh`
**OK 字樣：輸出含 `READ_ONLY_PREFLIGHT_PASSED`。**
若出現 `Risk latch remains`、`Another loop is RUNNING` 或任何其他錯誤：停下，交給 jack。

## 5. 正式安裝（在 VM 背景執行，只啟動一次）
啟動：腳本名 `5_apply_bg.sh`。**OK 字樣：`STEP5_STARTED`。**
安裝會在 VM 背景執行，PowerShell 斷線也不會中斷。若印出 `apply.log already exists`，代表之前已經啟動過，**不要再試**，直接看進度。

看進度：腳本名 `5_status.sh`。每隔約 1 分鐘執行一次，直到最後一行變成 `STEP5_OK` 或 `STEP5_FAILED`。
- `STEP5_RUNNING`：還在跑，通常 3–5 分鐘（先停 7 個服務，換檔，再冷重啟並觀察 20 秒）。
- **`STEP5_OK`**：成功。輸出中有 `CODE_INSTALLED_LIVE_NOT_ACTIVATED`、`backup` 路徑和 `rollback` 指令，把這段輸出完整交給 jack。
- `STEP5_FAILED`：安裝程式已自動還原舊版本。停下，把完整輸出交給 jack，不要重跑。

## 6. 切回只跑 BTC，並驗證新版本
腳本名：`6_btc_verify.sh`
它會先執行 `t6_coin.sh use BTC`，停掉 ETH/BNB producer，再檢查：
- `release-pin.env` = `PREDICTION_EXPECTED_RELEASE_FINGERPRINT=e8a460465d8234c531e1053d9eff9ce4d85ef164dfa06d537577a5cd9a02e4ef`
- 新程式已在位：`loop_lane_masked` 至少出現 1 次
- `migration_029/table/t69b_report=1 1 1`：migration 已套用、遮罩表存在、T6.9b 報表能產生且有「本輪 Lane」那一行
- 3 個 BTC 服務 active

**OK 字樣：最後一行 `STEP6_OK`。** 做到這裡，Grok 的工作就完成了，回報 jack。

## 7. 開新 loop（jack 在 Telegram 操作）
等約 10 分鐘熱機，然後依序：`/predict_market BTC` → `/predict_live on` → `/predict_lanemask` 選「全開」→ `/predict_loop 100`。
報表標頭會多一行「本輪 Lane：全開」。第一輪先用全開，確認決策和報表跟以前一樣，之後再開始用遮罩。

---

## 回退（只在 jack 要求時做）
`<備份路徑>` 用第 5 步印出的那個，形如 `/mnt/disks/data/cry3/operators/t69d-20261007/runs/1791xxxxxxxxx`。
先試跑（唯讀）：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6u/vm/rollback.sh <備份路徑>"
```
試跑通過後，才正式回退（尾端加 `--apply`）：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6u/vm/rollback.sh <備份路徑> --apply"
```
接著切回 BTC 並看版本：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6u/vm/btc_only.sh"
```
預期 pin 印出 `8aeb7317…`。新增的遮罩表會留在資料庫，舊版程式不會讀它，不影響運作。然後 jack 開新 loop。

## 連線出問題時
- 出現 `No active account` 或授權錯誤：先執行 `gcloud config set account pennyfamily9512f@gmail.com`，再重試同一步。
- IAP 連線偶發中斷時，同一步重跑即可，但有三個例外：
  - 第 2 步重跑若印出 `stage already exists`，代表上次已經建好，直接做第 3 步。
  - 第 5 步**不要**重跑 `5_apply_bg.sh`，只執行 `5_status.sh` 看進度。
  - 第 6 步可以重跑。

---

## 附錄：VM 端腳本內容（供閱讀，與 VM 上 `~/t6u/vm/` 的檔案一致）

### common.sh
```bash
# Shared settings for the t6u install steps (sourced by each step).
set -euo pipefail
PY=/home/jack_shih/cry3/testnet/.venv/bin/python
OPS=/mnt/disks/data/cry3/operators
DIR=$OPS/t6u-20261009
STAGE=t69-release-staged-t6u-v1-20261009
TGZ_SHA=19e8cb02e475241a499aeb69068708d8f044906bd7e61b0d8c5474381851256c
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
```

### 2_stage.sh
```bash
# Step 2: unpack the bundle and build the stage (writes only a new stage dir).
source "$(dirname "$0")/common.sh"
cd "$(dirname "$0")"
echo "$TGZ_SHA  t6u.tgz" | sha256sum -c
sudo install -d -o jack_shih -m 700 "$DIR"
sudo install -o jack_shih -m 600 t6u.tgz "$DIR/"
asj "cd $DIR && rm -rf overlay && mkdir overlay && tar -xzf t6u.tgz -C overlay && echo '$SCRIPT_SHA  overlay/deploy/t6u_stage_build.py' | sha256sum -c"
asj "$PY -B $DIR/overlay/deploy/t6u_stage_build.py --root /home/jack_shih/cry3 --overlay $DIR/overlay --stage $STAGE"
FP=$(stage_fp)
echo "STAGE_FP=$FP"
[ "$FP" = "$EXPECTED_FP" ] && echo STEP2_OK || { echo "STEP2_FP_MISMATCH expected $EXPECTED_FP"; exit 1; }
```

### 3_services.sh
```bash
# Step 3: the installer preflight needs all 7 services active (starts the ETH/BNB producers).
source "$(dirname "$0")/common.sh"
asj "systemctl --user start cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal; sleep 5; systemctl --user is-active $SERVICES" | tee /tmp/t6u_services.txt
[ "$(grep -cx active /tmp/t6u_services.txt)" = 7 ] && echo STEP3_OK || { echo STEP3_NOT_ALL_ACTIVE; exit 1; }
```

### 4_dryrun.sh
```bash
# Step 4: read-only preflight. Changes nothing.
source "$(dirname "$0")/common.sh"
FP=$(stage_fp); [ "$FP" = "$EXPECTED_FP" ] || { echo "STAGE_FP_MISMATCH $FP"; exit 1; }
asj "$PY $INSTALLER --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP $FLAGS"
```

### 5_apply.sh
```bash
# Step 5: install. Backs up first, cold-restarts the 7 services, rolls back by itself on failure.
source "$(dirname "$0")/common.sh"
FP=$(stage_fp); [ "$FP" = "$EXPECTED_FP" ] || { echo "STAGE_FP_MISMATCH $FP"; exit 1; }
asj "$PY $INSTALLER --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP $FLAGS --apply"
```

### 5_apply_bg.sh
```bash
# Step 5 (detached): runs 5_apply.sh in its own session, so a dropped SSH
# connection cannot hang it up halfway. Refuses to start a second time.
cd "$(dirname "$0")"
LOG="$HOME/t6u/apply.log"
if [ -e "$LOG" ]; then echo "apply.log already exists: step 5 was already started; do not start it again"; exit 1; fi
setsid nohup bash -c 'bash ./5_apply.sh; echo "EXIT=$?"' > "$LOG" 2>&1 < /dev/null &
sleep 2
echo STEP5_STARTED
```

### 5_status.sh
```bash
# Step 5 progress: prints the end of the install log and whether it finished.
LOG="$HOME/t6u/apply.log"
[ -e "$LOG" ] || { echo "STEP5_NOT_STARTED"; exit 1; }
tail -n 40 "$LOG"
if grep -q '^EXIT=' "$LOG"; then
  if grep -q CODE_INSTALLED_LIVE_NOT_ACTIVATED "$LOG" && grep -q '^EXIT=0$' "$LOG"; then echo STEP5_OK; else echo STEP5_FAILED; fi
else
  echo STEP5_RUNNING
fi
```

### 6_btc_verify.sh
```bash
# Step 6: back to BTC only (stops ETH/BNB producers), then verify the new release.
source "$(dirname "$0")/common.sh"
asj "/home/jack_shih/cry3/scripts/t6_coin.sh use BTC"
PIN=$(asj "cat /home/jack_shih/cry3/prediction/release-pin.env")
echo "$PIN"
N=$(asj "grep -c loop_lane_masked /home/jack_shih/cry3/src/gridbot/prediction/regime_t69a_bridge.py")
M=$(asj "$PY -B $DIR/overlay/deploy/t6u_check.py")
echo "loop_lane_masked=$N migration_029/table/t69b_report=$M"
asj "systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal" | tee /tmp/t6u_btc.txt
[ "$PIN" = "PREDICTION_EXPECTED_RELEASE_FINGERPRINT=$EXPECTED_FP" ] && [ "$N" -ge 1 ] && [ "$M" = "1 1 1" ] && [ "$(grep -cx active /tmp/t6u_btc.txt)" = 3 ] \
  && echo STEP6_OK || { echo STEP6_CHECK_FAILED; exit 1; }
```

### rollback.sh
```bash
# Rollback: bash rollback.sh <backup dir printed by step 5> [--apply]
# Without --apply it is a read-only dry run.
source "$(dirname "$0")/common.sh"
BACKUP=${1:?usage: rollback.sh <backup dir> [--apply]}
case "$BACKUP" in "$OPS"/t69d-20261007/runs/*) ;; *) echo "backup dir must be under $OPS/t69d-20261007/runs/"; exit 1;; esac
asj "$PY $ROLLBACK --backup $BACKUP $FLAGS ${2:-}"
```

### btc_only.sh
```bash
# After a rollback: back to BTC only and print the active release pin.
source "$(dirname "$0")/common.sh"
asj "/home/jack_shih/cry3/scripts/t6_coin.sh use BTC; cat /home/jack_shih/cry3/prediction/release-pin.env"
```
