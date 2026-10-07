# 給 Grok：T6.9a 部署（PR #44）完整指南 — 2026-10-08

## 你的任務
在 meihan 這台 Windows PC 上，用 **PowerShell** 把 PR #44 安裝到 GCP VM `cry3jack`，從第 0 步依序做到第 6 步。第 7 步由 jack 在 Telegram 操作。

**規則（一定要遵守）**
1. 每一步的輸出都要看到該步寫明的 OK 字樣，才能做下一步。看不到就**停下**，把完整輸出交給 jack，不要自己想辦法修。
2. 不要修改腳本，不要跳過檢查，不要動資料庫，也不要執行本文件以外的 VM 指令。
3. 第 5 步（正式安裝）中斷或失敗時**不要重跑**，直接交給 jack。
4. 每完成一步，回報一行：步驟編號、看到的 OK 字樣。第 5 步另外附上它印出的備份路徑。

## 這次安裝的內容
- 效果：`core_continuation_original` 停止下單（仍佔住該場，不讓其他子策略補位）；Flat F1–F4 Shadow 停止記錄；報表同步更新。
- 只換 VM 上 4 個檔：`src/gridbot/prediction/regime_t69a_{policy,bridge,report,shadow}.py`。VM 自己的 `release.py` 不動。

| 項目 | 值 |
|---|---|
| VM | `cry3jack`，project `project-f7b56371-5bd7-47cc-ad6`，zone `asia-east1-a`，走 IAP |
| gcloud 帳號 | `pennyfamily9512f@gmail.com` |
| 安裝前版本 | `f1aed6580bd5ab2d1eedb7baec59d96306fb69ffea2ed4e789cf4166f67afdb8` |
| 安裝後版本（預期） | `8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa` |
| 安裝包 `t6t.tgz` sha256 | `8c5cca1a03015850132483731f7b73adb0d9210b609b551e29a97b724e418e93` |
| VM 安裝工具（已在 VM 上） | `/mnt/disks/data/cry3/operators/t69d-20261007/`（t69_manual_install.py、t69_rollback.py） |

---

## 0. 前置檢查（不改任何東西）
先請 jack 在 Telegram 打 `/predict_status`，確認**沒有 loop 在跑**。有在跑就停。

然後在 PowerShell 執行：
```powershell
gcloud config set account pennyfamily9512f@gmail.com
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="date -Is; free -m | head -2"
```
預期：印出 VM 時間和記憶體兩行。連不上就停。**OK 字樣：看到 `Mem:` 那一行。**

## 1. 從本文件取出安裝包並上傳 VM
安裝包（`vm` 資料夾的 zip）以 base64 放在本文件最後的〔附錄 B〕。先把**本文件原封不動**存成 `$env:TEMP\GROK_DEPLOY.md`，然後執行：
```powershell
$md = Get-Content -Raw "$env:TEMP\GROK_DEPLOY.md"
$b64 = ($md -split '<!-- VMZIP-BEGIN -->')[-1] -split '<!-- VMZIP-END -->' | Select-Object -First 1
$b64 = $b64 -replace '[^A-Za-z0-9+/=]', ''
Remove-Item -Recurse -Force "$env:TEMP\t6t" -ErrorAction SilentlyContinue
New-Item -ItemType Directory "$env:TEMP\t6t" | Out-Null
[IO.File]::WriteAllBytes("$env:TEMP\t6t\t6t_vm.zip", [Convert]::FromBase64String($b64))
(Get-FileHash "$env:TEMP\t6t\t6t_vm.zip" -Algorithm SHA256).Hash
Expand-Archive "$env:TEMP\t6t\t6t_vm.zip" -DestinationPath "$env:TEMP\t6t"
(Get-FileHash "$env:TEMP\t6t\vm\t6t.tgz" -Algorithm SHA256).Hash
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="rm -rf ~/t6t; mkdir -p ~/t6t"
gcloud compute scp --recurse "$env:TEMP\t6t\vm" cry3jack:~/t6t/ --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="ls ~/t6t/vm"
```
預期：
- 第一個雜湊 `2DC9276C1FC787D723A10A1F169337CD7D23486325B4F145BD1C3853E1633AE2`
- 第二個雜湊 `8C5CCA1A03015850132483731F7B73ADB0D9210B609B551E29A97B724E418E93`
- 最後列出 9 個檔：`2_stage.sh 3_services.sh 4_dryrun.sh 5_apply.sh 6_btc_verify.sh btc_only.sh common.sh rollback.sh t6t.tgz`

**OK 字樣：兩個雜湊都相符，9 個檔都在。** 雜湊不符就停（通常是文件被改動或沒完整存下）。

（備用取得方式：若 meihan 的 `C:\Users\pipi\Desktop\cry3_t6` 能 `git fetch origin claude/project-thread-f5a7xi`，同一個 `vm` 資料夾在該 branch 的 `deploy/t6t-20261008/vm`。）

---

以下第 2–6 步都用同一個格式，只換腳本名：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/<腳本名>"
```

## 2. 建 stage（只寫一個新資料夾，不動服務）
腳本名：`2_stage.sh`
預期：兩行 `OK`（雜湊核對），一段 JSON（`"changed"` 剛好是 4 個 t69a 檔），`STAGE_FP=8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa`。
**OK 字樣：最後一行 `STEP2_OK`。**

## 3. 啟動 7 個服務（安裝程式要求全部 active）
腳本名：`3_services.sh`
它會啟動 ETH/BNB 的 4 個 producer（平常只跑 BTC，這 4 個原本是 inactive，屬正常）。
預期：7 行 `active`。**OK 字樣：最後一行 `STEP3_OK`。**

## 4. 試跑安裝（唯讀，不改任何東西）
腳本名：`4_dryrun.sh`
**OK 字樣：輸出含 `READ_ONLY_PREFLIGHT_PASSED`。**
若出現 `Risk latch remains` 或任何錯誤：停下，交給 jack。

## 5. 正式安裝
腳本名：`5_apply.sh`
安裝程式會先備份程式碼、manifest 和 pin，備份到 `/mnt/disks/data/cry3/operators/t69d-20261007/runs/<時間>`，然後冷重啟 7 個服務；失敗時會自動還原。
**OK 字樣：輸出含 `CODE_INSTALLED_LIVE_NOT_ACTIVATED`。** 它會印出備份資料夾路徑和 rollback 指令，**把這段輸出完整保存並交給 jack**。

## 6. 切回只跑 BTC，並驗證新版本
腳本名：`6_btc_verify.sh`
它會執行 `t6_coin.sh use BTC`（停掉 ETH/BNB producer），再檢查：
- `release-pin.env` = `PREDICTION_EXPECTED_RELEASE_FINGERPRINT=8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa`
- 新程式已在位（`branch_disabled` 出現 1 次）
- 3 個 BTC 服務 active

**OK 字樣：最後一行 `STEP6_OK`。** 做到這裡，Grok 的工作就完成了，回報 jack。

## 7. 開新 loop（jack 在 Telegram 操作）
等約 10 分鐘熱機 → `/predict_market BTC` → `/predict_live on` → `/predict_loop 100`。
報表標題應為「六路 Live（T6.7c＋First UP 5bp；continuation Original 停用）」，不再列 F1–F4。

---

## 回退（只在 jack 要求時做）
`<備份路徑>` 用第 5 步印出的那個（形如 `/mnt/disks/data/cry3/operators/t69d-20261007/runs/1791xxxxxxxxx`）。
先試跑（唯讀）：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/rollback.sh <備份路徑>"
```
試跑通過後才正式回退（尾端加 `--apply`）：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/rollback.sh <備份路徑> --apply"
```
接著切回 BTC 並看版本：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/btc_only.sh"
```
預期 pin 印出 `f1aed658…`。然後 jack 開新 loop。

## 連線出問題時
- 出現 `No active account` 或授權錯誤：先執行 `gcloud config set account pennyfamily9512f@gmail.com`，再重試同一步。
- IAP 連線偶發中斷：同一步重跑即可，但有兩個例外：
  - 第 2 步重跑時若印出 `stage already exists`，代表上次已經建好，直接做第 3 步。
  - 第 5 步中斷時**不要重跑**，交給 jack 確認版本。

---

## 附錄 A：VM 端腳本內容（供閱讀；與附錄 B 的 zip 內容一致）

### common.sh
```bash
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
```

### 2_stage.sh
```bash
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
```

### 3_services.sh
```bash
# Step 3: the installer preflight needs all 7 services active (starts the ETH/BNB producers).
source "$(dirname "$0")/common.sh"
asj "systemctl --user start cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal; sleep 5; systemctl --user is-active $SERVICES" | tee /tmp/t6t_services.txt
[ "$(grep -cx active /tmp/t6t_services.txt)" = 7 ] && echo STEP3_OK || { echo STEP3_NOT_ALL_ACTIVE; exit 1; }
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

### 6_btc_verify.sh
```bash
# Step 6: back to BTC only (stops ETH/BNB producers), then verify the new release.
source "$(dirname "$0")/common.sh"
asj "/home/jack_shih/cry3/scripts/t6_coin.sh use BTC"
PIN=$(asj "cat /home/jack_shih/cry3/prediction/release-pin.env")
echo "$PIN"
N=$(asj "grep -c branch_disabled /home/jack_shih/cry3/src/gridbot/prediction/regime_t69a_bridge.py")
asj "systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal" | tee /tmp/t6t_btc.txt
[ "$PIN" = "PREDICTION_EXPECTED_RELEASE_FINGERPRINT=$EXPECTED_FP" ] && [ "$N" = 1 ] && [ "$(grep -cx active /tmp/t6t_btc.txt)" = 3 ] \
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

`t6t.tgz` 內含 4 個新版 t69a 檔與建 stage 用的 `deploy/t6t_stage_build.py`（sha256 `bf5200a958dadd2df2fddeda7cb7e7febc02bc4f5cdd222d494a317cf62c999a`）；原始碼見 GitHub PR #44。

## 附錄 B：安裝包（base64；第 1 步會自動讀取，不要手動修改）
<!-- VMZIP-BEGIN -->
```
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
51dIKhTYS3uFpmRyhf72Rtvkuh3LuHJoMt19HLK+KS9j1gfT1FdtV9Z2NnwuqoarmbNnPTtN/tfs36ZaNnUwHW0auDdVpfM2LIYM
p8l8/udpaBqkBZVr9/uBbqxwmvw1fzO29Q9QSwMEFAAAAAgAAABIXYvjq8wBAQAAfQEAAA4AAAB2bS9yb2xsYmFjay5zaHWQy0rD
QBSG9/MUP9MsrJDmAlZMvFAFXRhoaBERcTHJDCaazIS5gKX03TvBLLrQzfCfA+c735kZNqrrKlZ/Z6iYaaCncuHz9RjcAN5qDLqV
VnBUOxgrBlzc4j0M2TB0uw8yw2trG+UsphZai9aAQQvGQyV9h+sdtJMLYpTTtQANzjxXsn6MMZ1Htep7Jf1eSu5XD88v5U2wT7I7
Z9inyP7zOrE4kJqZEfY7TdFKX6zLLY3s8oqHaZwukzi+jLyFic7nyHP4V9SNAj25tHfGohJwkguNEfDXPM0hfvyViacIw2rCzJdf
V74h2KyLYnTwnzFhJyUEj8XqaYtgn2bhgZIjUEsDBBQAAAAIAAAASF1ymRpzhUcAAHtHAAAKAAAAdm0vdDZ0LnRnegASQO2/H4sI
AAAAAAAAA+w8TXPbRpY581f0OgeQa5oWJVN25EFSskwlqsiSS5KTSblcCAg0JUQgwACgZM7UHPayNTns/oKt2tP+gb3sZX7NVqX2
OH9h30c30A2AFO3YmamasCox1R+vX7/v97qbeRY8/OQjf7bg83g0on/hU/+Xvg9H27vD0Wj3EbXvjnYefyJGHxsx/Czyws+E+DWW
+nv85MD/yywKJ2nx0eTgnfk/3Bo+Hv7G/1/jY/J/nskwCoooTT6sKLw7/7d3dn7j/6/yWcH/TF5GM+kVu5/53gS6L+VgvnzfNZDB
u48ereD/zqPRcLfG/92dRzufiK0PudFVn39w/t+7d+9id/CZL8JF5k9iKYI0kw+mUZYXwg/9eSGzPQEjHgcilzcyEbGfyFzM40Uu
iispDmnkq5diNJmLaZym2aDTOUnF8MlWLs7kVGYyCaSY+MH1NIrjgYC+NAtlBv8XWZRfi9ssKgBiuijyKJQENL/yQRTFbZpdS4AH
OHai2TzNCvFDnib6e/5jDDN3OtMsnQHaSSHfFnE0Eao7iNM8Si473D9QEo3o6xGhDOzOYnfHGiDj6DIConjyrQwWqBj18Y/t8XqY
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
```
<!-- VMZIP-END -->
