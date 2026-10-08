# T6.9a 部署指南（給在 meihan PC 執行的部署者）— 2026-10-08

**要裝什麼**：PR #44（branch `claude/project-thread-f5a7xi`，commit 5511f0a）。
效果：`core_continuation_original` 停止下單（仍佔住該場，不讓其他子策略補位）；Flat F1–F4 Shadow 停止記錄；報表同步更新。
只換 VM 上 4 個檔：`src/gridbot/prediction/regime_t69a_{policy,bridge,report,shadow}.py`。VM 自己的 `release.py` 不動。

| 項目 | 值 |
|---|---|
| VM | `cry3jack`，project `project-f7b56371-5bd7-47cc-ad6`，zone `asia-east1-a`，IAP |
| gcloud 帳號 | `pennyfamily9512f@gmail.com` |
| 現行版本（安裝前） | `f1aed6580bd5ab2d1eedb7baec59d96306fb69ffea2ed4e789cf4166f67afdb8` |
| 安裝後版本（預期） | `8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa` |
| 安裝包 | `t6t.tgz` sha256 `8c5cca1a03015850132483731f7b73adb0d9210b609b551e29a97b724e418e93` |
| stage 名稱 | `t69-release-staged-t6t-v1-20261008` |
| 安裝工具 | VM 上既有 `/mnt/disks/data/cry3/operators/t69d-20261007/`（t69_manual_install.py、t69_rollback.py） |

**規則**：任何一步的最後一行不是表上寫的 OK 字樣，就**停下，不要往下做**，把完整輸出交給 jack。不要修改腳本、不要跳過檢查、不要動資料庫。

所有指令在 meihan 的 **Windows PowerShell** 執行。VM 端每一步都是一個小 bash 腳本（在 `vm/`），可先打開看內容。

---

## 0. 前置檢查（不改任何東西）
```powershell
gcloud config set account pennyfamily9512f@gmail.com
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="date -Is; free -m | head -2"
```
預期：印出 VM 時間和記憶體。連不上就停。

另外請 jack 在 Telegram `/predict_status` 確認**沒有 loop 在跑**。有在跑就停。

## 1. 取得安裝包並上傳到 VM
```powershell
cd C:\Users\pipi\Desktop\cry3_t6
git fetch origin claude/project-thread-f5a7xi
Remove-Item -Recurse -Force $env:TEMP\t6t -ErrorAction SilentlyContinue
git archive FETCH_HEAD deploy/t6t-20261008 --format=zip -o $env:TEMP\t6t.zip
Expand-Archive $env:TEMP\t6t.zip -DestinationPath $env:TEMP\t6t
(Get-FileHash $env:TEMP\t6t\deploy\t6t-20261008\vm\t6t.tgz -Algorithm SHA256).Hash
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="rm -rf ~/t6t; mkdir -p ~/t6t"
gcloud compute scp --recurse $env:TEMP\t6t\deploy\t6t-20261008\vm cry3jack:~/t6t/ --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap
```
預期：雜湊印出 `8C5CCA1A03015850132483731F7B73ADB0D9210B609B551E29A97B724E418E93`（大寫無妨）。scp 不報錯。
（`git fetch`／`git archive` 不會改動 meihan 這個資料夾裡的檔案。）

以下每步都用同一個格式，只換腳本名：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/<腳本名>"
```

## 2. 建 stage（只寫一個新資料夾，不動服務）
腳本名：`2_stage.sh`
預期：兩行 `OK`（雜湊核對），一段 JSON（`"changed"` 剛好 4 個 t69a 檔），`STAGE_FP=8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa`，最後一行 **`STEP2_OK`**。

## 3. 啟動 7 個服務（安裝程式要求全部 active）
腳本名：`3_services.sh`
它會啟動 ETH/BNB 的 4 個 producer（平常只跑 BTC，所以這 4 個會是 inactive）。
預期：7 行 `active`，最後一行 **`STEP3_OK`**。

## 4. 試跑安裝（唯讀，不改任何東西）
腳本名：`4_dryrun.sh`
預期：輸出含 **`READ_ONLY_PREFLIGHT_PASSED`**。
若出現 `Risk latch remains` 或其他錯誤：停下，把輸出交給 jack。

## 5. 正式安裝
腳本名：`5_apply.sh`
安裝程式會先備份（程式碼、manifest、pin）到 `/mnt/disks/data/cry3/operators/t69d-20261007/runs/<時間>`，再冷重啟 7 個服務；失敗會自動還原。
預期：輸出含 **`CODE_INSTALLED_LIVE_NOT_ACTIVATED`**，並印出備份資料夾路徑與 rollback 指令。**把這段輸出完整保存**（回退要用備份路徑）。

## 6. 切回只跑 BTC，並驗證新版本
腳本名：`6_btc_verify.sh`
它執行 `t6_coin.sh use BTC`（停掉 ETH/BNB producer），再檢查：
- `release-pin.env` = `PREDICTION_EXPECTED_RELEASE_FINGERPRINT=8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa`
- 新程式在位（`branch_disabled` 出現 1 次）
- 3 個 BTC 服務 active

預期最後一行 **`STEP6_OK`**。

## 7. 開新 loop（jack 在 Telegram 做）
等約 10 分鐘熱機 → `/predict_market BTC` → `/predict_live on` → `/predict_loop 100`。
報表標題應為「六路 Live（T6.7c＋First UP 5bp；continuation Original 停用）」，不再列 F1–F4。

---

## 回退（只在需要時）
`<備份路徑>` 用第 5 步印出的那個（形如 `/mnt/disks/data/cry3/operators/t69d-20261007/runs/1791xxxxxxxxx`）。
先試跑：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/rollback.sh <備份路徑>"
```
試跑通過後才正式回退（尾端加 `--apply`）：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/rollback.sh <備份路徑> --apply"
```
回退後版本回到 `f1aed658…`。接著切回 BTC 並看版本（腳本名 `btc_only.sh`，預期 pin 印出 `f1aed658…`）：
```powershell
gcloud compute ssh cry3jack --project=project-f7b56371-5bd7-47cc-ad6 --zone=asia-east1-a --tunnel-through-iap --command="bash ~/t6t/vm/btc_only.sh"
```
然後開新 loop。

## 連線出問題時
- `No active account` 或授權錯誤：`gcloud config set account pennyfamily9512f@gmail.com` 後重試。
- IAP 連線偶發中斷：同一步重跑即可。第 2 步若已建成 stage 再跑會印 `stage already exists`，表示上次已成功，直接做第 3 步。第 5 步中斷時**不要重跑**，先找 jack 確認版本（看 `release-pin.env`）。
