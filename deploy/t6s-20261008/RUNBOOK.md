# 安裝 PR #42（淺回撤逆勢條件進 Live）— 2026-10-08

全部在 GCP 網頁 SSH 貼上，不含帳密。安裝程式不改資料庫、不解鎖。
安裝工具沿用 `operators/t69d-20261007`（上次 PR #38 用的同一套，含共用 MDD 鎖開關）。

| 項目 | 值 |
|---|---|
| 程式來源 | main 3426e10（PR #42） |
| 現行版本（parent） | c2e8cfcddbdddd1bb55c0271e4f47e18decaf5117ca3bb96724e21ce2d607749 |
| 改動檔案 | regime_t69a_policy.py、regime_t69a_bridge.py、regime_t69a_report.py、release.py；新增 regime_t69a_shallow_filter.py |
| 貼上包 | paste_t6s.txt → ~/t6s.tgz，sha256 2273ef68a81e1e933d71aa8af1997855516be3af3540e1d7dcffc581d5508399 |
| 建 stage 腳本 | deploy/t6s_stage_build.py，sha256 a7109e7cbcda094e1e9d0e573637fcac978224e7e9ea312d7f316ab1bbdd6394 |
| stage 名稱 | t69-release-staged-t6s-v1-20261008 |
| T6.9a policy fingerprint | 129fbf0ce8120df928df9bd370fc6e5a038a0f9b4225d4cd118e7621d211ba8d（會變，所以要開新 loop） |

## 0. 目前的 loop（已完成）
loop:1791362869075 已在 21:40 因 MDD 停單後取消（23:19 唯讀查到：CANCELLED，63/100，無未結部位，HS 未鎖）。下面指令裡的 `LOOP` 用 `loop:1791362869075`。
這輪是被自己的 MDD 停下的，第 5 步試跑可能會印「Risk latch remains」；遇到就停，把輸出貼回 thread。

## 1. 貼上包
打開 paste_t6s.txt，整份複製，貼進網頁 SSH，按 Enter。最後一行印出的雜湊要等於上表，不一樣就停。

## 2. 放進 operators、核對、解開
```bash
sudo install -d -o jack_shih -m 700 /mnt/disks/data/cry3/operators/t6s-20261008 && sudo install -o jack_shih -m 600 ~/t6s.tgz /mnt/disks/data/cry3/operators/t6s-20261008/ && sudo -u jack_shih bash -c 'cd /mnt/disks/data/cry3/operators/t6s-20261008 && sha256sum t6s.tgz && mkdir overlay && tar -xzf t6s.tgz -C overlay && sha256sum overlay/deploy/t6s_stage_build.py'
```
兩個雜湊要和上表一樣，不一樣就停。

## 3. 建 stage（只寫新目錄，不動服務）
```bash
sudo -u jack_shih bash -c 'cd /tmp; /home/jack_shih/cry3/testnet/.venv/bin/python -B /mnt/disks/data/cry3/operators/t6s-20261008/overlay/deploy/t6s_stage_build.py --root /home/jack_shih/cry3 --overlay /mnt/disks/data/cry3/operators/t6s-20261008/overlay --stage t69-release-staged-t6s-v1-20261008'
```
腳本先唯讀檢查：現行版本是 c2e8cfcd…、VM 上 4 個舊檔等於 main、新檔還不存在、上傳的 5 個檔等於 main 3426e10。任何一項不符就在寫入前停下。
通過會印出 `"fingerprint": "…"`，這就是新版本的 FP。**把輸出貼回 thread**，Claude 核對後才做第 4 步。

## 4. 確認 7 個服務都在跑
```bash
sudo -u jack_shih bash -c 'export XDG_RUNTIME_DIR=/run/user/$(id -u); systemctl --user start cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal; sleep 5; systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal'
```
7 行都要是 active。

## 5. 試跑（不改任何東西）
`FP` 換成第 3 步的 fingerprint，`LOOP` 換成第 0 步的 loop-id。
```bash
sudo -u jack_shih bash -c 'cd /tmp; export XDG_RUNTIME_DIR=/run/user/$(id -u); /home/jack_shih/cry3/testnet/.venv/bin/python /mnt/disks/data/cry3/operators/t69d-20261007/t69_manual_install.py --expected-fingerprint FP --stage t69-release-staged-t6s-v1-20261008 --loop-id LOOP --allow-cancelled-loop --allow-historical-closed-ledger --allow-shared-mdd-halt scheduled20_mdd_3.5'
```
要印 READ_ONLY_PREFLIGHT_PASSED。若印「Risk latch remains」或其他錯誤，停下把輸出貼回 thread（例如這輪是被自己的 MDD 停單，需要另外處理）。

## 6. 正式安裝
同第 5 步，在最後的單引號前加上 ` --apply`。安裝程式會先備份，再冷重啟 7 個服務；失敗會自動還原。成功印 CODE_INSTALLED_LIVE_NOT_ACTIVATED 和 rollback 指令。把輸出貼回 thread。

## 7. 切回 BTC
```bash
sudo -u jack_shih bash -c 'export XDG_RUNTIME_DIR=/run/user/$(id -u); cd /tmp && /home/jack_shih/cry3/scripts/t6_coin.sh use BTC'
```

## 8. 驗證
```bash
sudo -u jack_shih bash -c 'cat /home/jack_shih/cry3/prediction/release-pin.env; grep -c shallow_prior_not_against /home/jack_shih/cry3/src/gridbot/prediction/regime_t69a_bridge.py; export XDG_RUNTIME_DIR=/run/user/$(id -u); systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal'
```
pin 要等於 FP，grep 數字大於 0，3 個服務 active。

## 9. TG 開新 loop
1. /predict_status。若顯示共用 MDD 停單鎖，按「🔓 解除 T6 MDD 停單鎖」→ 60 秒內按「確認解除（執行）」。
2. 等約 10 分鐘熱機。
3. /predict_live on → /predict_loop 100。報表會多一段〔淺回撤逆勢條件〕。

## 回退（需要時）
用第 6 步印出的 rollback 指令（先不加 --apply 試跑，再加 --apply）。回退後 T6.9a 指紋回到舊值，要再開新 loop。
