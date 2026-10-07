# 安裝 PR #44（關掉 continuation Original、移除 F1–F4 Shadow）— 2026-10-08

全部在 GCP 網頁 SSH 貼上，不含帳密。安裝前先確認**沒有 loop 在跑**（TG /predict_status）。

| 項目 | 值 |
|---|---|
| 程式來源 | PR #44 commit 5511f0a |
| 現行版本（parent） | f1aed6580bd5ab2d1eedb7baec59d96306fb69ffea2ed4e789cf4166f67afdb8 |
| 改動檔案 | regime_t69a_policy.py、regime_t69a_bridge.py、regime_t69a_report.py、regime_t69a_shadow.py（release.py 不動） |
| 貼上包 | paste_t6t.txt → ~/t6t.tgz，sha256 8c5cca1a03015850132483731f7b73adb0d9210b609b551e29a97b724e418e93 |
| 建 stage 腳本 | deploy/t6t_stage_build.py，sha256 bf5200a958dadd2df2fddeda7cb7e7febc02bc4f5cdd222d494a317cf62c999a |
| stage 名稱 | t69-release-staged-t6t-v1-20261008 |
| 安裝後版本（預期） | 8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa |
| T6.9a policy fingerprint | c1aa56695e855de9120f19c11f48e346f1750d12464994fe9664aa4685693a45（會變，所以要開新 loop） |
| 安裝用 loop-id | loop:1791412028086（06:27 那輪，2 場後取消） |

## 1. 貼上包
打開 paste_t6t.txt，整份複製，貼進網頁 SSH，按 Enter。最後印出的雜湊要等於上表，不一樣就停。

## 2. 放好並建 stage（只寫新目錄，不動服務）
```bash
sudo install -d -o jack_shih -m 700 /mnt/disks/data/cry3/operators/t6t-20261008 && sudo install -o jack_shih -m 600 ~/t6t.tgz /mnt/disks/data/cry3/operators/t6t-20261008/ && sudo -u jack_shih bash -c 'cd /mnt/disks/data/cry3/operators/t6t-20261008 && sha256sum t6t.tgz && mkdir overlay && tar -xzf t6t.tgz -C overlay && sha256sum overlay/deploy/t6t_stage_build.py && cd /tmp && /home/jack_shih/cry3/testnet/.venv/bin/python -B /mnt/disks/data/cry3/operators/t6t-20261008/overlay/deploy/t6t_stage_build.py --root /home/jack_shih/cry3 --overlay /mnt/disks/data/cry3/operators/t6t-20261008/overlay --stage t69-release-staged-t6t-v1-20261008'
```
兩個雜湊要等於上表。最後印出 `"fingerprint": "…"`（新版本 FP），`changed` 要剛好是 4 個 t69a 檔。腳本先唯讀檢查，任何一項不符會在寫入前停下。**把輸出貼回 thread。**

## 3. 啟動 7 個服務（安裝程式要求全部在跑）
```bash
sudo -u jack_shih bash -c 'export XDG_RUNTIME_DIR=/run/user/$(id -u); systemctl --user start cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal; sleep 5; systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal cry3-t67c-ethusdt-feature cry3-t67c-bnbusdt-feature cry3-t67c-ethusdt-signal cry3-t67c-bnbusdt-signal'
```
7 行都要是 active。

## 4. 試跑（不改任何東西）
`FP` 換成第 2 步印出的 fingerprint。
```bash
sudo -u jack_shih bash -c 'cd /tmp; export XDG_RUNTIME_DIR=/run/user/$(id -u); /home/jack_shih/cry3/testnet/.venv/bin/python /mnt/disks/data/cry3/operators/t69d-20261007/t69_manual_install.py --expected-fingerprint FP --stage t69-release-staged-t6t-v1-20261008 --loop-id loop:1791412028086 --allow-cancelled-loop --allow-historical-closed-ledger --allow-shared-mdd-halt scheduled20_mdd_3.5'
```
要印 READ_ONLY_PREFLIGHT_PASSED。其他輸出（例如 Risk latch remains）就停，貼回 thread。

## 5. 正式安裝
同第 4 步，在最後的單引號前加上 ` --apply`。會先備份，再冷重啟 7 個服務；失敗會自動還原。成功印 CODE_INSTALLED_LIVE_NOT_ACTIVATED 和 rollback 指令，**把輸出貼回 thread**。

## 6. 切回 BTC 並驗證
```bash
sudo -u jack_shih bash -c 'export XDG_RUNTIME_DIR=/run/user/$(id -u); cd /tmp && /home/jack_shih/cry3/scripts/t6_coin.sh use BTC; cat /home/jack_shih/cry3/prediction/release-pin.env; grep -c branch_disabled /home/jack_shih/cry3/src/gridbot/prediction/regime_t69a_bridge.py; systemctl --user is-active cry3-predict-user cry3-regime-feature cry3-c180-favorite-signal'
```
pin 要等於 FP，數字是 1，3 個服務 active。

## 7. TG 開新 loop
等約 10 分鐘熱機 → /predict_market BTC → /predict_live on → /predict_loop 100。
報表標題會變成「六路 Live（…continuation Original 停用）」，Shadow 不再列 F1–F4。

## 回退（需要時）
用第 5 步印出的 rollback 指令（先不加 --apply 試跑，再加 --apply）。回退後要再開新 loop。
