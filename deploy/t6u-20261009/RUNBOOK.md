# 安裝 PR #48（T6.9b 每輪 Lane 遮罩）— 2026-10-09

安裝前**必須沒有 loop 在跑**。安裝程式會冷重啟 7 個服務；Live 不會被自動啟動，也不會自動開 loop。

| 項目 | 值 |
|---|---|
| 程式來源 | PR #48 commit 5a792b4 |
| 現行版本（parent） | 8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa（PR #44） |
| 改動 | 12 個既有檔 + 新增 `regime_t69a_lane_mask.py`、`migrations/029_loop_lane_mask.sql`；VM 的 `release.py` 只多兩行新檔路徑 |
| 安裝包 | vm/t6u.tgz，sha256 見 vm/common.sh 的 TGZ_SHA |
| 建 stage 腳本 | t6u_stage_build.py，sha256 見 SCRIPT_SHA |
| stage 名稱 | t69-release-staged-t6u-v1-20261009 |
| 安裝後版本（預期） | e8a460465d8234c531e1053d9eff9ce4d85ef164dfa06d537577a5cd9a02e4ef |
| T6.9a policy fingerprint | c1aa56695e855de9120f19c11f48e346f1750d12464994fe9664aa4685693a45（不變） |
| 安裝用 loop-id | loop:1791510510192（10-09 09:48–18:10，DONE 100/100） |
| 安裝工具 | VM 既有 `/mnt/disks/data/cry3/operators/t69d-20261007/`（t69_manual_install.py、t69_rollback.py） |

安裝期間（第 4–6 步）：
- **不要在 Telegram 下任何指令。** bot 重啟時會處理積壓的指令，例如排隊中的 `/predict_loop` 會在服務重啟後立刻執行，改動交易紀錄並觸發自動回退。
- **在 tmux 這類穩定的終端裡執行。** 連線中斷會送出 hangup，安裝程式設計上會因此自動還原。

每一步最後一行不是 `STEPn_OK`、`READ_ONLY_PREFLIGHT_PASSED` 或 `CODE_INSTALLED_LIVE_NOT_ACTIVATED`，就停下。

1. 把 `vm/` 上傳到 VM 的 `~/t6u/vm/`，`sha256sum -c SHA256SUMS` 全部 OK。給 Grok 的逐步指南見 `GROK_DEPLOY.md`。
2. `bash ~/t6u/vm/2_stage.sh`：只寫新的 stage 目錄。印出 `"changed"` 14 個檔與 `STAGE_FP=e8a46046…`，最後 `STEP2_OK`。
3. `bash ~/t6u/vm/3_services.sh`：啟動 ETH/BNB 4 個 producer（安裝程式要求 7 個服務都 active），最後 `STEP3_OK`。
4. `bash ~/t6u/vm/4_dryrun.sh`：唯讀試跑，要印 `READ_ONLY_PREFLIGHT_PASSED`。
5. `bash ~/t6u/vm/5_apply_bg.sh`（在 VM 背景執行 `5_apply.sh`，斷線不會中斷；只能啟動一次），再用 `bash ~/t6u/vm/5_status.sh` 看到 `STEP5_OK`。內容：備份 → 停服務 → 換檔 → 冷重啟 → 健康檢查；失敗自動還原。成功的 log 有 `CODE_INSTALLED_LIVE_NOT_ACTIVATED` 與 rollback 指令。
6. `bash ~/t6u/vm/6_btc_verify.sh`：切回只跑 BTC，確認 pin、`loop_lane_masked`、migration 029 與遮罩表、T6.9b 報表能產生且有「本輪 Lane」一行（安裝程式自己的健康檢查只測 T6.9），3 個服務 active，最後 `STEP6_OK`。
7. TG：等約 10 分鐘熱機 → `/predict_market BTC` → `/predict_live on` → 第一輪建議 `/predict_lanemask` 選「全開」→ `/predict_loop 100`。報表標頭會多一行「本輪 Lane：全開」。

說明：
- Migration 029 由 `cry3-predict-user` 啟動時建立新表 `prediction_loop_lane_masks`；不改任何既有表，安裝程式的交易紀錄雜湊不受影響。
- 回退：`bash ~/t6u/vm/rollback.sh <第 5 步印出的 backup 目錄>`（先試跑，再加 `--apply`），然後 `bash ~/t6u/vm/btc_only.sh`。回退後舊程式不讀新表；若當時有帶遮罩的 loop 在跑，舊程式會因 fingerprint 不符拒絕續跑。
- 安裝前已唯讀確認（10-09 21:3x）：沒有任何 029 migration 紀錄、binding 表 7 欄、沒有遮罩表、沒有 RUNNING loop、目標 loop DONE 100/100。
- 獨立審查（模擬 systemctl 與交易所，其餘用安裝程式真實邏輯）：安裝成功、強制健康檢查失敗時自動還原、`t69_rollback` 回退皆通過；交易紀錄雜湊在 029 執行後不變。
