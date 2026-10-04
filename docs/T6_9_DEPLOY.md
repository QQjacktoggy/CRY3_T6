# T6.9 部署與回退（一頁）

這是操作說明，不是部署紀錄。程式只安裝程式碼並冷載入服務；不 arm、不選策略、不建 loop、不發 TG。交易資料庫只讀不改。Live 由你在 Telegram 自己啟動。

## 部署步驟

| # | 誰 | 做什麼 | 通過的樣子 |
|---|---|---|---|
| 0 | VM session | 把下面五個檔案放到 `$OP`。依下面「建立候選目錄」建立 `prediction/t69-release-staged-*`。取得新 release fingerprint，交給你核准 | 你拿到一個 64 位 `FP` |
| 1 | VM session | 唯讀 preflight（指令 1） | `READ_ONLY_PREFLIGHT_PASSED` |
| 2 | 你 | 確認沒有 RUNNING loop、官方零持倉零掛單，然後同意安裝 | — |
| 3 | VM session | 安裝（指令 2） | `CODE_INSTALLED_LIVE_NOT_ACTIVATED`，並印出 backup 路徑 |
| 4 | VM session | 唯讀驗證（指令 3）。帶 `--backup` 會比對安裝前的帳本，所以只在 TG 選 T6.9 之前有意義 | `T69_VERIFIED`（這時還沒有 t69 表，屬正常） |
| 5 | 你 | 在 TG 選 T6.9、幣種和金額，確認 Live 並啟動 | TG 顯示 T6.9 Report |
| 6 | VM session | 跑過幾個市場後再驗一次，加 `--require-t69-tables`，不帶 `--backup` | `T69_VERIFIED` |

```sh
PY=/home/jack_shih/cry3/testnet/.venv/bin/python   # 一定要用 app venv；官方查詢需要 dotenv／telegram，系統 python3 可能沒有
OP=/mnt/disks/data/cry3/operators/t69-YYYYMMDD     # 放五個檔案；備份寫到 $OP/runs/
FP=<你核准的 64 位 release fingerprint>
STAGE=t69-release-staged-v1-YYYYMMDD
LOOP=<最後一輪 loop_id>

$PY $OP/t69_manual_install.py --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP          # 1 唯讀
$PY $OP/t69_manual_install.py --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP --apply  # 2 安裝
$PY $OP/t69_verify.py --expected-fingerprint $FP --backup $OP/runs/<毫秒>                             # 3 驗證（選 T6.9 之前）
```

**指令 2 和回退的 `--apply` 一定要在 tmux 裡跑**（`tmux new -s t69`）。IAP SSH 斷線時，tmux 讓程序繼續跑完。萬一沒用 tmux，程式也會把 SIGHUP／SIGTERM 轉成例外並自動還原，但仍以 tmux 為準。

最後一輪如果是你停下的 CANCELLED 輪，加 `--allow-cancelled-loop`。如果有舊的已結束 campaign 沒有結算紀錄，加 `--allow-historical-closed-ledger`。預設會冷載入 BTC 加 ETH／BNB 共 7 個服務（見下面「冷載入哪些服務」）。

## 回退

```sh
$PY $OP/t69_rollback.py --backup $OP/runs/<毫秒>            # 唯讀 preflight
$PY $OP/t69_rollback.py --backup $OP/runs/<毫秒> --apply    # 回到父版本
```

同樣的指令也寫在每次備份的 `rollback.txt`。

- 回退前，先在 TG 把選定的策略和排入下輪的策略都換成 T6.9 以外的版本，否則會拒絕。
- 回退需要：沒有 RUNNING loop、官方零曝險、現場檔案只能是父版本或 T6.9 的位元組。
- 已經跑過 T6.9 輪的話，用 `--loop-id` 指定那一輪。
- 回退中途失敗時，服務保持停止等人檢查。這時沒有 loop 在跑，不會交易。
- 安裝本身失敗時會自動還原，不需要另外跑回退。

## 建立候選目錄（待確認，請審查複核）

VM 上沒有建立 STAGE 的腳本或說明（2026-10-04 唯讀調查）。以下步驟依 T6.7d 的 STAGE 結構，以及 repo 既有的 release 工具（`release.py` 的 `build_release_manifest`、`scripts/build_t65_vm_overlay.py` 保留 VM inventory 的做法）推出，還沒在 VM 上跑過。T6.7d 的結構是：現行完整 release 的副本、疊上 PR20 的 13 檔 overlay、manifest 由 141 檔變 145 檔。

1. **先看空間**：STAGE 在 `/home/jack_shih/cry3/prediction/` 底下（系統碟），是一份完整 release 副本。先 `du` 現行 release 的大小，系統碟剩餘空間要大於它加 128 MB。
2. **複製父版本**：以現行 `prediction/release-manifest.json` 列出的每個檔案為準，用 `cp -p` 保留權限複製到 `prediction/t69-release-staged-v1-YYYYMMDD/` 的相同相對路徑；manifest 和 pin 也一起複製。
3. **列出 overlay**：取 repo 已合併的 main（含 T6.9，PR #24／#25／#26）中，T6.9 新增或改過的 runtime 檔。凡是 VM 父版本有、而 repo 的對應檔不同的路徑，都先用 `git log --format=%H -- <path>` 對每個歷史版本算 sha256，確認 VM 那份是 repo 歷史裡的某一版。找不到就停：那代表 VM 有未審查的本地修改，覆蓋會遺失它。
4. **疊上 overlay**：只複製步驟 3 確認過的檔案到 STAGE。VM 有、repo 沒有的檔案保留不動。
5. **更新 inventory**：STAGE 的 `src/gridbot/prediction/release.py` 以 VM 父版本那份為底，只在 `_REQUIRED_FIXED_RELEASE_PATHS` 加入 T6.9 新增的路徑，不整份換成 repo 版本。這和 `build_t65_vm_overlay.py` 的做法相同。
6. **重建 manifest 和 pin**：在 STAGE 目錄用 STAGE 自己的 `release.py` 執行 `build_release_manifest(STAGE)`，寫出 `prediction/release-manifest.json` 和 `prediction/release-pin.env`（格式同 `scripts/build_t6_release.py`），再用 `verify_release_manifest` 確認回傳空。
7. **在 STAGE 跑測試**：`cd STAGE && nice -n 19 $PY -m pytest -q tests/test_t69*.py`，暫存目錄放資料碟。
8. **寫 `candidate.json`**：
   - `version`：`6.9.2`。
   - `parent`：現行 manifest 的 fingerprint。
   - `expected_fingerprint`：步驟 6 的 fingerprint。
   - `files`：所有內容和父版本不同的路徑，各有 `path`、`before`（父版本 sha256，新檔為 null）、`after`。必須剛好等於 manifest 差異，installer 會用 `validate_candidate` 檢查。
9. **寫 `validation.json`**：
   - `status`：`STAGED_VERIFIED_NOT_DEPLOYED`。
   - `parent`、`fingerprint`。
   - 步驟 7 的測試結果。
   - `service_restart: false`、`live_activation: false`。
10. **交給 jack 核准**：把 fingerprint 交給 jack。他核准的值就是部署步驟裡的 `FP`。

## 冷載入哪些服務

預設 7 個（`deploy/t69_ops.py` 的 `SERVICES`）：

- BTC：`cry3-predict-user`、`cry3-regime-feature`、`cry3-c180-favorite-signal`。
- ETH／BNB producer：`cry3-t67c-{ethusdt,bnbusdt}-{feature,signal}`。

ETH／BNB 的四個服務名稱和角色來自 2026-10-04 的 VM 唯讀調查。它們用 `scripts/run_t67c_asset.sh` 跑 `regime_feature_service` 和 `c180_signal_runtime`，和 BTC 載入同一份 T6.9 程式。「需要一起重載」是讀程式碼推論的，請審查複核。

兩個觀測器 `cry3-first-multimarket-observer` 和 `cry3-t67c-multimarket-observer` 不重載。

保險：installer 還會從 systemd 找出所有 ExecStart 含 `run_t67c_asset.sh`、`regime_feature_service` 或 `c180_signal_runtime` 的 `cry3-*` unit。只要有一個不在清單裡就拒絕，並印出要補的 `--extra-service`。

## 檔案

| 檔案 | 用途 |
|---|---|
| `deploy/release_verifier.py` | 既有的可信 verifier；不 import 候選 Python |
| `deploy/t69_ops.py` | 共用：安全 snapshot、官方唯讀 GET、服務狀態、全新 interpreter 檢查 |
| `deploy/t69_manual_install.py` | 安裝；預設唯讀 |
| `deploy/t69_verify.py` | 唯讀驗證，隨時可跑 |
| `deploy/t69_rollback.py` | 回退；預設唯讀 |

## 安裝做了哪些檢查

依 VM 的 `operators/t67d-20261003/t67d_install.py` 調整（2026-10-04 唯讀調查）。

- **安全邊界**，每個階段前後都核對：
  - 最後一輪 DONE 100/100，或你授權的 CANCELLED 輪。
  - 沒有 RUNNING loop，沒有未終結或 UNKNOWN 的 intent／order，沒有 `pending_unknown` 或 `pending_intent_id` 的 campaign，沒有風控鎖。
  - 官方零持倉零掛單。
  - 8 張帳本表雜湊和受保護設定不變。
  - autoarm／autoloop 維持 false。
- **指紋**：stage manifest、`validation.fingerprint`、`candidate.expected_fingerprint` 和 `--expected-fingerprint` 四者必須相等。新舊兩邊先用 STAGE 外的 verifier 驗證，再各自用自己的 `release.py` 驗證；逐檔核對 `before`／`after`。
- **安裝**：
  1. 建立 `$OP/runs/<毫秒>/`（權限 0700），寫入被替換的 source、manifest、pin、`before.json`、`rollback.txt`。
  2. 停服務，再核對一次邊界。
  3. 寫入預檢時驗證過的位元組。
  4. 用部署後的 `release.py` 重建 manifest，必須等於核准的版本。
  5. 啟動服務，要求全部 active 且 MainPID 都換過；等 20 秒（`--settle-seconds`）再確認 MainPID 和 NRestarts 沒變。
- **功能檢查**：用全新 interpreter 確認 policy fingerprint `19b06579…1bd`、8 路 Live 加 6 路 Shadow、Shadow 表能建立、T6.9 報表能產生（不發送，存成 `report.txt`）。
- **結果**：`deployment.json` 寫進備份和 STAGE，內含 `ledger_unchanged`、`guard_unchanged`、`live_activated=false`。manifest 寫入的是 STAGE 原檔的位元組。
- **失敗時**：任何一步失敗（包括 SSH 斷線）都會自動還原 source、manifest、pin。每一步各自嘗試，例如某個 stop 逾時也不會跳過還原。父版本驗證通過才重啟服務；沒通過就讓服務停著，並印出 `RESTORE INCOMPLETE`。上次中斷留下的 `*.t69-new` 暫存檔會先清掉。
- **t69 表**：`t69_*` Shadow 表要等 TG 選定 T6.9 後，feature service 才會建立，所以第 4 步不檢查。
