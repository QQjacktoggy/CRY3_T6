# T6.9 部署與回退（一頁）

這是操作說明，不是部署紀錄。程式只安裝程式碼並冷載入服務；不 arm、不選策略、不建 loop、不發 TG。交易資料庫只讀不改。Live 由你在 Telegram 自己啟動。

## 部署步驟

| # | 誰 | 做什麼 | 通過的樣子 |
|---|---|---|---|
| 0 | VM session | 把下面五個檔案放到 `$OP`。建立候選目錄 `prediction/t69-release-staged-*`（建立方式待確認 1）。取得新 release fingerprint，交給你核准 | 你拿到一個 64 位 `FP` |
| 1 | VM session | 唯讀 preflight（指令 1） | `READ_ONLY_PREFLIGHT_PASSED` |
| 2 | 你 | 確認沒有 RUNNING loop、官方零持倉零掛單，然後同意安裝 | — |
| 3 | VM session | 安裝（指令 2） | `CODE_INSTALLED_LIVE_NOT_ACTIVATED`，並印出 backup 路徑 |
| 4 | VM session | 唯讀驗證（指令 3） | `T69_VERIFIED`（這時還沒有 t69 表，屬正常） |
| 5 | 你 | 在 TG 選 T6.9、幣種和金額，確認 Live 並啟動 | TG 顯示 T6.9 Report |
| 6 | VM session | 跑過幾個市場後再驗一次，加 `--require-t69-tables` | `T69_VERIFIED` |

```sh
OP=/mnt/disks/data/cry3/operators/t69-YYYYMMDD     # 放五個檔案；備份寫到 $OP/runs/
FP=<你核准的 64 位 release fingerprint>
STAGE=t69-release-staged-v1-YYYYMMDD
LOOP=<最後一輪 loop_id>

python3 $OP/t69_manual_install.py --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP          # 1 唯讀
python3 $OP/t69_manual_install.py --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP --apply  # 2 安裝
python3 $OP/t69_verify.py --expected-fingerprint $FP --backup $OP/runs/<毫秒>                         # 3 驗證
```

最後一輪如果是你停下的 CANCELLED 輪，加 `--allow-cancelled-loop`。如果有舊的已結束 campaign 沒有結算紀錄，加 `--allow-historical-closed-ledger`。需要多重載的服務，每個加一個 `--extra-service cry3-….service`（見待確認 2）。

## 回退

```sh
python3 $OP/t69_rollback.py --backup $OP/runs/<毫秒>            # 唯讀 preflight
python3 $OP/t69_rollback.py --backup $OP/runs/<毫秒> --apply    # 回到父版本
```

同樣的指令也寫在每次備份的 `rollback.txt`。

- 回退前，先在 TG 把選定的策略和排入下輪的策略都換成 T6.9 以外的版本，否則會拒絕。
- 回退需要：沒有 RUNNING loop、官方零曝險、現場檔案只能是父版本或 T6.9 的位元組。
- 已經跑過 T6.9 輪的話，用 `--loop-id` 指定那一輪。
- 回退中途失敗時，服務保持停止等人檢查。這時沒有 loop 在跑，不會交易。
- 安裝本身失敗時會自動還原，不需要另外跑回退。

## 待確認（不猜，結果到了再補）

1. **VM 怎麼建立候選目錄**。installer 只讀取、驗證已存在的候選目錄，格式和 T6.7d 相同：
   - `candidate.json`：`parent`、`expected_fingerprint`、逐檔 `before`／`after`。
   - `validation.json`：狀態 `STAGED_VERIFIED_NOT_DEPLOYED`，含 `parent`、`fingerprint`。
   - 完整的 manifest 和 pin。
2. **ETH／BNB producer 的服務名稱**。目前只知道觀察器 `cry3-first-multimarket-observer.service`。這些服務會載入 T6.9 改到的程式，名稱確認後要用 `--extra-service` 一起冷載入。

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
  - 沒有 RUNNING loop，沒有未終結或 UNKNOWN 的 intent／order，沒有風控鎖。
  - 官方零持倉零掛單。
  - 8 張帳本表雜湊和受保護設定不變。
  - autoarm／autoloop 維持 false。
- **指紋**：stage manifest、`validation.fingerprint`、`candidate.expected_fingerprint` 和 `--expected-fingerprint` 四者必須相等。新舊兩邊先用 STAGE 外的 verifier 驗證，再各自用自己的 `release.py` 驗證；逐檔核對 `before`／`after`。
- **安裝**：
  1. 建立 `$OP/runs/<毫秒>/`（權限 0700），寫入被替換的 source、manifest、pin、`before.json`、`rollback.txt`。
  2. 停服務，再核對一次邊界。
  3. 寫入預檢時驗證過的位元組。
  4. 用部署後的 `release.py` 重建 manifest，必須等於核准的版本。
  5. 啟動服務，要求全部 active 且 MainPID 都換過。
- **功能檢查**：用全新 interpreter 確認 policy fingerprint `19b06579…1bd`、8 路 Live 加 6 路 Shadow、Shadow 表能建立、T6.9 報表能產生（不發送，存成 `report.txt`）。
- **結果**：`deployment.json` 寫進備份和 STAGE，內含 `ledger_unchanged`、`guard_unchanged`、`live_activated=false`。任何一步失敗都會自動還原 source、manifest、pin，並重啟服務。
- **t69 表**：`t69_*` Shadow 表要等 TG 選定 T6.9 後，feature service 才會建立，所以第 4 步不檢查。
