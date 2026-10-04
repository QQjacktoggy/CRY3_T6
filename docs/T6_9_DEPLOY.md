# T6.9 部署、驗證與回退程式

這份是操作說明，不是部署紀錄。程式只安裝程式碼並冷載入服務；不 arm、不選策略、不建立 loop、不發 TG。交易資料庫只讀，不修改也不回復。

## 檔案

五個檔案一起放在 STAGE 以外的可信目錄，例如 `/mnt/disks/data/cry3/operators/t69-YYYYMMDD/`：

| 檔案 | 用途 |
|---|---|
| `deploy/release_verifier.py` | 既有的可信 verifier（不 import 候選 Python） |
| `deploy/t69_ops.py` | 共用：T6.8a 安全邊界 snapshot、官方唯讀 GET、服務、全新 interpreter 檢查 |
| `deploy/t69_manual_install.py` | 安裝；預設唯讀 preflight，`--apply` 才安裝 |
| `deploy/t69_verify.py` | 隨時可跑的唯讀驗證 |
| `deploy/t69_rollback.py` | 回退到備份中的父版本；預設唯讀 preflight |

## 前提

- 候選目錄 `prediction/t69-release-staged-*`，裡面有 `candidate.json`、`validation.json`（狀態 `STAGED_VERIFIED_NOT_DEPLOYED`）、完整 manifest 和 pin。候選目錄的建立方式沿用 VM 上既有流程，不在這個 repo。
- release fingerprint 由你另外核准，不能從 STAGE 讀。
- 最後一輪 DONE 100/100，或你明確授權的已停止 CANCELLED 輪；沒有 RUNNING、沒有未終結或 UNKNOWN 的 intent／order、官方零持倉零掛單、沒有風控鎖。
- `hs-recovery-startup.env` 保持 autoarm／autoloop=false。
- 備份根目錄 `/mnt/disks/data/cry3/operators/t69/runs` 必須事先存在（資料碟；系統碟空間不足）。

## 指令

```sh
OP=/mnt/disks/data/cry3/operators/t69-YYYYMMDD
FP=<你核准的 64 位 release fingerprint>
STAGE=t69-release-staged-v1-YYYYMMDD
LOOP=<最後一輪 loop_id>

# 1. 唯讀 preflight（不停服務、不寫檔）
python3 $OP/t69_manual_install.py --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP

# 2. 安裝（你明確同意後才跑）
python3 $OP/t69_manual_install.py --expected-fingerprint $FP --stage $STAGE --loop-id $LOOP --apply

# 3. 唯讀驗證（安裝後；之後任何時候都可再跑）
python3 $OP/t69_verify.py --expected-fingerprint $FP --backup <安裝輸出的 backup>
```

需要一併重載 ETH／BNB producer 時，每個 unit 加一個 `--extra-service cry3-...service`；unit 名稱要先在 VM 上查清楚，安裝與回退都會沿用備份裡記錄的清單。

`--apply` 的流程：

1. 重新驗證父版本。
2. 在備份根目錄建立新的 `runs/<毫秒>/`，寫入被替換的 source、manifest、pin、`before.json`（安全 snapshot、candidate、服務清單、父版本與新 fingerprint）和 `rollback.txt`（回退指令）。
3. 停止服務，重新確認交易紀錄沒變、官方零曝險。
4. 寫入 preflight 時驗證過的位元組，再更新 manifest 和 pin。
5. 啟動服務。要求全部 active，而且 MainPID 都換過（冷載入）。
6. 用全新 interpreter 檢查：policy fingerprint 是 `19b06579…1bd`、8 路 Live 加 6 路 Shadow、Shadow 表可以建立、T6.9 報表可以產生（不發送）。報表存到 `report.txt`。
7. 最後再核對一次交易紀錄和 guard，寫入 `deployment.json`。

任何一步失敗都會自動還原 source、manifest、pin，並重新啟動服務。

## 回退

```sh
python3 $OP/t69_rollback.py --backup <runs/毫秒>            # 唯讀 preflight
python3 $OP/t69_rollback.py --backup <runs/毫秒> --apply    # 回退
```

指令也寫在每次備份的 `rollback.txt`。回退前要先在 TG 把選定和排入下輪的策略換回非 T6.9，否則會拒絕。如果已經跑過 T6.9 輪，用 `--loop-id` 指定那一輪。

回退同樣要求沒有 RUNNING、官方零曝險。現場的檔案只能是父版本或 T6.9 的位元組，其他狀態一律拒絕。流程是：停服務、還原備份檔、刪除 T6.9 新增的檔案、還原 manifest 和 pin、驗證父版本 fingerprint、啟動服務，最後寫 `rollback.json`。

如果回退在中途失敗，服務會保持停止，等人檢查。這時沒有 loop 在跑，不會有交易。

## 安裝後

`t69_*` Shadow 表只有在 TG 選定 T6.9 之後，feature service 才會建立。所以安裝後 `t69_verify.py` 回報表格不存在是正常的。選定 T6.9 並跑過幾個市場後，加上 `--require-t69-tables` 再驗一次。

Live 照原流程：在 TG 選 T6.9、市場和金額，再確認 Live。
