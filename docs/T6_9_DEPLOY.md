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

- 候選目錄 `prediction/t69-release-staged-*` 的格式和 T6.7d 相同：
  - `candidate.json`：`parent`、`expected_fingerprint`，以及逐檔的 `before`／`after` 雜湊。
  - `validation.json`：狀態 `STAGED_VERIFIED_NOT_DEPLOYED`，含 `parent`、`fingerprint`。
  - 完整的 manifest 和 pin。
- release fingerprint 由你另外核准，不能從 STAGE 讀。
- 最後一輪 DONE 100/100，或你明確授權的已停止 CANCELLED 輪；沒有 RUNNING、沒有未終結或 UNKNOWN 的 intent／order、官方零持倉零掛單、沒有風控鎖。
- `hs-recovery-startup.env` 保持 autoarm／autoloop=false。
- 備份預設放在 installer 旁邊的 `runs/<毫秒>/`（和 T6.7d 一樣在 `operators/` 底下，也就是資料碟）。

## 和 VM 上 T6.7d installer 的對照

依 VM `operators/t67d-20261003/t67d_install.py`（2026-10-04 唯讀調查）調整，以下各點相同：

- 指紋由 `--expected-fingerprint` 另外提供。stage manifest、`validation.fingerprint`、`candidate.expected_fingerprint` 與這個值必須四者相等。
- 新舊兩邊先用 STAGE 外的 `release_verifier.py` 驗證；雜湊通過後，才各自用自己的 `release.py` 再驗一次 manifest。
- 逐檔核對 `before`／`after` 雜湊；8 張帳本表雜湊與受保護設定要在每個階段前後都相同；官方零持倉零掛單。
- 替換後要用部署後的 `release.py` 重建 manifest，必須等於核准的 manifest。
- 失敗時自動還原；成功後 `deployment.json` 同時寫進 STAGE 與備份目錄。

T6.9 另外加了：

- 冷載入：MainPID 必須換過。
- 全新 interpreter 檢查 policy fingerprint、8 Live 加 6 Shadow 和 Shadow 表。
- 備份裡的 `rollback.txt`，以及獨立的回退與驗證程式。
- STAGE 名稱改成參數，並限定 `t69-release-staged-` 開頭。T6.7d 是寫死在程式裡。

## 待確認（不猜）

1. **VM 怎麼建立 STAGE 目錄**：repo 和這次調查都沒有建立腳本或流程。installer 只讀取並驗證已存在的 STAGE。
2. **ETH／BNB producer 的服務名稱**：目前只知道觀察器 `cry3-first-multimarket-observer.service`。預設的三個受管服務以外，要不要一起重載，要等名稱確認後再用 `--extra-service` 加入。程式不會自己猜服務名稱。

## 指令

```sh
OP=/mnt/disks/data/cry3/operators/t69-YYYYMMDD   # 放五個檔案；備份會寫到 $OP/runs/
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

需要一併重載其他 unit 時，每個加一個 `--extra-service cry3-...service`。名稱見上面待確認第 2 點；安裝與回退都會沿用備份裡記錄的清單。

`--apply` 的流程：

1. 重新驗證父版本。
2. 建立新的 `$OP/runs/<毫秒>/`（權限 0700），寫入被替換的 source、manifest、pin、`before.json`（安全 snapshot、candidate、服務清單、父版本與新 fingerprint）和 `rollback.txt`（回退指令）。
3. 停止服務，重新確認交易紀錄沒變、官方零曝險。
4. 寫入 preflight 時驗證過的位元組；用部署後的 `release.py` 重建 manifest，必須等於核准的版本，再更新 manifest 和 pin。
5. 啟動服務。要求全部 active，而且 MainPID 都換過（冷載入）。
6. 用全新 interpreter 檢查：policy fingerprint 是 `19b06579…1bd`、8 路 Live 加 6 路 Shadow、Shadow 表可以建立、T6.9 報表可以產生（不發送）。報表存到 `report.txt`。
7. 最後再核對一次交易紀錄和 guard，把 `deployment.json` 寫進備份和 STAGE。

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
