# CRY3_T6

獨立管理 VM 正式 T6 系列程式碼。2026-09-30 從 `cry3jack` 擷取，包含 T6、T6.1、T6.2、T6.3、T6.3a、T6.3b，並以最新 3b 整合 [T6.5](docs/T65.md)：A／flat 與 M4／M6 先 Shadow，其他 T6／C 保留 Live。

T6.7 候選新增外部先行、reference 校正價格、淺回撤與 C-UP 鏡像順勢反轉四條 Live 路由，沿用原有風控且不建立新 shadow 交易。固定規則與人工啟動步驟見 [T6.7 驗證計畫](docs/T6_7_LIVE_VALIDATION.md)。候選套件不代表 VM 已切換或已啟動實盤。

T6.7a 以舊 T6／C 為基底：五條舊核心與 C-UP／淺回撤共七組 Live，外部先行與參考價模型兩組 Shadow。核心先凍結、空缺才新增；TG 逐子策略列 WR／PnL。規則與驗證見 [T6.7a](docs/T6_7A.md)。

T6.7c 承接 PR11／PR12 的恢復與報表修正，保留七條 Live、兩條 Shadow，
新增獨立版本與固定每 20 run 摘要；TG lane 選單只提供 T6 系列。
部署與啟動見 [T6.7c](docs/T6_7C.md)。

T6.8 保留原七路 Live 與風控，新增第八路核心後 180 秒 Reference 中價 Live 補位與 60／120／180／240 秒檢查點，並加入缺資料覆蓋率與拒絕診斷。版本範圍、容量限制與啟動前核對見 [T6.8](docs/T6_8.md)。本次 PR 不代表 VM 已部署或已啟動。

## 收錄範圍

- `src/gridbot/prediction/regime_*`：T6 策略、特徵、執行與風控。
- Prediction 共用下單、結算、資料庫 migrations、Telegram、報表與必要 scripts。
- `prediction/experiments/c180-original-mix75-v1-bda3e5a85a98`：T6 JEV 訊號必需的不可變來源；保留其完整雜湊清單。
- `tests`：VM 的 T6～T6.3 離線回歸測試。

排除舊 Futures 主程式、舊策略目錄、研究資料、歷史報表、資料庫、交易紀錄、環境檔、金鑰與 VM 備份。T6.4 的 9/29 待部署候選沒有納入 main，因其父版本早於最新 T6.3b。

共用 worker/strategy/Telegram 仍含歷史 Prediction 分支，是目前 T6 的必要依賴；這次沒有重寫交易邏輯或刪除共用分支。此 repo 是乾淨的來源管理基準，還不是只支援 T6 的重構版。

## 安裝與離線驗證

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -c requirements-lock.txt -e '.[dev]'
python -m pytest -q
python -m scripts.build_t6_release
```

Windows 使用 `.venv\Scripts\Activate.ps1` 啟用環境。複製 `.env.example` 為 `.env` 並在本機填入自己的值。建置 manifest 不會啟動服務或下單。

## 三個執行程序

在 repo 根目錄執行，分別管理各程序：

```sh
python predict_main.py --poll-telegram
python -m src.gridbot.prediction.regime_feature_service
sh scripts/run_t6_signal.sh
```

Signal 程序的 API keys 由環境變數提供。實盤仍依現有 Telegram 授權和 release 檢查；啟動服務不代表已授權 Live。此搬移沒有把 VM 現有服務改成從新 repo 執行。

## 版本與部署

`docs/vm-source-baseline.json` 記錄擷取來源、VM release fingerprint 與每個原始檔的 SHA-256。交易程式保持 VM 原始內容；只有 `release.py` 的固定檔案清單改為此 repo 範圍。測試移除了舊 VM staging 路徑，文字斷言更新至 VM 最新版本；864 場研究樣本的 parity 測試因未帶入研究資料而跳過。新 repo 的 fingerprint 因此與 VM 不同，不能沿用舊 pin。

後續修改建立分支、跑測試、合併，再以 commit/tag 指定部署版本。部署前在目標 checkout 建立 manifest 和外部 pin，核對實盤狀態與回復方案，再另行執行服務切換。資料庫、金鑰與 Live 授權不隨 Git 搬移。
