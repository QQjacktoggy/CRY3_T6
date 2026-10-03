# 三市場 First 觀測器

使用者2026-10-03授權先開始收集BTC、ETH、BNB及同步模擬WR/PnL。此授權不啟用多市場Live或自動selector。

源碼：`operators/first_multimarket_observer/`。依T6.7d First規則做獨立quote-only研究；不引用執行中的worker，不修改release inventory，不讀寫正式交易帳本。完整定義、限制與執行方式見該目錄README。

VM user unit：`cry3-first-multimarket-observer.service`（jack_shih）；服務源码：`/mnt/disks/data/cry3/operators/first-multimarket-observer-20261003/source`；部署記錄與SHA manifest在同目錄上層。

資料：`/mnt/disks/data/cry3/observers/first-multimarket-v1/first-observer.sqlite3`。報表每15秒更新同目錄`latest.json`、`latest.md`、`latest.html`，collector不發TG、不公開HTTP、不開單；使用者後續授權的獨立TG sender見下節。報表的20/40/100場以觀測槽共同時間計算；固定20場批次也持續累積。開機中途錯過的初始窗口／停機區段標missing，不補未保存的舊盤口。

現階段1U 模擬結果假設顯示深度可全部執行；128秒單次重檢不是完整Live延遲重播，不包含核心占位、claim、wallet、POST或風控。ETH/BNB沿用BTC條件，不代表已驗證Live獲利。官方結算尚未收到的候選保持pending；不能把候選率稱作fill rate。尚無自動選幣／交易建議。

核對狀態需先讀VM_CONNECTION.md，唯讀檢查user unit、health表、三市場feature/book raw evidence、官方resolution及latest報表，另核對Live服務PID與release/pin不變。不要重啟Live、清除MDD/HS或建立下一輪來驗證此觀測器。

## TG 獨立觀測報表

2026-10-03已依使用者要求接既有Live bot/chat，發送器來源為operators/first_multimarket_observer/telegram_report.py；專用設定prediction/telegram.env（部署時與Live實際環境比對相符）。獨立cry3-first-observer-telegram.service/.timer每分鐘查已完成批次：啟用快照一次、固定20槽摘要一次，結算晚到用edit更新原訊息，不發例行每分鐘進度。

collector與三個Live服務均不重啟，正式release/pin不改。TG發送狀態另存telegram-outbox.sqlite3，未知送達保留UNKNOWN不盲重送，具體處理/停用方式見source README。此授權僅發報表，不啟用跨幣交易或selector。

2026-10-04依使用者回報修正自動快照不更新：原啟用快照改為「最近20場（自動更新）」，每分鐘讀取已結束的最近20個共同觀測槽，有新場次或结算才edit同一訊息。使用原bootstrap outbox key/message ID，不重置SENDING/UNKNOWN；固定20場批次摘要保留。只更新獨立sender，不重新載入Live或collector，不改正式release/pin。手動 `/firstreport` 舊回覆仍是當時快照。

本地／VM各32項觀測器與TG測試通過；00:39部署後，00:40排程已自動edit，窗口從22:55–00:35前移至23:00–00:40，TG內容雜湊與新窗口一致，已發訊息數維持2、UNKNOWN／FAILED為0。Live／feature／signal／collector服務PID、正式release/pin、交易及風控帳本均未改。VM記錄在operator目錄`rolling20-staged-20261004/`；源碼及manifest備份在`rolling20-backups/1791045539586/`，回復只限sender檔案及manifest，不能回復交易DB。

## TG 手動呼叫

在同一個已授權的 bot/chat 輸入 `/firstreport`，預設最近20個共同觀測槽；可用 `/firstreport 40`、`/firstreport 100`。`/report` 仍是 Live 報表。手動呼叫只讀 observer 原子 JSON 快照，不存取交易帳本、不選幣或開单。資料超過120秒標示過期；來源、policy、檔案大小或參數錯誤不顯示假零損益。

手動 handler 使用主 bot 既有 polling，沒有第二個 getUpdates 程序。2026-10-03 在 loop 已 CANCELLED、無其他 RUNNING loop、本地與官方無未結訂單／持倉的空檔，僅套用 `predict_main.py` 的選單與 `telegram.py` 的唯讀指令，重新載入主服務。feature/signal/collector PID、交易帳本與持久風控均未改，autoarm/autoloop=false 保留。

正式145檔 fingerprint：`a4ac4135b670f07c380a92c818aa7afba4e60d5c1407653768b270885616716c`，父版 `41933712d8759688f1a3c59d4ff5f4f59200e586c5825a21f232a6e4f040ec89`。VM 候選／部署記錄：`prediction/first-observer-command-staged-20261003/`；源碼／manifest／pin 備份：`/mnt/disks/data/cry3/operators/first-observer-command-20261003/1791039706491`。不能整庫回復交易 DB。

新增指令13項測試在本地與VM通過。較大的本地報表測試組在40項通過後，既有非同步報表測試的 executor 收尾卡住而中止；未宣稱完整套件通過。

### 2026-10-04 報表與排程修正

「重檢通過」是候選筆數，並非執行次數。報表分列重檢執行／通過及凍結限價、深度、資料缺漏原因，無樣本的WR/PnL仍顯示「—」。最近20/40/100場JSON新增實際起止時間；漏K線保留分母，不歸類為策略不符合。

獨立觀測器把全歷史報表序列化及檔案空間檢查移至119–137秒以外。特徵／初始／重檢任務仍只送一次，在原有收件截止前容許排程較晚喚醒；所有收件deadline與報價規則保持原policy fingerprint。記錄feature dispatch／error／missed deadline；晚到K線僅保存稽核證據，不能補造可入場訊號。無法保證主機被長時間阻塞時不漏採；缺漏會明確留下證據。

獨立服務可在Live執行時更新，部署須核對主worker、BTC/ETH/BNB來源及First以外服務PID與release manifest/pin未變，不觸碰交易DB或Live設定。`src/gridbot/prediction/telegram.py` 的手動 `/firstreport` 格式更新需主worker載入；Live loop期間只暫存候選，等安全空檔部署，不熱注入或重啟交易程序。
