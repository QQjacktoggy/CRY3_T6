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
