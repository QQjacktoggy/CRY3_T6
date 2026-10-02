# 取消後延遲成交與漏帳修復

## 問題與修正

官方訂單 `26100200001934825334` 對應 2026-10-03 01:10–01:15 Asia/Taipei、T6.8a `reference_180_mid` DOWN。
取消快照的 `modifyTime=1790961182470`、成交0；最終官方 FILLED 快照 `modifyTime=1790961183616`、1.51股。
已 CLAIMED 的 exact-token 官方部位 realizedPnl 為 +0.5335875U。本地先採用取消快照，解除 pending，市場結束後直接寫入 NO_FILL。

Worker 在結算前重查已送出且取消／過期／失敗的已知訂單，驗證原官方訂單 ID、市場與方向，再沿用累計成交游標入帳。
本地拒絕且未送出的 intent 不增加 API 呼叫；查不到、仍未終結、身份不符或讀取延後時保留結算待核對，不能記成 NO_FILL。
已有 SETTLED 的歷史紀錄只由明確補帳工具修改，避免只補 fill 卻沿用舊零損益。
策略、報價門檻、TTL、one-market-one-BUY、既有 HS／MDD 設定不變。

## 已完成補帳

VM 正式帳本已經在 2026-10-03 01:49:52 Asia/Taipei 原子更正；原 loop `loop:1790957685357` completed 維持18。
補帳 ID `05c43916e2771a55bd21ba5b52da6ef258c39d2d4a6096926301513489ae775b`。
補回官方 fill、將同一 settlement 的 NO_FILL 改為官方 SETTLED、更新原 risk ledger 與 regime settlement observation，重算風控。
已驗證副本与正式各重跑一次均回傳 ALREADY_REPAIRED，沒有重複成交、增加 run 或解除任何風控鎖定。
原始取消／NO_FILL 快照與官方證據保存在 settlement audit payload，另有 LATE_FILL_REPAIRED event。
本次沒有重啟服務。

截至01:50:27，TG 使用的報表函數已顯示完成19/100、成交6、4勝2負、WR66.7%、PnL +0.38351608U；包含其他新成交，不全是本次更正。
Reference 補位共3筆、2勝1負、PnL +0.2898U。MDD仍為1.9978U，HS未鎖定。

## 備份與操作

正式交易庫約3.35GB，剩餘磁碟無法容納兩份整庫副本。
工具保存完整交易與風控表的 scoped accounting snapshot，另建驗證副本；大量未變更的事件日誌、Shadow及position歷史不複製。這不是整庫restore備份，不能拿它覆蓋正式DB。
正式帳本更正備份：`/mnt/disks/data/cry3/operators/late-fill-repair-20261003/runs/1790963385310/accounting-before.sqlite3`。
副本驗證備份：`.../runs/1790963327739/accounting-before.sqlite3`。
官方證據與執行結果在各 run 目錄的 evidence.json／result.json，權限受私有目錄保護。

`scripts/repair_cancelled_fill.py` 預設只修驗證副本；--apply 才修改正式帳本。
檢查 deployed manifest/pin 與獨立指定的父 fingerprint，逐檔驗證隔離 overlay。overlay只載入独立操作程序，不注入執行中的 worker。
只允許已 DONE／SETTLED／NO_FILL／零成交的已知送出訂單，必須有相符的官方 FILLED、已領取贏方部位、exact token、方向、市場、起止時間、原 claim 与零risk ledger。
所有成交、結算、risk ledger、observation、風控與audit一次提交；失敗整筆rollback。重跑不再寫入。
不新增loop、不下單、不兌領、不修改策略或機密。後續更正其他種類虧損／部分結算需要另行核查，不能把本工具當成通用覆寫。

## 驗證與部署邊界

新增17項回歸涵蓋取消後FILLED、取消部分成交、零成交、缺失／未終結／身份不符／讀取延後、未送出拒絕、冪等補帳、transaction rollback及官方證據拒絕。
本地全套1666 passed、1 skipped、6 subtests passed。
永久worker修正已備妥；目前Live仍RUNNING，尚未載入。僅在沒有任何RUNNING loop、未終結訂單、未結持倉或UNKNOWN，且官方orders/positions確認空時部署並短暫重載predict user service。
保留hs-recovery-startup.env的自動arm與自動新loop禁用，不重啟feature/signal服務，不自動接續Live。
