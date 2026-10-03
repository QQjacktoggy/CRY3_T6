# T6.8a 部署與容量整理 — 2026-10-02

2026-10-02 23:23:56 Asia/Taipei 已驗證 VM `cry3jack` 正式 runtime 為 T6.8a。
程式來源為 PR16 的 `379aa048efb4eb4d6310a3851336ee14600598e0`；PR 尚未合併。
這是安裝紀錄，不是即時交易狀態。

- 父 release：`dee93703d0d6436f9446b742d82a21d7831a0769e213d6a20c70bbdb127a5f71`。
- 新 release：`29cfdb12e5ec70c7d26c85d5707ad37484b7ffc06d6b28b11115f501ad591de5`。
- T6.8a policy：`5e6a17be3b36b722a83d2147324997c7b6974e9b9e346d8ed25cd18d6044967a`。
- 25 個 runtime overlay、139 個完整 inventory inputs；VM 既有額外 inventory 保留。
- 承接已合併 PR14／PR15。可信 installer 與 verifier 位於 `/mnt/disks/data/cry3/operators/t68a-20261002`，獨立於候選目錄。
- 候選／驗證：`/home/jack_shih/cry3/prediction/t68a-release-staged-v1-20261002`。
- source／manifest／pin 回復備份：`/home/jack_shih/cry3/prediction/t68a-rollback-1790954542270`。

## Live 與驗證

部署前後 `loop:1790920571009` 都是 T6.7c、DONE、100/100、21 成交、11勝10負、官方 PnL +1.96084254 USDT。
官方持倉／掛單皆零，無 RUNNING loop。交易、訂單、claim、官方結算、risk ledger 與受保護 runtime risk 設定的前後雜湊一致。
三個相關 user service 均 active/running，沒有自動重啟次數；T6.8a 與上一輪報表已用新 interpreter 驗證。
資料收集的 spot／futures／報價已恢復新鮮更新。Live loop 心跳在 DONE 後停止更新，不能拿舊 loop 的 started_at_ms 當作新服務的啟動時間；核對 user service MainPID、啟動時間及新收集證據。

本地完整 suite 1,646 passed、1 skipped、6 subtests，加上新增 installer preflight 3 passed；PR16 GitHub CI 成功。VM 的 `tests/test_t68a*.py` 301 passed。VM 首次完整測試因缺少測試支援檔、暫存父目錄及執行時間上限未完成；補齊環境後，以資料碟執行上述專項測試。完整套件以本地與 CI 結果為準。

安裝時選定 profile 仍是 T6.7c，沒有 arm、建立新輪次或改變金額／HS。
`hs-recovery-startup.env` 仍禁止自動 arm 及自動新 loop。下一輪由 Telegram 選擇 T6.8a、確認金額並明確啟動。

## 容量與資料保存

原本 `/home/jack_shih/cry3/prediction/data` 已是資料碟的 bind mount，對應 `/mnt/disks/data/cry3/prediction-data`。
跨這兩個掛載點的 `rename` 可能回報 EXDEV，即使 st_dev 相同；資料替換使用資料碟的實際路徑。
首次整理在替換前因此中止並回復服務，正式資料未替換；修正路徑後再次備份、核對並完成。

- 搬移 21 個舊備份／候選目錄、3,276 檔、約 416 MiB 至 `/mnt/disks/data/cry3/archive/t68a-predeploy-20261002`，逐檔 SHA256／大小／mode／mtime 核對，原路徑保留連結。
- 刪除未使用的 2026-09-14 驗證副本、已退出測試的暫存及本次完成後的 fixture，共約 504 MiB；清理前確認不在正式 inventory、無程序使用。正式交易 DB 未刪除或搬移。
- 訊號 DB：539,541,504 → 70,852,608 bytes；2,647 筆 `c180_signals`、267 筆 `c180_recovery_outcomes` 逐列雜湊不變。
- 公開報價 evidence DB：337,854,464 → 8,347,648 bytes。
- 只對備份工作副本套用已合併 PR14 的 raw quote retention，再 VACUUM、完整性檢查、schema／immutable audit 核對，最後在服務停下的安全空檔原子替換。

完整歷史仍保存於 `/mnt/disks/data/cry3/archive/t68a-evidence-v2-20261002/`：

| 完整備份 | 壓縮後 bytes | 解壓資料 SHA256 |
| --- | ---: | --- |
| `signals.full.sqlite3.gz` | 45,404,833 | `31789ebe33b16ba543efb92fbc23270776ef2cf0594173a432e41af7e1a41b32` |
| `t67-evidence.full.sqlite3.gz` | 42,678,583 | `7da518dca5a5a888a5878ba8d1ad5874793fa23671757845a0c8d0130d844589` |

壓縮檔已完整解壓串流核對 SHA256 後才移除未壓縮副本。`maintenance.json`、`backup-compression.json` 保存維護／清理明細。較早一次未套用的備份另在 `t68a-evidence-20261002`。
整理後可用空間：系統碟約 1.02 GiB、資料碟約 2.20 GiB。這些是當時快照，後續操作需重新查詢。
