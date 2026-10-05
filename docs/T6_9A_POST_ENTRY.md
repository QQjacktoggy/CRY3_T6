# T6.9a 進場後盤口記錄與 First DOWN 深度停損 Shadow

只記錄，不賣。T6.9a 的 `POLICY` 與 fingerprint 不變；這部分有自己的 `POST_ENTRY_POLICY` 與 `POST_ENTRY_FINGERPRINT`（`regime_t69a_post_entry.py`）。不下單、不寫 Live claim／風控／MDD／HS；交易 DB 只以唯讀開啟，寫入只到 feature DB。來源是 2026-10-05 的 First DOWN 停損研究：Live 單成交後 VM 沒有留下盤口，真實持倉沒辦法回測停損。

## 進場後盤口記錄

- 時機：本 T6.9a Live loop 在該市場有 BUY 成交後，從第一筆成交時間記到開盤後 290 秒。沒有 Live 持倉的市場不記。
- 來源：signal producer 本來就以 100ms 寫入 `t67-evidence.sqlite3` 的 `books`（保留 1 小時）。feature collector 每秒取一筆複製到 feature DB，collector 短暫重啟後可在 1 小時內補回。
- 內容（`t69a_post_entry_books`，每市場每秒一列）：UP／DOWN 的最佳 ask、bid，前 5 檔 ask 與 bid、盤口時間與延遲、fee。
- producer 的 evidence 盤口原本只留最佳 bid；現在多留 bid 檔位（最多 8 檔或累積 30 股）。Live 選單、價格與股數不讀這個欄位。
- 狀態（`t69a_post_entry_states`）：持倉方向、成交股數與金額、進場均價、分支、快照數、深度停損結果。
- 資源：每市場約 165 列、每列約 0.6KB；`t69a_post_entry_books` 只保留 21 天。

## First DOWN 深度停損（Shadow）

- 只看 `core_first_down`（DOWN）。開盤 150 秒起，第一筆盤口延遲 ≤1 秒、且 DOWN 最佳 bid ≤ 0.3 × 進場均價的快照觸發，之後不再觸發。150 秒與 0.3 在 `POST_ENTRY_POLICY['deep_stop']`。
- 觸發時凍結：時間、bid、bid 檔位、可賣股數、手續費（`fee_bps × min(p, 1−p)` 每股，與買入同一費率）、賣出所得。bid 深度不夠時，賣不掉的股數照官方結算計。
- 持有股數用與進場 walk 相同的費後股數估算（`股數 × (1 − fee×min(p,1−p)/p)`）。
- 報表〔First DOWN 深度停損〕列出觀察筆數、觸發數、已結算數、停損 PnL、持有 PnL 與差額，另列觸發後原本會贏的筆數與 bid 深度不足筆數；勝方取 Live 官方結算。快照每秒一筆，所以觸發時間精度約 1 秒。

## 部署

本 PR 不部署。需走 T6.9a 的 stage／manifest／fingerprint 流程重新產生 release（新增 `regime_t69a_post_entry.py`，更新 `c180_signal_runtime.py`、`regime_feature_service.py`、`regime_t69a_report.py`、`release.py`）。T6.9a policy fingerprint 不變，現有 loop 與風控狀態不需重建；換版要重啟 BTC signal producer 與 feature collector（worker 程式沒改）。沒有 ETH／BNB 依賴。
