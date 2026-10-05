# T6.9a R*：尾盤熱門方追價 Shadow

R*（`late_favourite_chase`）是掛在 T6.9a 上的 Shadow 子策略，只記錄假設報價與結算後的假設 PnL，不下單、不寫 Live 風控、不影響 MDD 與 HS。T6.9a 的 `POLICY` 與 fingerprint 完全不變；R* 有自己的 `RSTAR_POLICY` 與 `RSTAR_FINGERPRINT`（`regime_t69a_rstar_shadow.py`）。

## 規則

每個 BTC 5 分鐘市場：

1. 開盤價 = 開盤時刻（含）之前 1.5 秒內最後一筆 Binance 現貨成交。
2. σ = 開盤前 15 分鐘 Binance 現貨 1 秒 log 報酬的樣本標準差（每秒取最後一筆、空秒沿用前值；有成交的秒數不到 50% 就不算）。開盤價與 σ 在 270 秒第一次評估時凍結。
3. 270–295 秒內每個 tick：z = ln(現價／開盤價) ÷ (σ × √距開盤秒數)。|z| ≥ 3 時，熱門方 = 現價所在那一邊（z>0 為 UP）。
4. 熱門方的 ask 走 1U 深度後，最低價 ≥ 0.90 且最高成交價 ≤ 0.98，就記一筆 1U 假設買單，持有到結算。
5. 每個市場最多一筆：第一個符合的 tick。之後不再替換。295 秒後沒有訊號就記 `no_signal`。

所有門檻都在 `RSTAR_POLICY`，對回測公式時只改那裡。σ 用開盤前視窗而不是 1 小時，是因為 VM 證據庫只保留 20 分鐘的現貨資料；改成 1 小時需要放寬保留期（磁碟與記憶體成本），等回測公式確認後再決定。

## 記錄內容

- `t69a_shadow_quotes`（branch `late_favourite_chase`）：方向、時間、盤口時間、ask、另一邊 ask、1U 深度內最多 5 檔、現價、開盤價、σ、z、距開盤秒數、fee、cash、淨股數、同市場的 Live 分支（若有）。報價帶 T6.9a fingerprint，結算沿用 `t69a_shadow_outcomes` 的官方勝方。
- `t69a_rstar_states`：每市場的凍結開盤價／σ、tick 數、最大 |z|、未成單原因計數（z 不足、ask 超出價帶、深度不足、盤口過期等）與終止原因。

## 資源與範圍

- 只在 T6.9a 的 BTC Shadow 觀察器裡、270–296 秒時執行；σ 的歷史資料每市場只讀一次。不加 ETH／BNB 依賴，不恢復外部先行或 Reference 校正。
- 報表〔尾盤 R*〕一列顯示報價數、已知 paper WR、假設 PnL 與兩平 WR。

## 部署

本 PR 不部署。部署需走 T6.9a 的 stage／manifest／fingerprint 流程重新產生 release（新增 `regime_t69a_rstar_shadow.py`，並更新 `regime_t69a_shadow.py`、`regime_t69a_report.py`、`release.py`）。因 T6.9a policy fingerprint 不變，現有 loop 與風控狀態不需重建；換版需重啟 feature collector 與 worker。
