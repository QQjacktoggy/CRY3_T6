# T6.9a R*：尾盤近乎確定的熱門方 Shadow

R* 是掛在 T6.9a 上的 Shadow 子策略，只記錄假設報價與結算後的假設 PnL，不下單、不寫 Live 風控、不影響 MDD 與 HS。T6.9a 的 `POLICY` 與 fingerprint 完全不變；R* 有自己的 `RSTAR_POLICY` 與 `RSTAR_FINGERPRINT`（`regime_t69a_rstar_shadow.py`）。規則照 2026-10-05 回測（`R_STAR_SHADOW_SPEC.md`）。

## 規則

每個 BTC 5 分鐘市場，每一筆觀察到的盤口（`o = 盤口 captured_at_ms − 開盤`，`270000 ≤ o < 295000`）：

1. 熱門方 = 最佳 ask 較高的一邊（相同算 UP）；`ask` = 熱門方最佳 ask。
2. 價帶：主臂 `0.90 ≤ ask ≤ 0.98`；第二臂（`late_favourite_chase_99`）`ask = 0.99`。
3. `d_bp = ln(spot/ref)×1e4`；`tau = (300000−o)/1000`（剩餘秒數）；`sigma_s = rv60_bp/√60`；`z = d_bp/(sigma_s×√tau)`；UP 時 `z_fav = z`，DOWN 時 `z_fav = −z`。主臂 `z_fav ≥ 3`，第二臂 `z_fav ≥ 5`。
4. `rv60_bp`：開盤前 60 根 Binance BTCUSDT 1 分鐘 K（開盤時間在 `[start−3600s, start−60s]`），每根 `r = ln(close/open)×1e4`，`rv60 = √mean(r²)`。每市場在 270 秒第一次評估時用 Binance 公開 REST 抓一次（約 10KB，沿用 feature collector 的大小上限），失敗最多重試 3 次，之後記 `rv60_unavailable`。選這個做法而不是常駐 1 分鐘 K 緩衝：不佔記憶體、重啟後不用暖機，數值和回測同源。
5. `spot` = 盤口觀察時間點前收到的最新 Binance 現貨成交，超過 1.5 秒就跳過。`ref` = 市場 price-to-beat（盤口 `reference`）；過濾 `ref > 10000` 與 `|ln(spot/ref)| < 0.01`。
6. 每個市場每臂只記第一筆符合的盤口。

## PnL 與深度

- 假設 PnL 照回測：1U、每股成本 `c = ask/(1−0.02×min(ask,1−ask)/ask)`，`PnL = payout/c − 1`（勝 1、負 0、DRAW 0.5，官方結算）。
- 回測沒檢查深度。Shadow 另外用真實 ask 檔位、上限 = ask 走 1U，記 `fillable`、模擬成交成本與股數，不影響是否記訊號。

## 記錄內容

- `t69a_shadow_quotes`（branch `late_favourite_chase`／`late_favourite_chase_99`）：start、o、熱門方、ask、ask 檔位、bid、另一邊 ask/bid、spot 與時間、ref、rv60、d_bp、tau、z_fav、fillable、模擬成交、評估延遲、同市場的 Live 分支（若有）。報價帶 T6.9a fingerprint，結算沿用 `t69a_shadow_outcomes` 的官方勝方。
- `t69a_rstar_states`：每市場凍結的 rv60、檢查過的盤口數、各臂最大 z_fav、未成訊號原因計數與終止原因。

## 資源與範圍

- 只在 T6.9a 的 BTC Shadow 觀察器裡、270–296 秒時執行；每市場一次 K 線請求。不加 ETH／BNB 依賴，不恢復外部先行或 Reference 校正。
- 報表〔尾盤 R*〕兩列顯示報價數、已知 paper WR、假設 PnL 與兩平 WR。

## 部署

本 PR 不部署。部署需走 T6.9a 的 stage／manifest／fingerprint 流程重新產生 release（新增 `regime_t69a_rstar_shadow.py`，並更新 `regime_t69a_shadow.py`、`regime_t69a_report.py`、`release.py`）。因 T6.9a policy fingerprint 不變，現有 loop 與風控狀態不需重建；換版需重啟 feature collector 與 worker。
