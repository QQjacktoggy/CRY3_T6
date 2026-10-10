# 市場盤口記錄器（只記錄、不下單）

用途：找新的交易優勢之前，先有「每一個市場」的盤口。過去只有我們有下單的市場才留下長期盤口，沒交易的市場只保留 24 小時（`c180_book_events`）或 1 小時（T6.7 evidence），所以「熱門方 0.55–0.70」這類想法無法用新資料驗證。

## 做什麼

- 由 `cry3-c180-favorite-signal`（`c180_signal_runtime`）在它原本就收到的事件上取樣，不新增任何 WebSocket 或 REST 連線，也沒有任何下單路徑。
- 每個 5 分鐘市場在固定時間點取樣：0–100 秒每 10 秒、110–139 秒每 1 秒（T6 的決策與下單窗口）、140–295 秒每 5 秒，共 73 個點。
- 每個點記錄 UP／DOWN 的最佳買賣價與前 3 檔、盤口時間，以及當下最新的 Binance 現貨／期貨成交價。
- 盤口超過凍結邏輯的 2 秒新鮮度時，照樣記下但標 `stale`；讀不到盤口則記 `q: null`，缺口看得出來。
- 市場結束後寫入一筆壓縮紀錄到獨立檔案：`prediction/data/c180-favorite-live/market-recorder.sqlite3`（表 `market_samples`）。不寫 `prediction.sqlite3`，也不寫 features DB。
- 實測每個市場約 4.4 KB，一天約 1.3 MB。保留 90 天，最多 90×288 筆，檔案上限 256 MB；每次重開與每 48 筆寫入前先刪掉過期或超量的列，所以寫滿也能自己騰出空間。
- 磁碟剩餘不到約 1.15 GB（交易用資料庫的 128 MB 保留門檻再加 1 GB）就停止記錄，確保不會吃掉 signals、T6.7 evidence 等交易資料需要的空間。部署前先確認 VM 剩餘空間。
- 記錄器出錯只會少一筆研究資料：例外在呼叫端全部吞掉並每分鐘最多記一次警告，不會影響 C180／T6 的盤口、凍結封包或下單。
- 關閉：服務環境加 `PREDICTION_MARKET_RECORDER=0` 後重啟。

## 勝方

報表優先用官方勝方（結算、observer、各影子 outcome）；沒有時用「參考價鏈」：下一個市場的 Chainlink 起始參考價對比這個市場的參考價（相等為 DRAW）。這個方法在 1354 個已知官方勝方的 BTC 市場全部一致。記錄器本身存了每個市場的參考價，所以只要下一個市場也有記錄，就能判勝負。

## 報表（唯讀）

在 release 根目錄執行，所有資料庫都以 `mode=ro` 開啟：

```bash
python3 -m scripts.market_recorder_report \
  --recorder-db prediction/data/c180-favorite-live/market-recorder.sqlite3 \
  --prediction-db prediction/data/prediction.sqlite3 \
  --feature-db prediction/data/regime-target6/features.sqlite3 \
  --since 2026-10-11
```

規則在 2026-10-10 事先寫死，不可以用它要評分的資料回頭調參數：

| 項目 | 規則 |
|---|---|
| 熱門方 | 第 128.0 秒（124.0 秒為次要檢查），價格較高那一方的最佳賣價落在 [0.55, 0.70)，買 1 U。成交價用記錄的賣價各加 1 tick（0.01）模擬滑價，股數扣手續費。前 3 檔不夠 1 U 的另計 `unexecutable`。分組依 T6.9b 實際成交：全部、沒成交、成交同一邊、成交相反邊 |
| 追價成交 | T6.9b 的實際成交均價，比決策當下記錄到的同側最佳賣價高 0.02 以上 |
| 被遮 lane | 決策裡 `masked_first_quotes` 的第一筆可成交報價；2026-10-10 以前只存最後一次檢查的報價，不計分 |

輸出是 JSON：筆數、損益、每筆平均與以台灣日期分群的 bootstrap 95% 區間。`--since` 同時限制記錄器、成交與遮罩資料；記錄器檔案的幣種必須和 `--symbol` 相同。報表逐筆讀取，不會把幾個月的資料一次載入記憶體。研究結論需要的樣本量很大（熱門方約 2400 個訊號、約 25 天），不要用幾天的結果下結論。

## 部署

- 新檔：`src/gridbot/prediction/market_recorder.py`、`scripts/market_recorder_report.py`，已列入 repo 的 `release.py`。VM 上的 release.py 是另一份檔案，部署 bundle 要另外把這兩個路徑加進 VM 的清單（同 t6u 的做法）。
- 改動：`c180_signal_runtime.py`（掛記錄器）、`regime_t69a_bridge.py`（被遮 lane 改記第一次可成交的報價，見 [T6.9b Lane 遮罩](T6_9A_LANE_MASK.md)）。
- 要重啟 `cry3-c180-favorite-signal` 才會開始記錄；worker 重啟才會套用遮罩報價的修正。走既有 stage／manifest 流程，在兩個 loop 之間部署。
- 服務重啟後的第一個市場不會記錄（沿用 sidecar 不用不完整歷史的規則），之後每個市場一筆。
