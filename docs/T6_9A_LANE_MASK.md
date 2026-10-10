# T6.9b 每輪 Lane 遮罩

開新 loop 前，可以在 Telegram 選擇這一輪要關掉哪些 lane。不需要改程式、不需要新 release。遮罩只影響下一個新 loop（含 `/predict_one_run` 單筆試跑）；那一輪開始時就用掉，再下一輪自動回到全開。

只適用 T6.9b（profile `regime_target6_9a_v1`）。T6.7c、T6.9 選了遮罩會被拒絕。

## 操作

1. `/predict_lane` 選 T6.9b，`/predict_amount`、`/predict_market` 照舊。
2. `/predict_lanemask`：按鈕選一個預設組合，或在指令後面接 token 自訂。
3. 確認 Live 後用 `/predict_loop_N` 或 `/predict_start N` 開新 loop。開始回覆會顯示「本輪 Lane」。

預設組合：

| 按鈕 | 關掉的 lane |
|---|---|
| 全開 | 無 |
| 只關原 C DOWN | `core_c_down:DOWN` |
| 關全部 DOWN | `core_first_down:DOWN`、`core_stall_down:DOWN`、`core_c_down:DOWN`、`shallow_retracement:DOWN` |
| 關全部 UP | `core_first_up:UP`、`c_mirror_up_prior:UP`、`shallow_retracement:UP` |

自訂：`/predict_lanemask core_c_down:DOWN,shallow_retracement:DOWN`。token 是「lane:方向」，因為淺回撤兩個方向都會下單。不能把全部 lane 都關掉，也不能填已永久停用的 continuation Original。

- 執行中的 loop 不會被改。執行中選的遮罩排到下一個新 loop。
- 要中途換遮罩：`/predict_stop` → 等持倉與訂單清空 → 再選一次遮罩。這時如果選的和本輪不同，會結束這一輪（和換策略、換金額相同），回覆會寫「已結束暫停且出清完成的上一輪」→ 開新 loop。注意每個新 loop 的 3.5U MDD 會重新計算，只有共用的 T6 風控狀態會延續。
- 還沒出清就選了不同的遮罩（包括選「全開」）：只會排隊。這時 `/predict_loop_N`、`/predict_start N`、`/predict_resume` 都會拒絕續跑這一輪，避免它用舊的 lane 繼續下單；等出清後再選一次，或選回本輪的設定再續跑。
- 只用 `/predict_pause` 暫停（沒有 `/predict_stop`）的 loop 照常可以 `/predict_resume`，用的仍是這一輪自己的遮罩；排隊中的遮罩留給下一個新 loop。
- `/predict_status` 顯示「Lane：本輪 …｜下一輪 …」；`/predict_report` 標頭顯示「本輪 Lane：…」，被關的 lane 標「（本輪停用）」。

## 規則

- 和 `DISABLED_BRANCHES` 相同：被關的核心 lane 仍佔住核心位置，該市場直接跳過，增量 lane 不會接手。被關的增量 lane 讓出位置，另一條沒被關的增量 lane 可以進場（實際特徵中 C-UP 鏡像與淺回撤不會同時成立）。
- 遮罩最後才套用：只有其他規則（First UP 5bp、淺回撤逆勢 5bp、continuation 停用）都放行的 lane 才會記成被遮。決策的 `rejected_branches` 會記 `reason='loop_lane_masked'`、方向，以及若進場的金額與股數（`would_cash`、`would_net_shares`），之後可以拿官方勝方做紙上評分。worker 看到的原因是 `t69a_loop_lane_masked`。
- 紙上報價是窗口內「第一次可成交」的那一筆，也就是沒遮時會買進的時點，`quoted_at_ms` 記錄它的時間；之後的檢查不會覆蓋。一直沒有可成交報價時記 `would_execution='unavailable'`。2026-10-10 以前的紀錄沒有 `quoted_at_ms`，存的是窗口內最後一次檢查的報價。
- 遮罩存在獨立的表 `prediction_loop_lane_masks`（migration 029），和 loop binding 在同一個交易寫入；全開的 loop 沒有這一列。這張表有 UPDATE/DELETE trigger，寫入後不能改。排隊中的遮罩也在同一個交易取用並清除：如果排隊的內容在開 loop 當下變了，開 loop 失敗、排隊保留。

## 檢查點

遮罩在四個地方強制檢查，任何一處讀不到或對不上都是擋單：

1. 註冊市場（`regime_worker_bridge.register_market`）：從 binding 與遮罩表讀遮罩，並用讀到的內容重算 `execution_fingerprint` 核對；讀取失敗時遮罩設為未載入。
2. 選單決策（`regime_t69a_bridge`）：遮罩未載入就拒絕（`loop_lane_mask_unavailable`）；凍結的決策帶有遮罩，和本輪遮罩不一致就拒絕。
3. 原子 claim（`regime_live_ledger.reserve_c180_intent`）：重算含遮罩的 fingerprint；某方向的 lane 全被關時，該方向的買單一律拒絕（`loop_lane_masked`）。
4. 重啟與續跑（`repository.start_bound_loop`、`loop_market_worker`、`worker.start_loop`／`resume`）：續跑用 binding 的遮罩；暫停中的 loop 遇到不同的排隊遮罩時拒絕續跑。

## Fingerprint

- 遮罩不在 `POLICY` 裡，T6.9a policy `FINGERPRINT` 不變，現有 loop、決策、風控狀態全部延續。
- 空遮罩的 `execution_fingerprint` 和改版前完全相同；只有非空遮罩才把 `lane_mask` 加進該 loop 的雜湊。
- 分析時要用 binding 或決策裡的遮罩分組，不能只看 policy fingerprint。

## 部署

新增 `regime_t69a_lane_mask.py`、`migrations/029_loop_lane_mask.sql`，並更新 `release.py` 清單。029 只新增一張表，不改既有 binding 表：退回上一版後舊程式仍能正常開 loop；但退版時若有帶遮罩的 loop 在跑，舊程式會因 fingerprint 不符而拒絕續跑（不會用全開續跑），要先結束那一輪。走 T6.9a 的 stage／manifest 流程，在兩個 loop 之間部署（正在跑的 loop 不受影響，但 worker 要重啟才會載入新程式）。部署後第一輪建議先用全開，確認決策與報表和改版前一致，再開始使用遮罩。

要關哪一邊，依據是另外的研究，不是這個機制本身；目前研究結論是沒有可靠的方向指標，預設全開，想控下檔時只關原 C DOWN。
