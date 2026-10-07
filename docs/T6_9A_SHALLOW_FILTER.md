# T6.9a 淺回撤逆勢條件（Live）

T6.9a 的淺回撤（`shallow_retracement`）只在「開盤前 15 分鐘走勢和下單方向相反，且至少 5bp」時才進 Live；不符合就跳過，這一場不下單，也不讓其他增量子策略頂替。其他子策略不變。

## 條件

- 用決策凍結時的 `core_guard.features.prior_bp`（開盤前 15 分鐘走勢，First UP 5bp 篩選用的同一個值）。
- UP 要 `prior_bp ≤ −5`，DOWN 要 `prior_bp ≥ 5`。門檻在 `POLICY['shallow_retracement']['prior_against_min_bp']`。
- 被擋時決策寫 `rejected_branches`：`reason='shallow_prior_not_against_5bp'`，附 `prior_bp` 與方向；worker 看到的原因是 `t69a_shallow_prior_not_against_5bp`。
- 來源：2026-10-07 的分析。10-01～10-07 選中的 48 場淺回撤中，符合的 17 場勝率 82%、+4.33U；其他 31 場 48%、−6.43U（幣安推算輸贏）。30 天回測三幣前後兩段也都是符合組最好。

## 報表

〔淺回撤逆勢條件（前15分逆向≥5bp 才進 Live）〕兩行：
- 通過（Live）：選中數、成交數、已知勝負、假設 1U PnL（選中當下報價的費後股數）、待結算。
- 被擋（不下單）：被擋場數、若做會贏／會輸（官方勝方對照被擋的方向）、待結算。
只讀 feature DB 的 `t69a_decisions` 和已存的官方勝方，不寫任何資料庫；假設數字不併入 Live。

## 部署

T6.9a policy fingerprint 會變（`129fbf0c…`），舊 loop 不能沿用，部署後要開新 loop。本 PR 不部署：需走 T6.9a 的 stage／manifest 流程重新產生 release（新增 `regime_t69a_shallow_filter.py`，更新 `regime_t69a_policy.py`、`regime_t69a_bridge.py`、`regime_t69a_report.py`、`release.py`）。
