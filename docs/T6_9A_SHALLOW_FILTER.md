# T6.9a 淺回撤逆勢條件（Shadow，只記錄）

只在報表上分組記錄，不改 Live。T6.9a 的 `POLICY` 與 fingerprint 不變；這部分有自己的 `SHALLOW_FILTER_POLICY` 與 `SHALLOW_FILTER_FINGERPRINT`（`regime_t69a_shallow_filter.py`）。不寫任何資料庫：只唯讀 feature DB 的 `t69a_decisions` 和已存的官方勝方。

## 條件

- 只看本 loop 選中的 `shallow_retracement` 決策。
- 用決策凍結時的 `core_guard.features.prior_bp`（開盤前 15 分鐘的走勢，First UP 5bp 也用同一個值）。
- 「通過」＝前 15 分鐘走勢和下單方向相反，且至少 5bp：UP 要 `prior_bp ≤ −5`，DOWN 要 `prior_bp ≥ 5`。其他都算「不通過」。
- 來源：2026-10-07 的分析。10-01～10-07 選中的 48 場淺回撤中，通過的 17 場勝率 82%、+4.33U；其他 31 場 48%、−6.43U（幣安推算輸贏）。30 天回測三幣前後兩段也都是通過組最好。樣本還小，所以先記錄。

## 報表

〔淺回撤逆勢條件〕兩行（通過／不通過）：選中數、其中 Live 成交數、已知勝負與勝率、假設 1U PnL、待結算數。
- 假設 PnL 用選中當下報價的費後股數（`signal.entry.expected_shares`），勝方取官方結算；沒成交的選中也算，所以和 Live PnL 不同，不併入 Live。
- 身分（fingerprint、loop、市場）對不上或缺欄位的決策列為待核對，不列收益。

## 部署

本 PR 不部署。需走 T6.9a 的 stage／manifest 流程重新產生 release（新增 `regime_t69a_shallow_filter.py`，更新 `regime_t69a_report.py`、`release.py`）。T6.9a policy fingerprint 不變，現有 loop 與風控不需重建；只有報表程式變動（Telegram 報表），下單路徑沒改；換版要重啟 Telegram 服務才會看到新段落。
