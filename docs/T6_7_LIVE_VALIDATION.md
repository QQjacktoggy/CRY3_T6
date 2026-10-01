# T6.7 三策略 Live 驗證候選

Profile：`regime_target6_7_v1`，BTC 五分鐘二元市場。三條策略均接入現有
Live BUY／正式結算路徑；T6.7 不建立紙上交易，也不啟用 T6.6、舊 T6 shadow
或 FAV/P3 交易。保留歷史紀錄與公共行情證據，不刪交易資料庫。

這是實驗版，尚無足夠歷史逐筆行情證明其成交率或獲利優於 T6.5。
此前固定規則搜尋沒有找到可靠的大幅增益；以下採用新的可驗證方向，不能
把模型 EV、訊號或盤口參與率當成實際成交與收益。

| 子策略 | 固定條件 | 入場 |
|---|---|---|
| `external_lead_lag` 外部先行 | Binance Spot 一秒移動至少 2bp；同方向 prediction ask 漲幅小於 0.01；獨立 300ms 與 1s 公共樣本仍成立，每個 checkpoint 容許 300ms 延遲 | T+60–270 秒內，Fee-net EV／U ≥0.03，+0.02 價格壓力 EV／U ≥0.005 |
| `reference_value` 開場基差校正 | T+60／120／180／240 秒首次評估，容許 1.5 秒；兩側以費後 EV 選較大者 | 相同 EV 門檻；實際 execution 再檢查 ≥0.005／U |
| `shallow_retracement` 淺回撤 | 前兩根封閉 1m K 線方向相反；第一根至少 1bp 且幅度至少是第二根的兩倍 | T+124–134.5 秒；依複利淨變動方向，不使用 JEV；此規則沒有模型 EV 保證 |

最先符合者取得該市場唯一入場資格；同一時刻依表格順序。錯失已凍結的
2 秒報價期限後整場不換策略、不重挑方向。共同價格限制 0.10–0.75，完整
可執行深度，1／2／3U；延用原本一次 BUY、無加倉／避險、持有至官方結算。

## 模型與證據

使用官方 reference 驗證市場身分，以開場 0–1.5 秒收到的 Spot 價格校正 proxy
基差，假設 proxy 與官方結算來源的價差維持不變。機率是同一 feed session／
連線 generation 的過去最多 15 分鐘 log-return 變異模型；至少 20 returns、
60 秒跨度，波動下限 0.35bp／sqrt(second)。這不是經 holdout 校準的勝率，
官方來源與 Binance 的基差變動仍可能造成錯誤估值。

Spot 最多 1.5 秒、book 最多 1 秒。來源／接收時間均不得晚於决策時間；
重新連線或程序重啟不能借用上一個 generation 的 opening anchor。資料不足
便跳過模型策略；淺回撤仍可使用通過既有指紋／封閉 K 線時間驗證的 features。
300ms 與 1s 確認採各自時間點實際收到的 book／spot，不能用一秒樣本補寫
300ms。每市場只有一個外部先行 trigger，不事後挑下一個有利事件。

公共行情：`prediction/data/c180-favorite-live/t67-evidence.sqlite3`，決策：
`prediction/data/regime-target6/features.sqlite3` 的 `t67_decisions`。模型不讀
市場勝方或後來的 PnL。報表使用實際 BUY fill 與官方已核對結算，子策略
歸因需符合 topic／UP ID／時間與政策指紋；無法歸因保留總 Live PnL，不補零。

## 原有風控

- 接續 `regime_target6_risk_v1` 原 epoch、指紋與既有 T6 全系列績效。
- 固定 20 場（包含跳過場）1U 等值 MDD ≥3.5、跨輪累計 ≤−6 停新入場。
- 本輪 1U 等值高點回撤 ≥3.5 停新入場；不同 1／2／3U 歷史逐筆正規化。
- HS、未知訂單、未結曝險、wallet reconciliation、正式 release pin、提交前
  原子 claim、單市場一次 BUY 等檢查保留；不自動解 HS／停單鎖。
- 原 Regime 路徑使用以上專用帳本，沒有另外把通用設定的每日／loop −2U
  加為新的停損門檻。啟動 guard 的 auto-arm／auto-start-loop 仍須為 false。

## 人工部署與啟動

目前只準備候選與獨立 VM staging，沒有切正式檔案、重啟服務、選擇 T6.7、
Live arm 或建立新輪次。真正啟動自主實盤交易由使用者操作。

1. 在本輪完整結束且無其他 RUNNING loop、未結持倉、未終結／UNKNOWN 訂單
   的空檔部署；不可中斷目前 T6.5。官方 active orders／wallet positions 也須
   為零，不能只依本地資料庫判斷。
2. 核對候選 `validation.json`、全部 source hashes、父版本 manifest／pin。
   VM 的 inventory 比此 checkout 大；使用 VM 候選完整 manifest，不能用
   standalone manifest 覆蓋。父版本不同先查變更。
3. 備份被替換的 source、manifest／pin，僅套用候選變更與完整 inventory。
   保留 `hs-recovery-startup.env` 的 auto-arm／auto-start-loop=false。用新的
   `release.py` 驗證全部 inventory 後，在此安全空檔重新載入 main、feature、
   signal 三個服務。不可把 import cache 當成熱載入。

   已提供預設唯讀的人工安裝工具；在 VM 以 `jack_shih` 操作：

   ```sh
   cd /home/jack_shih/cry3
   testnet/.venv/bin/python prediction/t67-live-staged-20261001/deploy/t67_manual_install.py
   # 前置檢查通過後，由使用者明確執行安裝；不會選策略、arm 或開 loop。
   testnet/.venv/bin/python prediction/t67-live-staged-20261001/deploy/t67_manual_install.py --apply
   ```

   兩者均要求 `loop:1790817223795` DONE 100/100、安全空檔、官方零曝險及
   完整 release 驗證。父版本不同會拒絕。失敗回復 source／manifest／pin，
   不回復交易 DB。`--apply` 尚未執行，工具尚未經正式 VM 安裝驗證。
4. Telegram `/predict_lane` 選 **Regime T6.7 三策略 Live 驗證**；
   `/predict_amount` 選 **1U** 作起始驗證。選擇／金額更動會按原機制解除
   Live 授權，這是尚未啟用的安全狀態，不會執行 T6.7 paper trades。
5. 等 signal collector 在下一個完整市場開場前暖機至少 60 秒；
   `/predict_live on` 通過原有檢查並由使用者按確認。確認 profile／amount
   正確、HS／持久停單沒有鎖，再由使用者執行 `/predict_loop_100`。
   系統沒有自動下一轮／自動恢復；拒絕入場時保留理由，不跳過風控。
6. `/predict_status` 與 `/predict_report` 驗收各分支 Live fill／WR／PnL、
   pending／UNKNOWN、共同與整輪風控。失敗僅回復 source／manifest／pin，
   不回復交易 DB；保留已發生訂單與官方結算。

## 驗收口徑

整體 fill rate＝有實際 BUY fill 的已結束市場／已結束的正式登錄市場，
包括所有跳過場。另檢查 intent→submission→fill 流失；候選機會不算 fill。
WR＝費後淨獲利筆數／（淨獲利＋淨虧損），平手與未結不計。
總 PnL 與每分支 PnL 只含官方已核對的 Live 結算，不重扣費。

固定本輪參數，第一個 100 場以每分支成交量、缺資料／過期原因、實際成交率、
WR、PnL、MDD 與風控停單為驗證結果。未取得足夠實際成交便報不足，不編造
預期 30%／50% fill rate 或固定獲利目標；下一版須根據新成交結果再決定。
