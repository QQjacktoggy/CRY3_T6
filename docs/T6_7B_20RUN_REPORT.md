# T6.7b Telegram 固定每 20 run 摘要

新增段落位於 Live 子策略之後、Shadow 之前，沿用 `regime_target6_risk_v1`
的持久起點，每 20 個五分鐘市場一段；跨 loop 不重設。
顯示台灣時間範圍、風控全域 run 編號與「時段已結束／進行中 x/20」。
時間進度包含跳過與觀測空檔，不冒充實際登錄場數。

每段列出：

- 本輪已結束登錄市場數、實際成交市場數、fill rate、待結算／核對數。
- 本輪已核對 Live 勝負、WR、官方費後淨 PnL、MDD。
- 共用風控 MDD（按每筆 claim 的 1/2/3U 折算為 1U 等值，門檻 3.5U）。

本輪績效只包含目前 T6.7b loop，不混入以前 loop 或其他 T6 策略。
共用風控 MDD 包含同固定區段其他 T6 Live 的官方結算，與風控範圍一致。
Shadow 不納入任何 Live／風控金額。

待結算不補零；沒有本輪登錄的空檔顯示「—」。錯誤起點、重複結算、
官方／觀測金額不一致、無效 claim、收盤前／未來觀測或非有限數值會顯示待核對，
不宣稱風控通過。未經驗證登錄的成交保留待核對提示，不納入本輪或區段績效。
其他 T6 尚未有成交紀錄的 UNKNOWN／未終結意圖與訂單，也會阻止區段呈現已驗證風控。
最多顯示最近五段，若有截短則明示總段數。
Telegram 原有分頁按 UTF-16 長度限制保留。

## 驗證

23 項新測試覆蓋固定第 20/21 run 邊界、未成交市場與待結算、跨 loop／
跨 T6 共享風控但隔離本輪 PnL、3U 折算、觀測缺損及只讀／分頁。
本地相關報表測試 66 passed；VM 隔離完整 inventory 測試 598 passed、
1 skipped、6 subtests passed。VM 正式資料另以獨立候選程序預覽新報表，
不把正式程序 import cache 當成熱載入。

## 安全生效方式

目前 loop `loop:1790868923273` 尚為 RUNNING。依既有「本輪結束後再改報表」
要求，候選暫存於 VM，不改正式報表來源、不停入場、不重啟正式程序。

候選：`/home/jack_shih/cry3/prediction/t67b-20run-report-staged-v3-20261002/`。
其中保存 `candidate.json`、`validation.json`、`tests.log`、`report-preview.txt`、
`auto-install.py` 與 `installation-status.json`。

短期、一次性延後部署工作：`cry3-t67b-report-20run-update-v3.service`。
只讀輪次狀態等待原 loop DONE 且 completed=target=100。必須沒有其他
RUNNING loop、HS、UNKNOWN、未終結訂單、未結持倉，並由官方 API 再次證明
零持倉及零掛單，才可部署。下一輪若先開始則繼續等待，不中斷。
若原輪取消、父版本變更或超時，停止自動部署並保留原因；不自動解 HS。

到安全空檔後，備份 source 與完整 manifest/pin，僅替換
`src/gridbot/prediction/regime_t67b_report.py`，使用 VM deployed release.py
核對完整 123 檔 inventory。只重載主服務，不動 feature/signal，不 arm Live、
不建立新 loop、不改策略／金額／風控／交易 DB。
`hs-recovery-startup.env` 的兩項 startup 開關維持 false。
正常結束後此延後部署工作退出，不會循環重複部署。

父 fingerprint（已含 REJECTED 結算修正）：
`4c8f0158d7b5064f68f575e47e7d28fb0491067a77db25de1c059cdf53d143ca`。
候選 fingerprint 以 `validation.json` 為準。
REJECTED 結算修正已經由 PR11 合併 main；本次報表變更保留該修正。

以上是部署安排，不表示 TG 正式程序已載入新格式。即時生效狀態請查
`installation-status.json`；只有 `DEPLOYED_VERIFIED` 才代表正式報表已驗證。
