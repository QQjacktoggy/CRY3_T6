# 共享請求保護停滯修正

T6.8a loop:1790957685357 在74/100時停止登錄新市場，最後成功API約2026-10-03 06:25台灣時間。主worker持續記api_start，沒有api_ack/api_error；同一共享預算庫正常、沒有backoff，signal sidecar仍可HTTP200。

離線重現兩種觸發：note_response成功保存冷卻journal後的單次SQLite寫入鎖，及完成journal在列舉/開檔間被peer刪除。前者不再永久設不可用旗標，下一次請求重新讀取DB/journal；後者視為正常已完成紀錄競爭。未知pending、損壞journal、missing ready、真正418/429冷卻及journal保存失敗仍阻擋，不清除風控或繞過請求計量。首次VM例外被旧程式吞掉，無法唯一辨認首次觸發。

新增api_deferred與每分鐘最多一次DISCOVERY_REQUEST_DEFERRED持久診斷，記錄固定分類與預算狀態，沒有簽名URL或機密。

操作員恢復入口 scripts/resume_prediction_existing_loop.py 只接受私有檔案的一次性授權；檢查完整release/pin、exact loop/profile/unit/target/completed及有效期，在Live權限轉換前消耗授權，使用正常controller preflight/arm。worker在鎖內再次檢查expected_loop_id，拒絕缺失/更換/已停止loop，不新增或延長loop、不解除HS。服務的常態autoarm/autoloop仍false，操作員短暫啟動覆寫在恢復後移除。

修正繼承PR17的取消後成交對帳。已補回的order26100200001934825334 +0.5335875U不再次補帳。部署前後保護金融帳本，回復只涉及源碼/manifest/pin，不整庫回復交易DB。策略、C0.65、FirstUP5bp與所有既有風控不變。

驗證：相關140項通過；全套1689 passed、1 skipped、6 subtests passed。涵蓋SQLite鎖後200及418/429跨重啟、journal刪除競爭、未知/損壞journal阻擋、恢復錯誤loop/策略/目標/已停止拒絕、原loop74/100不新建/延長。

## VM部署與恢復

PR18 runtime commit `66bb6fd8174369d34b1d7532e5956911ce7fa1e0`，GitHub CI成功；VM隔離候選40項測試通過。核對本地及官方零曝險後部署，完整141檔fingerprint `efec6d48a10691d05c9cef29c018c5a8814b4e83a16c7f1429a0bc233a47761a`。保留deployed release.py全inventory，僅增加late_fill_repair.py及操作員啟動script；沒有覆蓋VM既有控制模組。

source/manifest/pin備份：VM `/home/jack_shih/cry3/prediction/shared-budget-rollback-1790986888683`。部署前後金融帳本雜湊一致。正式記錄位於 `prediction/shared-budget-fix-staged-20261003/deployment.json`、`resume-completed.json`。

一次性授權必須放在服務允許寫入的 `prediction/data/operators/` 下；直接使用資料碟的其他目錄會被systemd寫入限制阻擋。最初授權未消耗且未啟動Live，已撤銷。有效授權已消耗，暫時ExecStart覆寫已刪除且daemon-reload，常態autoarm/autoloop仍false。

2026-10-03 08:30台灣時間，原loop從74前進到75/100，08:25市場已DONE、08:30市場已登錄OBSERVE；主worker PID2530673的API/行情心跳正常，無HS。feature PID2515081及signal PID2515078未重啟。漏記+0.5335875U仍在，尚未成交新單時PnL仍+2.80226525U。剩餘場次仍須通過原風控，另有完成追蹤，不自動建立下一輪。
