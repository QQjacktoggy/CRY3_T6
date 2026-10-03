# 共享請求保護停滯修正

T6.8a loop:1790957685357 在74/100時停止登錄新市場，最後成功API約2026-10-03 06:25台灣時間。主worker持續記api_start，沒有api_ack/api_error；同一共享預算庫正常、沒有backoff，signal sidecar仍可HTTP200。

離線重現兩種觸發：note_response成功保存冷卻journal後的單次SQLite寫入鎖，及完成journal在列舉/開檔間被peer刪除。前者不再永久設不可用旗標，下一次請求重新讀取DB/journal；後者視為正常已完成紀錄競爭。未知pending、損壞journal、missing ready、真正418/429冷卻及journal保存失敗仍阻擋，不清除風控或繞過請求計量。首次VM例外被旧程式吞掉，無法唯一辨認首次觸發。

新增api_deferred與每分鐘最多一次DISCOVERY_REQUEST_DEFERRED持久診斷，記錄固定分類與預算狀態，沒有簽名URL或機密。

操作員恢復入口 scripts/resume_prediction_existing_loop.py 只接受私有檔案的一次性授權；檢查完整release/pin、exact loop/profile/unit/target/completed及有效期，在Live權限轉換前消耗授權，使用正常controller preflight/arm。worker在鎖內再次檢查expected_loop_id，拒絕缺失/更換/已停止loop，不新增或延長loop、不解除HS。服務的常態autoarm/autoloop仍false，操作員短暫啟動覆寫在恢復後移除。

修正繼承PR17的取消後成交對帳。已補回的order26100200001934825334 +0.5335875U不再次補帳。部署前後保護金融帳本，回復只涉及源碼/manifest/pin，不整庫回復交易DB。策略、C0.65、FirstUP5bp與所有既有風控不變。

驗證：相關140項通過；全套1689 passed、1 skipped、6 subtests passed。涵蓋SQLite鎖後200及418/429跨重啟、journal刪除競爭、未知/損壞journal阻擋、恢復錯誤loop/策略/目標/已停止拒絕、原loop74/100不新建/延長。
