# T6.7b REJECTED 意圖造成空倉結算停滯

## 原因與修正

`load_unresolved_intents()` 沒有把 `REJECTED` 視為終結狀態。當最終入場檢查
拒絕送單時，意圖已記錄 `REJECTED`、`unknown=0`、`not_submitted=true`，且沒有
submission 時間、訂單或成交，但結算仍把它視為未完成訂單。過期空倉因此
進入 `CLOSED_PENDING_REDEEM`，原輪次無法完成此市場及進入下一場。

修正只將 `REJECTED` 加入終結狀態清單，保留 `OR unknown = 1`。真正持倉、
實際成交、未終結訂單及 UNKNOWN 仍沿用原有對帳與結算保護。
交易策略、0.65 門檻、金額及風控沒有變更。

## 回歸驗證

新增 23 項測試，覆蓋全部 T6 共用風控 profile：

- 沒有成交的已拒絕 BUY 能以 `NO_FILL`、零 PnL 完成原輪次的一場。
- 包含先前已落入 `CLOSED_PENDING_REDEEM` 的空倉。
- 重複結算不會重複增加進度或 PnL，不會建立新輪次或送單。
- REJECTED 帶 UNKNOWN、實際 BUY 成交或持倉時，不會被當成空倉關閉。

本地完整測試：586 passed、1 skipped、6 subtests passed。
VM 完整 inventory 暫存版本：575 passed、1 skipped、6 subtests passed；
不包含 11 項需完整 Git checkout 的 standalone release 測試。
VM 測試只使用隔離測試資料庫。

## 操作限制

只在明確授權恢復原輪次，且正式/官方均無未結持倉、非終結訂單、UNKNOWN、
HS 或其他 RUNNING loop 的空檔重載主服務。先備份本次 source、
完整 inventory manifest/pin，核對父 fingerprint；不得直接修改交易 DB。
由既有 worker 的 `NO_FILL` 結算與 durable loop recovery 恢復原輪次。

本次啟動恢復 Live 使用既有 controller 的官方 preflight 與一次性
`PREDICTION_LIVE_ARM_ON_START=true`。自動建立新輪次維持 false。
一次性 EnvironmentFile 必須比所有既有 drop-in 更晚載入，尤其既有
`zzzzzzzzzz-hs-recovery.conf`。需核對 systemd 合併後的 EnvironmentFiles 順序；
不能只依新檔案名稱推定生效。啟動後移除一次性 drop-in 與 env，daemon-reload；
永久 `hs-recovery-startup.env` 兩項開關維持 false。

不重啟 feature/signal、不注入執行中程序、不解除 HS、不回復交易 DB。
不能以服務 active 當作恢復完成：還需核對新程序取得 Live 授權、原卡住市場
SETTLED/NO_FILL、原輪次進度與新市場推進，以及舊成交/結算不變。

## 本次 VM 部署與核對

2026-10-02 07:07:30（台灣時間）唯讀核對：

- 原輪次 `loop:1790868923273`、T6.7b、LIVE/RUNNING，30/100 → 31/100，
  目標仍 100，沒有建立新輪次或停入場，HS=false。
- 卡住的 `btc-updown-5m-1790878200` 已由 worker 正常結算為 DONE、
  SETTLED/NO_FILL、零 PnL；原 REJECTED 意圖保持不變。
- 新市場 `btc-updown-5m-1790895900` 已進入 INITIAL_POSITION、buy_count=1，
  原輪次繼續實際交易。之前的成交及 SETTLED 紀錄逐筆 hash 均不變，
  已結算累計 PnL 仍 +4.55534012 USDT。
- 主程序 PID 2482240 正常，feature/signal 原 PID 2469286/2469283 保持運行。
- 永久 startup guard 兩項仍 false，一次性 drop-in/env 已移除。

全 inventory 123 檔父版本：
`36996e0aff2720b665f04077b7691be9a370b92568b0c0b015ad9e87945a72d5`。
本次正式 fingerprint（僅 repository.py 的 runtime 變更）：
`4c8f0158d7b5064f68f575e47e7d28fb0491067a77db25de1c059cdf53d143ca`。
使用 VM deployed release.py 核對完整 manifest/pin 通過。

候選與驗證：
`/home/jack_shih/cry3/prediction/t67b-rejected-recovery-staged-20261002/`。
原 source、manifest/pin 與部署前核對備份：
`/home/jack_shih/cry3/prediction/t67b-rejected-recovery-rollback-1790895825752/`。
交易 DB 未經部署工具直接寫入或回復；恢復後的交易、結算及進度由正式 worker 執行。

以上是部署時快照，不能作為之後的即時狀態。
