# T6.7c 三幣完整觀測

`/t67creport [20|40|100] [BTC|ETH|BNB]` 讀取獨立觀測器快照，預設三幣最近20個已結束市場。
`/firstreport` 保留 First 子集；兩者的重檢定義不同，不能直接當成相同 fill rate。

七路：First DOWN、First UP、Stall DOWN、C DOWN、Continuation Original、C-UP 前趨勢鏡像、淺回撤。
重用正式 `freeze_core`、`additions`、核心及補充執行函式。核心優先，僅 verified-empty 才能使用補充；
每幣每市場最多一筆七路候選。初始窗口124–126秒、選擇截止134.5秒，核心136秒到期、補充保留2秒期限，
1秒新鮮度、C的0.65門檻及原優先順序不變。選中後必須在原到期前觀測到下一筆新鮮盤口才能成為模擬候選。
本觀測為獨立1U報價執行假設，不建模真實掛單排隊、送單延遲、實際fill、帳戶曝險或Live風控。

原有外部先行、Reference兩條Shadow另外計算，不加入七路合計。所有勝方來自既有First觀測器保存的
官方結算與market/topic/token/起止/費率身份核對，平局0.5 payout，扣費一次；未結不補零，沒有樣本顯示—。
研究服務只讀三幣feature/signal/evidence及First觀測DB；只寫`prediction/data/t67c-multimarket-observer/observer.sqlite3`
與`latest.json`，不建立Live repository/client、不開單／arm／selector、不改交易帳本。

使用者明確接受三幣Original費用後，在三個既有signal producer加入
`PREDICTION_T67C_OBSERVER_ORIGINAL_ENABLED=1`，共用原有每市場單次模型程序及symbol隔離資料庫。
預設關閉；關閉時維持既有僅active交易幣產生Original的行為。
此開關同時讓三幣public evidence在沒有Live loop時仍可供兩條獨立Shadow使用。

新觀測自epoch啟用起收集；不以後來取得的K線／盤口回填舊市場模擬成交。
最近20/40/100場保留缺漏分母，顯示K線、Original及初始判定完整度。
First觀測器修正：117–140秒保護段將研究資料暫存記憶體，窗口外一次FULL transaction寫入；
心跳磁碟寫入由每100ms改為窗口外每5秒，保留原收到資料時間與截止驗證。
程序在保護段崩潰可能遺失尚未落盤研究資料，必須標示缺漏，不能重建假候選。
舊重檢原始盤口在報表渲染時唯讀診斷，盤口過期與時間超前不再誤標成資料缺漏。

部署必須核對現行完整release inventory/pin，保留VM額外檔案、autoarm/autoloop=false、所有風控與交易紀錄。
新增TG命令需在安全空檔重載主服務；三幣Original開關需重載signal producers並重新warmup。
不可在RUNNING loop中途重載這些服務。新研究服務獨立啟動，收到SIGTERM才flush自己的研究DB。
