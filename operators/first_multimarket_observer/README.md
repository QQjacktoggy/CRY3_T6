# BTC／ETH／BNB First 唯讀觀測器

獨立程序、資料庫及行情請求預算。只有 GET market/list、market/detail、order-book、Spot klines/time；不載入主交易程式，不讀写正式交易DB，collector不提供任何下單、取消、arm、HS、loop或Telegram操作。Telegram由另一個獨立sender處理，需使用者明確授權。

依T6.7d First規則：兩根完成1m相反且各abs>=0.5bp，First UP prior15m>=1bp、DOWN<=-1bp，初始完整1U深度價0.15–0.45，fee-net分享扣費及gross 0.01分享floor。三市場門檻固定；ETH/BNB的適用性未驗證。

初始T+124..126第一次有效取樣凍結cap與gross數量。T+128..129.5固定單次重檢，價格不能高於cap，完整原數量深度足夠。HTTP失敗、晚到、未知fee、時間/合約/幣別/token不符、stale/future、官方勝方矛盾均不建立模擬成交。無追價或事後最佳報價搜索，重啟不回填已錯過決策。Raw證據壓縮留存。

提供INITIAL_ONLY初始報價基準與重檢sim_quote兩套帳本。官方detail必須同topic/market/token/start/end/reference/fee，terminal後才能結算。DRAW需雙winner且0.5/0.5；WR排除DRAW、PnL含DRAW且費只扣一次。pending不算loss/0Pnl。MDD按官方結果收到時間排序；仍有pending時標示未完整。

模擬假設當時顯示深度可全數執行，不包含真實排隊、claim、wallet、POST延遲、其他核心占位及Live風控。候選率不是fill率，結果不是官方Live PnL。沒有推薦分數或自動選市場。後續selector需用時間外推/heldout驗證，不能回頭挑最佳PnL。

資料由啟動時間起每5m建立三市場觀測槽，20/40/100共用相同時間槽（缺資料保留分母），固定20槽批次；非實際Live20run。服務停機的槽明確missing，不補過去quote。

```bash
python -B service.py --data /mnt/disks/data/cry3/observers/first-multimarket-v1 --credential-file /home/jack_shih/cry3/prediction/live.env
python -B report.py --data /mnt/disks/data/cry3/observers/first-multimarket-v1
```

每15秒更新latest.json/latest.md/latest.html（沒有Web server、不發TG）。資料獨立存first-observer.sqlite3；flock阻止雙開。每分鐘至多40個HTTP、最大6並行、1.2秒總逾時、1MiB響應限額。418/429至少15分鐘停止請求；取消重導向，簽名URL/憑證不入庫不記log。SQLite最多512MiB、磁碟至少留1GiB，超出停止observer；不動Live。

systemd用獨立user unit，Nice=10、CPUQuota=10%、MemoryMax=96M、NoNewPrivileges、ProtectSystem=strict、ProtectHome=read-only、僅觀測資料路徑可寫。正式release inventory/pin未更動。停用：systemctl --user disable --now cry3-first-multimarket-observer.service。

## 獨立 Telegram 觀測報表

2026-10-03使用者明確授權連接TG。`telegram_report.py`由獨立user oneshot `cry3-first-observer-telegram.service`執行，timer每分鐘檢查，不重啟collector或Live。2026-10-04修正啟用快照不滾動：原快照訊息改為「最近20場（自動更新）」，每有新已結束場次或晚到結算，用edit更新同一message。窗口取實際最近20個已結束共同觀測槽，不使用尚未完成的固定批次，不用當下時鐘製造無內容變化的更新。

原固定每20槽摘要另保留，晚到結算只更新該批同一message。兩種報表均包含BTC/ETH/BNB各自ALL/UP/DOWN、K線/盤口完整度、訊號→趨勢→初始→重檢、候選率、W/L/D、pending、WR/PnL/MDD及阻擋原因。`/firstreport`的手動回覆是呼叫當下的快照；持續更新的是自動觀測訊息。

使用`/home/jack_shih/cry3/prediction/telegram.env`既有專用token與chat allow-list；部署時已核對與正在執行的Live TG目的地相同。不得改用root `.env`中的佔位專用token，也不得自動fallback到其他bot或chat。沒有getUpdates、webhook、command或Live report變動；僅sendMessage/editMessageText，不支持其他Telegram method。

觀測DB以mode=ro讀取，發送狀態獨立`telegram-outbox.sqlite3`，flock防重入。同內容不重送；送前先持久記錄SENDING，timeout/ACK無法确认會標UNKNOWN且不自動重送新訊息；明確429依Retry-After延後，edit失敗可對同message id重試。未知送達需人工核查，不宣稱exactly-once。`telegram-status.json`僅含數量與狀態，不包含token/chat id/簽名URL。停用sender：systemctl --user disable --now cry3-first-observer-telegram.timer（collector繼續收集）。

```bash
python -B telegram_report.py --data /mnt/disks/data/cry3/observers/first-multimarket-v1 --credential-file /home/jack_shih/cry3/prediction/telegram.env --preview
```
