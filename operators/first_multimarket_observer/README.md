# BTC／ETH／BNB First 唯讀觀測器

獨立程序、資料庫及行情請求預算。只有 GET market/list、market/detail、order-book、Spot klines/time；不載入主交易程式，不讀写正式交易DB，不提供任何下單、取消、arm、HS、loop或Telegram操作。

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
