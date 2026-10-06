# T6 單幣運行：BTC／ETH／BNB 切換

2026-10-06 使用者要求：觀測器太多導致 VM 卡頓，改為「三幣可切換、可以跑就好」，觀測報告不再發到 Telegram。

## 切換幣別

從 meihan（`~\Desktop\cry3_t6`）一行完成部署與切換：

```powershell
powershell -ExecutionPolicy Bypass -File scripts\deploy_coin_switch.ps1 ETH
```

省略幣別即為 BTC。腳本依序 `git pull`、把 `t6_coin.sh` 複製到 VM、以 jack_shih 安裝並執行 `use`，任何一步失敗即停止。

或直接在 VM 上在 VM 以 `jack_shih` 執行（OS Login 使用者先 `cd /tmp`）：

```bash
sudo -n -u jack_shih sh -c 'cd /tmp && /home/jack_shih/cry3/scripts/t6_coin.sh use ETH'
```

`use BTC|ETH|BNB` 會：

1. 讀正式帳本（唯讀）。若有 RUNNING loop 且綁定的是別的幣，拒絕並不動任何服務。
2. 停用兩個研究觀測器與觀測 Telegram 發送器（同 `slim`）。
3. 保持 BTC 基準 producer（`cry3-regime-feature`、`cry3-c180-favorite-signal`）運行。
4. ETH／BNB 只 enable+start 選定那一幣的 feature／signal producer，另一幣的停用。
5. 印出服務狀態與記憶體，並提示 Telegram 下一步。

producer 跑滿約 10 分鐘（兩個市場，signal 需在市場開始前 60 秒已啟動）後，在 Telegram：

```
/predict_market ETH  →  /predict_live on  →  /predict_loop 20
```

其他指令：`t6_coin.sh status`（只讀）、`t6_coin.sh slim`（只停觀測器與觀測 TG）。腳本不 arm Live、不開 loop、不下單、不寫交易 DB。

## Telegram 變更

停掉（`slim`／`use` 立即生效，不需重新部署）：

- `cry3-first-observer-telegram.timer/.service`：三幣 First 觀測「最近20場（自動更新）」及每 20 槽固定摘要。
- 研究服務 `cry3-first-multimarket-observer`、`cry3-t67c-multimarket-observer`（不發 TG，但占 CPU／記憶體）。

從 Bot 移除（下次部署主程式後生效）：`/firstreport`、`/t67creport` 指令與選單。

保留：所有 `/predict_*` 操作、`/report`／`/predict_report`（Live 報表，內含本輪 Flat Shadow 段落）、`/shadow_report`、`/predict_monitor`，以及主程式的市況 RED／跳動停單警示。

## 注意

若 signal producer 設有 `PREDICTION_T67C_OBSERVER_ORIGINAL_ENABLED=1`（三幣觀測用付費 Original），`status` 會提示；觀測器停用後可在無 RUNNING loop 時移除該設定並重啟該 producer，恢復「只替當前交易幣呼叫 Original」的預設。
