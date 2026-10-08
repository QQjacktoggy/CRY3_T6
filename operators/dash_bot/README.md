# /dash 儀表板機器人（獨立、唯讀）

在 TG 跟這個新機器人說 `/dash`，它會回一則摘要，加一個網頁檔。點開網頁檔就是完整的儀表板，包括三幣反轉圖。內容和 Custom view 一樣：狀態、最近兩輪、本輪成交統計、全部 T6 子策略成交統計、本輪逐場紀錄、三幣反轉比較。

## 不影響下單的保證

- 這是另一個機器人，用自己的 token。不改下單程式，安裝時也不重啟任何下單服務。
- 資料庫只用唯讀方式打開（`mode=ro` 加 `query_only`），程式寫不進資料庫。
- 每場 1:30–2:20（下單時間）和台灣時間 02:00–07:30 不讀 VM，只回上一次的資料。
- 只有你按 `/dash` 才讀，不會定時讀。60 秒內重複按，就直接回上一次的資料。
- 子策略全歷史統計最多 30 分鐘重讀一次，三幣資料最多 15 分鐘向幣安重抓一次。
- systemd 鎖住資源：記憶體上限 64 MB（超過只會關掉它自己）、CPU 最多 25% 且優先順序最低、硬碟讀取排在最後。
- 每次讀取最多 20 秒，超過就停掉。
- 只回應 `DASH_CHAT_IDS` 列出的聊天室，別人傳指令不會有反應。
- 要停用的話跑 `systemctl --user disable --now cry3-dash-bot`，下單完全不受影響。

## 安裝（由 jack 或 Grok 執行，Claude 不部署）

1. 在 TG 找 @BotFather，用 `/newbot` 建一個新機器人，記下 token。**不要用下單機器人的 token。**
2. 先對新機器人說一聲 `/start`。然後在瀏覽器開 `https://api.telegram.org/bot<token>/getUpdates`，找到 `"chat":{"id":...}`，那串數字就是你的 chat id。
3. 在 VM 上以 jack_shih 身分執行：

```bash
mkdir -p ~/cry3/operators/dash_bot ~/.config/cry3 ~/.config/systemd/user
# 把本資料夾的 dash_bot.py、dash_data.py、dash_page.py、dash_coins.py 放到 ~/cry3/operators/dash_bot/
cat > ~/.config/cry3/dash_bot.env <<'EOF'
DASH_BOT_TOKEN=<新機器人的 token>
DASH_CHAT_IDS=<你的 chat id>
EOF
chmod 600 ~/.config/cry3/dash_bot.env
cp ~/cry3/operators/dash_bot/cry3-dash-bot.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now cry3-dash-bot
systemctl --user status cry3-dash-bot --no-pager
```

4. 在 TG 對新機器人說 `/dash` 試一次。

## 檔案

- `dash_bot.py`：TG 長輪詢，負責時間檢查和回傳。它是常駐的小程式，平常只在等 TG 訊息。
- `dash_page.py`：短暫的子程式，每次 `/dash` 跑一次組出網頁，跑完就結束。
- `dash_data.py`：唯讀查詢，和 `dashboard/slim/dash_slim_ro.py` v2、`t6_branch_fill_ro.py` 的讀法相同。
- `dash_coins.py`：三幣反轉區塊，和 `notes/coin_regime/coin_block.py` 相同，只讀幣安公開 K 線。
- `cry3-dash-bot.service`：systemd user 服務，含資源上限。

快取和最後一頁放在 `~/.cache/cry3-dash/`，刪掉也沒關係。
