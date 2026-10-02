# CRY3 VM 連線與唯讀查詢

2026-09-30 已驗證以下 IAP / OS Login 流程。這是連線指南，不是即時狀態報告；每次查詢都要重新讀取 VM。

2026-10-01 已安裝 T6.7 四策略與 Telegram 專用報表；尚未選擇 T6.7、arm 或建立新 Live 輪次。
既有 T6.5 本輪已由使用者取消。正式版本、驗證與回復位置見
[T6.7 部署紀錄](T6_7_STAGING_20261001.md)。查詢時仍須重新讀取 VM，不以此文件推定即時狀態。

T6.7a 延續核心的版本與部署核對見 [T6.7a](T6_7A.md)。連線後仍須查詢實際選定 profile 與最新 loop，安裝版本不等於已啟動 Live。

T6.7b 的 PR9 送單優化、唯讀重檢及獨立版本／報表見 [T6.7b](T6_7B.md)。
部署後也須重新核對正式 release、選定 profile 與 loop；不能只依文件推定已開始 Live。

T6.7c 的固定 20 run 報表、T6-only lane 選單與版本隔離見 [T6.7c](T6_7C.md)。
安裝版本不等於已選定或已啟動 Live，仍須重新查詢。

## 目標

- VM：`cry3jack`
- GCP project：`project-f7b56371-5bd7-47cc-ad6`
- Zone：`asia-east1-a`
- Google 帳號：`pennyfamily9512f@gmail.com`
- OS Login 使用者：`pennyfamily9512f_gmail_com`
- 應用程式使用者：`jack_shih`
- VM 程式目錄：`/home/jack_shih/cry3`
- 目前 T6 共用資料庫：`/home/jack_shih/cry3/prediction/data/prediction.sqlite3`

## 此雲端 workspace 的快速連線

若既有腳本存在：

```bash
/workspace/cry3-vm --quiet --command='date -Is; uptime'
```

腳本載入 `/workspace/cry3-vm-env.sh`，並使用 IAP、OS Login 和 `/workspace/.gcloud/google_compute_engine`。

腳本不在時，使用完整指令：

```bash
export PATH="/workspace/tools/google-cloud-sdk/bin:$PATH"
export CLOUDSDK_CONFIG=/workspace/.gcloud
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE=/etc/ssl/certs/ca-certificates.crt

gcloud auth list
gcloud compute ssh cry3jack \
  --project=project-f7b56371-5bd7-47cc-ad6 \
  --zone=asia-east1-a \
  --tunnel-through-iap \
  --ssh-key-file=/workspace/.gcloud/google_compute_engine \
  --ssh-flag='-F /dev/null' \
  --quiet \
  --command='date -Is; uptime'
```

上述 SDK、CA 與金鑰路徑屬於此 Linux 雲端環境。在本機或新環境先確認路徑，使用該環境的 SDK 與受信任 CA，不要假設檔案存在。

## 登入、金鑰與跨 session

若環境支援環境變數型機密，名稱為 `CRY3_SSH_PRIVATE_KEY`，值為完整多行私鑰（不是路徑）。使用者的「管理機密」介面曾顯示「請檢查網域」，此介面的可用網域設定與機密注入尚未驗證，不能假定選「沒有網域」便能儲存。此次已驗證連線使用 workspace 中既有金鑰及 OAuth 憑證。

目前 workspace 的 `/workspace/cry3-vm` 已支援在私鑰檔缺少時將此變數寫成權限 600 的檔案，且不印出值；既有私鑰不會被覆蓋。該腳本尚未隨 repo 發佈，新環境仍需取得腳本與 SDK。機密是否成功注入要在套用設定後確認，不能只依介面儲存判定。Google OAuth 登入仍需另外配置。

- 先執行 `gcloud auth list`；有正確帳號時沿用，不要不必要地重做登入。
- 沒有帳號時，執行 `gcloud auth login pennyfamily9512f@gmail.com --no-launch-browser`，由使用者完成 OAuth。驗證碼只適用於該次等待中的登入。
- 登入、API 查詢與 IAP 程序都需要執行工具的網路權限。本環境使用 `exec_command` 時可指定 `sandbox_permissions: "with_additional_permissions"` 及 `additional_permissions: {network: {enabled: true}}`。
- SSH 私鑰：`/workspace/.gcloud/google_compute_engine`（權限 `600`）；公鑰：同路徑加 `.pub`。gcloud 可在金鑰缺少時建立並登錄公鑰。
- 憑證目錄與私鑰不能加入 Git、貼進對話或寫入說明文件。
- 共用同一份檔案系統的 session 才能沿用登入。獨立環境要透過受支援的持久化檔案／Secrets 設定或重新登入。
- 本指南只保存非敏感連線資訊，不會自動配置雲端環境、安裝 SDK 或移轉憑證。

## 網路與已知環境問題

環境需要允許以下 HTTPS 目的地：

```text
accounts.google.com
oauth2.googleapis.com
www.googleapis.com
cloudresourcemanager.googleapis.com
compute.googleapis.com
oslogin.googleapis.com
iap.googleapis.com
tunnel.cloudproxy.app
```

保留環境提供的 HTTP_PROXY / HTTPS_PROXY 與 CA 信任設定。被政策封鎖時，透過環境設定介面修正，不要繞過代理。

- `CERTIFICATE_VERIFY_FAILED`：gcloud IAP WebSocket 需要 `CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE` 指向環境受信任 CA；只設定 REQUESTS_CA_BUNDLE 不一定足夠。不可停用 TLS 驗證。
- `/home/agent/.ssh` 唯讀：使用 workspace 內的 `--ssh-key-file`。
- 系統 SSH 設定檔擁有者／權限錯誤：此環境用 `--ssh-flag='-F /dev/null'` 忽略有問題的系統設定，IAP 仍由 gcloud 建立。
- 已驗證指令可能顯示無法寫入 `google_compute_known_hosts` 的警告。確認命令退出碼與 VM 回應；若遇到 host key 不符，先核對主機身分，不要盲目忽略。
- `No active account`：檢查 CLOUDSDK_CONFIG 與登入狀態。
- API 未啟用：先確認專案 ID，不要為查狀態自動啟用其他專案的 API。

## 查詢 live 狀態

使用 OS Login 登入後，以 `sudo -n -u jack_shih` 讀取應用程式檔案。先 `cd /tmp` 避免該使用者無權讀取 OS Login 使用者的家目錄。

以 `sqlite3.connect('file:/home/jack_shih/cry3/prediction/data/prediction.sqlite3?mode=ro', uri=True)` 開啟資料庫，僅執行 SELECT 或唯讀 PRAGMA。

交叉確認：

1. VM 狀態、時間、uptime、磁碟與記憶體。
2. 實際 Python 程序及命令；不能只依賴 `cry3-mainnet-v2.service` 的狀態判断 T6 live。
3. `prediction_loops` 最新 LIVE 輪次的策略、state、target、completed、net_pnl。
4. `prediction_runtime_config` 的 `prediction_heartbeat`、`prediction_hard_stop_latched`、`prediction_risk_state` 與當前策略風控。
5. 該輪次的 campaigns、orders、近期 risk events 與實際程序日誌。舊的 eth-live.log 可能已停止更新，不代表当前 T6。
6. 對外報告使用台灣時間，標明查詢時間與證據範圍；資料庫損益不等於已驗證交易所餘額。

查狀態的請求只授權唯讀檢查；不要因此重啟程序、解除 HS、修改風控、下單、刪除 Telegram webhook 或變更資料庫。任何其他操作依使用者當次授權範圍執行。
