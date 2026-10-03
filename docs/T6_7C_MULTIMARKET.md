# T6.7c 整輪選幣

本版保留 `regime_target6_7c_v1` 七條Live分支和兩條原有Shadow。策略policy fingerprint、核心占位、C下限0.65、124–136秒入場、1/2/3U及共用風控不變。沿用最新晚到成交／共享請求修正，沒有回退舊runtime，也沒有加入Flat或180秒補位。

## TG 操作

1. 用 `/predict_lane` 選T6.7c。
2. 用 `/predict_market` 點選BTC／ETH／BNB，亦支援 `/predict_market BNB` 或 `ETH`。
3. 無RUNNING loop且本地／官方帳戶無未結曝險，才能套用市場。換幣使原Live授權失效，重新用 `/predict_live on` 確認。
4. 用 `/predict_loop 20` 開20場；金額沿用 `/predict_amount`。選幣不自動開單或建立loop。
5. 交易期間選幣只保存下一輪偏好。前輪結束後再次點選該幣套用並確認Live；若尚未套用，Start會拒絕以舊幣偷偷開下一輪。

每輪symbol、profile、unit、target、execution fingerprint不可變；含綁定的新輪不支援中途延長target。CANCELLED不代表已無曝險。UNKNOWN、晚到成交、未結訂單／intent／持倉均阻止切換。HS／跨輪風控不因換幣解除。程式重啟讀取DB綁定，不能用環境預設BTC重解釋BNB輪。

## 資料與部署

正式帳本仍為 `prediction/data/prediction.sqlite3`，新增migration026及不可變綁定表。BTC沿用既有feature/signal位置；ETH／BNB使用：

```
prediction/data/t67c-multimarket/ETHUSDT/{features,signals}.sqlite3
prediction/data/t67c-multimarket/BNBUSDT/{features,signals}.sqlite3
```

每個來源DB有symbol marker；拒絕把已有資料的BTC DB標成BNB。K線、Spot、Spot/Futures WebSocket、官方symbol及variantData.priceFeedSymbol、Original模型問題／輸入hash都跟隨幣種。公開資料可持續收集三幣，付費Original僅供當前已綁定的交易幣；原BTC未綁定流程保持舊行為。

正式部署須在安全空檔完成完整release inventory/pin核對、源碼與manifest/pin備份，新增schema後驗證服務與帳本不變，保留autoarm/autoloop=false。本PR提供讀取來源的啟動器：

```
scripts/run_t67c_asset.sh feature ETHUSDT
scripts/run_t67c_asset.sh signal ETHUSDT
scripts/run_t67c_asset.sh feature BNBUSDT
scripts/run_t67c_asset.sh signal BNBUSDT
```

以jack_shih的systemd user service常駐，各symbol兩個來源程序；signal服務沿用既有signal服務的受保護環境檔、網路及共享request-weight DB。這些程序沒有交易worker／TG poller，不可用另一個交易程序取代。啟動器選VM的testnet venv或checkout的.venv；不可將憑證加入unit、Git或報表。源碼變更涉及既有BTC producer，正式部署需受控重載相關服務；候選測試不會重載正式服務。

來源應預先啟動並跨越完整warmup與新市場截止點。資料不足時維持不入場，不用其他幣資料補缺。不能使用舊的T6.7c installer直接覆蓋現行VM新版；部署需以現行完整manifest為父版保留VM額外inventory。本PR候選測試和正式部署是兩個階段，提交PR不代表已安裝或開啟BNB／ETH Live。

## 報表與觀測限制

T6.7c Report顯示本輪綁定幣種，七路成交／WR／PnL及Shadow從該輪幣種來源讀取，與帳戶共用20run風控分開歸因。舊輪未有symbol binding時標記「歷史未綁定」，不批次改寫交易歷史。既有 `/firstreport` 仍是三幣First子集的研究觀測，並非整套T6.7c的排名或Live績效。

首版手動選幣；完整七路三幣觀測排名、自動selector與預期盈利驗證為後續工作。不將近期First模擬PnL當成整套T6.7c的預期收益。
