# ETH T6.7c Shadow v1

Experimental profile `eth_t67c_shadow_v1`, based on BTC T6.7c at commit
`a3cd39896437149775a9f3273f8df037e776d17b`. BTC policies, bridges, worker,
immutable C180 experiment and running services are not changed by this port.
This document is an offline/engineering rollout guide, not authorization to deploy.

## Scope and limitations

The standalone engine reuses the frozen T6.5/T6.7c pure routing and depth math:
`core_first_down`, `core_first_up`, `core_stall_down`, `core_c_down`, then
`c_mirror_up_prior` / `shallow_retracement` only after verified empty core.
Core reservation stays sticky when later price/depth is adverse. Quotes are
`PAPER_QUOTE_ONLY`, not simulated fills, real trades, Live win rate or Live PnL.
Official-result scoring is explicitly hypothetical quote PnL and is not used
to arm anything or modify BTC risk. The isolated store has no trading tables,
intents, claims, positions, Live arm or automatic promotion capabilities.

`core_continuation_original` remains unavailable: an ETH Original probability
producer and calibration have not been validated. Missing Original data in
continuation/flat/empty stall is unknown, never proof of empty core. External
lead/lag and reference-value research branches are outside this first phase.
Spot/futures ETH public streams are collected, with source event/receipt clocks,
generation and opening anchors; futures is diagnostic-only in this deterministic
phase. No BTC probability or model prompt is reused as an ETH prediction.

BTC numerical thresholds are retained as experimental candidate hypotheses,
not an assertion that they are suitable or profitable for ETH.

## Official market specification gate

No official ETH specification is bundled or inferred. A locally reviewed JSON
specification must contain these keys:

| Key | Required meaning |
| --- | --- |
| `verified` | Boolean `true`, after independent official-source review |
| `evidence_sha256` | SHA-256 of the retained, non-secret official specification evidence |
| `symbol`, `underlying`, `duration_ms` | `ETHUSDT`, `ETH`, `300000` |
| `oracle_provider`, `oracle_feed_id` | Exact reviewed ETH settlement oracle identity |
| `vendor`, `chain_id` | Official Prediction venue and chain; do not infer chain from ETH underlying |
| `tick_size`, `min_cash_usdt`, `min_shares`, `share_step`, `fee_bps` | Official contract execution specifications as decimal strings |

The first-phase depth math supports only an officially confirmed `share_step`
of `0.01`; a different step is rejected, not rounded using guessed ETH rules.
Per-market detail must explicitly match `symbol`, `underlying`, categories,
`settlementOracle.provider/feedId`, `tickSize`, `minOrderAmount`, `minShares`,
`shareStep`, `feeRateBps`, vendor/chain and canonical binary markets/outcome
indexes/tokens. These adapter field mappings themselves have not been verified
against a current ETH production response. If official metadata does not expose
these fields, collection records diagnostic failures and creates no candidates.
Review the real schema and add a separately tested mapping before accepting it;
do not fabricate fields or weaken the gate to get quotes.

No-spec collection is allowed only as a missing-data diagnostic run. A supplied
but unverified spec is refused at startup. Spec hash, policy fingerprint, source
mode and finite window target are pinned in the namespace. Change any of them
only by choosing a fresh parent directory with leaf `eth-t67c-shadow`.

## Offline replay

From the repository root, using the existing virtual environment:

```sh
.venv/bin/python -m src.gridbot.prediction.eth_t67c_service \
  --replay /absolute/path/eth-evidence.jsonl \
  --market-spec /absolute/path/reviewed-eth-spec.json \
  --root /absolute/path/replay-01/eth-t67c-shadow --windows 20
```

Each JSONL event includes `symbol: "ETHUSDT"`, `kind`, and the original
`received_at_ms`. Event receipt clocks may be equal but must not decrease.
Events: `trade` (source `spot`/`futures`, generation, native aggTrade packet);
`disconnect` (source); `observe` (start, optional official market detail, 17
closed candles, feature_received_ms, canonical full-depth book); `resolution`
(start and official terminal market detail). Book identity is the output of
`eth_t67c_core.identity`, plus `full_depth`, `book_at_ms`, `received_at_ms`,
`captured_at_ms`, and UP/DOWN `quote[side].ask_levels`. All clocks are milliseconds.
Synthetic examples exist only in tests and are not an official ETH spec.

Replay binds the entire file SHA-256, resumes its event cursor and refuses a
changed file or collector namespace. Input is bounded to 256 KiB per event and
256 MiB per file. Repeated replay and report callbacks cannot create a second
quote/outcome. Output is under the explicit root: `namespace.json`,
`shadow.sqlite3`, and the process lock. Symlinks, foreign DB tables and namespace
configuration changes are refused. There is no access to the BTC trading DB.

## Read-only collector (do not start production as part of this PR)

Only after a separate engineering-run decision, reuse existing dedicated
Prediction read credentials already available in the process environment:
`PREDICTION_BINANCE_API_KEY` / `PREDICTION_BINANCE_API_SECRET`. The service does not
load `.env`, fall back to legacy Binance credentials, or acquire new credentials.

```sh
.venv/bin/python -m src.gridbot.prediction.eth_t67c_service \
  --collect --market-spec /absolute/path/reviewed-eth-spec.json \
  --root /absolute/path/engineering-01/eth-t67c-shadow --windows 20 \
  --shared-weight-db /absolute/path/existing-shared/request-weight.sqlite3 \
  --resolution-grace-seconds 600
```

The catalog transport only exposes signed GET market/list and market/detail;
there are no wallet/trade/quote/redeem endpoints or POST transport. Prediction
book subscriptions reuse the existing read-only WS parser. Public ETH spot and
futures feeds are parameterized independently of BTC C180. Shared budget must
be the existing account/IP budget, not a second private full quota; it preserves
BTC reserves and stops/defer reads when unavailable. Foreign trading DBs cannot
be used as a budget file. Catalog and public K-line requests both use that same
budget and durable response journal.
K-line weight is 2 per the [official Spot API documentation](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints).
Catalog/refetch/resolution tasks run separately from
the 100 ms decision loop and feature timer; the feature receipt timestamp is
captured after the request returns. Start before the intended opening boundary;
an unwarmed first window is counted as skipped, not retrospectively repaired.

The finite schedule counts skipped and gap windows in its denominator. Missing
T+120..123 features, no first usable book at T+124..126, stale books, unknown
specs and broken opening anchors produce diagnostics, never late selection.
Parent last selection T+134.5, expiry T+136 and additive 2-second TTL remain
unchanged. Disconnect/reconnect invalidates anchors; a new generation cannot
reuse the pre-disconnect opening price. Official results rotate fairly across
pending windows until bounded grace expires; unresolved outcomes remain pending.

Telegram is optional. Only an already-existing separate ETH Shadow bot may use
`--poll-telegram`, configured by `ETH_SHADOW_TELEGRAM_BOT_TOKEN` and
`ETH_SHADOW_TELEGRAM_CHAT_IDS`. No token fallback or new bot creation is part of
this PR; equality with a configured BTC token is refused. Operators must also
verify the token is a distinct bot before polling when BTC config is not present
in this process. Handlers are only `/eth_shadow_status`, `/eth_shadow_report`
and read-only, fingerprint-bound report callbacks, using existing chat
authorization. Generic `/predict_live`, amount and loop handlers are not exposed.
Without an existing separate bot, omit polling and inspect the final status or
`ShadowStore.report()` offline. No Telegram messages are sent by this PR work.

## 20–50 window engineering acceptance

1. Run 20 windows in a fresh isolated namespace, then a separate 50-window run;
   count every scheduled window, including startup, gap, rejection and missing data.
2. Confirm zero trading HTTP calls, intents, Live claims/arm, BTC DB/risk writes,
   unchanged BTC service status, namespace source/identity integrity and bounded
   shared-budget load. No production concurrency is authorized by this document.
3. Inspect feature receipt lag from actual `received_at_ms`, initial selection
   timestamp, selected book age, connection generations and all deadline/gap
   diagnostics. Inject late HTTP, missing first book, 50–145 second worker gaps,
   clock reversal, reconnect, conflicting metadata, duplicate callbacks, crash
   between quote/window writes and a permanently pending first outcome.
4. Every selected quote must be causal, frozen once, fee/precision/minimum-size
   valid and attributable to exact ETH market/tokens/spec. Compare deterministic
   branches against the frozen BTC pure rules using identical synthetic inputs.
5. Check 100% eventual official outcome reconciliation for admitted verified
   windows after the allowed grace; keep unresolved evidence explicit. Confirm
   DRAW schema is official and no proxy feed chooses the winner.

These are engineering gates, not statistical investment validation. Later
calibration requires independent ETH data, time-separated evaluation, fee/slippage
stress, enough samples per branch and explicit simulation assumptions. There is
no Live promotion threshold or Live implementation in this profile.

## Offline checks

```sh
.venv/bin/python -m pytest -q
.venv/bin/python -m scripts.build_t6_release
```

New regression cases cover asset/spec contamination, no trading capabilities,
parent parity, namespace/symlink rejection, source mode, immutable atomic quotes,
generation watermarks, stale/future data, replay resume, TG authorization and
shared-budget failure handling. Generated release pins describe the new source
checkout only; never replace the running BTC release pin during this task.
