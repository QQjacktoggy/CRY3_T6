# T6 shared entry latency change

This change targets the shared execution path from T6 through T6.7a. The observed T6.7a shallow UP candidate reached the claim call 1,744 ms after selection, leaving 256 ms of its original 2-second lifetime. Historical T6.3/T6.5 also had multi-second claim-to-submission intervals. The old `claimed_at_ms` is an input clock, not proof of COMMIT completion. These observations justify removing repeated work and measuring the actual boundaries; they do not prove how much latency or missed fill this implementation will recover.

## Execution and durability

- A single persistent SELECT validates the complete fixed schedule, the current topic/UP identity and existence of the original risk epoch. An absent or changed row falls back to the existing registration path. No process-local registration cache authorizes trading. Admission and atomic claim still check current risk.
- Each complete historical risk check batches the original snapshot SELECTs across all Live T6 loops, in groups of at most 200 loop IDs, on the same existing transaction snapshot. The original single-loop validation code remains the reference. The cache ends with that check: settlement, incomplete pairing, orphan fills, UNKNOWN, unresolved exposure, cumulative/scheduled loss and per-loop MDD remain active. The orphan `LIMIT 1` probe becomes an all-loop probe so another loop cannot disappear behind a global limit.
- The Regime claim transaction saves intent, unique market claim, full campaign entry payload and attempt counters together. The worker removes the redundant campaign commit before marking submission. The separate durable submission marker still precedes every POST; no durability setting changes.
- Clock, wallet freshness and absolute expiry are checked after acquiring the operation gate and SQLite writer, and again after risk calculation. A commit that completes after expiry retains the claim but rejects the known unsubmitted intent. It cannot restart entry or create UNKNOWN without possible HTTP execution.
- Fresh frozen-signal equality, depth/price, loop/control state and expiry are rechecked after the claim. Fresh book is checked again after the submission-marker commit. Dispatch and the native client's actual transport boundary check deadline and original profile book age (T6/T6.1/T6.2: 2 seconds; T6.3 and later: 1 second). The transport boundary also reads current loop/HS, lane/per-loop MDD latch and T6 UNKNOWN state using a separate read-only connection; failure denies submission.
- Generic risk-state persistence now merges the current same-day HS/reset state under one write transaction, preventing an older admission snapshot from clearing a newly committed operator HS. Explicit HS reset retains its separate existing path.
- Official active orders and positions are read concurrently only when the transport explicitly supports concurrent reads. The default stateless urllib transport does; requests sessions and custom transports remain serial unless they explicitly opt in. Both reads must finish and prove zero exposure. Their existing request-budget/signing/GET-retry rules remain active.
- Missing local feature/book readiness can be retried every 100 ms, at most eight waits and within the original T+124..126 selection window. This does not repeat schedule, account API or full risk work, and does not reselect a frozen rejection. Global polling is unchanged. Quote and balance remain in their original order.

The C DOWN 0.65 minimum, branch routing/priorities, units, price/fee caps, frozen signals, 2-second new-branch TTL, T+136 deadline (original T6.7 retains T+270), HS/MDD, single-market claim and official fill/settlement accounting are unchanged. There is no schema migration, trading-data backfill, auto-arm, auto-loop or automatic POST retry.

## Timing evidence

`EXECUTION_TIMING.payload` retains original capture time (`at_ms`, `monotonic_ns`), with an `attempt_id`, profile, elapsed time and remaining lifetime where applicable. Flush time in `event_time_ms` is not execution time.

| Event/span | Interpretation |
|---|---|
| `entry_selected` | This attempt accepted an already frozen candidate. For first-selection latency, join the strategy's persisted selection timestamp. |
| `prepare_history_risk` | Complete admission ledger check, including its persistence. |
| `prepare_risk_and_official_exposure` | Whole prepare path; official orders/positions each retain API timings. Parallel API durations overlap. |
| `generic_risk_snapshot`, `generic_risk_persist` | Shared worker admission reads and risk-state persistence. |
| `api_start`, `api_ack`, `api_error` | Dispatch/response spans for quote, balance and account reads, correlated to the same attempt. `api_start` alone does not prove a place-order transport call. |
| `fresh_book_wait` | Fresh book validation and bounded waits. |
| `claim_total`, `claim_lock`, `claim_begin`, `claim_history_risk`, `claim_commit` | Whole claim, gate wait, SQLite writer acquisition, full ledger check and actual COMMIT. Remaining identity/insert/payload work is the difference from the total. |
| `submission_marker` | Durable timestamp/deadline update immediately before submission. |
| `entry_http_start` | Native client reached the actual place-order transport boundary after all guards; `http_at_ms` and `http_monotonic_ns` are captured there. |
| `entry_finished` | Explicit pre-POST rejection/exception, or `post_started`. Actual HTTP error/ack retains attempt correlation. |

No HTTP arguments/results, wallet, credentials, signing values or URLs enter these events. The in-memory queue remains capped at 2,000. Telemetry flush inserts at most 32 events with one commit; a failed flush retains them. Heartbeat delays telemetry during local selection/active admission. Queue overflow drops telemetry only, with a drop counter. Trade/claim/fill/settlement writes are never queued as telemetry.

## Offline validation and rollout

`tests/test_t6_entry_critical_path.py` covers all nine T6 profiles, original-versus-batched snapshots, 20-loop bounded reads, orphan/UNKNOWN/settlement changes, atomic campaign+intent rollback, competing claims, cancellation and restart barriers, lock/risk/commit expiry, operator HS/stop-buy, native post-budget/signing guards, original quote freshness and telemetry failure/overflow. The synthetic 20-loop empty-slot test performs at most 12 SELECTs for its batched snapshots, compared with the repeated per-loop path. This is a query-count check, not a production latency benchmark.

Run the whole offline suite and `python -m scripts.build_t6_release`. The builder regenerates and verifies the complete source inventory plus external pin; generated manifest/pin remain ignored by Git. Tests use temporary SQLite and fake HTTP; no VM orders are placed.

No VM deployment is part of this PR. After merge and deployment authorization, wait for an idle gap with no RUNNING Live loop, open positions, unfinished orders or UNKNOWN. Verify the current full release inventory/pin first; back up source, manifest and pin; deploy the complete changed runtime inventory and verify with the deployed release verifier. Reload only the main service in that gap. Feature/signal services, trading databases and arm/HS configuration are not deployment targets. If validation fails, restore source and manifest/pin; never restore the trading database.

After deployment, compare matching branches with the same TTL/price conditions: first selection-to-actual HTTP latency, claim subspans, admission rejection reasons and submission rate; report p50/p95 and sample counts. Design goals remain selected-to-HTTP p95 <= 1,500 ms and claim p95 <= 250 ms; neither is claimed as achieved by offline tests. Increased submission does not itself establish higher fill, WR or PnL.
