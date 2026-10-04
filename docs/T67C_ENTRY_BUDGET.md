# T6.7c entry timing and refusals

This patch addresses sampled book gaps and the historical BUY lookup in the
shared worker. It depends on the multi-market T6.7c branch in PR21. It does not
deploy, resume or create a Live loop.

## Observed failure paths

The October 4 BNB investigation found valid initial books arriving between
one-second worker ticks inside T+124..126. A selected First candidate also
encountered a stale book shortly before the next update. Two shallow candidates
expired before HTTP; their claim stages took about 624–635ms. Profiling the
unchanged historical BUY union on the VM found a no-match lookup taking about
477ms and scanning intents/fills. These observations identify avoidable local
work; they do not establish an expected increase in realized fill or profit.

## Changes and boundaries

- Migration 027 adds a covering campaign index on official topic, market start
  and campaign ID. The original claim query and all historical BUY evidence,
  including rejected intents and cross-loop barriers, remain unchanged. The
  index applies to the shared claim path for existing T6 profiles.
- T6.7c refusals distinguish missing/stale/future books, identity, fee, execution
  price band, depth and EV. Known exception messages map to fixed reason codes;
  unknown failures emit only the existing exception class. Failed candidate
  evaluation records bounded codes in the decision payload. A denied recheck
  with no signal no longer claims that the frozen signal changed.
- Initial T6.7c readiness includes missing/stale local books in the existing
  100ms retry loop (at most eight waits). It checks the original initial deadline
  again after waking. Identity, future clocks, price/depth/EV and frozen core
  refusals are not retried. It neither selects using a historical future sample
  nor reconstructs a missed initial window.
- Selected readiness can wait for a missing/stale/newer local book before claim,
  after claim and after the submission marker, with at most seven 100ms waits
  per check. Each wait is bounded by the original frozen absolute deadline. A
  post-claim retry keeps the same intent, signal, side, unit, cap and claim; it
  cannot create a second BUY. Schedule, account API and full risk work are not
  repeated by book polling. Existing admission and final HTTP checks still
  execute after the wait. Other profiles retain single post-claim/final checks.
- BUY telemetry separates shared-weight admission, signing, request journal, durable
  admission and final HTTP admission. Spans contain only stage names/durations
  and existing campaign/intent correlation. They are buffered during the client
  call and passed to the existing telemetry queue afterward. Journal durability,
  cooldown checks and final admission are unchanged; callback failure cannot
  authorize a request or mask its rejection.

The C lower price bound remains 0.65, book freshness 1,000ms, additive quote TTL
2,000ms, and existing core/additive priority, absolute deadlines, risk thresholds,
amounts, UNKNOWN handling and official fill/settlement accounting remain intact.
There is no automatic POST retry, re-arm, next loop, or accounting backfill.

## Verification and later deployment

`tests/test_t67c_entry_budget.py` covers between-tick book arrival, delayed wakeup,
expiry, structural refusal, frozen selected rechecks under a competing writer,
populated index migration/reopen, the real claim query plan, cross-loop rejected
BUY barriers, separate official markets, telemetry failures and HS during a
post-claim wait. Existing HTTP admission and execution tests remain required.

Deployment requires separate authorization and an idle gap verified against
local and official positions/orders, active loops and UNKNOWN. Check the current
full release manifest/pin first, back up source/manifest/pin, and include migration
027 in the inventory. SQLite builds the index on service initialization and may
hold a writer lock while doing so; do not introduce it during a running Live loop.
Never restore a whole trading database to roll back this source change.
