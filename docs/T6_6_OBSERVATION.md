# T6.6 prospective observation release

T6.6 is an observation release. The trading profile remains
`regime_target6_5_v1`: first UP/DOWN, existing stall/fade_latest,
continuation/original and C retain their exact candidate rules, priority,
amount selection and persistent risk epoch. No new branch can create an order.

## New cohort

Explicit activation seeds 500 consecutive five-minute markets starting at the
next market boundary in the feature database. Collection does not require a
running Live loop. Restarting the services neither resets the cohort nor
replays missed historical quotes. At 500 markets, new collection stops;
outcome resolution continues. There is no automatic Live promotion or loop.

At T+124..126, freeze the existing T6 features, initial book and core
candidates. Features must consist of closed candles received at T+120..123.
New branches require all frozen core candidates to be empty; an unfilled
existing candidate does not free the market for another branch.

| Candidate | Frozen signal | Side and price |
|---|---|---|
| M6a | Absolute compounded two-minute net <1bp; initial cheaper side aligned with 15-minute prior of at least 1bp | Initial cheaper side (tie DOWN), 0.25–0.40 |
| M8 UP | First >=0.5bp, absolute last <0.5bp, prior >=1bp | UP, 0.55–0.75 |
| M4a | Opposite minute signs; absolute first >=1bp; 0.5 <= absolute last <= half absolute first; first aligned with prior of at least 1bp | First direction, 0.15–0.55 |
| M7 DOWN | First and last <=−0.5bp; compounded net <=−1bp; prior <=−1bp | DOWN, 0.55–0.75 |

Keep original M4/M6 and opposite M7/M8 directions as controls. Controls may
record overlapping core markets but are never counted as incremental portfolio
opportunities. At most one incremental quote per market is included in the
portfolio, ordered by first observed executable time, then M6a/M4a/M8/M7.

All paper quotes use 1U, full requested depth, fee-adjusted shares and 0.01-share
flooring. First quote requires a fresh book <=1s old during T+128..134.5.
The original quote is immutable. Separately record a +0.02 price stress and
the first fresh book at 300ms/1s after the quote (maximum 300ms sampling
tolerance); an unexecutable latency quote is not retried until favorable.
Missed samples are unknown, not losses or filled orders.

Official market detail must match topic, UP ID, start/end and resolved status.
The report rejects disagreement with official Live settlement records. Shadow
PnL never changes the trading or risk ledgers.

## Retired Shadow branches

From the activation boundary onward, new T6.5 decisions no longer persist
A, flat/original, B or fallback Shadow payloads. Their underlying routing is
still evaluated to preserve T6.5 core priority exactly. Existing decisions,
quotes, settlements and historical reports are retained. Original M4/M6
remain available as controls. A failure in the observation state cannot block
the existing core or reactivate the four retired paper branches.

## Evidence and promotion gates

Historical research covered 753 registered T6 markets, 744 with valid inputs
and official outcomes, and 21 quoted strategy variants. The directional M7/M8
selection was exploratory, not an independent holdout. The proposed branches
had 15/9/4/27 incremental quoted opportunities and fee-net paper PnL
+5.1788/+4.5471/+3.6991/+3.3825 USDT respectively. These are not actual fills.

Freeze rules during the new 500-market cohort. Before a branch can be proposed
for Live it needs at least 30 resolved executable observations, positive net
PnL in two disjoint consecutive periods and with +0.02 price stress, latency
coverage, uncertainty analysis and a full replay of the existing risk gates
using actual quote/decision/official-known timestamps. Insufficient samples
require a separately authorized extension, not automatic promotion. M7 DOWN
is second priority because historical diagnostic risk replay added an HS case.

## Operation and deployment

Runtime modules: `regime_t66_policy.py`, `regime_t66_observer.py`, and
`regime_t66_report.py`. Tables are `t66_observation_state`,
`t66_observation_markets`, `t66_shadow_quotes`, `t66_shadow_outcomes` in
`prediction/data/regime-target6/features.sqlite3`. No trading table is added.

The feature service freezes/observes the cohort. The signal service resolves
official outcomes outside the entry window using its existing shared API
budget. Telegram's existing Report appends the T6.6 cohort to the official
Live report, distinguishing the two scopes.

Only deploy at an idle boundary: no running Live loop, unresolved exposure,
nonterminal/UNKNOWN intent or official open orders/positions. Verify the full
parent release inventory and external pin, back up changed source/manifest/pin,
then update the full release inventory. Reload main, feature and signal
services to load their changed modules. Retain the existing startup guards
that disable automatic arm and new-loop creation.

The VM has a broader, separately pinned inventory than this standalone
checkout. `deploy/t66-vm-release.patch` adds only the three new modules to
that inventory; do not replace it with the standalone checkout inventory.
The reviewed parent VM release module SHA256 is
`6cee5d2e583ba328f0a18d578aea31add9f31fc375172c09e13b684cca91243f`.
The release build/verification functions match the standalone implementation;
only the literal platform-specific inventory differs.

Activate explicitly as the application user, after code/release verification:

```python
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t66_observer import activate
import time
with connect('prediction/data/regime-target6/features.sqlite3') as db:
    activate(db, time.time_ns() // 1_000_000)
```

Rollback restores code, manifest and pin; disable the new observation state
if activation occurred, retaining all observations. Never restore a trading
database backup. Verify existing official PnL, risk state and trade-ledger
digests remain unchanged across deployment.

This release also includes the previously deployed M6 report fix: a blank
generic observer ID is resolved only from validated official topic/UP-ID/time
metadata, retaining independent official winner conflict checks.
