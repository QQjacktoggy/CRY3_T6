# T6.6 observation deployment — 2026-10-01

Runtime verified at **08:54:39 Asia/Taipei**. The deployed source is commit
`d62757b00feb07acf1cd67f52ce0d5b58112c713` in
[PR #2](https://github.com/QQjacktoggy/CRY3_T6/pull/2). The PR remains open.

- Full VM release fingerprint:
  `f4932f5e556a2a41a6454bd988b260888a001488755ee69fca8c29edff3377e1`.
- Observation policy fingerprint:
  `6cd4a7811fba58299a4a2c26eb5d31b2c87033e749b6d3f3b3a36d45b3ab032b`.
- Fixed cohort: 500 consecutive five-minute markets from 2026-10-01 08:55
  (`1790816100000`), ending 2026-10-03 02:35 Asia/Taipei.
- Observe M6a, M8 UP, M4a and M7 DOWN. Retain M4/M6 and opposite-direction
  controls. Stop new A/flat/B/fallback Shadow records; retain historical data.

## Live status and safeguards

Live strategy remains `regime_target6_5_v1`, including first UP/DOWN,
stall/fade_latest, continuation/original and C. The deployment did not start
a Live loop, change the order unit, arm trading or reset HS/risk state.
The last Live loop `loop:1790763827947` is DONE, completed=target=100,
with official settled PnL **+0.22986887 USDT**. No loop was RUNNING.

Before each deployment, official active orders and open positions were zero;
the local checks found no nonterminal/UNKNOWN intent or unresolved exposure.
Hashes of six protected trading ledgers and selected strategy/risk settings
matched before and after deployment. Automatic arm and loop creation remain
disabled in `prediction/hs-recovery-startup.env`.

Main, feature and signal services loaded the new modules in fresh processes.
All were active with fresh feature health and main observer cursor timestamps.
The full deployed `release.py` inventory and external pin verified successfully.
The existing Report rendered the independent T6.6 cohort without changing
official Live PnL. No Telegram message was sent by the deployment script.

## Validation and observation limits

- Local full suite before the final depth regression: 157 passed, 1 skipped,
  6 subtests passed.
- Final observer module: 21 passed, including stress pricing of full-depth
  books containing prices that would exceed the binary-price domain.
- Final VM staging suite under Python 3.13: 66 passed.

The 08:55 market is recorded as UNOBSERVED_DECISION. The final signal-service
restart at 08:54:39 was inside its existing 60-second pre-open warmup guard;
it correctly refused an incomplete tape. This missing observation remains in
the cohort denominator and is never backfilled or represented as a fill/loss.

At 09:03:04, the 09:00 market was verified as OBSERVED. Its decision was
frozen at T+124077 ms using a T+124027 ms book, with no eligible core or new
Shadow candidate. This confirms prospective collection while Live is idle;
it does not establish quote/fill performance. The report showed one frozen
market and one missed warmup market, with zero paper quotes so far.

## VM evidence and rollback

- Pre-T6.6 backup: `/home/jack_shih/cry3/prediction/t66-rollback-20261001`.
- Final cost-stress fix backup:
  `/home/jack_shih/cry3/prediction/t66-cost-rollback-20261001`.
- Each contains the changed source, manifest/pin, before/result JSON and
  report evidence. The second backup restores the first T6.6 release; the
  first restores the pre-T6.6 release.

Rollback only at a verified idle boundary. Restore source/manifest/pin and
disable observation collection while retaining its records. Never restore a
trading database backup. Follow [the observation design](T6_6_OBSERVATION.md)
for prospective evidence and manual Live-promotion gates.
