# Security scan remediation — 2026-10-02

Baseline: `367959f316a12dddd02524857addc7049983da05` (PRs #11–13 and T6.7c).
Original static scan target: `7f5db6b8d4243c2e9d0c3dfa01dd8c9e78cdbcc7`,
completed 2026-10-02 01:55:43 UTC, 179 tracked files, no runtime PoC.
All seven findings were rechecked against the new baseline. GitHub PR search
found no open PR duplicating this work.

| Finding | Current exposure and remediation |
| --- | --- |
| 1. Execute staged Python before verification | All four T6.7/a/b/c installers were affected. A reviewed, stdlib-only `deploy/release_verifier.py` now verifies hashes, pin and inventory without executing candidate code. Inventory is parsed as literal AST data. Both preflight and apply use it before importing deployed runtime code or stopping a service. |
| 2. Candidate copy list outside manifest | Candidate paths must be canonical, unique, contained and free of symlink components. The list must equal the verified old/new manifest delta and match both hashes. Installers copy the exact bytes verified before stopping services. Runtime manifest validation also rejects unsafe paths. |
| 3. Unbounded high-frequency evidence | Only disposable raw quote/spot rows are pruned by age and row count. Per-database and payload caps, free-space reserve and a WAL threshold stop collection on exhaustion. Signals, recovery outcomes, decision/shadow audit and trading ledgers are not pruned. SQLite reuses freed pages. |
| 4. Unbounded Retry-After | The current native client already fails immediately on 418/429; the remaining shared-budget issue is malformed/huge deadline handling. Parsing and persistence are bounded, blocking waits have a short ceiling, and requests defer promptly while the full valid exchange ban remains active across restart. This is not a ban bypass. |
| 5. Stop during BUY quote | Every BUY is explicitly marked at dispatch and checks local controls and durable loop/global stop state before invoking a client, then again at the native HTTP boundary. Existing C180 expiry/book guards remain. Known pre-submit rejection is recorded as REJECTED, including P3; SELL management remains available. |
| 6. Optimized Python strips assertions | T6.7 installer and T6.5 overlay safety assertions become explicit exceptions. T6.7a/b/c operational checks already used explicit exceptions. |
| 7. Unbounded HTTP bodies | Native and frozen Prediction clients, Original JEV and klines use bounded stream readers before JSON parsing. Wire and decompressed sizes are both counted; missing/incorrect lengths cannot bypass limits. Error bodies have a smaller budget. Frozen source inventory is regenerated with its existing packaging workflow and verified normally. |

## Operational semantics

Ship the reviewed installer and `release_verifier.py` together outside the staged
release. Pass the required `--expected-fingerprint <approved SHA-256>` from the reviewed
build through a trusted channel independent of STAGE. Do not compute that argument
from the stage's manifest, pin or validation file. The stage pin must match both
the computed file identity and that external approved fingerprint. These checks
are not publisher signatures; the operator still authenticates the installer
and approved fingerprint through the review/distribution path. Candidate metadata must list the exact changed files,
not a superset or an arbitrary selection. Installation still requires all existing
safe-boundary, exposure and protected-ledger checks. Candidate path checks reject
static symlinks and traversal, and staged bytes are cached after verification.
The destination still uses pathname-based operations; the application root and
installer directory must remain operator-controlled during installation. This
is not protection against another same-UID process repeatedly swapping destination
directories between filesystem operations.

Evidence defaults are 256 MiB per database, 128 MiB free-space reserve,
8 MiB WAL check threshold and 64 KiB per UTF-8 payload. The WAL threshold is
checked between transactions, not a hard file-size cap: a large legacy-data prune
can exceed it in one transaction. A pinned reader prevents further ingestion
once the threshold is crossed. The SQLite database cap also bounds the amount of
data a single cleanup can touch; free-space checks do not constitute a hard
filesystem quota. T6.7 spots retain at most
20 minutes/30,000 rows; T6.7 books 1 hour/36,000 rows; C180 book events
24 hours/40,000 rows and latest books 24 hours/300 markets. Age or count may
expire data first, with up to 63 rows between sweeps. C180 runtime collects only
T+124..136 seconds of each five-minute market, so its 40,000-row cap covers
more than a day at the normal 10 Hz cadence. Immutable audit can fill the database;
then collection fails closed instead of deleting audit. A blocked WAL reader or
low free space must clear before collection resumes. These budgets do not reclaim
an already oversized historical file. Deleting rows alone does not reduce its
SQLite page count, so retrying startup after a DELETE is insufficient. Follow
an independently authorized offline maintenance procedure:

1. Stop only the relevant evidence writer and readers at the existing safe
   service boundary; do not operate on the trading database. Take a consistent
   SQLite backup containing the WAL state, and retain the original plus backup.
2. On a separate working copy with adequate free disk, apply the documented
   retention only to raw `spot`/`books` or `c180_book_events`/`c180_books` tables.
   Never delete signal, recovery-outcome, decision, settlement or trading audit.
3. Compact the working copy with SQLite `VACUUM INTO` a new file. Compare schema,
   integrity and exact immutable-table content against the consistent backup;
   verify required recovery evidence is still present. Check that the resulting
   file fits 256 MiB and leaves at least the 128 MiB reserve before considering
   an operator-authorized replacement of that evidence DB.
4. If immutable audit itself exceeds the budget, keep ingestion stopped and
   arrange a reviewed archival/budget change. Moving away the entire DB or
   silently recreating an empty one loses recovery audit and is not supported.

No such maintenance is executed by this patch; no trading database is migrated,
vacuumed or deleted.

HTTP limits are 2 MiB for Prediction responses, 64 KiB for error bodies,
256 KiB for Original JEV, and 64 KiB for the 17-candle klines response.
Unsupported compression and malformed compressed streams fail closed.
A huge valid Retry-After remains a durable exchange restriction; an operator must
investigate it instead of using a local timeout to resume requests early.

The shared budget has a sibling `<budget-db>.cooldown` directory. Each HTTP
request first fsyncs a locked write-ahead record. A completed response durably
records its deadline independently of the SQLite writer lock. Peers can continue
around another live request, but an unlocked incomplete/corrupt record fails
closed across restart. The journal must be backed up and retained together with
the budget DB; deleting it cannot be used to reset a ban. Unknown records require
operator investigation of the failed request and exchange restriction before any
separately reviewed recovery. Journal storage failure before dispatch denies the
HTTP call. Extra local durability I/O is covered for correctness; production
latency impact has not been measured.

## Validation scope

Validation uses temporary SQLite databases, mocked exchange responses and
installer/service effects. The validation interpreter denies network socket
connections, including subprocesses. Tests cover malicious staged code without
side effects, path/copy-list attacks, Python optimization, retention convergence,
WAL readers, low-space recovery, BUY races, durable backoff and compressed bodies.
Integrated validation (Python 3.12.14): **1061 passed, 1 skipped, 6 subtests passed**.
The 337 additional passing cases cover the new security boundaries. The existing
installer recovery fixtures now exercise real trusted verification instead of
mocking the removed staged `runpy` loader. Full release build, normal frozen
`live.verify_source()`, 131-file release/pin verification, `compileall`, and
CRLF-aware `git diff --check` pass.

Release fingerprint:
`a07cae1a10cfe2546055e767c6d41c63eca15ed45187721cb7ab8133f1650b76`.

Reproduce the repository checks with the README's locked environment:
`python -m pytest -q` and `python -m scripts.build_t6_release`.
Independent review of the initial implementation found three P2 gaps: a stage-
supplied trust anchor, loss of a ban across peers/restart after SQLite write
failure, and missing rate-limit metadata on bounded-body errors. Follow-up
regressions cover those complete attack/failure sequences. Re-review also found
that prepaid weight reservations bypassed a newly recorded local ban. Weight
reservation and cooldown admission are now separate: every request checks the
local ban again at actual transport, after signing/journal I/O and the durable
BUY guard, without debiting prepaid weight twice. Final re-review and
remote CI results are recorded in the PR.

No VM connection, deployment, real exchange API, secret access, trading-ledger
rewrite, merge, or Security Cloud finding-state update is part of this work.
Production disk sizing, live response distributions, VM Python/systemd behavior,
and actual installer apply are untested. The historical parity dataset remains
excluded from this repository and its existing parity test remains skipped.
