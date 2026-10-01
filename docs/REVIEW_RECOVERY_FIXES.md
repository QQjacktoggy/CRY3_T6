# Review recovery and accounting fixes

These fixes address the seven findings reviewed on main
`e70a18f029ad7a8985b27f068b58756eef5be0f8` (T6.7a / package 6.7.1).
PR #7's Shadow depth validation remains intact. No strategy parameters, entry
windows, amounts, or normalized risk thresholds are changed.

## Behavior and regression coverage

| Finding | Result after the fix | Regression coverage |
| --- | --- | --- |
| F1 / P1: Live fills finalized as paper after restart or Live-off | Settlement routing uses durable loop and execution evidence. Live campaigns remain pending while the process lacks Live authorization. Shadow campaigns remain paper even when the process is Live. Unknown provenance remains pending. | `test_live_fill_survives_restart_or_live_off_in_shadow`, `test_shadow_campaign_stays_paper_when_process_is_live`, `test_legacy_shadow_loop_with_live_execution_never_becomes_paper`, `test_missing_execution_provenance_defers_without_signed_calls`, `test_authorized_live_resumes_existing_terminal_settlement_observation` |
| F2 / P1: cancellation denial cannot recover a restarted worker with no local loop id | Adopt the existing durable id, target, origin and completed count only when local identity is compatible and that loop owns a campaign needing management. Keep HS and admission/BUY closed. | `test_denied_cancel_recovers_durable_settlement_without_buy`, `test_recovery_does_not_adopt_mismatched_durable_identity` |
| F3 / P2: fully reduced but unsettled fills have no management task | Recorded fills count as settlement responsibility even when shares and pending execution are zero. The recovery task reconciles pending execution and waits for settlement without evaluating new strategy actions. | `test_denied_cancel_recovers_durable_settlement_without_buy` (reduced cases); existing `test_loop_cancel_recovery.py` covers task reuse, UNKNOWN, clean cancellation and empty campaigns |
| F4 / P2: legacy daily/consecutive HS unexpectedly applies to T6.7a | The repository uses `RISK_PROFILES` for its existing LIVE settlement exemption. Existing manual/UNKNOWN latch remains preserved; Regime risk thresholds stay unchanged. | `test_t67a_settlement_uses_shared_risk_not_legacy_thresholds` (1/2/3U and daily/consecutive edges); existing `test_t67a_safety.py` covers shared epoch, normalized losses, UNKNOWN and persistent MDD |
| F5 / P2: reports miss authoritative manual HS | T6.7/T6.7a reports combine current-day `prediction_risk_state` with legacy stop evidence and the existing loop flag. Missing/invalid global evidence remains unknown. | `test_real_manual_hard_stop_is_visible_in_t67a_report`, `test_t67a_report_includes_authoritative_and_legacy_hs`, `test_report_matches_worker_day_boundary_and_does_not_guess_invalid_hs` |
| F6 / P2: profile changes strand pending T6.7a paper outcomes | Poll stored T6.7a outcomes independently of the current admission profile, retaining the 20-second cadence and entry-window exclusion. Confirmed outcomes are not refetched. | `test_stored_t67a_paper_outcomes_resolve_after_profile_switch` (T6.7a/T6.7/T6.5), `test_paper_backlog_never_polls_in_live_entry_window` |
| F7 / P2: Python optimization removes installer safety assertions | T6.7a installer checks raise explicit errors under normal and optimized Python. No installer is applied by these tests. | `test_installer_snapshot_safety_survives_python_optimization` (real subprocesses, normal/-O); `test_installer_preflight_checks_run_even_when_asserts_are_optimized` (compiled normal/optimized code, mocked effects, safe/parent/pin/source/guard cases) |

## Validation

In the isolated fix checkout, using Python 3.12.14:

```sh
python -m pytest -q
python -m scripts.build_t6_release
python -m compileall -q src scripts deploy tests/test_review_recovery.py
git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol diff --check
```

The full suite passed 418 tests, with one documented historical-data skip and
six passing subtests. The 54 new regression cases use temporary SQLite databases
and mocked official responses. Network was blocked for local tests. Existing
CI runs pytest and the release build under Python 3.12. This repository has no
configured dedicated lint or type-check command; compilation and whitespace
checks were run without introducing a new toolchain.

The sole skip is `tests/test_regime_lane.py:121`: the historical research parity
dataset is deliberately absent from this source-only repository.

## Before any later VM installation

This change only prepares code for review; VM installation remains a separate
operator-authorized task during a safe gap.

- Reconfirm the approved commit, current deployed parent, full inventory hashes,
  manifest and external pin. Build a fresh candidate and retain a rollback copy.
- Require a completed or explicitly authorized cancelled loop boundary, no
  RUNNING loop, no UNKNOWN/nonterminal order or intent, and official zero
  positions and active orders. Resolve pending Live settlement responsibility
  before proceeding.
- Keep startup auto-arm and auto-start-loop disabled. Installing this change
  does not grant Live signing permission or clear a Hard Stop.
- Review any campaign previously marked DONE with only a paper settlement
  despite real execution evidence. This patch prevents new misclassification;
  it does not rewrite historical records or automatically redeem them.
- Recheck protected DB/config snapshots and official zero exposure immediately
  before source replacement; verify release pin, report and service health
  afterward using the approved operator workflow.

No real API contract, order, redemption, Telegram delivery, VM Python 3.13,
service restart, or installer `--apply` was exercised by this validation.
