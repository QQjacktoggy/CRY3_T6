# Import validation — 2026-09-30 (Asia/Taipei)

- VM source: cry3jack `/home/jack_shih/cry3`; source release fingerprint `815b69d5eca71ee0887161f8b751f2f64b17a3cb91f02fd4363d976de70c6e2e`.
- 102 source files selected; source hashes checked against a second VM read. Only the release inventory in `release.py` differs from imported runtime sources.
- Independent Windows/Python 3.12 environment installed from this repo's `pyproject.toml`.
- Offline suite: 87 passed, 1 skipped, 6 subtests passed. The skipped 864-market parity test requires excluded historical research data.
- Latest T6.3a/T6.3b strategy configuration and selection, frozen Original JEV source loading, release verification and mismatched-pin rejection passed.
- Manifest built and verified locally; entrypoint `--help` passed. No trading API or Telegram messages were sent.
- Credential-pattern and hardcoded credential scans found no matches. Environment files and databases are ignored.
- Existing Prediction, C180 signal and regime feature services remained active with their existing MainPIDs during both read-only VM checks. No service restart, strategy switch or VM source installation was performed.

The independent checkout is a source-management baseline. Production credentials, databases, Live authorization, and deployment migration remain outside Git. No live execution behavior was requalified on this new checkout.
