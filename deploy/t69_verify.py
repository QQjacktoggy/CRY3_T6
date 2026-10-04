"""Read-only T6.9 deployment verifier; safe to run at any time.

Checks the live release against an operator-approved fingerprint, the
autoarm/autoloop guard, service state, a fresh-interpreter import of T6.9
(policy fingerprint, 8 Live + 6 Shadow, Shadow schema, report render without
sending), the t69 tables in each coin's feature DB, and optionally that the
trading ledger still matches an installer backup. Writes nothing.
"""
import argparse
import json
import sys
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_verifier import approved_fingerprint, safe_path, verify_release
import t69_ops as ops


def check(name, fn, results):
    try:
        results[name] = dict(ok=True, value=fn())
    except Exception as exc:  # each check reports independently
        results[name] = dict(ok=False, error=f'{type(exc).__name__}: {exc}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-fingerprint', required=True, type=approved_fingerprint)
    parser.add_argument('--backup', help='Installer run directory; compares ledger hashes with before.json (meaningful only before T6.9 is selected or traded)')
    parser.add_argument('--extra-service', action='append', default=[], type=ops.service_name)
    parser.add_argument('--require-t69-tables', action='store_true',
                        help='After T6.9 is selected: require t69 Shadow tables in the BTC feature DB')
    args = parser.parse_args(argv)
    services = tuple(dict.fromkeys(ops.SERVICES + tuple(args.extra_service)))
    results = {}

    def release():
        manifest = json.loads(safe_path(ops.ROOT, ops.MANIFEST).read_text())
        verify_release(ops.ROOT, manifest, pin_text=safe_path(ops.ROOT, ops.PIN).read_text(),
                       expected_fingerprint=args.expected_fingerprint)
        return manifest['release_fingerprint']

    def services_ok():
        # Producer units found in systemd (ETH/BNB) are checked too.
        names = tuple(dict.fromkeys(services + ops.producer_units()))
        states = {name: ops.service_state(name) for name in names}
        if not all(s['active'] == 'active' for s in states.values()):
            raise RuntimeError('service not active: ' + json.dumps(states))
        return states

    def fresh():
        value = ops.fresh_check()
        return dict(policy=value['policy'], live=value['live'], shadow=value['shadow'],
                    report_head=value['report'].splitlines()[:3])

    def tables():
        value = ops.t69_tables()
        if args.require_t69_tables and not set(ops.T69_TABLES) <= set(value['BTCUSDT'].get('tables', ())):
            raise RuntimeError('BTC feature DB has no complete t69 Shadow tables: ' + json.dumps(value))
        return value

    def ledger():
        before = json.loads((Path(args.backup) / 'before.json').read_text())['snapshot']
        with closing(ops.prediction_db()) as db:
            hashes, protected = ops.ledger(db)
        changed = sorted(t for t in hashes if hashes[t] != before['ledger'].get(t))
        if changed or protected != before['protected']:
            raise RuntimeError('changed since install: ' + json.dumps(dict(
                tables=changed, protected=protected != before['protected'])))
        return 'unchanged since install'

    check('release', release, results)
    check('guard', lambda: ops.guard_bytes() and 'autoarm=false autoloop=false', results)
    check('services', services_ok, results)
    check('fresh_import', fresh, results)
    check('t69_tables', tables, results)
    check('selected_profile', ops.selected_profile, results)
    if args.backup:
        check('ledger', ledger, results)
    ok = all(r['ok'] for r in results.values())
    print(json.dumps(dict(status='T69_VERIFIED' if ok else 'T69_VERIFY_FAILED', checks=results),
                     indent=2, ensure_ascii=False))
    if not ok:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
