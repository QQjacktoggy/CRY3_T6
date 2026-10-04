"""Operator-run T6.9 rollback to the parent release in one installer backup.

Default is a read-only preflight. --apply stops the services, restores the
backed-up source, manifest and pin, deletes files T6.9 added, verifies the
parent fingerprint and starts the services. It never touches trading
databases, risk latches, the selected strategy, arm state or loops.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_verifier import safe_path, verify_release
import t69_ops as ops

T69 = 'regime_target6_9_v1'


def load(backup):
    backup = Path(backup)
    if backup.is_symlink() or not (backup / 'before.json').is_file():
        raise RuntimeError('Not a T6.9 installer backup: ' + str(backup))
    before = json.loads((backup / 'before.json').read_text())
    candidate = before['candidate']
    old = json.loads(safe_path(backup, ops.MANIFEST).read_text())
    old_pin = safe_path(backup, ops.PIN).read_text()
    if old.get('release_fingerprint') != before['parent'] or candidate.get('parent') != before['parent']:
        raise RuntimeError('Backup manifest is not the recorded parent release')
    if before['parent'] not in old_pin:
        raise RuntimeError('Backup pin is not the recorded parent release')
    for row in candidate['files']:
        if row['before'] is not None and ops.digest(safe_path(backup, row['path'])) != row['before']:
            raise RuntimeError('Backup file hash differs: ' + row['path'])
    # Each live file must be exactly the parent or the T6.9 bytes; anything else
    # means someone changed the tree after install and a human must inspect it.
    for row in candidate['files']:
        current = ops.digest(safe_path(ops.ROOT, row['path']))
        if current not in (row['before'], row['after']):
            raise RuntimeError('Live file is neither parent nor T6.9: ' + row['path'])
    current = json.loads(safe_path(ops.ROOT, ops.MANIFEST).read_text()).get('release_fingerprint')
    if current not in (before['parent'], before['fingerprint']):
        raise RuntimeError('Live manifest is neither parent nor T6.9')
    services = tuple(ops.service_name(name) for name in before['services'])
    return backup, before, candidate, old, old_pin, services, current


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup', required=True, help='Run directory printed by t69_manual_install.py')
    parser.add_argument('--loop-id', help='Defaults to the loop recorded in the backup')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--allow-cancelled-loop', action='store_true')
    parser.add_argument('--allow-historical-closed-ledger', action='store_true')
    args = parser.parse_args(argv)
    if os.getuid() == 0:
        raise RuntimeError('Run as application user jack_shih')
    backup, before, candidate, old, old_pin, services, current = load(args.backup)
    loop_id = args.loop_id or before['loop_id']
    take = lambda: ops.snapshot(loop_id, allow_cancelled=args.allow_cancelled_loop,
                                allow_historical_closed=args.allow_historical_closed_ledger)
    guard = ops.guard_bytes()
    # Parent code cannot run a T6.9 selection; switch strategy in Telegram first.
    if T69 in (ops.selected_profile(), ops.selected_profile('prediction_pending_strategy')):
        raise RuntimeError('T6.9 is selected or pending; select the previous strategy in Telegram before rollback')
    state = take()
    ops.official_clear()
    if take() != state:
        raise RuntimeError('Trading records changed during preflight')
    if not args.apply:
        print(json.dumps(dict(status='ROLLBACK_PREFLIGHT_PASSED', current=current, restore_to=before['parent'],
                              files=len(candidate['files']), services=list(services), live_activated=False)))
        return
    for name in services:
        ops.service('stop', name)
    try:
        if take() != state:
            raise RuntimeError('Trading records changed after stopping services')
        ops.official_clear()
        for row in candidate['files']:
            dest = safe_path(ops.ROOT, row['path'])
            if row['before'] is None:
                dest.unlink(missing_ok=True)
                continue
            temp = dest.with_name(dest.name + '.t69-rollback')
            with temp.open('xb') as output:
                output.write((backup / row['path']).read_bytes())
            temp.chmod((backup / row['path']).stat().st_mode & 0o777)
            os.replace(temp, dest)
        safe_path(ops.ROOT, ops.MANIFEST).write_text((backup / ops.MANIFEST).read_text())
        safe_path(ops.ROOT, ops.PIN).write_text(old_pin)
        verify_release(ops.ROOT, old, pin_text=old_pin, expected_fingerprint=before['parent'])
        if (ops.ROOT / ops.GUARD).read_bytes() != guard or take() != state:
            raise RuntimeError('Guard or trading records changed during rollback')
    except BaseException:
        # A half-restored tree must not run; leave services stopped for a human.
        print('Rollback stopped before completion; services left stopped. Inspect ' + str(backup), file=sys.stderr)
        raise
    for name in reversed(services):
        ops.service('start', name)
    after = {name: ops.service_state(name) for name in services}
    result = dict(status='ROLLED_BACK_LIVE_NOT_ACTIVATED' if all(s['active'] == 'active' for s in after.values())
                  else 'ROLLED_BACK_SERVICE_NOT_ACTIVE', fingerprint=before['parent'], services=after,
                  selected_profile=ops.selected_profile(), at_ms=time.time_ns() // 1000000)
    (backup / 'rollback.json').write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps(result, ensure_ascii=False))
    if result['status'] != 'ROLLED_BACK_LIVE_NOT_ACTIVATED':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
