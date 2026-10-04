"""Operator-run T6.9 installer. Default is read-only; never arms, selects or starts a loop.

Same boundary as T6.8a: target loop finished, nothing RUNNING, official zero
positions/orders, no UNKNOWN. --apply backs up source/manifest/pin to a new
data-disk run directory, cold-reloads the services, verifies fingerprint,
services, the T6.9 report and Shadow schema, and rolls back automatically on
any failure. Trading databases are never modified or restored.
"""
import argparse
import json
import os
import runpy
import shutil
import sys
import time
from pathlib import Path

# The reviewed installer, helpers and verifier must be shipped together outside STAGE.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_verifier import approved_fingerprint, safe_path, verify_release, validate_candidate
import t69_ops as ops

# Like T6.7d: runs/<ms> beside this installer, i.e. operators/t69-YYYYMMDD/runs on the data disk.
BACKUP_ROOT = Path(__file__).resolve().parent / 'runs'
RELEASE = 'src/gridbot/prediction/release.py'
STAGE_PREFIX = 't69-release-staged-'


def stage_path(name):
    if not (isinstance(name, str) and name.startswith(STAGE_PREFIX)):
        raise ValueError('stage must be a prediction/' + STAGE_PREFIX + '* directory name')
    return safe_path(ops.ROOT, 'prediction/' + name)


def load(args):
    """Every check that needs no service, official API or write."""
    if os.getuid() == 0:
        raise RuntimeError('Run as application user jack_shih')
    stage = stage_path(args.stage)
    candidate = json.loads(safe_path(stage, 'candidate.json').read_text())
    validation = json.loads(safe_path(stage, 'validation.json').read_text())
    if validation.get('status') != 'STAGED_VERIFIED_NOT_DEPLOYED':
        raise RuntimeError('Stage is not STAGED_VERIFIED_NOT_DEPLOYED')
    old = json.loads(safe_path(ops.ROOT, ops.MANIFEST).read_text())
    # Install the stage manifest's own bytes (as T6.7d copies the file), not a re-serialization.
    new_manifest = safe_path(stage, ops.MANIFEST).read_bytes()
    new = json.loads(new_manifest)
    if not (old.get('release_fingerprint') == candidate.get('parent') == validation.get('parent')):
        raise RuntimeError('Parent changed; inspect before installing')
    # Same identity chain as T6.7d: stage manifest == validation == candidate == operator value.
    if not (new.get('release_fingerprint') == validation.get('fingerprint')
            == candidate.get('expected_fingerprint') == args.expected_fingerprint):
        raise RuntimeError('Release differs from operator-approved fingerprint')
    old_pin = safe_path(ops.ROOT, ops.PIN).read_text()
    new_pin = safe_path(stage, ops.PIN).read_text()
    old_bytes = verify_release(ops.ROOT, old, pin_text=old_pin, expected_fingerprint=candidate['parent'])
    new_bytes = verify_release(stage, new, pin_text=new_pin, expected_fingerprint=args.expected_fingerprint)
    validate_candidate(ops.ROOT, stage, candidate, old_bytes, new_bytes)
    for row in candidate['files']:
        if ops.digest(safe_path(ops.ROOT, row['path'])) != row['before']:
            raise RuntimeError('Deployed source changed: ' + row['path'])
        if ops.digest(safe_path(stage, row['path'])) != row['after']:
            raise RuntimeError('Candidate source changed: ' + row['path'])
    # Only after both trees' bytes match the approved hashes: each tree's own
    # release.py must also accept its manifest (T6.7d does the same).
    if runpy.run_path(str(safe_path(ops.ROOT, RELEASE)))['verify_release_manifest'](ops.ROOT, old, pin_path=ops.ROOT/ops.PIN):
        raise RuntimeError('Parent release.py rejects the live manifest')
    if runpy.run_path(str(safe_path(stage, RELEASE)))['verify_release_manifest'](stage, new, pin_path=stage/ops.PIN):
        raise RuntimeError('Candidate release.py rejects the stage manifest')
    modes = {row['path']: safe_path(stage, row['path']).stat().st_mode & 0o777 for row in candidate['files']}
    return dict(stage=stage, candidate=candidate, old=old, new=new, new_manifest=new_manifest, old_pin=old_pin, new_pin=new_pin,
                new_bytes=new_bytes, modes=modes)


def backup_dir(root):
    root = Path(root)
    if root.is_symlink():
        raise RuntimeError('Backup root must not be a symlink: ' + str(root))
    path = root / str(time.time_ns() // 1000000)
    path.mkdir(mode=0o700, parents=True)
    return path


def write_backup(backup, plan, before, services, args):
    for relative in [r['path'] for r in plan['candidate']['files'] if r['before'] is not None] + [ops.MANIFEST, ops.PIN]:
        dest = backup / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(safe_path(ops.ROOT, relative), dest)
    (backup / 'before.json').write_text(json.dumps(dict(
        snapshot=before, candidate=plan['candidate'], services=list(services),
        parent=plan['old']['release_fingerprint'], fingerprint=plan['new']['release_fingerprint'],
        stage=str(plan['stage']), loop_id=args.loop_id), indent=2, ensure_ascii=False) + '\n')
    here = Path(__file__).resolve().parent
    flags = ' --allow-cancelled-loop' if args.allow_cancelled_loop else ''
    flags += ' --allow-historical-closed-ledger' if args.allow_historical_closed_ledger else ''
    command = f'{ops.ROOT / ops.VENV_PYTHON} {here}/t69_rollback.py --backup {backup}{flags}'
    (backup / 'rollback.txt').write_text(
        '# Read-only rollback preflight, then the same command with --apply.\n'
        + command + '\n' + command + ' --apply\n')
    return command


def restore(backup, plan, services):
    """Best effort: every step is attempted even if an earlier one fails."""
    errors = []
    def attempt(label, fn):
        try:
            fn()
        except BaseException as exc:  # keep restoring; report all failures at the end
            errors.append(f'{label}: {type(exc).__name__}: {exc}')
    for name in services:
        attempt('stop ' + name, lambda name=name: ops.service('stop', name))
    stop_errors = len(errors)
    for row in plan['candidate']['files']:
        dest = safe_path(ops.ROOT, row['path'])
        if row['before'] is None:
            attempt('remove ' + row['path'], lambda dest=dest: dest.unlink(missing_ok=True))
        else:
            attempt('restore ' + row['path'], lambda row=row, dest=dest: shutil.copy2(backup / row['path'], dest))
        attempt('clean temp ' + row['path'], lambda dest=dest: dest.with_name(dest.name + '.t69-new').unlink(missing_ok=True))
    for relative in (ops.MANIFEST, ops.PIN):
        attempt('restore ' + relative, lambda relative=relative: shutil.copy2(backup / relative, safe_path(ops.ROOT, relative)))
    attempt('verify parent', lambda: verify_parent(plan))
    # Start only on a verified parent tree; a half-restored tree must not run.
    restored = len(errors) == stop_errors
    if restored:
        for name in reversed(services):
            attempt('start ' + name, lambda name=name: ops.service('start', name))
    return restored, errors


def verify_parent(plan):
    verify_release(ops.ROOT, plan['old'], pin_text=plan['old_pin'], expected_fingerprint=plan['old']['release_fingerprint'])
    if runpy.run_path(str(safe_path(ops.ROOT, RELEASE)))['verify_release_manifest'](ops.ROOT, plan['old'], pin_path=ops.ROOT/ops.PIN):
        raise RuntimeError('Parent release.py rejects the restored manifest')


def settled(services, after, seconds):
    """Catch a service that crashes a few seconds after start (T6.7c idle_health)."""
    time.sleep(seconds)
    later = {name: ops.service_state(name) for name in services}
    for name in services:
        if later[name]['active'] != 'active' or later[name]['main_pid'] != after[name]['main_pid'] \
                or later[name]['restarts'] != after[name]['restarts']:
            raise RuntimeError('Service did not stay up after start: ' + name)
    return later


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-fingerprint', required=True, type=approved_fingerprint,
                        help='SHA-256 approved out of band; never obtain this value from STAGE')
    parser.add_argument('--stage', required=True, help='prediction/' + STAGE_PREFIX + '* directory name')
    parser.add_argument('--loop-id', required=True, help='Last finished loop; must be DONE 100/100 or authorized CANCELLED')
    parser.add_argument('--backup-root', default=str(BACKUP_ROOT), help='Run backups go to <backup-root>/<ms>/ (default: runs/ beside this installer)')
    parser.add_argument('--extra-service', action='append', default=[], type=ops.service_name,
                        help='Additional cry3 user unit to cold-reload (for example ETH/BNB producers)')
    parser.add_argument('--settle-seconds', type=int, default=20, help='Wait before the post-start health re-check')
    parser.add_argument('--apply', action='store_true', help='Install code and cold-reload services; no Live activation')
    parser.add_argument('--allow-cancelled-loop', action='store_true')
    parser.add_argument('--allow-historical-closed-ledger', action='store_true')
    args = parser.parse_args(argv)
    ops.require_runtime()
    services = tuple(dict.fromkeys(ops.SERVICES + tuple(args.extra_service)))
    take = lambda: ops.snapshot(args.loop_id, allow_cancelled=args.allow_cancelled_loop,
                                allow_historical_closed=args.allow_historical_closed_ledger)
    plan = load(args)
    guard = ops.guard_bytes()
    ops.evidence_preflight()
    before = take()
    ops.official_clear()
    if take() != before:
        raise RuntimeError('Trading records changed during preflight')
    producers = ops.producer_units()
    missing = [name for name in producers if name not in services]
    if missing:
        # ETH/BNB producers import T6.9 code; leaving them on old code mixes versions.
        raise RuntimeError('Producer units must be reloaded too; add ' +
                           ' '.join('--extra-service ' + name for name in missing))
    states = {name: ops.service_state(name) for name in services}
    if not all(s['active'] == 'active' for s in states.values()):
        raise RuntimeError('A service is not active before install')
    if not args.apply:
        print(json.dumps(dict(status='READ_ONLY_PREFLIGHT_PASSED', candidate=plan['new']['release_fingerprint'],
                              parent=plan['old']['release_fingerprint'], files=len(plan['candidate']['files']),
                              services=list(services), producers=list(producers), live_activated=False)))
        return
    # Re-check the live parent right before taking the backup.
    verify_release(ops.ROOT, plan['old'], pin_text=plan['old_pin'], expected_fingerprint=plan['old']['release_fingerprint'])
    backup = backup_dir(args.backup_root)
    rollback_command = write_backup(backup, plan, before, services, args)
    ops.raise_on_hangup()
    try:
        for name in services:
            ops.service('stop', name)
        if take() != before:
            raise RuntimeError('Trading records changed after stopping services')
        ops.official_clear()
        ops.evidence_preflight()
        for row in plan['candidate']['files']:
            dest = safe_path(ops.ROOT, row['path'])
            temp = dest.with_name(dest.name + '.t69-new')
            dest.parent.mkdir(parents=True, exist_ok=True)
            temp.unlink(missing_ok=True)  # leftover from an interrupted earlier run
            # Install the immutable bytes verified before stopping services.
            with temp.open('xb') as output:
                output.write(plan['new_bytes'][row['path']])
            temp.chmod(plan['modes'][row['path']])
            os.replace(temp, dest)
        verify_release(ops.ROOT, plan['new'], pin_text=plan['new_pin'], expected_fingerprint=args.expected_fingerprint)
        deployed = runpy.run_path(str(safe_path(ops.ROOT, RELEASE)))
        if deployed['build_release_manifest'](ops.ROOT) != plan['new']:
            raise RuntimeError('Deployed tree does not rebuild the approved manifest')
        safe_path(ops.ROOT, ops.MANIFEST).write_bytes(plan['new_manifest'])
        safe_path(ops.ROOT, ops.PIN).write_text(plan['new_pin'])
        if deployed['verify_release_manifest'](ops.ROOT, plan['new'], pin_path=ops.ROOT/ops.PIN):
            raise RuntimeError('Deployed release.py rejects the installed manifest')
        if (ops.ROOT / ops.GUARD).read_bytes() != guard or take() != before:
            raise RuntimeError('Guard or trading records changed during install')
        for name in reversed(services):
            ops.service('start', name)
        after = {name: ops.service_state(name) for name in services}
        if not all(s['active'] == 'active' for s in after.values()):
            raise RuntimeError('A service is not active after install')
        if any(after[n]['main_pid'] in (None, '0', states[n]['main_pid']) for n in services):
            raise RuntimeError('A service was not cold-reloaded')
        after = settled(services, after, args.settle_seconds)
        fresh = ops.fresh_check()
        (backup / 'report.txt').write_text(fresh['report'])
        if take() != before or (ops.ROOT / ops.GUARD).read_bytes() != guard:
            raise RuntimeError('Guard or trading records changed after restart')
        result = dict(status='CODE_INSTALLED_LIVE_NOT_ACTIVATED', fingerprint=plan['new']['release_fingerprint'],
                      parent=plan['old']['release_fingerprint'], policy=fresh['policy'], backup=str(backup),
                      services_before=states, services_after=after, selected_profile=ops.selected_profile(),
                      t69_tables=ops.t69_tables(), rollback=rollback_command, services=list(services),
                      ledger_unchanged=True, guard_unchanged=True, report_rendered_not_sent=True, live_activated=False,
                      next_step='Operator selects T6.9, market and amount, then confirms Live in Telegram')
        for path in (backup / 'deployment.json', plan['stage'] / 'deployment.json'):
            path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
        print(json.dumps(result, ensure_ascii=False))
    except BaseException:
        ops.ignore_hangup()
        restored, errors = restore(backup, plan, services)
        if not restored:
            print('RESTORE INCOMPLETE; services left stopped. Inspect ' + str(backup) + ': ' + '; '.join(errors),
                  file=sys.stderr)
        else:
            print('Source/manifest/pin rolled back from ' + str(backup) + '; trading DB retained; Live not activated.'
                  + (' Warnings: ' + '; '.join(errors) if errors else ''), file=sys.stderr)
        raise


if __name__ == '__main__':
    main()
