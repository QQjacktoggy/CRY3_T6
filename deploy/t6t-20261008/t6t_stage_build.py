"""T6.9a: continuation Original off + Flat F1-F4 Shadow retired (PR #44, commit 5511f0a).

Builds a NEW stage for t69_manual_install.py. Four existing files change; no file
is added or removed, so the VM's own release.py is copied unchanged.

Writes only inside <root>/prediction/<stage> (must not exist). Never stops a
service and never touches the live release or any database. Usage:
  $PY -B t6t_stage_build.py --root /home/jack_shih/cry3 --overlay <dir> --stage t69-release-staged-t6t-v1-20261008
"""
import argparse, hashlib, json, runpy, shutil, sys
from pathlib import Path

PARENT = 'f1aed6580bd5ab2d1eedb7baec59d96306fb69ffea2ed4e789cf4166f67afdb8'  # live since 10-08 00:06 (PR #42)
RELEASE = 'src/gridbot/prediction/release.py'
MANIFEST, PIN = 'prediction/release-manifest.json', 'prediction/release-pin.env'
P = 'src/gridbot/prediction/'
# sha256 at main 3426e10 (= live VM, read 10-08) and at PR #44 (new).
BEFORE = {
    P+'regime_t69a_policy.py': 'd627021479eef6ecd9532e34b532dd8448a9dc30739a201d0472517a5214fd96',
    P+'regime_t69a_bridge.py': 'd890155591d627cad2381c7f31ff48e25df0ec9fd30e611f4c59743b1574df17',
    P+'regime_t69a_report.py': 'cd3804c747b34da753d6619b93a5a877eff30e7c6666442143ebab1d9572b512',
    P+'regime_t69a_shadow.py': '385d08f561deaf48425a0d67f2f0f8c1d26091eeee9cdac97b209fd8479108c7',
}
AFTER = {
    P+'regime_t69a_policy.py': '93621cbb83a8141ca1bdabff1a57ac1c60bafedaa1741a7cd50cfc7cb5463d47',
    P+'regime_t69a_bridge.py': '5d41e64a14b077530019f231b332b92e14b04f2c9efc075ba3813a7996360f44',
    P+'regime_t69a_report.py': '5329aedb65e3cd7cf8db2f168224540f5921ad223b235753a82ad185da744b0c',
    P+'regime_t69a_shadow.py': '36c735367c907f9e7881b34c69779801d3de12eda9de43de482b4f474a668792',
}


def sha(b):
    return hashlib.sha256(b).hexdigest()


def fingerprint(entries):
    return sha(json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', required=True)
    ap.add_argument('--overlay', required=True)
    ap.add_argument('--stage', required=True)
    ap.add_argument('--parent', default=PARENT)
    a = ap.parse_args()
    root, overlay = Path(a.root).resolve(), Path(a.overlay).resolve()
    if not a.stage.startswith('t69-release-staged-') or '/' in a.stage:
        sys.exit('stage must be a plain t69-release-staged-* name')
    stage = root / 'prediction' / a.stage
    # 1. Read-only checks before writing anything.
    live = json.loads((root / MANIFEST).read_text())
    if live['release_fingerprint'] != a.parent or fingerprint(live['files']) != a.parent:
        sys.exit('live manifest is not the expected parent ' + a.parent)
    for row in live['files']:
        if sha((root / row['path']).read_bytes()) != row['sha256']:
            sys.exit('live file differs from live manifest: ' + row['path'])
    listed = {row['path'] for row in live['files']}
    for path, want in BEFORE.items():
        if path not in listed:
            sys.exit('not in live manifest: ' + path)
        if sha((root / path).read_bytes()) != want:
            sys.exit('live file is not main 3426e10: ' + path)
    for path, want in AFTER.items():
        if sha((overlay / path).read_bytes()) != want:
            sys.exit('overlay file hash mismatch: ' + path)
    if stage.exists():
        sys.exit('stage already exists: ' + str(stage))
    # 2. Writes, all inside the new stage directory.
    stage.mkdir()
    try:
        for row in live['files']:
            dest = stage / row['path']
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / row['path'], dest)  # keeps the live mode
        for path in AFTER:
            (stage / path).write_bytes((overlay / path).read_bytes())
        # 3. Recompute with the stage's (unchanged) release.py; no trading module is imported.
        rel = runpy.run_path(str(stage / RELEASE))
        manifest = rel['build_release_manifest'](stage)
        old = {r['path']: r['sha256'] for r in live['files']}
        new = {r['path']: r['sha256'] for r in manifest['files']}
        changed = sorted(p for p in new if new[p] != old.get(p))
        if set(new) != set(old) or changed != sorted(AFTER):
            raise RuntimeError('unexpected manifest delta: ' + repr(changed))
        (stage / MANIFEST).write_text(json.dumps(manifest, indent=2) + '\n')
        (stage / PIN).write_text('PREDICTION_EXPECTED_RELEASE_FINGERPRINT=' + manifest['release_fingerprint'] + '\n')
        problems = rel['verify_release_manifest'](stage, manifest, pin_path=stage / PIN)
        if problems:
            raise RuntimeError('stage release.py rejects its manifest: ' + repr(problems))
        fp = manifest['release_fingerprint']
        candidate = {'version': '6.9.9', 'parent': a.parent, 'expected_fingerprint': fp,
                     'files': [{'path': p, 'before': old.get(p), 'after': new[p]} for p in changed]}
        (stage / 'candidate.json').write_text(json.dumps(candidate, indent=2) + '\n')
        validation = {'status': 'STAGED_VERIFIED_NOT_DEPLOYED', 'version': '6.9.9',
                      'source': 'PR #44 commit 5511f0a; VM release.py unchanged',
                      'parent': a.parent, 'fingerprint': fp, 'changed': changed,
                      'policy_fingerprint': 'c1aa56695e855de9120f19c11f48e346f1750d12464994fe9664aa4685693a45',
                      'tests': 'PR #44 5511f0a: 2400 passed, 1 skipped'}
        (stage / 'validation.json').write_text(json.dumps(validation, indent=2) + '\n')
    except BaseException:
        shutil.rmtree(stage)  # only the directory this run created
        raise
    print(json.dumps(dict(stage=str(stage), files=len(manifest['files']), parent=a.parent,
                          fingerprint=fp, changed=changed), indent=2))


if __name__ == '__main__':
    main()
