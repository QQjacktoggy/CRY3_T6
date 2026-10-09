"""T6.9b per-loop lane mask (PR #48, commit 5a792b4).

Builds a NEW stage for t69_manual_install.py. Twelve existing release files
change and two are added (regime_t69a_lane_mask.py, migration 029). The VM's
own release.py gets only the two new paths; every other byte is the VM's.

Writes only inside <root>/prediction/<stage> (must not exist). Never stops a
service and never touches the live release or any database. Usage:
  $PY -B t6u_stage_build.py --root /home/jack_shih/cry3 --overlay <dir> --stage t69-release-staged-t6u-v1-20261009
"""
import argparse, hashlib, json, runpy, shutil, sys
from pathlib import Path

PARENT = '8aeb73172852334f229ce566c92d1098a55d2083e56bf2460afc65a1d478f7aa'  # live since PR #44 (10-08)
RELEASE = 'src/gridbot/prediction/release.py'
MANIFEST, PIN = 'prediction/release-manifest.json', 'prediction/release-pin.env'
# Live VM bytes, read 10-09 (= main 15af26e for all but the VM's own release.py).
BEFORE = {
    'predict_main.py': 'dbe858077c2fd885b9e0f36e22c0b0e9bb50fde0d0da73b7d801d44f9c362e4f',
    'src/gridbot/prediction/controller.py': '50e2f351147e05594074ac3521b932b1c23ef101850dc059a2df26f248056bf5',
    'src/gridbot/prediction/loop_market.py': 'fd13f31529692fdae9c104d2d04c97b2ed59854a0a8cfcbaf4e6a704e6126cf0',
    'src/gridbot/prediction/loop_market_worker.py': 'f5a083e80f7e1ebea285b3462c08a3ad4c770d44483376cf77ae787d49cf615d',
    'src/gridbot/prediction/regime_live_ledger.py': 'fa4e54503724b2909aad394bff04487a371ee58857e6f89a6896907fe481cf63',
    'src/gridbot/prediction/regime_t69a_bridge.py': '5d41e64a14b077530019f231b332b92e14b04f2c9efc075ba3813a7996360f44',
    'src/gridbot/prediction/regime_t69a_report.py': '5329aedb65e3cd7cf8db2f168224540f5921ad223b235753a82ad185da744b0c',
    'src/gridbot/prediction/regime_worker_bridge.py': 'e27334661ab4bba86de1e6c2b81c541549475a579453ed4495382e321b91586c',
    'src/gridbot/prediction/release.py': 'eed8e223b091005a83068fc0d9ed30bed11a64420ea3c45cb5867cb04318137e',
    'src/gridbot/prediction/repository.py': 'cbd195d67c21d947f9e27d70f96c842a0201db921a439a3f819d558087548942',
    'src/gridbot/prediction/telegram.py': 'd81306ab57652c73035fe7e9541663cbc0d002cdb9c21e42a6638b34d1a294f1',
    'src/gridbot/prediction/worker.py': '5153260078df5e092d4d86855b032696bee90adb02c1d2f206fac488183c83b4',
}
# PR #48 commit 5a792b4 (release.py = live bytes + two paths).
AFTER = {
    'predict_main.py': 'a8391b199d1af197ac5accb9e2ee06006b98828970f8bfa26149e817d0ef58a8',
    'src/gridbot/prediction/controller.py': '7b962e40422960237cab638b51cfe21977c071de9d41fcd756b183d5dc70487d',
    'src/gridbot/prediction/loop_market.py': 'a362cb41892c05bb143d5c6f6fe24e370a7498481dbc9783b2ec3fed322e0430',
    'src/gridbot/prediction/loop_market_worker.py': '231e881c0916874f4b75ad496e400232c5649213fac3958696472bf25b3a7819',
    'src/gridbot/prediction/migrations/029_loop_lane_mask.sql': '7b47ea1b309fe7909046b0857bb1fcf91e0502b60f076cb5caa9a7f071ec95c4',
    'src/gridbot/prediction/regime_live_ledger.py': 'b7c1c8fe99d040180361264c403eef31fd98e290262a659e69a9640526c36760',
    'src/gridbot/prediction/regime_t69a_bridge.py': 'c60a7389816ff1422ef72cc1906c6e8306deffb70e2f81d80a433c76ab711d31',
    'src/gridbot/prediction/regime_t69a_lane_mask.py': '8ed16adb6351b76b4f5c6cef74f0353d6a71337acbf5304a4c874c95c3586884',
    'src/gridbot/prediction/regime_t69a_report.py': 'a26dfa1463284d7d2e2a706dd63dd7880f3dd2f202bb5823a928549fa1c6f314',
    'src/gridbot/prediction/regime_worker_bridge.py': '2fb4ed5abcfb1dedc20abe12d86805ab1a570e91ecdc2c04eaa0cbe1ad501d04',
    'src/gridbot/prediction/release.py': '69a37c5283ce01e71b7f91ee609e951aa0a7799a89f915a793b0d94da4bddef3',
    'src/gridbot/prediction/repository.py': 'ec6a048fb3f8c022f2848ad621b2d02e6f9542fcaf5481c76d006ec3acd479a6',
    'src/gridbot/prediction/telegram.py': 'baa73c809424a352b2aa7a063b33dadd1d139411ffa3ad339e244183f0d1f0a1',
    'src/gridbot/prediction/worker.py': '3ab26cd628841e19679ed8cc37dffe94812d30e341cbc77595cb0376df65191b',
}
NEW = ['src/gridbot/prediction/regime_t69a_lane_mask.py', 'src/gridbot/prediction/migrations/029_loop_lane_mask.sql']
EXPECTED = 'e8a460465d8234c531e1053d9eff9ce4d85ef164dfa06d537577a5cd9a02e4ef'
T69A_POLICY = 'c1aa56695e855de9120f19c11f48e346f1750d12464994fe9664aa4685693a45'  # unchanged


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
            sys.exit('live file is not the expected parent bytes: ' + path)
    for path in NEW:
        if path in listed or (root / path).exists():
            sys.exit('new path already exists on the live tree: ' + path)
    if sorted(AFTER) != sorted(list(BEFORE) + NEW):
        sys.exit('AFTER must be exactly BEFORE plus NEW')
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
        modes = {row['path']: (root / row['path']).stat().st_mode & 0o777 for row in live['files']}
        for path in AFTER:
            dest = stage / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes((overlay / path).read_bytes())
            if path in NEW:
                sibling = next(p for p in sorted(listed) if p.rsplit('/', 1)[0] == path.rsplit('/', 1)[0])
                dest.chmod(modes[sibling])  # same mode as a reviewed file in that directory
        # 3. Recompute with the stage's new release.py; no trading module is imported.
        rel = runpy.run_path(str(stage / RELEASE))
        manifest = rel['build_release_manifest'](stage)
        old = {r['path']: r['sha256'] for r in live['files']}
        new = {r['path']: r['sha256'] for r in manifest['files']}
        changed = sorted(p for p in new if new[p] != old.get(p))
        if set(new) != set(old) | set(NEW) or changed != sorted(AFTER):
            raise RuntimeError('unexpected manifest delta: ' + repr(changed))
        if manifest['release_fingerprint'] != EXPECTED:
            raise RuntimeError('stage fingerprint differs from the reviewed value: ' + manifest['release_fingerprint'])
        (stage / MANIFEST).write_text(json.dumps(manifest, indent=2) + '\n')
        (stage / PIN).write_text('PREDICTION_EXPECTED_RELEASE_FINGERPRINT=' + manifest['release_fingerprint'] + '\n')
        problems = rel['verify_release_manifest'](stage, manifest, pin_path=stage / PIN)
        if problems:
            raise RuntimeError('stage release.py rejects its manifest: ' + repr(problems))
        fp = manifest['release_fingerprint']
        candidate = {'version': '6.9.10', 'parent': a.parent, 'expected_fingerprint': fp,
                     'files': [{'path': p, 'before': old.get(p), 'after': new[p]} for p in changed]}
        (stage / 'candidate.json').write_text(json.dumps(candidate, indent=2) + '\n')
        validation = {'status': 'STAGED_VERIFIED_NOT_DEPLOYED', 'version': '6.9.10',
                      'source': 'PR #48 commit 5a792b4; VM release.py plus two reviewed paths',
                      'parent': a.parent, 'fingerprint': fp, 'changed': changed,
                      'policy_fingerprint': T69A_POLICY,
                      'tests': 'PR #48 5a792b4: 2562 passed, 1 skipped'}
        (stage / 'validation.json').write_text(json.dumps(validation, indent=2) + '\n')
    except BaseException:
        shutil.rmtree(stage)  # only the directory this run created
        raise
    print(json.dumps(dict(stage=str(stage), files=len(manifest['files']), parent=a.parent,
                          fingerprint=fp, changed=changed), indent=2))


if __name__ == '__main__':
    main()
