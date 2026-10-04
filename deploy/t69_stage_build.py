"""T6.9 step 4: build a NEW candidate directory and recompute its fingerprint.

Writes only inside --stage (must not exist yet). Never stops services, never
touches the live release. Usage (run from anywhere, with the app venv python):
  $PY -B t69_stage_build.py --root /home/jack_shih/cry3 --overlay <dir> --stage <name>
"""
import argparse, ast, hashlib, json, os, runpy, shutil, sys
from pathlib import Path

PARENT = '71a19f2a95444521baaa08501fd27178533ec6cfb1199dcbe9fc38c319ba4330'
EXPECTED_FP = '5bbfbdb13981f0046b6844da76796a17a08b2dddf90c236666990c6c9452f672'
RELEASE = 'src/gridbot/prediction/release.py'
MANIFEST, PIN = 'prediction/release-manifest.json', 'prediction/release-pin.env'
ADDED = ['src/gridbot/prediction/regime_t69_policy.py', 'src/gridbot/prediction/regime_t69_bridge.py',
         'src/gridbot/prediction/regime_t69_shadow.py', 'src/gridbot/prediction/regime_t69_flat_shadow.py',
         'src/gridbot/prediction/regime_t69_reference.py', 'src/gridbot/prediction/regime_t69_report.py']
# sha256 of each overlay file at main 15c67fb (release.py: VM file plus the six ADDED lines).
OVERLAY = {
    'predict_main.py': 'eed9ab533772c577484742ba3038b1021099d006cb42d6f9c289d0075ec6c463',
    'src/gridbot/prediction/c180_signal_runtime.py': '844147fa9a05d5ce73290c0d562507d842833dfe11519680acf181a0b6012f1a',
    'src/gridbot/prediction/live_report.py': '747348cf75980fd4aafc63ebf640531df50d59f83494b7e66feaec8976a941de',
    'src/gridbot/prediction/loop_market.py': 'b2b8bcf447f3d561c3abfa3421d5f798c31b54dafb514346b7048a9591b39099',
    'src/gridbot/prediction/loop_market_worker.py': '162ee5905d29ee75f1313015756c6504419f30f68e2f08fc4275c3c40ebd07bc',
    'src/gridbot/prediction/regime_feature_service.py': '9ab71ecf6a5f7979ea4306801b5143480bf3f245c9528ed166e0413ea627ccf9',
    'src/gridbot/prediction/regime_live_ledger.py': '1b01c8cd43da0cdb9eabc5a24c5673187c7931cd62051a2c13942bb9ebaa2734',
    'src/gridbot/prediction/regime_t69_bridge.py': '8ca9e451ce20978337b26b6ca75db10430727f6e937ef570b3d684c9643fa0bc',
    'src/gridbot/prediction/regime_t69_flat_shadow.py': '2f47faa7605b761de994d1ba74182d609d9116f48d248d5caa32e4ee4e6c15d3',
    'src/gridbot/prediction/regime_t69_policy.py': 'be6321ac24b48778585670d1ad04ab84aae7d0592c57d0d11aa1b20f68303648',
    'src/gridbot/prediction/regime_t69_reference.py': 'a560960fb5dc02b17813d47d2eb3d22595a493f30bd6eb5e2f888bcfbb02295d',
    'src/gridbot/prediction/regime_t69_report.py': '702d5e6a50932fd3528f5aefb600f7c0176620e7bca1dab02eaaa3aac11f17d6',
    'src/gridbot/prediction/regime_t69_shadow.py': 'f676cb15beb925931d0ed74bc28ad49c7a8b5abf2235c35c530d7af4fbf206d9',
    'src/gridbot/prediction/regime_worker_bridge.py': 'c262d9fbd2095fbfa3120add459d9090759f2f230181731d9d8a398350726cf2',
    'src/gridbot/prediction/repository.py': 'cbd195d67c21d947f9e27d70f96c842a0201db921a439a3f819d558087548942',
    'src/gridbot/prediction/strategy.py': 'b2487af6dac3ed742d4cef9756796a20c6121331d1de47caab351cb97a733a2a',
    'src/gridbot/prediction/telegram.py': '3ad6c69e26a907f09dd8f24b20766f252a06de4ec2269a04d8f870dd0ea306f9',
    'src/gridbot/prediction/worker.py': '4bbbbb544aa1561893bea1f7bbb1effb20bcb732bf47d0f4b9c4e9a05ef60f28',
}
RELEASE_AFTER = '27e9ec823c8d688bf440225ba40f1b2e33a9ec99b56395731dc5376066a3f845'


def sha(b):
    return hashlib.sha256(b).hexdigest()


def fingerprint(entries):
    return sha(json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode())


def patch_release(source):
    text = source.decode()
    node, = [n for n in ast.parse(source).body if isinstance(n, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == '_REQUIRED_FIXED_RELEASE_PATHS' for t in n.targets)]
    newline = '\r\n' if '\r\n' in text else '\n'
    lines = text.split(newline)
    closing = node.end_lineno - 1
    last = lines[closing - 1]
    indent = last[:len(last) - len(last.lstrip())]
    quote = '"' if last.lstrip().startswith('"') else "'"
    lines[closing:closing] = [f'{indent}{quote}{p}{quote},' for p in ADDED]
    return newline.join(lines).encode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', required=True)
    ap.add_argument('--overlay', required=True, help='Directory extracted from the git archive tar')
    ap.add_argument('--stage', required=True, help='New directory name under <root>/prediction/')
    ap.add_argument('--parent', default=PARENT)
    ap.add_argument('--expected', default=EXPECTED_FP)
    a = ap.parse_args()
    root, overlay = Path(a.root).resolve(), Path(a.overlay).resolve()
    if not a.stage.startswith('t69-release-staged-') or '/' in a.stage:
        sys.exit('stage must be a plain t69-release-staged-* name')
    stage = root / 'prediction' / a.stage
    # 1. Read-only checks before writing anything.
    live = json.loads((root / MANIFEST).read_text())
    if live['release_fingerprint'] != a.parent or fingerprint(live['files']) != a.parent:
        sys.exit('live manifest is not the expected parent')
    for row in live['files']:
        if sha((root / row['path']).read_bytes()) != row['sha256']:
            sys.exit('live file differs from live manifest: ' + row['path'])
    for path, want in OVERLAY.items():
        if sha((overlay / path).read_bytes()) != want:
            sys.exit('overlay file hash mismatch: ' + path)
    if any((root / p).exists() for p in ADDED):
        sys.exit('a T6.9 module already exists in the live tree')
    if stage.exists():
        sys.exit('stage already exists: ' + str(stage))
    # 2. Writes, all inside the new stage directory.
    stage.mkdir()
    try:
        for row in live['files']:
            dest = stage / row['path']
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / row['path'], dest)
        for path in OVERLAY:
            dest = stage / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes((overlay / path).read_bytes())
            mode = (root / path).stat().st_mode & 0o777 if (root / path).exists() else 0o644
            dest.chmod(mode)  # keep the live file's mode
        patched = patch_release((root / RELEASE).read_bytes())
        if sha(patched) != RELEASE_AFTER:
            raise RuntimeError('patched release.py hash mismatch')
        (stage / RELEASE).write_bytes(patched)
        (stage / RELEASE).chmod((root / RELEASE).stat().st_mode & 0o777)
        # 3. Recompute with the stage's own release.py (no stage module is imported).
        rel = runpy.run_path(str(stage / RELEASE))
        manifest = rel['build_release_manifest'](stage)
        (stage / 'prediction').mkdir(exist_ok=True)
        (stage / MANIFEST).write_text(json.dumps(manifest, indent=2) + '\n')
        (stage / PIN).write_text('PREDICTION_EXPECTED_RELEASE_FINGERPRINT=' + manifest['release_fingerprint'] + '\n')
        problems = rel['verify_release_manifest'](stage, manifest, pin_path=stage / PIN)
        if problems:
            raise RuntimeError('stage release.py rejects its manifest: ' + repr(problems))
        old = {r['path']: r['sha256'] for r in live['files']}
        new = {r['path']: r['sha256'] for r in manifest['files']}
        changed = sorted(p for p in new if new[p] != old.get(p))
        candidate = {'version': '6.9.2', 'parent': a.parent, 'expected_fingerprint': manifest['release_fingerprint'],
                     'files': [{'path': p, 'before': old.get(p), 'after': new[p]} for p in changed]}
        (stage / 'candidate.json').write_text(json.dumps(candidate, indent=2) + '\n')
    except BaseException:
        shutil.rmtree(stage)  # only the directory this run created
        raise
    result = dict(stage=str(stage), files=len(manifest['files']), fingerprint=manifest['release_fingerprint'],
                  matches_cloud=manifest['release_fingerprint'] == a.expected, changed=len(changed),
                  manifest_sha256=sha((stage / MANIFEST).read_bytes()),
                  candidate_sha256=sha((stage / 'candidate.json').read_bytes()),
                  pin_sha256=sha((stage / PIN).read_bytes()))
    print(json.dumps(result, indent=2))
    if not result['matches_cloud']:
        sys.exit(1)


if __name__ == '__main__':
    main()
