"""Build a hash-checked overlay while preserving the VM's full release inventory."""
import argparse
import hashlib
import json
import tarfile
from pathlib import Path

CHANGED = ('worker.py', 'strategy.py', 'telegram.py', 'repository.py', 'regime_feature_service.py',
           'regime_live_ledger.py', 'regime_worker_bridge.py', 'live_report.py', 'release.py',
           'regime_t65_lane.py', 'regime_t65_bridge.py', 'regime_t65_shadow.py')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--vm-release', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    out = Path(args.output)
    stage = out/'overlay'
    baseline = json.loads((root/'docs/vm-source-baseline.json').read_text())
    old_release = Path(args.vm_release).read_bytes()
    release_path = 'src/gridbot/prediction/release.py'
    assert hashlib.sha256(old_release).hexdigest() == baseline['vm_source_hashes'][release_path]
    text = old_release.decode()
    marker = '    "src/gridbot/prediction/regime_t63b_risk.py",'
    assert text.count(marker) == 1
    newline = '\r\n' if '\r\n' in text else '\n'
    text = text.replace(marker, marker + newline + newline.join(
        f'    "src/gridbot/prediction/{file}",' for file in CHANGED if file.startswith('regime_t65')))
    manifest = {'version': '6.5', 'parent_release': baseline['vm_release_fingerprints']['release-manifest.json'], 'files': []}
    for file in CHANGED:
        relative = 'src/gridbot/prediction/'+file
        content = text.encode() if file == 'release.py' else (root/relative).read_bytes()
        dest = stage/relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        manifest['files'].append({'path': relative, 'before': baseline['vm_source_hashes'].get(relative),
                                  'after': hashlib.sha256(content).hexdigest()})
    (stage/'candidate-manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    with tarfile.open(out/'t65-overlay.tar.gz', 'w:gz') as archive:
        for path in sorted(stage.rglob('*')):
            if path.is_file():
                archive.add(path, arcname=path.relative_to(stage))
    print(json.dumps({'bundle': str(out/'t65-overlay.tar.gz'), 'files': len(manifest['files'])}))


if __name__ == '__main__':
    main()
