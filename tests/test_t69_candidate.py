import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import build_t69_candidate as cand  # noqa: E402

REL = 'src/gridbot/prediction/release.py'


def release(paths):
    body = ''.join(f'    "{p}",\n' for p in paths)
    return f'X = 1\n_REQUIRED_FIXED_RELEASE_PATHS = (\n{body})\n\n\ndef f():\n    return X\n'.encode()


def sha(b):
    return hashlib.sha256(b).hexdigest()


def manifest(files):
    entries = sorted(({'path': p, 'sha256': sha(c)} for p, c in files.items()), key=lambda r: r['path'])
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return {'schema': 'prediction-release-v1', 'files': entries, 'release_fingerprint': sha(canonical.encode())}


class Repo:
    def __init__(self, root):
        self.root = root
        subprocess.run(['git', 'init', '-q', str(root)], check=True)
        for key, value in (('user.email', 't@t'), ('user.name', 't')):
            subprocess.run(['git', '-C', str(root), 'config', key, value], check=True)

    def commit(self, files):
        for path, content in files.items():
            dest = self.root / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
        subprocess.run(['git', '-C', str(self.root), 'add', '-A'], check=True)
        subprocess.run(['git', '-C', str(self.root), 'commit', '-qm', 'c'], check=True)


@pytest.fixture
def setup(tmp_path):
    repo = Repo(tmp_path / 'repo')
    a, b = 'src/gridbot/prediction/a.py', 'src/gridbot/prediction/b.py'
    v1 = {REL: release([REL, a, b]), a: b'a1\n', b: b'b1\n'}
    repo.commit(v1)
    new, op = 'src/gridbot/prediction/regime_t69.py', 'operators/obs/engine.py'
    repo.commit({REL: release([REL, a, b, new, op]), a: b'a2\n', new: b'n\n', op: b'o\n'})
    return repo, v1, (a, b, new, op)


def test_overlay_fingerprint_matches_stage_build(setup):
    repo, vm, (a, b, new, op) = setup
    overlay, candidate, out, report = cand.build(repo.root, 'HEAD', manifest(vm), vm[REL])
    assert sorted(overlay) == sorted([REL, a, new, op]) and report['added'] == [new, op]
    assert overlay[REL].startswith(b'X = 1\n') and overlay[REL].endswith(b'    return X\n')
    stage = dict(vm, **overlay)  # the tree the VM session would build
    assert out == manifest(stage) and candidate['expected_fingerprint'] == out['release_fingerprint']
    assert candidate['parent'] == manifest(vm)['release_fingerprint']
    assert {r['path']: r['before'] for r in candidate['files']} == {
        REL: sha(vm[REL]), a: sha(vm[a]), new: None, op: None}


def test_keep_out_leaves_existing_vm_file_out_of_inventory(setup):
    repo, vm, (a, b, new, op) = setup
    overlay, candidate, out, report = cand.build(repo.root, 'HEAD', manifest(vm), vm[REL], keep_out=(op,))
    assert op not in overlay and op not in {r['path'] for r in out['files']}
    assert cand.inventory(overlay[REL])[0] == [REL, a, b, new]
    with pytest.raises(RuntimeError, match='keep-out'):
        cand.build(repo.root, 'HEAD', manifest(vm), vm[REL], keep_out=(a,))


def test_refuses_vm_bytes_missing_from_history(setup):
    repo, vm, (a, *_rest) = setup
    edited = dict(vm, **{a: b'local edit\n'})
    with pytest.raises(RuntimeError, match='unreviewed VM edit.*a.py'):
        cand.build(repo.root, 'HEAD', manifest(edited), vm[REL])


def test_refuses_inconsistent_vm_inputs(setup):
    repo, vm, _ = setup
    with pytest.raises(RuntimeError, match='differs from the VM manifest'):
        cand.build(repo.root, 'HEAD', manifest(vm), vm[REL] + b'#\n')
    short = {p: c for p, c in vm.items() if not p.endswith('b.py')}
    with pytest.raises(RuntimeError, match='inventory differs'):
        cand.build(repo.root, 'HEAD', manifest(short), vm[REL])


def test_vm_only_path_is_kept(setup, tmp_path):
    repo, vm, (a, b, new, op) = setup
    extra = 'src/gridbot/prediction/vm_only.py'
    vm2 = dict(vm, **{extra: b'v\n', REL: release([REL, a, b, extra])})
    overlay, candidate, out, report = cand.build(repo.root, 'HEAD', manifest(vm2), vm2[REL])
    assert report['vm_only'] == [extra] and extra not in overlay
    assert out == manifest(dict(vm2, **overlay))
