import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('t69_stage_build', ROOT / 'deploy/t69_stage_build.py')
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)
REF = '15c67fbad20d15aca3b6eddc12149db1f465f560'


def show(path):
    run = subprocess.run(['git', '-C', str(ROOT), 'show', f'{REF}:{path}'], capture_output=True)
    if run.returncode:
        pytest.skip('reviewed main commit not available in this clone')
    return run.stdout


def test_overlay_hashes_are_main_bytes():
    assert len(build.OVERLAY) == 18 and set(build.ADDED) <= set(build.OVERLAY)
    for path, want in build.OVERLAY.items():
        assert hashlib.sha256(show(path)).hexdigest() == want, path


def test_patch_appends_only_t69_paths_in_file_style():
    source = b"A = 1\n_REQUIRED_FIXED_RELEASE_PATHS = (\n    'x.py',\n    'y.py',\n)\n"
    patched = build.patch_release(source).decode()
    assert patched == ("A = 1\n_REQUIRED_FIXED_RELEASE_PATHS = (\n    'x.py',\n    'y.py',\n"
                       + ''.join(f"    '{p}',\n" for p in build.ADDED) + ")\n")


def test_refuses_before_writing_when_live_tree_is_not_parent(tmp_path, monkeypatch, capsys):
    (tmp_path / 'prediction').mkdir()
    (tmp_path / 'a.py').write_text('a\n')
    rows = [{'path': 'a.py', 'sha256': hashlib.sha256(b'a\n').hexdigest()}]
    (tmp_path / build.MANIFEST).write_text(json.dumps(
        {'schema': 'prediction-release-v1', 'files': rows, 'release_fingerprint': build.fingerprint(rows)}))
    monkeypatch.setattr('sys.argv', ['x', '--root', str(tmp_path), '--overlay', str(tmp_path),
                                     '--stage', 't69-release-staged-test'])
    with pytest.raises(SystemExit, match='expected parent'):
        build.main()
    assert not (tmp_path / 'prediction/t69-release-staged-test').exists()
