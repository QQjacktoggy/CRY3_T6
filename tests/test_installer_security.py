"""Adversarial, offline installer boundaries, including Python optimization."""
import copy
import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from deploy.release_verifier import safe_path, verify_release

ROOT = Path(__file__).resolve().parents[1]
RELEASE = 'src/gridbot/prediction/release.py'
INSTALLERS = ('t67', 't67a', 't67b', 't67c')


def manifest(base, files):
    entries = []
    for relative, data in sorted(files.items()):
        path = base / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        entries.append({'path': relative, 'sha256': hashlib.sha256(data).hexdigest()})
    fingerprint = hashlib.sha256(json.dumps(entries, ensure_ascii=False, sort_keys=True,
                                           separators=(',', ':')).encode()).hexdigest()
    result = {'schema': 'prediction-release-v1', 'files': entries, 'release_fingerprint': fingerprint}
    (base / 'prediction').mkdir(exist_ok=True)
    (base / 'prediction/release-manifest.json').write_text(json.dumps(result))
    (base / 'prediction/release-pin.env').write_text('PREDICTION_EXPECTED_RELEASE_FINGERPRINT=' + fingerprint)
    return result


@pytest.fixture
def candidate(tmp_path):
    root, stage = tmp_path / 'root', tmp_path / 'stage'
    inventory = (f'_REQUIRED_FIXED_RELEASE_PATHS = ({RELEASE!r}, "source.py")\n'
                 "raise RuntimeError('verified module must never execute')\n").encode()
    old = manifest(root, {RELEASE: inventory, 'source.py': b'old'})
    new = manifest(stage, {RELEASE: inventory, 'source.py': b'new'})
    plan = {'parent': old['release_fingerprint'], 'files': [{
        'path': 'source.py', 'before': hashlib.sha256(b'old').hexdigest(),
        'after': hashlib.sha256(b'new').hexdigest()}]}
    (stage / 'candidate.json').write_text(json.dumps(plan))
    (stage / 'validation.json').write_text(json.dumps({
        'status': 'STAGED_VERIFIED_NOT_DEPLOYED', 'parent': old['release_fingerprint'],
        'fingerprint': new['release_fingerprint']}))
    (root / 'prediction/hs-recovery-startup.env').write_text(
        'PREDICTION_LIVE_ARM_ON_START=false\nPREDICTION_AUTO_START_LOOP=false\n')
    return root, stage, plan


def installer(name, optimize, candidate, monkeypatch, apply=False):
    path = ROOT / 'deploy' / f'{name}_manual_install.py'
    namespace = {'__name__': 'installer_test', '__file__': str(path)}
    exec(compile(path.read_text(), str(path), 'exec', optimize=optimize), namespace)
    root, stage, _ = candidate
    namespace.update(ROOT=root, STAGE=stage, snapshot=Mock(return_value={'fixed': True}),
                     official_clear=Mock(), service=Mock(return_value='active'))
    monkeypatch.setattr(namespace['os'], 'getuid', lambda: 1000)
    monkeypatch.setattr(sys, 'argv', [str(path)] + (['--apply'] if apply else []))
    return namespace


@pytest.mark.parametrize('name', INSTALLERS)
@pytest.mark.parametrize('optimize', [0, 2])
@pytest.mark.parametrize('apply', [False, True])
def test_tampered_verifier_never_executes_even_in_preflight(candidate, tmp_path, monkeypatch, name, optimize, apply):
    root, stage, _ = candidate
    marker = tmp_path / 'executed'
    (stage / RELEASE).write_text(f'from pathlib import Path\nPath({str(marker)!r}).touch()\n')
    ns = installer(name, optimize, candidate, monkeypatch, apply)
    with pytest.raises(RuntimeError, match='hash mismatch'):
        ns['main']()
    assert not marker.exists()
    ns['service'].assert_not_called()
    ns['official_clear'].assert_not_called()
    assert (root / 'source.py').read_bytes() == b'old'


@pytest.mark.parametrize('name', INSTALLERS)
@pytest.mark.parametrize('attack', ['extra', 'absolute', 'traversal', 'duplicate', 'alias', 'symlink', 'parent_symlink', 'omitted', 'wrong_hash'])
def test_candidate_rejected_before_service_or_official_reads(candidate, tmp_path, monkeypatch, name, attack):
    root, stage, plan = candidate
    row = copy.deepcopy(plan['files'][0])
    if attack == 'extra':
        row['path'] = 'unverified.py'
        (stage / row['path']).write_bytes(b'new')
        row['before'] = None
        plan['files'].append(row)
    elif attack == 'absolute':
        row['path'] = str(tmp_path / 'escape.py')
        plan['files'].append(row)
    elif attack == 'traversal':
        row['path'] = '../escape.py'
        plan['files'].append(row)
    elif attack == 'duplicate':
        plan['files'].append(row)
    elif attack == 'alias':
        row['path'] = './source.py'
        plan['files'] = [row]
    elif attack == 'symlink':
        (root / 'source.py').unlink()
        target = tmp_path / 'target.py'
        target.write_bytes(b'old')
        (root / 'source.py').symlink_to(target)
    elif attack == 'parent_symlink':
        # Even an in-root symlink alias is rejected, preventing destination traversal.
        (stage / 'alias').symlink_to(stage, target_is_directory=True)
        row['path'] = 'alias/source.py'
        plan['files'].append(row)
    elif attack == 'omitted':
        plan['files'] = []
    elif attack == 'wrong_hash':
        plan['files'][0]['after'] = '0' * 64
    (stage / 'candidate.json').write_text(json.dumps(plan))
    ns = installer(name, 2, candidate, monkeypatch, apply=True)
    with pytest.raises(RuntimeError):
        ns['main']()
    ns['service'].assert_not_called()
    ns['official_clear'].assert_not_called()
    assert not list((root / 'prediction').glob('*rollback*'))


@pytest.mark.parametrize('name', INSTALLERS)
@pytest.mark.parametrize('optimize', [0, 2])
def test_valid_preflight_never_runs_even_verified_python(candidate, tmp_path, monkeypatch, name, optimize):
    root, stage, _ = candidate
    ns = installer(name, optimize, candidate, monkeypatch)
    ns['main']()
    ns['official_clear'].assert_called_once()
    assert all(call.args[0] == 'is-active' for call in ns['service'].call_args_list)
    assert (root / 'source.py').read_bytes() == b'old'


@pytest.mark.parametrize('relative', ['', '/etc/passwd', '../file', 'a/../file', './file', 'a//file', 'C:/file', 'a\\file'])
def test_canonical_paths_required(tmp_path, relative):
    with pytest.raises(RuntimeError):
        safe_path(tmp_path, relative)


@pytest.mark.parametrize('optimize', [0, 2])
def test_legacy_builder_rejects_untrusted_release_hash_under_optimization(tmp_path, monkeypatch, optimize):
    script = ROOT / 'scripts/build_t65_vm_overlay.py'
    root = tmp_path / 'repo'
    (root / 'docs').mkdir(parents=True)
    (root / 'docs/vm-source-baseline.json').write_text(json.dumps({
        'vm_source_hashes': {RELEASE: '0' * 64}}))
    release = tmp_path / 'release.py'
    release.write_text('untrusted')
    output = tmp_path / 'output'
    monkeypatch.setattr(sys, 'argv', [str(script), '--vm-release', str(release), '--output', str(output)])
    ns = {'__name__': 'builder_test', '__file__': str(root / 'scripts/build_t65_vm_overlay.py')}
    exec(compile(script.read_text(), str(script), 'exec', optimize=optimize), ns)
    with pytest.raises(RuntimeError):
        ns['main']()
    assert not output.exists()


@pytest.mark.parametrize('name', INSTALLERS)
def test_apply_installs_verified_bytes_even_if_stage_changes_after_preflight(candidate, monkeypatch, name):
    root, stage, _ = candidate
    (stage / 'source.py').chmod(0o750)
    ns = installer(name, 2, candidate, monkeypatch, apply=True)
    def mutate_stage():
        (stage / 'source.py').write_bytes(b'tampered after verification')
        (stage / 'prediction/release-pin.env').write_text('tampered')
    ns['official_clear'].side_effect = mutate_stage
    report = Mock(return_value=Mock(returncode=0, stdout='offline mocked report'))
    monkeypatch.setattr(ns['subprocess'], 'run', report)
    ns['main']()
    assert (root / 'source.py').read_bytes() == b'new'
    assert (root / 'source.py').stat().st_mode & 0o777 == 0o750
    deployed = json.loads((root / 'prediction/release-manifest.json').read_text())
    verify_release(root, deployed, pin_text=(root / 'prediction/release-pin.env').read_text())
    assert [call.args[0] for call in ns['service'].call_args_list].count('stop') == 3


@pytest.mark.parametrize('optimize', [0, 2])
def test_legacy_snapshot_stop_check_survives_optimization(candidate, monkeypatch, optimize):
    import sqlite3
    root, _, _ = candidate
    db_path = root / 'prediction/data/prediction.sqlite3'
    db_path.parent.mkdir()
    with sqlite3.connect(db_path) as db:
        db.execute('CREATE TABLE prediction_loops(loop_id TEXT,state TEXT,completed INTEGER,target INTEGER,new_entries_stopped INTEGER)')
        db.execute("INSERT INTO prediction_loops VALUES('target','RUNNING',0,100,0)")
    path = ROOT / 'deploy/t67_manual_install.py'
    ns = {'__name__': 'snapshot_test', '__file__': str(path)}
    exec(compile(path.read_text(), str(path), 'exec', optimize=optimize), ns)
    ns['ROOT'] = root
    with pytest.raises(RuntimeError, match='Target loop'):
        ns['snapshot']('target')


@pytest.mark.parametrize('attack', ['duplicate', 'traversal', 'absolute', 'alias', 'symlink'])
def test_runtime_release_manifest_rejects_unsafe_paths(candidate, tmp_path, monkeypatch, attack):
    from src.gridbot.prediction import release
    root, _, _ = candidate
    current = json.loads((root / 'prediction/release-manifest.json').read_text())
    monkeypatch.setattr(release, '_REQUIRED_FIXED_RELEASE_PATHS', (RELEASE, 'source.py'))
    row = next(row for row in current['files'] if row['path'] == 'source.py')
    if attack == 'duplicate':
        current['files'].append(row.copy())
    elif attack == 'traversal':
        row['path'] = 'src/../source.py'
    elif attack == 'absolute':
        row['path'] = str(root / 'source.py')
    elif attack == 'alias':
        row['path'] = './source.py'
    else:
        (root / 'source.py').unlink()
        outside = tmp_path / 'outside.py'
        outside.write_bytes(b'old')
        (root / 'source.py').symlink_to(outside)
    reasons = release.verify_release_manifest(root, current, expected_fingerprint=current['release_fingerprint'])
    assert any('duplicate' in reason or 'path' in reason for reason in reasons)
