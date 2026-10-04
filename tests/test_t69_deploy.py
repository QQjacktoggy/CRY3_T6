"""T6.9 operator installer, rollback and verifier stay offline here; no VM is touched."""
import hashlib
import importlib.util
import json
import subprocess
import sys
from itertools import count
from pathlib import Path
from unittest.mock import Mock

import pytest

DEPLOY = Path(__file__).resolve().parents[1] / 'deploy'
RELEASE = 'src/gridbot/prediction/release.py'
SOURCE = 'src/gridbot/prediction/regime_t69_policy.py'
ADDED = 'src/gridbot/prediction/regime_t69_new.py'
STAGE = 't69-release-staged-v1-test'


def module(name):
    spec = importlib.util.spec_from_file_location(name, DEPLOY / f'{name}.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


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


RELEASE_CODE = """
import hashlib, json
from pathlib import Path
def build_release_manifest(root):
    entries = [{'path': p, 'sha256': hashlib.sha256((Path(root) / p).read_bytes()).hexdigest()}
               for p in sorted(_REQUIRED_FIXED_RELEASE_PATHS)]
    fingerprint = hashlib.sha256(json.dumps(entries, ensure_ascii=False, sort_keys=True,
                                            separators=(',', ':')).encode()).hexdigest()
    return {'schema': 'prediction-release-v1', 'files': entries, 'release_fingerprint': fingerprint}
def verify_release_manifest(root, manifest, pin_path):
    return [] if build_release_manifest(root) == manifest else ['mismatch']
"""


def inventory(*paths):
    return (f'_REQUIRED_FIXED_RELEASE_PATHS = {tuple(paths)!r}\n' + RELEASE_CODE).encode()


class Env:
    def __init__(self, tmp_path, monkeypatch):
        self.root = tmp_path / 'root'
        self.stage = self.root / 'prediction' / STAGE
        self.backups = tmp_path / 'data/operators/t69/runs'
        self.backups.mkdir(parents=True)
        old = manifest(self.root, {RELEASE: inventory(RELEASE, SOURCE), SOURCE: b'parent'})
        new = manifest(self.stage, {RELEASE: inventory(RELEASE, SOURCE, ADDED), SOURCE: b't69', ADDED: b'added'})
        self.old, self.new = old['release_fingerprint'], new['release_fingerprint']
        sha = lambda data: hashlib.sha256(data).hexdigest()
        files = [{'path': RELEASE, 'before': sha(inventory(RELEASE, SOURCE)), 'after': sha(inventory(RELEASE, SOURCE, ADDED))},
                 {'path': SOURCE, 'before': sha(b'parent'), 'after': sha(b't69')},
                 {'path': ADDED, 'before': None, 'after': sha(b'added')}]
        (self.stage / 'candidate.json').write_text(json.dumps({'parent': self.old, 'expected_fingerprint': self.new, 'files': files}))
        (self.stage / 'validation.json').write_text(json.dumps({
            'status': 'STAGED_VERIFIED_NOT_DEPLOYED', 'parent': self.old, 'fingerprint': self.new}))
        (self.root / 'prediction/hs-recovery-startup.env').write_text(
            'PREDICTION_LIVE_ARM_ON_START=false\nPREDICTION_AUTO_START_LOOP=false\n')
        self.installer = module('t69_manual_install')
        self.rollback = module('t69_rollback')
        self.verify = module('t69_verify')
        self.ops = sys.modules['t69_ops']
        pids = count(100)
        self.pid = {}
        def state(name):
            return dict(active='active', main_pid=self.pid.setdefault(name, str(next(pids))), restarts='0')
        def service(action, name):
            if action == 'start':
                self.pid.pop(name, None)
            return 'active'
        self.profile = {'prediction_selected_strategy': 'regime_target6_8a_v1'}
        self.mocks = dict(
            ROOT=self.root, snapshot=Mock(return_value={'fixed': True}), official_clear=Mock(),
            evidence_preflight=Mock(), service=Mock(side_effect=service), service_state=Mock(side_effect=state),
            fresh_check=Mock(return_value={'policy': self.ops.POLICY_FINGERPRINT, 'live': 8, 'shadow': 6,
                                           'report': '📊 T6.9 Report｜八路 Live＋六路 Shadow\nmock'}),
            t69_tables=Mock(return_value={'BTCUSDT': {'database': 'present', 'tables': []}}),
            selected_profile=Mock(side_effect=lambda key='prediction_selected_strategy': self.profile.get(key)))
        for key, value in self.mocks.items():
            monkeypatch.setattr(self.ops, key, value)
        monkeypatch.setattr(self.installer.os, 'getuid', lambda: 1000)

    def install(self, *extra, fingerprint=None):
        self.installer.main(['--expected-fingerprint', fingerprint or self.new, '--stage', STAGE,
                             '--loop-id', 'loop:done', '--backup-root', str(self.backups), *extra])

    def actions(self):
        return [c.args[0] for c in self.mocks['service'].call_args_list]

    def backup(self):
        (run,) = self.backups.iterdir()
        return run

    def live_release(self):
        from deploy.release_verifier import verify_release
        manifest = json.loads((self.root / 'prediction/release-manifest.json').read_text())
        verify_release(self.root, manifest, pin_text=(self.root / 'prediction/release-pin.env').read_text())
        return manifest['release_fingerprint']


@pytest.fixture
def env(tmp_path, monkeypatch):
    return Env(tmp_path, monkeypatch)


def test_default_is_read_only_preflight(env, capsys):
    env.install()
    assert json.loads(capsys.readouterr().out)['status'] == 'READ_ONLY_PREFLIGHT_PASSED'
    env.mocks['official_clear'].assert_called_once()
    assert env.actions() == []  # only state reads; no stop, start or restart
    assert env.live_release() == env.old and not list(env.backups.iterdir())


def test_apply_installs_backs_up_and_never_activates(env, capsys):
    env.install('--apply')
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'CODE_INSTALLED_LIVE_NOT_ACTIVATED' and result['fingerprint'] == env.new
    assert env.live_release() == env.new and (env.root / ADDED).read_bytes() == b'added'
    assert env.actions().count('stop') == 3 and env.actions().count('start') == 3
    assert not {'enable', 'restart'} & set(env.actions())
    run = env.backup()
    assert (run / SOURCE).read_bytes() == b'parent' and not (run / ADDED).exists()
    assert json.loads((run / 'prediction/release-manifest.json').read_text())['release_fingerprint'] == env.old
    before = json.loads((run / 'before.json').read_text())
    assert (before['parent'], before['fingerprint'], before['loop_id']) == (env.old, env.new, 'loop:done')
    assert 'T6.9 Report' in (run / 'report.txt').read_text()
    assert json.loads((run / 'deployment.json').read_text())['backup'] == str(run)
    stage = json.loads((env.stage / 'deployment.json').read_text())
    assert stage['live_activated'] is False and stage['ledger_unchanged'] and stage['guard_unchanged']
    assert 't69_rollback.py --backup ' + str(run) in (run / 'rollback.txt').read_text()


def test_extra_services_are_cold_reloaded(env):
    env.install('--apply', '--extra-service', 'cry3-t67c-feature-ethusdt.service')
    assert env.actions().count('stop') == 4
    with pytest.raises(SystemExit):
        env.install('--extra-service', '../evil.service')


@pytest.mark.parametrize('failure', ['fresh', 'warm_reload', 'records'])
def test_failed_apply_restores_parent_and_restarts(env, failure):
    if failure == 'fresh':
        env.mocks['fresh_check'].side_effect = RuntimeError('Fresh deployed T6.9 import/report failed')
    elif failure == 'warm_reload':
        env.mocks['service'].side_effect = lambda action, name: 'active'
    else:
        env.mocks['snapshot'].side_effect = [{'fixed': True}] * 3 + [{'changed': True}]
    with pytest.raises(RuntimeError):
        env.install('--apply')
    assert env.live_release() == env.old
    assert (env.root / SOURCE).read_bytes() == b'parent' and not (env.root / ADDED).exists()
    assert env.actions()[-3:] == ['start'] * 3
    assert not (env.backup() / 'deployment.json').exists()


@pytest.mark.parametrize('case', ['running', 'fingerprint', 'stage', 'guard', 'validation', 'tampered_stage', 'candidate_fingerprint'])
def test_unsafe_boundaries_refused_before_services(env, case):
    kwargs = {}
    if case == 'running':
        env.mocks['snapshot'].side_effect = RuntimeError('Another loop is RUNNING')
    elif case == 'fingerprint':
        kwargs['fingerprint'] = '0' * 64
    elif case == 'stage':
        env.installer.STAGE_PREFIX = 't69-release-staged-'
        with pytest.raises((ValueError, RuntimeError)):
            env.installer.stage_path('../t69-release-staged-x')
        with pytest.raises(ValueError):
            env.installer.stage_path('t68a-release-staged-v1')
        return
    elif case == 'guard':
        (env.root / 'prediction/hs-recovery-startup.env').write_text('PREDICTION_LIVE_ARM_ON_START=true\n')
    elif case == 'candidate_fingerprint':
        plan = json.loads((env.stage / 'candidate.json').read_text())
        plan['expected_fingerprint'] = '1' * 64
        (env.stage / 'candidate.json').write_text(json.dumps(plan))
    elif case == 'validation':
        (env.stage / 'validation.json').write_text(json.dumps({'status': 'DEPLOYED'}))
    else:
        (env.stage / SOURCE).write_bytes(b'changed after review')
    with pytest.raises(RuntimeError):
        env.install('--apply', **kwargs)
    assert env.actions() == [] and not list(env.backups.iterdir())
    assert env.live_release() == env.old


def roll(env, *extra):
    env.rollback.main(['--backup', str(env.backup()), *extra])


def test_rollback_preflight_then_apply_restores_parent(env, capsys):
    env.install('--apply')
    capsys.readouterr()
    calls = len(env.actions())
    roll(env)
    assert json.loads(capsys.readouterr().out)['status'] == 'ROLLBACK_PREFLIGHT_PASSED'
    assert len(env.actions()) == calls and env.live_release() == env.new
    roll(env, '--apply')
    assert json.loads(capsys.readouterr().out)['status'] == 'ROLLED_BACK_LIVE_NOT_ACTIVATED'
    assert env.live_release() == env.old and not (env.root / ADDED).exists()
    assert (env.root / SOURCE).read_bytes() == b'parent'
    assert env.actions()[calls:] == ['stop'] * 3 + ['start'] * 3
    assert json.loads((env.backup() / 'rollback.json').read_text())['fingerprint'] == env.old


@pytest.mark.parametrize('case', ['selected', 'pending', 'tampered', 'running'])
def test_rollback_refuses_unsafe_state(env, case):
    env.install('--apply')
    calls = len(env.actions())
    if case == 'selected':
        env.profile['prediction_selected_strategy'] = 'regime_target6_9_v1'
    elif case == 'pending':
        env.profile['prediction_pending_strategy'] = 'regime_target6_9_v1'
    elif case == 'tampered':
        (env.root / SOURCE).write_bytes(b'hand edit')
    else:
        env.mocks['snapshot'].side_effect = RuntimeError('Another loop is RUNNING')
    with pytest.raises(RuntimeError):
        roll(env, '--apply')
    assert len(env.actions()) == calls and (env.root / ADDED).exists()


def test_rollback_failure_after_stop_leaves_services_stopped(env):
    env.install('--apply')
    calls = len(env.actions())
    env.mocks['snapshot'].side_effect = [{'fixed': True}, {'fixed': True}, {'changed': True}]
    with pytest.raises(RuntimeError, match='changed after stopping'):
        roll(env, '--apply')
    assert env.actions()[calls:] == ['stop'] * 3


def test_verifier_is_read_only_and_reports_each_check(env, capsys):
    env.install('--apply')
    capsys.readouterr()
    calls = len(env.actions())
    snapshot = {p: p.read_bytes() for p in env.root.rglob('*') if p.is_file()}
    env.mocks['t69_tables'].return_value = {'BTCUSDT': {'database': 'present', 'tables': []}}
    env.verify.main(['--expected-fingerprint', env.new])
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'T69_VERIFIED' and result['checks']['release']['value'] == env.new
    with pytest.raises(SystemExit):
        env.verify.main(['--expected-fingerprint', env.old])
    assert json.loads(capsys.readouterr().out)['checks']['release']['ok'] is False
    with pytest.raises(SystemExit):
        env.verify.main(['--expected-fingerprint', env.new, '--require-t69-tables'])
    capsys.readouterr()
    assert len(env.actions()) == calls
    assert {p: p.read_bytes() for p in env.root.rglob('*') if p.is_file()} == snapshot


def test_reviewed_policy_fingerprint_matches_main():
    from src.gridbot.prediction.regime_t69_policy import FINGERPRINT
    ops = module('t69_ops')
    assert ops.POLICY_FINGERPRINT == FINGERPRINT


def test_fresh_check_script_runs_against_this_checkout(tmp_path):
    import sqlite3
    from test_live_report import SCHEMA
    ops = module('t69_ops')
    (tmp_path / 'prediction/data').mkdir(parents=True)
    with sqlite3.connect(tmp_path / 'prediction/data/prediction.sqlite3') as db:
        db.executescript(SCHEMA)
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, '-B', '-c', ops.FRESH_CHECK, str(tmp_path)],
                            cwd=root, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value['policy'] == ops.POLICY_FINGERPRINT and (value['live'], value['shadow']) == (8, 6)
    assert set(ops.T69_TABLES) <= set(value['tables']) and 'T6.9 Report' in value['report']


def test_feature_tables_are_read_only(tmp_path, monkeypatch):
    import sqlite3
    ops = module('t69_ops')
    monkeypatch.setattr(ops, 'ROOT', tmp_path)
    path = tmp_path / ops.FEATURE_DBS['BTCUSDT']
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE t69_shadow_quotes(start INTEGER,branch TEXT,payload TEXT)')
        db.execute("INSERT INTO t69_shadow_quotes VALUES(5,'flat_favorite','{}')")
    data = path.read_bytes()
    value = ops.t69_tables()
    assert value['BTCUSDT'] == {'database': 'present', 'tables': ['t69_shadow_quotes'], 'quotes': 1, 'latest_quote_start': 5}
    assert value['ETHUSDT'] == {'database': 'missing'} and path.read_bytes() == data
