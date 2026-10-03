"""Deployment rejects PR14-incompatible evidence before touching services."""
import importlib.util
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest


def installer():
    spec=importlib.util.spec_from_file_location('t68a_installer',Path(__file__).parents[1]/'deploy/t68a_manual_install.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def databases(root):
    base=root/'prediction/data/c180-favorite-live';base.mkdir(parents=True)
    for name in ('signals.sqlite3','t67-evidence.sqlite3'):
        with sqlite3.connect(base/name) as db:db.execute('CREATE TABLE audit(id INTEGER)')


def test_fitting_evidence_is_not_modified(tmp_path,monkeypatch):
    m=installer();m.ROOT=tmp_path;databases(tmp_path)
    monkeypatch.setattr(m.shutil,'disk_usage',lambda _:SimpleNamespace(free=200*1024*1024))
    paths=list(tmp_path.rglob('*.sqlite3'));before={p:p.read_bytes() for p in paths}
    m.evidence_preflight()
    assert all(p.read_bytes()==before[p] for p in paths)


def test_oversized_evidence_aborts_readonly(tmp_path,monkeypatch):
    m=installer();m.ROOT=tmp_path;databases(tmp_path)
    class Database:
        def execute(self,sql):
            self.value=70000 if sql=='PRAGMA page_count' else 4096
            return self
        def fetchone(self):return (self.value,)
        def close(self):pass
    monkeypatch.setattr(m.sqlite3,'connect',lambda *a,**k:Database())
    with pytest.raises(RuntimeError,match='Evidence maintenance required'):m.evidence_preflight()


def test_missing_evidence_and_low_disk_fail_before_install(tmp_path,monkeypatch):
    m=installer();m.ROOT=tmp_path
    with pytest.raises(RuntimeError,match='database missing'):m.evidence_preflight()
    databases(tmp_path)
    monkeypatch.setattr(m.shutil,'disk_usage',lambda _:SimpleNamespace(free=127*1024*1024))
    with pytest.raises(RuntimeError,match='reserve unavailable'):m.evidence_preflight()
