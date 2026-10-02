"""Operator installer defaults to read-only and refuses unsafe boundaries."""
import hashlib
import json
import runpy
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.mark.parametrize('unsafe', [False,True])
def test_installer_does_not_stop_services_or_write_source_without_explicit_apply(tmp_path,monkeypatch,unsafe):
    script = Path(__file__).resolve().parents[1]/'deploy/t67_manual_install.py'
    main = runpy.run_path(str(script))['main']
    namespace = main.__globals__
    root, stage = tmp_path/'root', tmp_path/'stage'
    for base in (root,stage):
        (base/'prediction').mkdir(parents=True)
        (base/'prediction/release-manifest.json').write_text(json.dumps({'release_fingerprint':'old' if base==root else '1'*64}))
        (base/'prediction/release-pin.env').write_text('fake-test-pin')
        (base/'source.py').write_text('old' if base==root else 'new')
    sha = lambda value: hashlib.sha256(value.encode()).hexdigest()
    (stage/'candidate.json').write_text(json.dumps({'parent':'old','files':[{'path':'source.py','before':sha('old'),'after':sha('new')}]}))
    (stage/'validation.json').write_text(json.dumps({'status':'STAGED_VERIFIED_NOT_DEPLOYED','parent':'old','fingerprint':'1'*64}))
    (root/'prediction/hs-recovery-startup.env').write_text('PREDICTION_LIVE_ARM_ON_START=false\nPREDICTION_AUTO_START_LOOP=false\n')
    snapshot = Mock(side_effect=RuntimeError('Another loop is RUNNING') if unsafe else None,return_value={'fixed':True})
    official, service = Mock(), Mock(return_value='active')
    for key,value in [('ROOT',root),('STAGE',stage),('snapshot',snapshot),('official_clear',official),('service',service)]:
        monkeypatch.setitem(namespace,key,value)
    monkeypatch.setitem(namespace, 'verify_release', lambda base, *a, **k: {'source.py': b'old' if base == root else b'new'})
    monkeypatch.setattr(namespace['os'],'getuid',lambda:1000)
    monkeypatch.setattr(namespace['sys'],'argv',['t67_manual_install.py', '--expected-fingerprint', '1'*64])
    if unsafe:
        with pytest.raises(RuntimeError,match='RUNNING'):
            main()
        official.assert_not_called()
        service.assert_not_called()
    else:
        main()
        official.assert_called_once()
        assert all(call.args[0]=='is-active' for call in service.call_args_list)
    assert (root/'source.py').read_text()=='old'
    assert not list((root/'prediction').glob('t67-rollback-*'))


@pytest.mark.parametrize('state,stopped,authorized,accepted', [
    ('CANCELLED',1,True,True), ('CANCELLED',1,False,False),
    ('CANCELLED',0,True,False), ('RUNNING',1,True,False), ('DONE',1,False,True),
])
def test_snapshot_only_accepts_explicit_cancelled_safe_boundary(tmp_path,state,stopped,authorized,accepted,monkeypatch):
    import sqlite3
    from test_live_report import SCHEMA
    script = Path(__file__).resolve().parents[1]/'deploy/t67_manual_install.py'
    snapshot = runpy.run_path(str(script))['snapshot']
    monkeypatch.setitem(snapshot.__globals__,'ROOT',tmp_path)
    path=tmp_path/'prediction/data/prediction.sqlite3'
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA)
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN buy_count INTEGER DEFAULT 0')
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN state TEXT')
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN end_time_ms INTEGER')
        db.execute('CREATE TABLE prediction_orders(status TEXT)')
        db.execute("INSERT INTO prediction_loops VALUES('target','regime_target6_5_v1','LIVE',?,100,?,1,?,0)",(state,100 if state=='DONE' else 52,stopped))
    if accepted:
        assert snapshot('target',allow_cancelled=authorized)['loop']['state']==state
        with sqlite3.connect(path) as db:
            db.execute("INSERT INTO prediction_orders VALUES('UNKNOWN')")
        with pytest.raises(RuntimeError,match='Nonterminal order'):
            snapshot('target',allow_cancelled=authorized)
    else:
        with pytest.raises(RuntimeError,match='Target loop'):
            snapshot('target',allow_cancelled=authorized)


@pytest.mark.parametrize('campaign_loop,campaign_state,end,unknown,allow,accepted',[
    ('old','DONE',9,0,True,True), ('old','DONE',9,0,False,False),
    ('target','DONE',9,0,True,False), ('old','DONE',11,0,True,False),
    ('old','SETTLING',9,0,True,False), ('old','DONE',None,0,True,False),
    ('old',None,9,0,True,False), ('old','DONE',0,0,True,False), ('old','DONE',9,1,True,False),
])
def test_legacy_closed_exception_never_accepts_current_or_unknown_exposure(tmp_path,monkeypatch,campaign_loop,campaign_state,end,unknown,allow,accepted):
    import sqlite3
    from test_live_report import SCHEMA
    snapshot=runpy.run_path(str(Path(__file__).resolve().parents[1]/'deploy/t67_manual_install.py'))['snapshot']
    monkeypatch.setitem(snapshot.__globals__,'ROOT',tmp_path)
    path=tmp_path/'prediction/data/prediction.sqlite3';path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.executescript(SCHEMA)
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN buy_count INTEGER DEFAULT 0')
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN state TEXT')
        db.execute('ALTER TABLE prediction_campaigns ADD COLUMN end_time_ms INTEGER')
        db.execute('CREATE TABLE prediction_orders(status TEXT)')
        db.execute("INSERT INTO prediction_loops VALUES('target','regime_target6_5_v1','LIVE','CANCELLED',100,52,10,1,0)")
        db.execute("INSERT INTO prediction_campaigns VALUES('legacy',?,1,?,1,?,?)",(campaign_loop,unknown,campaign_state,end))
    if accepted:
        assert snapshot('target',allow_cancelled=True,allow_historical_closed=allow)['loop']['state']=='CANCELLED'
    else:
        with pytest.raises(RuntimeError,match='Unsettled campaign'):
            snapshot('target',allow_cancelled=True,allow_historical_closed=allow)


def test_stop_waits_for_configured_systemd_teardown(monkeypatch):
    service=runpy.run_path(str(Path(__file__).resolve().parents[1]/'deploy/t67_manual_install.py'))['service']
    run=Mock(return_value='active\n')
    monkeypatch.setattr(service.__globals__['subprocess'],'check_output',run)
    assert service('stop','cry3-predict-user.service')=='active'
    assert run.call_args.kwargs['timeout']==120
    service('is-active','cry3-predict-user.service')
    assert run.call_args.kwargs['timeout']==30
