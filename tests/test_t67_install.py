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
        (base/'prediction/release-manifest.json').write_text(json.dumps({'release_fingerprint':'old' if base==root else 'new'}))
        (base/'prediction/release-pin.env').write_text('fake-test-pin')
        (base/'source.py').write_text('old' if base==root else 'new')
    sha = lambda value: hashlib.sha256(value.encode()).hexdigest()
    (stage/'candidate.json').write_text(json.dumps({'parent':'old','files':[{'path':'source.py','before':sha('old'),'after':sha('new')}]}))
    (stage/'validation.json').write_text(json.dumps({'status':'STAGED_VERIFIED_NOT_DEPLOYED','parent':'old','fingerprint':'new'}))
    (root/'prediction/hs-recovery-startup.env').write_text('PREDICTION_LIVE_ARM_ON_START=false\nPREDICTION_AUTO_START_LOOP=false\n')
    snapshot = Mock(side_effect=AssertionError('Another loop is RUNNING') if unsafe else None,return_value={'fixed':True})
    official, service = Mock(), Mock(return_value='active')
    for key,value in [('ROOT',root),('STAGE',stage),('snapshot',snapshot),('official_clear',official),('service',service)]:
        monkeypatch.setitem(namespace,key,value)
    monkeypatch.setattr(namespace['runpy'],'run_path',lambda path: {'verify_release_manifest':lambda *a,**k: ()})
    monkeypatch.setattr(namespace['os'],'getuid',lambda:1000)
    monkeypatch.setattr(namespace['sys'],'argv',['t67_manual_install.py'])
    if unsafe:
        with pytest.raises(AssertionError,match='RUNNING'):
            main()
        official.assert_not_called()
        service.assert_not_called()
    else:
        main()
        official.assert_called_once()
        assert all(call.args[0]=='is-active' for call in service.call_args_list)
    assert (root/'source.py').read_text()=='old'
    assert not list((root/'prediction').glob('t67-rollback-*'))
