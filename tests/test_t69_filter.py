"""A rejected First UP keeps its causal core reservation across all entry windows."""
import json
import sqlite3
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_t69_bridge as live, regime_worker_bridge as b
from src.gridbot.prediction.regime_t69_policy import POLICY, FINGERPRINT
from test_t63 import S, feature, book
from test_t69 import setup, state
from test_t69_reference_live import late


@pytest.mark.parametrize('prior,allowed', [('1',False),('4.999999',False),('5',True),('5.000001',True),('15',True)])
@pytest.mark.parametrize('unit', [D(1),D(2),D(3)])
def test_first_up_exact_positive_prior_floor(tmp_path,prior,allowed,unit):
    bridge,check=setup(tmp_path,feature(2,-1,prior),book('.3','.7',124000))
    ready=check(unit=unit)
    assert ready.allowed is allowed, ready.reason
    d=state(bridge)
    assert d['core_guard']['verified'] and not d['core_guard']['empty']
    assert d['core_guard']['candidates'][0]['branch']=='core_first_up'
    if allowed:
        assert d['branch']=='core_first_up' and ready.signal.original_input_sha256==FINGERPRINT
        assert d['rejected_branches']==[]
    else:
        assert ready.reason=='t69_first_up_prior_below_5bp'
        assert not d['selected'] and not d['eligible_branches'] and 'signal' not in d
        assert d['rejected_branches']==[dict(branch='core_first_up',reason='first_up_prior_below_5bp',prior_bp=prior)]


def test_rejected_first_up_never_falls_through_or_reopens_after_features_change(tmp_path):
    bridge,check=setup(tmp_path,feature(2,-1,2),book('.3','.7',124000))
    assert not check().allowed
    with sqlite3.connect(bridge.feature_db) as db:
        db.execute('UPDATE features SET payload=? WHERE start=?',(json.dumps(feature(2,-1,20)),S))
    # These quotes would admit shallow; frozen weak core must remain reserved.
    for offset in (125000,128000,134500):
        result=check(book('.6','.4',offset))
        assert not result.allowed and result.reason=='t69_first_up_prior_below_5bp'
        assert state(bridge)['core_guard']['features']['prior_bp']=='2'
    # Restart the adapter; no in-memory filter flag may be required.
    fresh=b.RegimeWorkerBridge(None,bridge.signal_db,feature_db=bridge.feature_db,profile=bridge.profile)
    fresh._registered_loop_id='testloop'
    result=late(fresh)
    assert not result.allowed and result.reason=='t69_reference_denied:reference_core_not_verified_empty'
    assert not state(fresh)['selected']


def test_selected_valid_first_up_uses_original_features_on_refresh(tmp_path):
    bridge,check=setup(tmp_path,feature(2,-1,5),book('.3','.7',124000))
    before=check();assert before.allowed
    with sqlite3.connect(bridge.feature_db) as db:
        db.execute('UPDATE features SET payload=? WHERE start=?',(json.dumps(feature(2,-1,1)),S))
    with patch.object(live,'_persist_selection',side_effect=AssertionError('selected recheck writes')):
        after=check(book('.29','.71',125000))
    assert after.allowed and after.signal==before.signal


def test_policy_diff_is_only_version_lineage_flat_and_markets():
    from src.gridbot.prediction.regime_t68a_policy import POLICY as parent, FINGERPRINT as parent_fp
    from src.gridbot.prediction.regime_t67d_policy import POLICY as flat_parent, FINGERPRINT as flat_fp
    changed={k for k in POLICY.keys()|parent.keys() if POLICY.get(k)!=parent.get(k)}
    assert changed=={'profile','parent_fingerprint','live','validation_mode','flat_favorite',
                     'flat_source_fingerprint','markets','market_binding'}
    assert POLICY['parent_fingerprint']==parent_fp and POLICY['flat_source_fingerprint']==flat_fp
    assert POLICY['first_up_prior_min_bp']=='5'
    rule=dict(POLICY['flat_favorite']);source=dict(flat_parent['flat_favorite'])
    assert rule.pop('core')=='verified_empty_only; after_additions' and source.pop('core')=='verified_empty_only'
    assert rule.pop('reference_after')=='selected_flat_blocks_180s_backfill'
    assert rule==source
    assert POLICY['markets']==('BTCUSDT','ETHUSDT','BNBUSDT')


def test_t68_history_and_t69_decisions_are_independent(tmp_path):
    bridge,check=setup(tmp_path,feature(2,-1,2),book('.3','.7',124000))
    with sqlite3.connect(bridge.feature_db) as db:
        db.execute('CREATE TABLE t68_decisions(start INTEGER PRIMARY KEY,payload TEXT)')
        db.execute('INSERT INTO t68_decisions VALUES(?,?)',(S,'original-t68-history'))
    assert not check().allowed
    with sqlite3.connect(bridge.feature_db) as db:
        assert db.execute('SELECT payload FROM t68_decisions').fetchone()[0]=='original-t68-history'
