"""C mirror signal, preserved priorities, frozen guard and real binary reporting."""
import json
import sqlite3
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_t67_lane as lane
from src.gridbot.prediction.regime_t65_lane import candidates as t65_candidates
from src.gridbot.prediction.regime_t67_policy import BRANCHES, FINGERPRINT, PROFILE
from src.gridbot.prediction.regime_t67_report import branch_metrics
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge
from test_t63 import S, book, feature, original
from test_t67 import decision, snap, tape


def guard(a=-1, b=3, prior=2, up='.70', down='.31', unit=D(1)):
    f = feature(a, b, prior)
    return lane.freeze_c_mirror_guard(f, book(up, down, 124000), S+124000, unit)


@pytest.mark.parametrize('a,b,prior,eligible', [(-1,3,2,True), (3,-1,2,True),
    (-1,3,1,True), (-1,3,'.9999',False), (-1,3,-2,False),
    (1,3,2,False), (-3,1,2,False), (-1,1,2,False), ('-.49',3,2,False)])
def test_mirror_compounded_signal_and_prior_boundaries(a,b,prior,eligible):
    assert guard(a,b,prior)['eligible'] is eligible


def test_core_guard_is_signal_eligibility_not_future_fill():
    # First UP would be eligible T6 even though its price is too cheap for C mirror.
    assert guard(3,-1,2,up='.30',down='.71')['reason'] == 't65_core_present'
    assert not guard(3,-1,2,up='.30',down='.71')['eligible']
    # A conflict/paid original cannot change retained T6.5 core for this reversal.
    for a,b in [(-1,3), (3,-1)]:
        f = feature(a,b);q = book()
        assert t65_candidates(f, None, q, D(1))[0] == t65_candidates(f, original(), q, D(1))[0]


@pytest.mark.parametrize('unit', [D(1),D(2),D(3)])
@pytest.mark.parametrize('price,eligible', [('.6499',False),('.65',True),('.75',True),('.7501',False)])
def test_mirror_price_band_units_and_execution_recheck(unit,price,eligible):
    choices = lane.candidates(snap(124000,up=price), [], feature(-1,3),
                              {'c_mirror_guard':guard(unit=unit)}, S+124000, unit)
    assert any(c['branch']=='c_mirror_up_prior' for c in choices) is eligible
    if not eligible:
        with pytest.raises(ValueError):
            lane.execution(snap(124000,up=price),'UP',unit,lower='.65')


def test_mirror_preserves_existing_t67_priority_and_never_reverses_low_price_c():
    state={'c_mirror_guard':guard(3,-1)}
    choices=lane.candidates(snap(124000,up='.70'),[],feature(3,-1),state,S+124000,D(1))
    assert [c['branch'] for c in choices] == ['shallow_retracement','c_mirror_up_prior']
    with pytest.raises(ValueError):lane.freeze_c_mirror_guard(feature(-1,3),book(),S+126001,D(1))
    assert not lane.candidates(snap(134501,up='.70'),[],feature(-1,3),
                               {'c_mirror_guard':guard()},S+134501,D(1))
    assert not lane.candidates(snap(124000,up='.70'),[],feature(3,-5),
                               {'c_mirror_guard':guard(3,-5)},S+124000,D(1))


def initial_bridge(tmp_path, up='.70', down='.31', f=None):
    obj=RegimeWorkerBridge(None,tmp_path/'signals.sqlite3',feature_db=tmp_path/'features.sqlite3',profile=PROFILE)
    with patch.object(obj,'_first_book',return_value=book(up,down,124000)):
        _,ready=decision(tmp_path,snap(124000,up=up,down=down),f=f or feature(-1,3),bridge=obj)
    return obj,ready


def test_mirror_frozen_guard_survives_restart_lower_floor_and_expiry(tmp_path):
    obj,ready=initial_bridge(tmp_path,up='.64')
    assert not ready.allowed
    with closing(connect(obj.feature_db)) as db:
        before=json.loads(db.execute('SELECT payload FROM t67_decisions').fetchone()[0])
        assert before['c_mirror_guard']['eligible']
        assert not db.execute('SELECT 1 FROM decisions').fetchone()
        assert not db.execute('SELECT 1 FROM t65_shadow_quotes').fetchone()
    obj=RegimeWorkerBridge(None,obj.signal_db,feature_db=obj.feature_db,profile=PROFILE)
    with patch.object(obj,'_first_book',side_effect=AssertionError('must not recompute guard')):
        _,ready=decision(tmp_path,snap(124500,up='.70'),bridge=obj)
    assert ready.allowed and 'c_mirror_up_prior' in ready.reason
    _,floor=decision(tmp_path,snap(124600,up='.64'),bridge=obj)
    assert not floor.allowed
    _,expired=decision(tmp_path,snap(126500,up='.70'),bridge=obj)
    assert not expired.allowed and 'expired' in expired.reason


@pytest.mark.parametrize('bad', ['topic','thin','fee','initial_window','features'])
def test_mirror_guard_unknown_inputs_cannot_become_empty_core(tmp_path,bad):
    obj=RegimeWorkerBridge(None,tmp_path/'signals.sqlite3',feature_db=tmp_path/'features.sqlite3',profile=PROFILE)
    initial=book(t=124000);f=feature(-1,3);at=124000
    if bad=='topic':initial['market_topic']='wrong'
    elif bad=='thin':initial['quote']['DOWN']['ask_levels']=[['.3','.1']]
    elif bad=='fee':initial['fee_bps']=201
    elif bad=='initial_window':at=126001
    else:f['fingerprint']='bad'
    with patch.object(obj,'_first_book',return_value=initial):
        _,ready=decision(tmp_path,snap(at,up='.70'),f=f,bridge=obj)
    assert not ready.allowed


def test_mirror_missing_initial_book_does_not_block_existing_retracement(tmp_path):
    obj=RegimeWorkerBridge(None,tmp_path/'signals.sqlite3',feature_db=tmp_path/'features.sqlite3',profile=PROFILE)
    with patch.object(obj,'_first_book',side_effect=sqlite3.OperationalError('missing')):
        _,ready=decision(tmp_path,snap(124000,up='.70'),f=feature(3,-1),bridge=obj)
    assert ready.allowed and 'shallow_retracement' in ready.reason


def test_mirror_selection_deadline_shortens_ttl_to_original_submission_window(tmp_path):
    obj,ready=initial_bridge(tmp_path,up='.64')
    _,ready=decision(tmp_path,snap(134500,up='.70'),bridge=obj)
    assert ready.allowed and ready.execution.expires_at_ms==S+136000
    _,ready=decision(tmp_path,snap(136000,up='.70'),bridge=obj)
    assert not ready.allowed and 'expired' in ready.reason


@pytest.mark.parametrize('bad', [None,'up_id','topic','loop','unverified','start','end','generic'])
def test_mirror_report_uses_verified_admission_for_blank_generic_market_id(tmp_path,bad):
    root=tmp_path;directory=root/'prediction/data/regime-target6';directory.mkdir(parents=True)
    d=dict(selected=True,fingerprint=FINGERPRINT,branch='c_mirror_up_prior',market_topic='topic',
           market_id='up',market_start_ms=S,market_end_ms=S+300000)
    campaign=dict(campaign_id='c',loop_id='new',start_time_ms=S,end_time_ms=S+300000,
                  market_topic_id='topic',market_id='')
    slot=dict(loop_id='new',market_start_ms=S,market_topic_id='topic',market_id='up',verified_at_ms=S)
    if bad=='up_id':slot['market_id']='wrong'
    elif bad=='topic':slot['market_topic_id']='wrong'
    elif bad=='loop':slot['loop_id']='old'
    elif bad=='unverified':slot['verified_at_ms']=None
    elif bad=='start':d['market_start_ms']=S+300000
    elif bad=='end':campaign['end_time_ms']=S+600000
    elif bad=='generic':campaign['market_id']='conflict'
    with closing(sqlite3.connect(directory/'features.sqlite3')) as db,db:
        db.execute('CREATE TABLE t67_decisions(start INTEGER,payload TEXT)')
        db.execute('INSERT INTO t67_decisions VALUES(?,?)',(S,json.dumps(d)))
    metrics=branch_metrics(root,{'c':campaign},{'c'},{'c'},
                           [{'cid':'c','pnl':D('.2'),'unit':D(1),'known':S+300000,'id':'s'}],
                           fingerprint=FINGERPRINT,slots=[slot])
    assert metrics['unattributed']==(1 if bad else 0)
    assert metrics['c_mirror_up_prior']['fills']==(0 if bad else 1)
