"""Core preservation, causal additive routing, and independently observed paper quotes."""
import json
import sqlite3
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t69_policy import PROFILE, FINGERPRINT, LIVE_BRANCHES, SHADOW_BRANCHES
from src.gridbot.prediction.regime_t69_shadow import observe, resolve_once, schema
from src.gridbot.prediction.regime_t67_evidence import EvidenceStore, evidence_path
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction.c180_favorite import C180EntryDecision
from test_t63 import S, feature, book
from test_t67 import snap, tape


def setup(tmp_path, f, initial, original=None):
    path=tmp_path/'features'
    with closing(connect(path)) as db, db:
        db.execute('INSERT INTO features VALUES(?,?)',(S,json.dumps(f)))
    bridge=b.RegimeWorkerBridge(None,tmp_path/'signals',feature_db=path,profile=PROFILE)
    bridge._registered_loop_id='testloop'
    market=SimpleNamespace(start_time_ms=S,market_topic_id='topic',up_market_id='up')
    def check(current=initial, at=None, unit=D(1), seen=0):
        with patch.object(bridge,'_first_book',return_value=initial),patch.object(b,'read_c180_book',return_value=current),patch.object(b,'read_c180_signal',return_value=original):
            return bridge.check_signal(market=market,unit_usdt=unit,at_ms=at or current['captured_at_ms'],last_seen_book_at_ms=seen)
    return bridge,check


def state(bridge):
    with closing(connect(bridge.feature_db)) as db:
        return json.loads(db.execute('SELECT payload FROM t69_decisions').fetchone()[0])


@pytest.mark.parametrize('first,last,prior,up,down,name,side',[
    (2,-1,5,'.3','.7','core_first_up','UP'),
    (-2,1,-2,'.7','.3','core_first_down','DOWN'),
    (2,'.2',2,'.6','.4','core_stall_down','DOWN'),
    (2,-4,-2,'.3','.7','core_c_down','DOWN'),
])
def test_old_core_executes_before_additions(tmp_path,first,last,prior,up,down,name,side):
    bridge,check=setup(tmp_path,feature(first,last,prior),book(up,down,124000))
    ready=check()
    assert ready.allowed,ready.reason
    assert ready.signal.entry.side==side
    d=state(bridge)
    assert d['branch']==name and d['loop_id']=='testloop'
    assert d['core_guard']['verified'] and not d['core_guard']['empty']
    assert ready.signal.original_input_sha256==FINGERPRINT
    assert ready.execution.expires_at_ms==S+136000


def test_continuation_original_probability_ev_and_pipeline(tmp_path):
    entry=C180EntryDecision('DOWN','original','DOWN',D(1),D(5),None)
    signal=C180Signal(S,'topic','up',S+120000,S+120500,'entry_positive_cost_after_ev',entry,D('.2'),None,None,D(200))
    bridge,check=setup(tmp_path,feature(2,1,2),book('.8','.2',124000),signal)
    ready=check();assert ready.allowed,ready.reason
    assert state(bridge)['branch']=='core_continuation_original'
    assert ready.signal.original_p_up==D('.2')
    bad=check(book('.8','.26',125000))
    assert not bad.allowed


@pytest.mark.parametrize('first,last,prior,up,down,name,side',[
    (-1,3,2,'.7','.3','c_mirror_up_prior','UP'),
    (2,-1,-2,'.6','.4','shallow_retracement','UP'),
])
def test_additive_requires_frozen_verified_empty_core(tmp_path,first,last,prior,up,down,name,side):
    bridge,check=setup(tmp_path,feature(first,last,prior),book(up,down,124000))
    ready=check();assert ready.allowed,ready.reason
    d=state(bridge)
    assert d['core_guard']['empty'] and d['branch']==name and d['side']==side
    assert ready.execution.expires_at_ms==S+126000
    assert not check(book(up,down,126000)).allowed
    assert state(bridge)['branch']==name


def test_later_core_quote_failure_never_authorizes_additive(tmp_path):
    bridge,check=setup(tmp_path,feature(2,-1,5),book('.3','.7',124000))
    # Initial core qualifies; newer quote exceeds its original price cap but
    # would qualify for the .75 shallow branch. Preserve the core reservation.
    assert not check(book('.6','.4',125000)).allowed
    d=state(bridge)
    assert not d['core_guard']['empty'] and not d['selected']
    assert check(book('.29','.71',128000)).allowed
    assert state(bridge)['branch']=='core_first_up'


@pytest.mark.parametrize('bad',['missing_features','bad_features','thin_empty','missing_initial','late_initial','fee','future','missing_original'])
def test_unknown_is_not_core_empty(tmp_path,bad):
    f=feature(2,-1,-2);initial=book('.6','.4',124000)
    if bad=='bad_features':f['fingerprint']='invalid'
    if bad=='thin_empty':initial['quote']['DOWN']['ask_levels']=[['.4','.01']]
    if bad=='future':initial['received_at_ms']+=1
    if bad=='missing_original':f=feature(2,1,2)
    bridge,check=setup(tmp_path,f,initial)
    if bad=='missing_features':
        with closing(connect(bridge.feature_db)) as db,db:db.execute('DELETE FROM features')
    if bad=='missing_initial':
        bridge._first_book=lambda *args:None
        with patch.object(b,'read_c180_book',return_value=initial):
            ready=bridge.check_signal(market=SimpleNamespace(start_time_ms=S,market_topic_id='topic',up_market_id='up'),unit_usdt=D(1),at_ms=S+124000,last_seen_book_at_ms=0)
    elif bad=='late_initial':ready=check(book('.6','.4',126001))
    elif bad=='fee':
        newer=book('.6','.4',125000);newer['fee_bps']=300;ready=check(newer)
    else:ready=check()
    assert not ready.allowed,ready.reason


def test_retired_fallback_remains_c_blocker(tmp_path):
    # UP fallback qualifies at .70. A missing original does not permit new
    # original C DOWN even though reversal is net-down.
    bridge,check=setup(tmp_path,feature(-4,2,2),book('.7','.7',124000))
    ready=check();assert ready.allowed,ready.reason
    assert state(bridge)['branch']=='core_c_down'
    # On net-UP eligible fallback, old C cannot be created by new integration.
    other=tmp_path/'other';other.mkdir()
    bridge,check=setup(other,feature(-2,4,-2),book('.7','.7',124000))
    ready=check();assert not ready.allowed,ready.reason
    assert 'core_c_down' not in state(bridge).get('eligible_branches',[])


def test_selected_direction_identity_unit_stay_frozen(tmp_path):
    bridge,check=setup(tmp_path,feature(2,-1,-2),book('.6','.4',124000))
    assert check().allowed
    assert not check(unit=D(2)).allowed
    bridge._registered_loop_id='different'
    assert not check().allowed
    bridge._registered_loop_id='testloop'
    assert not check(book('.6','.4',125000),seen=S+125000).allowed
    assert state(bridge)['side']=='UP'


def paper_db(path):
    with sqlite3.connect(path) as db:
        db.executescript('CREATE TABLE prediction_loops(loop_id TEXT,strategy_profile TEXT,mode TEXT,state TEXT);'
          'CREATE TABLE prediction_regime_slots(loop_id TEXT,market_start_ms INTEGER,market_topic_id TEXT,market_id TEXT,verified_at_ms INTEGER);'
          'CREATE TABLE prediction_runtime_config(config_key TEXT,config_value_json TEXT);')
        db.execute('INSERT INTO prediction_loops VALUES(?,?,?,?)',('testloop',PROFILE,'LIVE','RUNNING'))
        db.execute('INSERT INTO prediction_regime_slots VALUES(?,?,?,?,?)',('testloop',S,'topic','up',S+50000))
        db.execute('INSERT INTO prediction_runtime_config VALUES(?,?)',('prediction_selected_order_unit',json.dumps({'order_unit_usdt':'1'})))


def test_shadow_quotes_observe_after_live_without_touching_trade_db(tmp_path):
    pred=tmp_path/'pred';paper_db(pred);before=pred.read_bytes();sig=tmp_path/'signals'
    with closing(EvidenceStore(evidence_path(sig))) as evidence:
        evidence.book(snap())
        with evidence.db:
            evidence.db.executemany('INSERT INTO spot VALUES(?,?,?,?,?)',[(s['source'],s['generation'],s['event_ms'],s['received_ms'],s['price']) for s in tape()])
    with closing(connect(tmp_path/'features')) as db:
        assert observe(db,pred,sig,S+60000)=='shadow_observed'
        assert db.execute('SELECT branch FROM t69_shadow_quotes').fetchall()==[('reference_value',)]
        assert not db.execute('SELECT * FROM decisions').fetchall()
        assert not db.execute('SELECT * FROM t65_shadow_quotes').fetchall()
    assert pred.read_bytes()==before
    assert set(SHADOW_BRANCHES).isdisjoint(LIVE_BRANCHES)


@pytest.mark.asyncio
async def test_shadow_outcome_rejects_identity_and_keeps_confirmed_immutable(tmp_path):
    quote=dict(fingerprint=FINGERPRINT,loop_id='testloop',market_topic='topic',market_id='up',market_start_ms=S,market_end_ms=S+300000,end_ms=S+300000)
    with closing(connect(tmp_path/'features')) as db:
        schema(db)
        with db:db.execute('INSERT INTO t69_shadow_quotes VALUES(?,?,?)',(S,'reference_value',json.dumps(quote)))
        market=SimpleNamespace(market_topic_id='wrong',up_market_id='up',start_time_ms=S,end_time_ms=S+300000,status='CLOSED')
        with patch('src.gridbot.prediction.models.MarketInfo.from_api',return_value=market),patch('src.gridbot.prediction.worker.PredictionWorker._official_shadow_resolution',return_value='UP'):
            assert await resolve_once(db,S+310000,AsyncMock(return_value={}))=='pending'
            market.market_topic_id='topic'
            assert await resolve_once(db,S+330000,AsyncMock(return_value={}))=='resolved'
        winner=json.loads(db.execute('SELECT payload FROM t69_shadow_outcomes').fetchone()[0]);assert winner['winner']=='UP'
        fetch=AsyncMock(return_value={'winner':'DOWN'})
        assert await resolve_once(db,S+350000,fetch)=='complete'
        fetch.assert_not_called()


@pytest.mark.parametrize('message,code', [
    ('candidate_price_band','price_band'),
    ('candidate_ev','original_ev'),
    ('insufficient requested depth','insufficient_depth'),
    ('book receipt stale or future','book_receipt_clock'),
    ('signature=https://private.example?token=secret','input_value_invalid'),
])
def test_diagnostic_reason_codes_never_copy_exception_payload(message, code):
    from src.gridbot.prediction.regime_t69_bridge import _reason_code
    assert _reason_code(ValueError(message)) == code


def test_t69_live_rules_equal_parent_except_explicit_version_metadata():
    from src.gridbot.prediction.regime_t69_policy import POLICY as current
    from src.gridbot.prediction.regime_t67c_policy import POLICY as parent
    for field in ('core_fingerprint','research_fingerprint','new_priority',
                  'routing','one_market_one_buy','decision_ms','last_selection_ms',
                  'entry_ms','quote_ttl_ms','core_expiry_ms','original_input_ms',
                  'book_max_age_ms','units','c_mirror_up_prior','shallow_retracement',
                  'risk_state_key','loop_mdd_1u'):
        assert current[field] == parent[field], field
    assert current['reference_backfill_mode'] == 'live'
    assert current['live'][:-2] == parent['live']
    assert current['live'][-2:] == ('flat_favorite', 'reference_180_mid')
    assert 'reference_180_mid' not in current['shadow_branches']


def test_checkpoints_remain_readonly_diagnostics_for_live_reference(tmp_path):
    from src.gridbot.prediction import regime_t69_shadow as observer
    from test_t69_reference import empty_core
    pred = tmp_path/'prediction.sqlite3'
    paper_db(pred)
    with sqlite3.connect(pred) as db:
        db.executescript(
            'CREATE TABLE prediction_regime_entry_claims(market_start_ms INTEGER);'
            'CREATE TABLE prediction_campaigns(campaign_id TEXT,start_time_ms INTEGER,buy_count INTEGER,pending_unknown INTEGER);'
            'CREATE TABLE prediction_order_intents(campaign_id TEXT,unknown INTEGER);')
    before = pred.read_bytes()
    with closing(connect(tmp_path/'features')) as db:
        core = empty_core()
        core['loop_id'] = 'testloop'
        db.execute('CREATE TABLE t69_decisions(start INTEGER PRIMARY KEY,payload TEXT)')
        db.execute('INSERT INTO t69_decisions VALUES(?,?)', (S, json.dumps(core)))
        db.commit()
        with patch.object(observer, 'read_inputs', return_value=([snap(180000, up='.4', down='.6')], tape(180000))):
            assert observe(db, pred, tmp_path/'signals', S+180000) == 'shadow_observed'
        frozen = db.execute('SELECT payload FROM t69_reference_checkpoints WHERE checkpoint_ms=180000').fetchone()[0]
        record = json.loads(frozen)
        assert record['evaluation']['candidate']['branch'] == 'reference_180_mid'
        assert record.get('paper_quote') is None
        assert 'reference_180_mid' not in {r[0] for r in db.execute('SELECT branch FROM t69_shadow_quotes')}
        assert db.execute('SELECT payload FROM t69_decisions').fetchone()[0] == json.dumps(core)
        with patch.object(observer, 'read_inputs', return_value=([snap(180500, up='.41', down='.59')], tape(180500))):
            assert observe(db, pred, tmp_path/'signals', S+180500) == 'shadow_observed'
        assert db.execute('SELECT payload FROM t69_reference_checkpoints WHERE checkpoint_ms=180000').fetchone()[0] == frozen
    assert pred.read_bytes() == before


def test_observer_records_missing_checkpoint_without_authorizing_live(tmp_path):
    from src.gridbot.prediction import regime_t69_shadow as observer
    pred = tmp_path/'prediction.sqlite3'
    paper_db(pred)
    before = pred.read_bytes()
    with closing(connect(tmp_path/'features')) as db:
        with patch.object(observer, 'read_inputs', return_value=([], [])):
            assert observe(db, pred, tmp_path/'signals', S+60000) == 'shadow_book_missing'
        record = json.loads(db.execute('SELECT payload FROM t69_reference_checkpoints').fetchone()[0])
        assert record['status'] == 'INPUTS_UNAVAILABLE'
        assert not record['backfill_eligible'] and record.get('paper_quote') is None
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='t69_decisions'").fetchall()
    assert pred.read_bytes() == before
