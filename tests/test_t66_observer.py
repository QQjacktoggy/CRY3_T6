import json
import sqlite3
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.gridbot.prediction import regime_t66_policy as policy, regime_t66_observer as obs
from src.gridbot.prediction.regime_t65_lane import candidates as t65_candidates, FINGERPRINT as T65_FP
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t66_report import metrics, format_observation_report
from src.gridbot.prediction.live_report import format_live_report
from src.gridbot.prediction.worker import PredictionWorker
from test_t63 import S, feature, book


def setup(tmp_path, f=None, initial=None):
    path = tmp_path/'prediction/data/regime-target6/features.sqlite3'
    db = connect(path)
    obs.activate(db, S-1)
    sig = tmp_path/'signal.sqlite3'
    with sqlite3.connect(sig) as c:
        c.execute('CREATE TABLE c180_signals(market_start_ms INTEGER,signal_json TEXT)')
        c.execute('CREATE TABLE c180_book_events(market_start_ms INTEGER,book_at_ms INTEGER,captured_at_ms INTEGER,snapshot_json TEXT)')
    with db:
        db.execute('INSERT INTO features VALUES(?,?)', (S, json.dumps(f or feature(2, '-.1', 2))))
    put_book(sig, initial or book('.6', '.4'))
    obs.tick(db, sig, S+125000)
    return db, sig


def put_book(path, b):
    with sqlite3.connect(path) as c:
        c.execute('INSERT INTO c180_book_events VALUES(?,?,?,?)', (S, b['book_at_ms'], b['captured_at_ms'], json.dumps(b)))


def quote(db, branch='M8_UP'):
    row = db.execute('SELECT payload FROM t66_shadow_quotes WHERE start=? AND branch=?', (S, branch)).fetchone()
    return json.loads(row[0]) if row else None


def outcome(db, winner='UP'):
    value = dict(fingerprint=policy.FINGERPRINT, market_start_ms=S, market_topic='topic', market_id='up',
                 complete=True, winner=winner, known_at_ms=S+300000)
    with db:
        db.execute('INSERT OR REPLACE INTO t66_shadow_outcomes VALUES(?,?)', (S, json.dumps(value)))


@pytest.mark.parametrize('first,last,prior,up,down', [
    (2,-2,2,'.3','.7'),(-2,2,-2,'.7','.3'),(2,-4,-2,'.3','.7'),
    (2,'.1',2,'.6','.4'),(0,0,2,'.3','.7'),(-2,-2,-2,'.4','.6')])
def test_core_parity_and_observations_cannot_select_live(first,last,prior,up,down):
    f,b=feature(first,last,prior),book(up,down)
    core,new=policy.candidates(f,None,b)
    assert core==t65_candidates(f,None,b,D(1))[0]
    assert policy.PROFILE not in PredictionWorker._selectable_strategy_profiles()
    assert policy.CORE_FINGERPRINT==T65_FP
    assert all(c['branch'] not in policy.BRANCHES for c in core)


def test_frozen_direction_and_boundaries():
    _,new=policy.candidates(feature(0,0,-2),None,book('.3','.3'))
    assert new['M6a']['side']=='DOWN'
    _,new=policy.candidates(feature(2,-1,2),None,book('.5','.5'))
    assert new['M4a']['side']=='UP'
    assert 'M6a' not in new  # cheap tie chooses DOWN, opposed to prior
    _,new=policy.candidates(feature(2,'-.4999',2),None,book('.6','.4'))
    assert 'M8_UP' in new and 'M4a' not in new
    _,new=policy.candidates(feature(-2,-2,-1),None,book('.4','.6'))
    assert new['M7_DOWN']['side']=='DOWN'


def test_activation_idempotent_bounded_and_no_trade_schema(tmp_path):
    db,sig=setup(tmp_path)
    old=obs.state(db)
    assert obs.activate(db,S+999999)==old
    assert db.execute('SELECT COUNT(*) FROM t66_observation_markets').fetchone()[0]==500
    assert not db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'prediction_%'").fetchone()
    assert not old['auto_promote']
    obs.tick(db,sig,S+501*300000)
    assert db.execute('SELECT COUNT(*) FROM t66_observation_markets').fetchone()[0]==500
    db.close()


def test_observe_without_live_loop_delays_immutable_and_report(tmp_path):
    db,sig=setup(tmp_path)
    assert quote(db)['status']=='AWAITING_WINDOW'
    original=db.execute('SELECT payload FROM features').fetchone()[0]
    put_book(sig,book('.6','.4',128000));obs.tick(db,sig,S+128000)
    first=quote(db);assert first['status']=='PAPER_QUOTE_ONLY'
    put_book(sig,book('.61','.39',128300));obs.tick(db,sig,S+128300)
    put_book(sig,book('.62','.38',129000));obs.tick(db,sig,S+129000)
    final=quote(db)
    assert final['quote']==first['quote']
    assert final['delays']['300']['status']==final['delays']['1000']['status']=='EXECUTABLE'
    assert db.execute('SELECT payload FROM features').fetchone()[0]==original
    before='\n'.join(db.iterdump());m=metrics(tmp_path,S+600000)
    assert m['branches']['M8_UP']['unknown']==1 and m['portfolio_known']==0
    assert '\n'.join(db.iterdump())==before
    outcome(db);m=metrics(tmp_path,S+600000)
    assert m['branches']['M8_UP']['wins']==1 and m['portfolio_known']==1
    assert m['portfolio_pnl']>0
    assert '非實際成交' in format_observation_report(tmp_path,S+600000)
    db.close()


@pytest.mark.parametrize('invalid', ['stale','identity','fee','thin','early'])
def test_bad_window_data_never_becomes_quote(tmp_path,invalid):
    db,sig=setup(tmp_path)
    b=book('.6','.4',128000)
    if invalid=='stale':b['book_at_ms']=S+126999
    if invalid=='identity':b['market_id']='wrong'
    if invalid=='fee':b['fee_bps']=201
    if invalid=='thin':b['quote']['UP']['ask_levels']=[['.6','1']]
    if invalid=='early':b=book('.6','.4',127999)
    put_book(sig,b);obs.tick(db,sig,S+128000)
    obs.tick(db,sig,S+136000)
    assert quote(db)['quote'] is None
    assert quote(db)['status']==('NO_EXECUTABLE_QUOTE' if invalid=='thin' else 'UNOBSERVED_WINDOW')
    db.close()


def test_delay_reject_does_not_retry_until_profitable(tmp_path):
    db,sig=setup(tmp_path)
    for b in [book('.6','.4',128000),book('.8','.2',128300),book('.6','.4',128500)]:
        put_book(sig,b);obs.tick(db,sig,b['captured_at_ms'])
    assert quote(db)['delays']['300']['status']=='UNEXECUTABLE'
    db.close()


def test_core_overlap_keeps_controls_but_blocks_new_candidates(tmp_path):
    db,sig=setup(tmp_path,feature(2,-1,2),book('.3','.7'))
    assert quote(db,'M4a')['status']=='CORE_OVERLAP'
    put_book(sig,book('.3','.7',128000));obs.tick(db,sig,S+128000)
    assert quote(db,'M4a')['quote'] is None
    assert quote(db,'M4_control')['quote'] is not None
    db.close()


def test_missing_decision_restart_never_replays_old_books(tmp_path):
    db=connect(tmp_path/'f.db');obs.activate(db,S-1)
    obs.tick(db,'does-not-exist',S+136000)
    r=json.loads(db.execute('SELECT payload FROM t66_observation_markets WHERE start=?',(S,)).fetchone()[0])
    assert r['status']=='UNOBSERVED_DECISION'
    assert not db.execute('SELECT 1 FROM t66_shadow_quotes').fetchone()
    db.close()


def test_retire_paper_branches_without_touching_history_or_core(tmp_path):
    db=connect(tmp_path/'f.db');shadows=dict(shadow_a={'old':1},shadow_m4={'keep':1})
    assert obs.filter_retired_shadows(db,S,shadows)==shadows
    obs.activate(db,S-1)
    assert obs.filter_retired_shadows(db,S-300000,shadows)==shadows
    assert obs.filter_retired_shadows(db,S,shadows)=={'shadow_m4':{'keep':1}}
    with db:db.execute("UPDATE t66_observation_state SET payload='{}'")
    assert obs.filter_retired_shadows(db,S,shadows)=={'shadow_m4':{'keep':1}}
    db.close()


@pytest.mark.asyncio
async def test_official_resolution_identity_and_no_rewrite(tmp_path):
    db,sig=setup(tmp_path)
    market=SimpleNamespace(market_topic_id='topic',up_market_id='wrong',start_time_ms=S,end_time_ms=S+300000,status='RESOLVED')
    fetch=AsyncMock(return_value={'public':'fixture'})
    with patch('src.gridbot.prediction.models.MarketInfo.from_api',return_value=market),patch.object(PredictionWorker,'_official_shadow_resolution',return_value='UP'):
        assert await obs.resolve_once(db,S+400000,fetch)=='pending'
        market.up_market_id='up'
        assert await obs.resolve_once(db,S+420000,fetch)=='resolved'
        assert await obs.resolve_once(db,S+440000,fetch)=='complete'
        assert fetch.await_count==2
    db.close()


def test_report_rejects_official_live_conflict(tmp_path):
    db,sig=setup(tmp_path);put_book(sig,book('.6','.4',128000));obs.tick(db,sig,S+128000);outcome(db)
    main=tmp_path/'prediction/data/prediction.sqlite3'
    with sqlite3.connect(main) as c:
        c.execute('CREATE TABLE prediction_campaigns(campaign_id TEXT,start_time_ms INTEGER)')
        c.execute('CREATE TABLE prediction_settlements(campaign_id TEXT,status TEXT,winner TEXT)')
        c.execute('INSERT INTO prediction_campaigns VALUES(?,?)',('c',S))
        c.execute("INSERT INTO prediction_settlements VALUES('c','SETTLED','DOWN')")
    with pytest.raises(ValueError,match='conflicting official'):
        metrics(tmp_path,S+600000)
    db.close()
