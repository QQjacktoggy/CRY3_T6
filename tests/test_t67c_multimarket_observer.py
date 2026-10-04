import json
import sqlite3
from copy import deepcopy
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch,Mock,AsyncMock
import pytest
from operators.t67c_multimarket_observer.engine import new_state,evaluate,settle,observe_shadow,VERSION
from operators.t67c_multimarket_observer.report import snapshot,render,load_render
from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.regime_t67c_policy import FINGERPRINT,LIVE_BRANCHES,SHADOW_BRANCHES
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction.c180_favorite import C180EntryDecision
from test_t63 import S,feature,book
from test_t67c import setup,state as live_state


def original():
    return C180Signal(S,'topic','up',S+120000,S+120500,'entry_positive_cost_after_ev',C180EntryDecision('DOWN','original','DOWN',D(1),D(5),None),D('.2'),None,None,D(200))


def market():return SimpleNamespace(start_time_ms=S,market_topic_id='topic',up_market_id='up')


def bridge(initial):
    obj=b.RegimeWorkerBridge.__new__(b.RegimeWorkerBridge);obj.signal_db='unused';obj.symbol='BTCUSDT';obj._first_book=lambda *a:initial
    return obj


CASES=[(2,-1,2,'.3','.7','core_first_up'),(-2,1,-2,'.7','.3','core_first_down'),
       (2,'.2',2,'.6','.4','core_stall_down'),(2,-4,-2,'.3','.7','core_c_down'),
       (2,1,2,'.8','.2','core_continuation_original'),(-1,3,2,'.7','.3','c_mirror_up_prior'),
       (2,-1,-2,'.6','.4','shallow_retracement')]


@pytest.mark.parametrize('first,last,prior,up,down,name',CASES)
def test_all_seven_match_production_selection_priority_and_execution(tmp_path,first,last,prior,up,down,name):
    f=feature(first,last,prior);q=book(up,down,124000);sig=original() if name=='core_continuation_original' else None
    prod,check=setup(tmp_path,f,q,sig);ready=check();assert ready.allowed
    ours=new_state('BTCUSDT',S);obj=bridge(q)
    with patch.object(b,'read_c180_signal',return_value=sig):
        evaluate(ours,obj,market(),f,q,S+124000)
        assert ours['selected']['candidate']['branch']==live_state(prod)['branch']==name
        assert ours['selected']['expires_at_ms']==ready.execution.expires_at_ms
        assert ours['quote'] is None
        later=book(up,down,124300);evaluate(ours,obj,market(),f,later,S+124300)
    assert ours['quote']['branch']==name
    actual=check(later);assert actual.allowed
    assert D(ours['quote']['cash'])==actual.execution.expected_cash_usdt
    assert D(ours['quote']['net_shares'])==actual.execution.expected_shares


def test_no_requote_same_book_expiry_and_core_reservation():
    f=feature(2,-1,2);q=book('.3','.7',124000);obj=bridge(q);r=new_state('BTCUSDT',S)
    with patch.object(b,'read_c180_signal',return_value=None):
        evaluate(r,obj,market(),f,q,S+124000)
        evaluate(r,obj,market(),f,q,S+124100);assert r['quote'] is None
        evaluate(r,obj,market(),f,book('.6','.4',125000),S+125000)
        assert r['quote'] is None and r['selected']['candidate']['branch']=='core_first_up'
        evaluate(r,obj,market(),f,book('.3','.7',136000),S+136000);assert r['quote'] is None
        f=feature(2,-1,-2);q=book('.6','.4',124000);r=new_state('BTCUSDT',S)
        evaluate(r,bridge(q),market(),f,q,S+124000)
        evaluate(r,bridge(q),market(),f,book('.6','.4',126000),S+126000)
        assert r['quote'] is None and r['reason']=='selected_expired'


@pytest.mark.parametrize('bad',['features','identity','fee','late','stale','original'])
def test_bad_data_never_authorizes_replacement(bad):
    f=feature(2,1,2) if bad=='original' else feature(2,-1,-2);initial=book('.6','.4',124000);q=deepcopy(initial);at=S+124000
    if bad=='features':f=None
    if bad=='identity':q['market_topic']='wrong'
    if bad=='fee':q['fee_bps']=201
    if bad=='late':q=book('.6','.4',126001);at=S+126001
    if bad=='stale':at+=1001
    r=new_state('BTCUSDT',S)
    with patch.object(b,'read_c180_signal',return_value=None):evaluate(r,bridge(initial),market(),f,q,at)
    assert r['selected'] is None and r['quote'] is None


def test_settlement_identity_draw_pending_and_exclusive_total():
    rows=[]
    for s in ('BTCUSDT','ETHUSDT','BNBUSDT'):
        r=new_state(s,S);r['meta']=dict(symbol=s,start=S,end=S+300000,topic='t',market_id='m',tokens={'UP':'1','DOWN':'2'},fee_bps='200',reference='100')
        r['quote']=dict(branch=LIVE_BRANCHES[0],side='DOWN',cash='1',net_shares='2',at_ms=S+124500)
        r['shadow'][SHADOW_BRANCHES[0]]=dict(r['quote'])
        official=dict(meta=dict(r['meta']),winner='DRAW',known_at_ms=S+310000)
        settle(r,{**official,'meta':{**r['meta'],'symbol':'WRONG'}},S+310000);assert 'winner' not in r
        settle(r,official,S+299999);assert 'winner' not in r
        settle(r,official,S+310000);assert r['quote']['pnl']=='0.0'
        rows.append(r)
    p=snapshot(rows,S+310000,S,{'at_ms':S+310000})
    assert p['rolling']['20']['markets']['BTCUSDT']['all']['candidates']==1
    assert p['rolling']['20']['markets']['BTCUSDT']['all']['wr'] is None
    text=render(p,now_ms=S+310000)
    assert len(text)<3900
    for name in ('First UP','First DOWN','Stall DOWN','Continuation Original','Reference','非fill','實際1/20','判定完整0'):assert name in text
    assert '資料過期' in render(p,now_ms=S+500000)


def test_fresh_epoch_no_historical_simulation_and_sources_read_only(tmp_path):
    from operators.t67c_multimarket_observer.service import Collector
    from src.gridbot.prediction.loop_market import data_paths
    pred=tmp_path/'prediction/data/prediction.sqlite3'
    for s in ('BTCUSDT','ETHUSDT','BNBUSDT'):
        f,q=data_paths(pred,s);f.parent.mkdir(parents=True,exist_ok=True);q.parent.mkdir(parents=True,exist_ok=True)
        for path in (f,q):
            with sqlite3.connect(path) as db:db.execute('CREATE TABLE asset_identity(id INTEGER,symbol TEXT)');db.execute('INSERT INTO asset_identity VALUES(1,?)',(s,))
    output=tmp_path/'t67c-multimarket-observer'
    with patch('operators.t67c_multimarket_observer.service.now',return_value=S+150000):c=Collector(tmp_path,output)
    c.tick(S+150000);assert c.db.execute('SELECT MIN(start) FROM windows').fetchone()[0]==S
    assert not pred.exists();assert all(r['selected'] is None for r in c.states.values());c.db.close()


def test_original_three_asset_flag_explicit_and_default_unchanged(monkeypatch):
    from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
    # The concrete runtime name is checked by import; never instantiate feeds.
    obj=C180SignalRuntime.__new__(C180SignalRuntime);obj.symbol='ETHUSDT';obj.prediction_db='unused';obj.signals=SimpleNamespace(on_frozen=Mock())
    monkeypatch.delenv('PREDICTION_T67C_OBSERVER_ORIGINAL_ENABLED',raising=False)
    with patch('src.gridbot.prediction.loop_market.signal_asset_active',return_value=False):obj._on_frozen('f')
    obj.signals.on_frozen.assert_not_called()
    monkeypatch.setenv('PREDICTION_T67C_OBSERVER_ORIGINAL_ENABLED','1');obj._on_frozen('f');obj.signals.on_frozen.assert_called_once_with('f')


@pytest.mark.asyncio
async def test_telegram_auth_and_pure_read_path():
    from src.gridbot.prediction.telegram import PredictionTelegramService
    svc=PredictionTelegramService(object(),['1'],now_ms=lambda:S);svc._deny_if_unauthorized=AsyncMock(return_value=True);svc._reply=AsyncMock()
    with patch('operators.t67c_multimarket_observer.report.load_render',side_effect=AssertionError):await svc.cmd_t67creport(None,SimpleNamespace(args=[]))
    svc._reply.assert_not_called();svc._deny_if_unauthorized.return_value=False
    with patch('operators.t67c_multimarket_observer.report.load_render',return_value='report') as reader:await svc.cmd_t67creport(None,SimpleNamespace(args=['40','bnb']))
    assert reader.call_args.args[1:]==(40,'BNBUSDT',S);svc._reply.assert_awaited_once_with(None,'report',parse_mode=None)
