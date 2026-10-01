import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from test_t63 import S, book, feature
from test_t65 import freeze
from test_live_report import SCHEMA
from src.gridbot.prediction import regime_t65_lane as lane
from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime, C180SignalStore
from src.gridbot.prediction.live_report import _shadow_metrics, format_live_report
from src.gridbot.prediction.models import Campaign, MarketInfo
from src.gridbot.prediction.regime_t65_shadow import collect_once, finalize_expired, resolve_outcome_once
from src.gridbot.prediction.worker import PredictionWorker, RuntimeMode


def official():
    return dict(marketTopicId='topic', upMarketId='up', startTime=S,
                endTime=S+300000, status='SETTLED', finalOutcome='DOWN')


def report_database(root):
    with closing(sqlite3.connect(root/'prediction/data/prediction.sqlite3')) as db, db:
        db.executescript(SCHEMA)
        db.execute('ALTER TABLE prediction_settlements ADD COLUMN winner TEXT')
        db.execute('CREATE TABLE prediction_shadow_observer_markets(market_topic_id TEXT, '
                   'market_id TEXT,start_time_ms INTEGER,winner TEXT,state TEXT)')
        db.execute("INSERT INTO prediction_loops VALUES('new',?,'LIVE','RUNNING',100,1,1,0,0)", (lane.PROFILE,))
        db.execute("INSERT INTO prediction_campaigns VALUES('c','new',?,0)", (S,))
        db.execute("INSERT INTO prediction_regime_slots VALUES('new',?,1,?,?)", (S,S,S+300000))
        db.execute("INSERT INTO prediction_settlements VALUES('c','c','SETTLED','0',NULL)")


def shadow_metrics(root, **changes):
    return _shadow_metrics(root, 'new', [dict(market_start_ms=S)],
        [dict(loop_id='new',start_time_ms=S,campaign_id='c')],
        [dict(campaign_id='c',status='SETTLED',winner=None)], profile=lane.PROFILE,
        key='shadow_m6',branch='M6_neutral_cheap',now_ms=S+600000, **changes)


def test_first_executable_event_survives_newer_ineligible_latest_book(tmp_path):
    db, _, ready = freeze(tmp_path, feature(2,-1), book('.3','.7'))
    with closing(db), closing(C180SignalStore(tmp_path/'signals')) as store:
        assert ready.allowed
        frozen = db.execute('SELECT payload FROM decisions').fetchone()[0]
        store.persist_book(book('.30','.70',128100))
        store.persist_book(book('.60','.40',128200))
        collect_once(db,S,store.path,S+128250)
        quote = json.loads(db.execute("SELECT payload FROM t65_shadow_quotes WHERE branch='M4_first_pullback'").fetchone()[0])
        assert quote['fill_status'] == 'PAPER_QUOTE_ONLY'
        assert quote['captured_at_ms'] == S+128100
        assert quote['quote']['limit'] == '0.30'
        collect_once(db,S,store.path,S+134501)
        assert json.loads(db.execute("SELECT payload FROM t65_shadow_quotes WHERE branch='M4_first_pullback'").fetchone()[0]) == quote
        assert db.execute('SELECT payload FROM decisions').fetchone()[0] == frozen


def test_event_trail_does_not_create_historical_or_stale_quotes(tmp_path):
    db, _, _ = freeze(tmp_path,feature(2,-1),book('.3','.7'))
    with closing(db), closing(C180SignalStore(tmp_path/'signals')) as store:
        store.persist_book(book('.3','.7',128000))
        collect_once(db,S,store.path,S+130000)
        assert all(json.loads(r[0])['quote'] is None for r in db.execute('SELECT payload FROM t65_shadow_quotes'))
        finalize_expired(db,S+600000)
        assert all(json.loads(r[0])['fill_status'] == 'UNOBSERVED_WINDOW' for r in db.execute('SELECT payload FROM t65_shadow_quotes'))


@pytest.mark.parametrize('observed', [False,True])
def test_restart_finalizes_interrupted_windows_without_replaying_books(tmp_path,observed):
    db, _, _ = freeze(tmp_path,feature(0,0),book('.7','.3'))
    with closing(db), closing(C180SignalStore(tmp_path/'signals')) as store:
        if observed:
            thin = book('.7','.3',128000)
            thin['quote']['DOWN']['ask_levels'] = [['.3','1']]
            store.persist_book(thin)
            collect_once(db,S,store.path,S+128000)
            collect_once(db,S,store.path,S+128200)
            row = json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0])
            assert row['eligible_books'] == 1
        store.persist_book(book('.7','.3',128500))
        assert finalize_expired(db,S+600000) == 1
        row = json.loads(db.execute('SELECT payload FROM t65_shadow_quotes').fetchone()[0])
        assert row['fill_status'] == ('NO_EXECUTABLE_WINDOW_QUOTE' if observed else 'UNOBSERVED_WINDOW')
        assert row['quote'] is None
        assert finalize_expired(db,S+600001) == 0


def test_report_marks_expired_unobserved_window_without_running_collector(tmp_path):
    directory = tmp_path/'prediction/data/regime-target6'
    directory.mkdir(parents=True)
    db, _, _ = freeze(directory,feature(0,0),book('.7','.3'))
    with closing(db):
        report_database(tmp_path)
        metrics = shadow_metrics(tmp_path)
        assert metrics['unobserved'] == 1 and metrics['pending'] == 0
        assert '觀測遺漏 1｜待觀測 0' in format_live_report(tmp_path,now_ms=S+600000)


@pytest.mark.asyncio
async def test_signal_service_resolves_no_fill_shadow_after_loop_completion(tmp_path):
    directory = tmp_path/'prediction/data/regime-target6'
    directory.mkdir(parents=True)
    db, _, ready = freeze(directory,feature(0,0),book('.7','.3'))
    with closing(db), closing(C180SignalStore(tmp_path/'signals')) as store:
        assert not ready.allowed
        store.persist_book(book('.7','.3',128000))
        collect_once(db,S,store.path,S+128000)
        campaign = Campaign('c',MarketInfo('topic','up','slug',S,S+300000))
        worker = SimpleNamespace(_effective_mode=RuntimeMode.LIVE,
            settings=SimpleNamespace(wallet_address='test'),
            repository=SimpleNamespace(get_settlement=AsyncMock(return_value=None),
                get_fills=AsyncMock(return_value=[]),load_unresolved_intents=AsyncMock(return_value=[])),
            _selected_strategy_profile=lane.PROFILE,_now_ms=lambda:S+600000,
            _finalize_settlement_with_c180=AsyncMock())
        settled = await PredictionWorker.settle_campaign(worker,campaign)
        assert settled['winner'] is None
        report_database(tmp_path)
        runtime = object.__new__(C180SignalRuntime)
        runtime.feature_db = directory/'features.sqlite3'
        runtime._last_t65_shadow_scan_ms = 0
        runtime._detail = AsyncMock(return_value=official())
        with patch('src.gridbot.prediction.c180_signal_runtime._now_ms',return_value=S+600000):
            await runtime.t65_shadow_scan_once()
        runtime._detail.assert_awaited_once_with('topic')
        metrics = shadow_metrics(tmp_path)
        assert metrics['known'] == metrics['wins'] == 1 and metrics['pnl'] > 2
        assert '本輪已知淨 PnL +0.0000 USDT' in format_live_report(tmp_path,now_ms=S+600000)
        runtime._last_t65_shadow_scan_ms = 0  # A restarted service keeps the durable result.
        with patch('src.gridbot.prediction.c180_signal_runtime._now_ms',return_value=S+900000):
            await runtime.t65_shadow_scan_once()
        assert runtime._detail.await_count == 1
        with closing(sqlite3.connect(tmp_path/'prediction/data/prediction.sqlite3')) as main, main:
            main.execute("INSERT INTO prediction_shadow_observer_markets VALUES('topic','up',?,'UP','SETTLED')",(S,))
        with pytest.raises(ValueError,match='conflicting'):
            shadow_metrics(tmp_path)


@pytest.mark.asyncio
async def test_outcome_poll_retries_mismatched_and_unresolved_official_metadata(tmp_path):
    db, _, _ = freeze(tmp_path,feature(0,0),book('.7','.3'))
    with closing(db):
        wrong = {**official(),'marketTopicId':'other'}
        unclosed = {**official(),'status':'OPEN'}
        fetch = AsyncMock(side_effect=[wrong,unclosed,official()])
        for now in (S+600000,S+620000):
            assert await resolve_outcome_once(db,now,fetch) == 'pending'
            assert json.loads(db.execute('SELECT payload FROM t65_shadow_outcomes').fetchone()[0])['winner'] is None
        assert await resolve_outcome_once(db,S+640000,fetch) == 'resolved'
        assert await resolve_outcome_once(db,S+660000,fetch) == 'complete'
        assert fetch.await_count == 3


@pytest.mark.asyncio
async def test_official_draw_is_stored_only_with_explicit_half_payouts(tmp_path):
    db, _, _ = freeze(tmp_path,feature(0,0),book('.7','.3'))
    with closing(db):
        draw = official()
        del draw['finalOutcome']
        draw['markets'] = [dict(status='SETTLED',outcomes=[
            dict(name=side,winner=True,price='.5') for side in ('UP','DOWN')])]
        fetch = AsyncMock(return_value=draw)
        assert await resolve_outcome_once(db,S+600000,fetch) == 'resolved'
        outcome = json.loads(db.execute('SELECT payload FROM t65_shadow_outcomes').fetchone()[0])
        assert outcome['winner'] == 'DRAW' and outcome['known_at_ms'] == S+600000


@pytest.mark.asyncio
async def test_signal_outcome_scan_yields_during_feature_and_execution_windows(tmp_path):
    db, _, _ = freeze(tmp_path,feature(0,0),book('.7','.3'))
    with closing(db):
        runtime = object.__new__(C180SignalRuntime)
        runtime.feature_db = tmp_path/'features.sqlite3'
        runtime._last_t65_shadow_scan_ms = 0
        runtime._detail = AsyncMock(return_value=official())
        with patch('src.gridbot.prediction.c180_signal_runtime._now_ms',return_value=S+300000+125000):
            await runtime.t65_shadow_scan_once()
        runtime._detail.assert_not_awaited()
        assert not db.execute('SELECT 1 FROM t65_shadow_outcomes').fetchone()


def test_fill_rate_uses_ended_registered_markets_including_pending_settlement(tmp_path):
    directory = tmp_path/'prediction/data/regime-target6'
    directory.mkdir(parents=True)
    db, _, _ = freeze(directory,feature(0,0),book('.7','.3'))
    with closing(db):
        report_database(tmp_path)
        with closing(sqlite3.connect(tmp_path/'prediction/data/prediction.sqlite3')) as main, main:
            main.execute("INSERT INTO prediction_campaigns VALUES('c2','new',?,0)",(S+300000,))
            main.execute("INSERT INTO prediction_regime_slots VALUES('new',?,2,?,NULL)",(S+300000,S+300000))
            main.execute("INSERT INTO prediction_regime_entry_claims VALUES('new',?,'c2','i2','1')",(S+300000,))
            main.execute("INSERT INTO prediction_order_intents VALUES('i2','c2','FILLED','o2',0,?)",(S+425000,))
            main.execute("INSERT INTO prediction_fills VALUES('c2','BUY')")
            main.execute("INSERT INTO prediction_regime_slots VALUES('new',?,3,NULL,NULL)",(S+600000,))
        report = format_live_report(tmp_path,now_ms=S+600001)
        assert 'Live fill rate 50.0%（1/2 已結束登錄市場）' in report
        assert '待結算/核對 1' in report


def observer_quote_fixture(root, detail, *, observer_id='', observer_start=S, winner='DOWN'):
    directory = root/'prediction/data/regime-target6'
    directory.mkdir(parents=True)
    db, _, _ = freeze(directory,feature(0,0),book('.7','.3'))
    with closing(db), closing(C180SignalStore(root/'signals')) as store:
        store.persist_book(book('.7','.3',128000))
        collect_once(db,S,store.path,S+128000)
        report_database(root)
        with closing(sqlite3.connect(root/'prediction/data/prediction.sqlite3')) as main, main:
            main.execute('ALTER TABLE prediction_shadow_observer_markets ADD COLUMN payload_json TEXT')
            main.execute("INSERT INTO prediction_shadow_observer_markets VALUES('topic',?,?,?,'SETTLED',?)",
                         (observer_id,observer_start,winner,json.dumps(detail) if detail is not None else None))


@pytest.mark.parametrize('admission_snapshot', [False,True])
def test_report_resolves_empty_observer_id_from_saved_binary_detail_read_only(tmp_path,admission_snapshot):
    detail = official()
    del detail['upMarketId']
    del detail['finalOutcome']
    detail['markets'] = [dict(marketId='up',status='SETTLED',outcomes=[
        dict(name='UP',tokenId='u',winner=False,price='0'),
        dict(name='DOWN',tokenId='d',winner=True,price='1')])]
    if admission_snapshot:
        detail['status'] = 'REGISTERED'
        detail['markets'][0]['status'] = 'REGISTERED'
        for outcome in detail['markets'][0]['outcomes']:
            del outcome['winner']
            outcome['price'] = '.5'
    observer_quote_fixture(tmp_path,detail)
    paths = [tmp_path/'prediction/data/prediction.sqlite3',
             tmp_path/'prediction/data/regime-target6/features.sqlite3']
    def dumps():
        result = []
        for path in paths:
            with closing(sqlite3.connect(path)) as db:
                result.append('\n'.join(db.iterdump()))
        return result
    before = dumps()
    metric = shadow_metrics(tmp_path)
    assert metric['candidates'] == metric['quoted'] == metric['known'] == metric['wins'] == 1
    report = format_live_report(tmp_path,now_ms=S+600000)
    assert 'M6 Shadow 已知WR 100.0%' in report
    assert 'M6 Shadow紀錄無法核對' not in report
    assert '本輪已知淨 PnL +0.0000 USDT' in report
    assert dumps() == before


@pytest.mark.parametrize('changes', [
    dict(marketTopicId='other'),dict(upMarketId='wrong'),
    dict(startTime=S+300000),dict(endTime=S+600000),
    dict(finalOutcome='UP'),
])
def test_report_rejects_wrong_saved_identity_or_outcome(tmp_path,changes):
    observer_quote_fixture(tmp_path,{**official(),**changes})
    with pytest.raises(ValueError,match='identity mismatch'):
        shadow_metrics(tmp_path)


@pytest.mark.parametrize('kwargs', [
    dict(observer_id='wrong'),dict(observer_start=S+300000),dict(detail=None),
])
def test_report_does_not_guess_missing_or_conflicting_observer_identity(tmp_path,kwargs):
    observer_quote_fixture(tmp_path,**{'detail':official(),**kwargs})
    with pytest.raises((TypeError,ValueError)):
        shadow_metrics(tmp_path)


@pytest.mark.asyncio
async def test_report_keeps_outcome_conflict_check_with_empty_observer_id(tmp_path):
    observer_quote_fixture(tmp_path,{**official(),'finalOutcome':'UP'},winner='UP')
    with closing(sqlite3.connect(tmp_path/'prediction/data/regime-target6/features.sqlite3')) as db:
        await resolve_outcome_once(db,S+600000,AsyncMock(return_value=official()))
    with pytest.raises(ValueError,match='conflicting'):
        shadow_metrics(tmp_path)
