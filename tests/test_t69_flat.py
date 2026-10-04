"""T6.9 Live never selects Flat; flat markets keep the T6.8a Reference 180s backfill."""
import json
import sqlite3
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import regime_t69_bridge as live
from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction.loop_market import SYMBOLS, bind_data_db, data_paths
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT, LIVE_BRANCHES, PROFILE
from test_t63 import S, feature, book
from test_t67 import snap, tape


def quote(up='.70', down='.30', at=128000, asset=None):
    q = book(up, down, at)
    for side in ('UP', 'DOWN'):
        q['quote'][side]['ask'] = q['quote'][side]['ask_levels'][0][0]
    if asset:
        q.update(market_topic=asset+'-topic', market_id=asset+'-up')
    return q


def no_entry(topic='topic', up='up'):
    return C180Signal(S, topic, up, S+120000, S+120500, 'no_entry', None, None, None, None, D(200))


class Flat:
    def __init__(self, tmp_path, first='.2', last='-.1', prior=2, initial=None, asset=None):
        if asset:
            self.feature_db, self.signal_db = data_paths(tmp_path/'prediction.sqlite3', asset)
            for path in (self.feature_db, self.signal_db):
                path.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(path) as db:
                    bind_data_db(db, asset)
            self.market = SimpleNamespace(start_time_ms=S, end_time_ms=S+300000,
                market_topic_id=asset+'-topic', up_market_id=asset+'-up',
                raw={'symbol': asset, 'variantData': {'priceFeedSymbol': asset}})
        else:
            self.feature_db, self.signal_db = tmp_path/'features.sqlite3', tmp_path/'signals.sqlite3'
            self.market = SimpleNamespace(start_time_ms=S, market_topic_id='topic', up_market_id='up')
        f = feature(first, last, prior)
        if asset:
            f['symbol'] = asset
        with closing(connect(self.feature_db)) as db, db:
            db.execute('INSERT INTO features VALUES(?,?)', (S, json.dumps(f)))
        pred = tmp_path/'exposure.sqlite3'
        with sqlite3.connect(pred) as db:
            db.executescript('CREATE TABLE prediction_campaigns(campaign_id TEXT, start_time_ms INTEGER,buy_count INTEGER,pending_unknown INTEGER);'
                             'CREATE TABLE prediction_order_intents(campaign_id TEXT,order_side TEXT,unknown INTEGER,status TEXT);'
                             'CREATE TABLE prediction_regime_entry_claims(loop_id TEXT,market_start_ms INTEGER);')
        self.bridge = b.RegimeWorkerBridge(SimpleNamespace(db_path=pred), self.signal_db,
                                           feature_db=self.feature_db, profile=PROFILE)
        self.bridge._registered_loop_id = 'loop'
        self.asset = asset
        self.original = no_entry(self.market.market_topic_id, self.market.up_market_id)
        self.initial = initial or quote(at=124000, asset=asset)
        with sqlite3.connect(self.signal_db) as db:
            db.execute('CREATE TABLE IF NOT EXISTS c180_book_events(market_start_ms INTEGER,captured_at_ms INTEGER,book_at_ms INTEGER,snapshot_json TEXT)')
        self.put(self.initial)
        assert not self.check(self.initial).allowed

    def put(self, q):
        with sqlite3.connect(self.signal_db) as db:
            db.execute('INSERT INTO c180_book_events VALUES(?,?,?,?)',
                       (S, q['captured_at_ms'], q['book_at_ms'], json.dumps(q)))

    def check(self, current, unit=D(1), at=None):
        with patch.object(self.bridge, '_first_book', return_value=self.initial), \
                patch.object(b, 'read_c180_book', return_value=current), \
                patch.object(b, 'read_c180_signal', return_value=self.original):
            return self.bridge.check_signal(market=self.market, unit_usdt=unit,
                                            at_ms=at or current['captured_at_ms'], last_seen_book_at_ms=0)

    def late(self, offset=180000):
        current = snap(offset, up='.4', down='.6')
        with patch.object(live, 'read_inputs', return_value=([current], tape(offset))):
            return self.bridge.check_signal(market=self.market, unit_usdt=D(1), at_ms=S+offset,
                                            last_seen_book_at_ms=0)

    def state(self):
        with closing(connect(self.feature_db)) as db:
            return json.loads(db.execute('SELECT payload FROM t69_decisions').fetchone()[0])


def test_live_inventory_has_no_flat_lane():
    assert len(LIVE_BRANCHES) == 8
    assert 'flat_favorite' not in LIVE_BRANCHES and LIVE_BRANCHES[-1] == 'reference_180_mid'


@pytest.mark.parametrize('side,unit', [('UP', D(1)), ('DOWN', D(1)), ('UP', D(2)), ('DOWN', D(3))])
def test_stable_flat_favorite_is_never_a_live_entry(tmp_path, side, unit):
    up, down = ('.7', '.3') if side == 'UP' else ('.3', '.7')
    obj = Flat(tmp_path, initial=quote(up, down, 124000))
    with sqlite3.connect(obj.feature_db) as db:
        db.execute('DELETE FROM t69_decisions')
    assert not obj.check(obj.initial, unit=unit).allowed
    for at in (126000, 128000, 129000):
        q = quote(up, down, at)
        obj.put(q)
        ready = obj.check(q, unit=unit)
        assert not ready.allowed and ready.reason == 't69_wait_reference_checkpoint'
    d = obj.state()
    assert d['selected'] is False and 'flat_guard' not in d
    assert d['core_guard']['empty'] is True


def test_flat_market_still_uses_reference_180(tmp_path):
    obj = Flat(tmp_path)
    q = quote()
    obj.put(q)
    assert not obj.check(q).allowed
    late = obj.late()
    assert late.allowed, late.reason
    assert obj.state()['branch'] == 'reference_180_mid'


def test_flat_requires_valid_original_for_empty_core(tmp_path):
    obj = Flat(tmp_path)
    obj.original = None
    with sqlite3.connect(obj.feature_db) as db:
        db.execute('DELETE FROM t69_decisions')
    q = quote()
    obj.put(q)
    assert not obj.check(obj.initial).allowed
    assert not obj.check(q).allowed


@pytest.mark.parametrize('asset', SYMBOLS)
def test_flat_is_not_live_on_any_bound_asset(tmp_path, asset):
    obj = Flat(tmp_path, asset=asset)
    obj.bridge.symbol = asset
    q = quote(asset=asset)
    obj.put(q)
    assert not obj.check(q).allowed
    d = obj.state()
    assert d['selected'] is False and d['core_guard']['features']['symbol'] == asset
