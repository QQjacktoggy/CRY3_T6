"""Live Flat checkpoints and execution boundaries; no future winner in selection."""
import json
import sqlite3
from decimal import Decimal as D
from unittest.mock import patch
import pytest
from test_t67d import setup, state
from test_t63 import S, feature, book
from src.gridbot.prediction.c180_signal_service import C180Signal
from src.gridbot.prediction import regime_worker_bridge as b
from src.gridbot.prediction.regime_t67d_policy import LIVE_BRANCHES


def quote(up='.70', down='.30', at=128000):
    q=book(up,down,at)
    for side in ('UP','DOWN'):
        q['quote'][side]['ask']=q['quote'][side]['ask_levels'][0][0]
    return q


def flat(tmp_path, first='.2', last='-.1', initial=None, missing_original=False):
    initial=initial or quote(at=124000)
    original=None if missing_original else C180Signal(S,'topic','up',S+120000,S+120500,'no_entry',None,None,None,None,D(200))
    bridge,check=setup(tmp_path,feature(first,last,2),initial,original)
    with sqlite3.connect(bridge.signal_db) as db:
        db.execute('CREATE TABLE c180_book_events(market_start_ms INTEGER,captured_at_ms INTEGER,book_at_ms INTEGER,snapshot_json TEXT)')
        db.execute('INSERT INTO c180_book_events VALUES(?,?,?,?)',(S,initial['captured_at_ms'],initial['book_at_ms'],json.dumps(initial)))
    assert not check().allowed
    def put(q):
        with sqlite3.connect(bridge.signal_db) as db:
            db.execute('INSERT INTO c180_book_events VALUES(?,?,?,?)',(S,q['captured_at_ms'],q['book_at_ms'],json.dumps(q)))
    return bridge,check,put


@pytest.mark.parametrize('side,unit', [('UP',D(1)),('DOWN',D(1)),('UP',D(2)),('DOWN',D(3))])
def test_flat_fixed_checkpoint_and_unit(tmp_path,side,unit):
    initial=quote('.7','.3',124000) if side=='UP' else quote('.3','.7',124000)
    # Freeze at the actual selected unit, rather than silently changing an existing guard.
    original=C180Signal(S,'topic','up',S+120000,S+120500,'no_entry',None,None,None,None,D(200))
    bridge,check=setup(tmp_path,feature('.2','-.1',2),initial,original)
    with sqlite3.connect(bridge.signal_db) as db:
        db.execute('CREATE TABLE c180_book_events(market_start_ms INTEGER,captured_at_ms INTEGER,book_at_ms INTEGER,snapshot_json TEXT)')
        for q in (initial,quote('.7','.3',128000) if side=='UP' else quote('.3','.7',128000)):
            db.execute('INSERT INTO c180_book_events VALUES(?,?,?,?)',(S,q['captured_at_ms'],q['book_at_ms'],json.dumps(q)))
    assert not check(unit=unit).allowed
    q=quote('.7','.3') if side=='UP' else quote('.3','.7')
    ready=check(q,unit=unit)
    assert ready.allowed,ready.reason
    assert ready.signal.entry.side==side and ready.signal.entry.stake_usdt==unit
    assert state(bridge)['branch']=='flat_favorite'
    assert ready.signal.completed_at_ms==S+128000
    assert ready.execution.expires_at_ms==S+130000
    assert ready.execution.worst_ask_limit==D('.7')


@pytest.mark.parametrize('failure',['below','above','thin','flip','tie','fee','ask_mismatch'])
def test_first_confirmation_failure_cannot_search_later_better_quote(tmp_path,failure):
    bridge,check,put=flat(tmp_path)
    q=quote()
    if failure=='below':q=quote('.64','.36')
    if failure=='above':q=quote('.81','.19')
    if failure=='thin':q['quote']['UP']['ask_levels'][0][1]='.01'
    if failure=='flip':q=quote('.3','.7')
    if failure=='tie':q=quote('.5','.5')
    if failure=='fee':q['fee_bps']=300
    if failure=='ask_mismatch':q['quote']['UP']['ask']='.72'
    put(q)
    # Identity failure in latest execution is safe too; retry later must still
    # honor the failed first checkpoint when the source becomes valid again.
    assert not check(q).allowed
    later=quote(at=128500);put(later)
    assert not check(later).allowed
    assert state(bridge)['flat_guard']['terminal']
    assert not state(bridge)['selected']


def test_frozen_limit_expiry_readonly_and_identity(tmp_path):
    bridge,check,put=flat(tmp_path);q=quote();put(q);first=check(q)
    assert first.allowed
    frozen=state(bridge)['signal']
    assert not check(quote('.71','.29',128500)).allowed
    with patch.object(b,'connect',side_effect=AssertionError('selected path must be readonly')):
        better=check(quote('.69','.31',128800))
        assert better.allowed and better.signal==first.signal
    assert not check(quote(at=130000)).allowed
    assert not check(quote(at=128500),unit=D(2)).allowed
    bridge._registered_loop_id='other'
    assert not check(quote(at=128500)).allowed
    assert state(bridge)['signal']==frozen


def test_late_worker_uses_original_checkpoint_time_and_never_fabricates_fresh_selection(tmp_path):
    bridge,check,put=flat(tmp_path);put(quote())
    # Even though current book is fresh, the immutable selection TTL expired.
    assert not check(quote(at=130100)).allowed
    d=state(bridge)
    assert d['selected_at_ms']==S+128000 and d['expires_at_ms']==S+130000


@pytest.mark.parametrize('first,last',[('.5','0'),('-.5','.1'),('.1','.5'),('.1','-.5'),('1','1')])
def test_nonflat_boundaries_do_not_get_flat(tmp_path,first,last):
    bridge,check,put=flat(tmp_path,first,last,initial=quote('.3','.7',124000));q=quote();put(q)
    assert not check(q).allowed
    assert state(bridge)['flat_guard']['reason']=='non_flat'


def test_missing_confirmation_never_uses_later_quote(tmp_path):
    bridge,check,put=flat(tmp_path);q=quote(at=129501);put(q)
    assert not check(q).allowed
    assert state(bridge)['flat_guard']['reason']=='confirmation_missing'


def test_flat_requires_valid_old_core_empty_original(tmp_path):
    initial=quote(at=124000)
    bridge,check=setup(tmp_path,feature('.2','-.1'),initial,None)
    assert not check().allowed


def test_exact_live_inventory():
    assert len(LIVE_BRANCHES)==8 and LIVE_BRANCHES[-1]=='flat_favorite'
