"""T6.9a post-entry book recorder and First DOWN deep-stop Shadow: record only."""
import json
import sqlite3
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from test_live_report import START
from test_t69_report import main_database
from src.gridbot.prediction import regime_t69a_post_entry as post
from src.gridbot.prediction.live_report import T69A_PROFILE, format_live_report
from src.gridbot.prediction.regime_feature_service import connect
from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT, POLICY

# T6.9a policy fingerprint after the shallow retracement counter-trend floor; this Shadow must not move it.
T69A_FINGERPRINT = 'c1aa56695e855de9120f19c11f48e346f1750d12464994fe9664aa4685693a45'


class World:
    def __init__(self, root):
        self.root = root
        self.db = main_database(root)
        self.db.execute('UPDATE prediction_loops SET strategy_profile=?', (T69A_PROFILE,))
        for column, kind in (('shares', 'TEXT'), ('price', 'TEXT'), ('gross_amount', 'TEXT'), ('event_time_ms', 'INTEGER')):
            self.db.execute(f'ALTER TABLE prediction_fills ADD COLUMN {column} {kind}')
        self.db.commit()
        data = root/'prediction/data'
        self.prediction = data/'prediction.sqlite3'
        self.signal = data/'c180-favorite-live/signals.sqlite3'
        self.signal.parent.mkdir(parents=True)
        self.evidence = sqlite3.connect(self.signal.with_name('t67-evidence.sqlite3'))
        self.evidence.execute('CREATE TABLE books(start INTEGER,book_ms INTEGER,captured_ms INTEGER,payload TEXT,'
                              'PRIMARY KEY(start,book_ms))')
        self.feature = connect(data/'regime-target6/features.sqlite3')
        self.feature.execute('CREATE TABLE IF NOT EXISTS t69a_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        self.feature.commit()
        post._LAST_RUN.clear()

    def position(self, start=START, *, side='DOWN', branch='core_first_down', price='.31',
                 shares='3.22', fill_ms=125500, winner=None, pnl='0'):
        cid = 'c'+str(start)
        self.db.execute('INSERT INTO prediction_regime_slots (loop_id,market_start_ms,run_ordinal,verified_at_ms,'
                        'empty_attested_at_ms,market_topic_id,market_id) VALUES(?,?,?,?,NULL,?,?)',
                        ('current', start, (start-START)//300000+1, start, 'topic'+str(start), 'up'+str(start)))
        self.db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?,?,?,?,?,?)',
                        (cid, 'current', start, 0, 'topic'+str(start), 'up'+str(start), start+300000, side, '{}'))
        self.db.execute('INSERT INTO prediction_regime_entry_claims VALUES(?,?,?,?,?)', ('current', start, cid, 'i'+cid, '1'))
        self.db.execute('INSERT INTO prediction_order_intents VALUES(?,?,?,?,?,?)', ('i'+cid, cid, 'FILLED', 'o'+cid, 0, start+125000))
        self.db.execute("INSERT INTO prediction_fills VALUES(?,'BUY',?,?,?,?,?)",
                        (cid, side, shares, price, str(D(shares)*D(price)), start+fill_ms))
        if winner:
            self.db.execute("INSERT INTO prediction_settlements VALUES(?,?,'SETTLED',?,?)", ('s'+cid, cid, pnl, winner))
            self.db.execute('INSERT INTO prediction_regime_settlement_observations VALUES(?,?,?,?)', ('s'+cid, cid, pnl, start+300000))
        self.db.commit()
        decision = dict(fingerprint=FINGERPRINT, loop_id='current', market_topic='topic'+str(start),
                        market_id='up'+str(start), market_start_ms=start, end_ms=start+300000,
                        selected=True, branch=branch, side=side)
        self.feature.execute('INSERT INTO t69a_decisions VALUES(?,?)', (start, json.dumps(decision)))
        self.feature.commit()

    def book(self, offset, *, down_bid='.30', bids=None, start=START, age=50, market='up'):
        at = start+offset
        bids = [[down_bid, '100']] if bids is None else bids
        quote = dict(UP=dict(ask_levels=[[str(1-D(down_bid)), '50']], ask=str(1-D(down_bid)), bid=str(D('.98')-D(down_bid)),
                             bid_levels=[[str(D('.98')-D(down_bid)), '40']]),
                     DOWN=dict(ask_levels=[[str(D(down_bid)+D('.02')), '50']], ask=str(D(down_bid)+D('.02')),
                               bid=bids[0][0] if bids else None, bid_levels=bids))
        payload = dict(market_start_ms=start, market_topic='topic'+str(start), market_id=market+str(start),
                       fee_bps=200, reference='60000', captured_at_ms=at, book_at_ms=at-age,
                       received_at=at-age, received_at_ms=at-age, full_depth=True, quote=quote)
        self.evidence.execute('INSERT INTO books VALUES(?,?,?,?)', (start, at-age, at, json.dumps(payload)))
        self.evidence.commit()

    def tick(self, offset, start=START):
        post._LAST_RUN.clear()
        return post.observe(self.feature, self.prediction, self.signal, start+offset)

    def state(self, start=START):
        return json.loads(self.feature.execute('SELECT payload FROM t69a_post_entry_states WHERE start=?', (start,)).fetchone()[0])

    def snapshots(self, start=START):
        return [json.loads(r[0]) for r in self.feature.execute(
            'SELECT payload FROM t69a_post_entry_books WHERE start=? ORDER BY offset_ms', (start,))]


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


def test_live_policy_and_fingerprint_are_untouched():
    assert FINGERPRINT == T69A_FINGERPRINT
    assert post.POST_ENTRY_POLICY['base_fingerprint'] == FINGERPRINT != post.POST_ENTRY_FINGERPRINT
    assert 'deep_stop' not in json.dumps(POLICY) and 'post_entry' not in json.dumps(POLICY)
    text = json.dumps(post.POST_ENTRY_POLICY)
    assert 'external_lead' not in text and 'reference_value' not in text
    assert post.DEEP_STOP['after_ms'] == 150000 and post.DEEP_STOP['bid_ratio_max'] == '0.3'
    assert post.DEEP_STOP['branch'] == 'core_first_down' and post.DEEP_STOP['side'] == 'DOWN'
    assert post.RECORDER['end_ms'] == 290000 and post.RECORDER['sample_ms'] == 1000


def test_nothing_recorded_without_a_live_fill(world):
    world.book(130000)
    assert world.tick(130000) == 'no_verified_live_market'
    assert world.tick(100000) == 'outside_post_entry_window'
    assert not world.feature.execute("SELECT name FROM sqlite_master WHERE name='t69a_post_entry_books'").fetchone() \
        or not world.snapshots()


def test_records_one_book_per_second_from_fill_to_290s(world):
    world.position(fill_ms=125500)
    for offset in range(124000, 293000, 250):
        world.book(offset, down_bid='.25')
    assert world.tick(140000) == 'post_entry_recorded'
    rows = world.snapshots()
    # Backfilled from the first fill, one per second bucket.
    assert rows[0]['offset_ms'] == 125500 and rows[1]['offset_ms'] == 126000
    assert len(rows) == 1+14+1
    world.tick(300000-1)
    rows = world.snapshots()
    assert rows[-1]['offset_ms'] == 290000
    assert len({r['offset_ms']//1000 for r in rows}) == len(rows)
    s = world.state()
    assert s['complete'] is True and s['snapshots'] == len(rows) and s['branch'] == 'core_first_down'
    first = rows[0]
    assert first['DOWN']['bid'] == '0.25' and first['DOWN']['bid_levels'] == [['0.25', '100']]
    assert first['UP']['ask'] == '0.75' and first['age_ms'] == 50
    assert world.tick(299999) == 'post_entry_complete'


def test_other_market_books_are_ignored(world):
    world.position()
    world.book(130000, market='other')
    world.book(131000)
    world.tick(132000)
    assert [r['offset_ms'] for r in world.snapshots()] == [131000]


def test_record_only_never_touches_the_trading_database(world):
    world.position()
    world.book(160000, down_bid='.05')
    before = world.db.execute("SELECT group_concat(name) FROM sqlite_master").fetchone()[0]
    counts = {t: world.db.execute(f'SELECT count(*) FROM {t}').fetchone()[0]
              for t in ('prediction_fills', 'prediction_order_intents', 'prediction_regime_entry_claims', 'prediction_settlements')}
    world.tick(161000)
    assert world.state()['deep_stop']['trigger'] is not None
    assert world.db.execute("SELECT group_concat(name) FROM sqlite_master").fetchone()[0] == before
    for table, count in counts.items():
        assert world.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == count


def test_deep_stop_first_fresh_trigger_after_150s(world):
    world.position(price='.30', shares='10')
    world.book(140000, down_bid='.05')               # before 150s: recorded, not a trigger
    world.book(151000, down_bid='.10')               # 0.333x entry: no
    world.book(152000, down_bid='.08', age=1500)     # stale book: skipped
    world.book(153000, down_bid='.09', bids=[['.09', '4'], ['.08', '3'], ['.05', '100']])
    world.book(154000, down_bid='.02')               # later and deeper: first trigger wins
    world.tick(155000)
    s = world.state()
    stop = s['deep_stop']
    assert stop['eligible'] is True and stop['skipped_snapshots'] == 1 and stop['checked_snapshots'] == 2
    t = stop['trigger']
    assert t['offset_ms'] == 153000 and t['bid'] == '0.09' and D(t['bid_ratio']) == D('.3')
    held = D(10)*(1-D('.02')*D('.3')/D('.3'))
    assert D(s['net_shares']) == held == D('9.8')
    fee = D('.02')
    proceeds = 4*(D('.09')-fee*D('.09'))+3*(D('.08')-fee*D('.08'))+(held-7)*(D('.05')-fee*D('.05'))
    assert D(t['proceeds']) == proceeds and D(t['sell_shares']) == held and D(t['unsold_shares']) == 0
    assert t['depth_known'] is True and t['bid_levels'][0] == ['0.09', '4']


def test_thin_bid_leaves_remainder_to_settlement(world):
    world.position(price='.30', shares='10', winner='DOWN', pnl='6.8')
    world.book(160000, down_bid='.05', bids=[['.05', '2']])
    world.tick(161000)
    s = world.state()
    t = s['deep_stop']['trigger']
    assert D(t['sell_shares']) == 2 and D(t['unsold_shares']) == D('7.8')
    stop, hold = post.hypothetical(s, 'DOWN')
    assert hold == D('9.8')-3
    assert stop == D(t['proceeds'])+D('7.8')-3
    stop, hold = post.hypothetical(s, 'UP')
    assert hold == -3 and stop == D(t['proceeds'])-3


@pytest.mark.parametrize('branch,side', [('core_c_down', 'DOWN'), ('core_first_up', 'UP'), ('shallow_retracement', 'DOWN')])
def test_other_lanes_are_recorded_but_never_deep_stopped(world, branch, side):
    world.position(branch=branch, side=side)
    world.book(160000, down_bid='.01')
    world.tick(161000)
    s = world.state()
    assert s['snapshots'] == 1 and s['deep_stop']['eligible'] is False and s['deep_stop']['trigger'] is None


def test_retention_prunes_only_old_markets(world):
    old = START-22*86400000
    world.feature.execute('CREATE TABLE IF NOT EXISTS t69a_post_entry_books(start INTEGER,offset_ms INTEGER,'
                          'payload TEXT NOT NULL,PRIMARY KEY(start,offset_ms))')
    world.feature.execute("INSERT INTO t69a_post_entry_books VALUES(?,1,'{}')", (old,))
    world.feature.execute("INSERT INTO t69a_post_entry_books VALUES(?,1,'{}')", (START-86400000,))
    world.feature.commit()
    world.position()
    world.book(130000)
    world.tick(131000)
    starts = {r[0] for r in world.feature.execute('SELECT start FROM t69a_post_entry_books')}
    assert starts == {START-86400000, START}


def test_report_shows_stop_vs_hold_and_never_changes_live(world):
    # Trigger and DOWN wins (stop hurt), trigger and UP wins (stop helped), no trigger.
    a, b, c = START, START+300000, START+600000
    world.position(a, price='.30', shares='10', winner='DOWN', pnl='6.8')
    world.position(b, price='.30', shares='10', winner='UP', pnl='-3')
    world.position(c, price='.30', shares='10', winner='DOWN', pnl='6.8')
    for start in (a, b):
        world.book(160000, start=start, down_bid='.05')
        world.tick(161000, start=start)
    world.book(160000, start=c, down_bid='.25')
    world.tick(161000, start=c)
    text = format_live_report(world.root, now_ms=START+900000, profile_filter=T69A_PROFILE)
    s = world.state(a)
    proceeds = D(s['deep_stop']['trigger']['proceeds'])
    stop = (proceeds-3)*2+D('6.8')
    hold = D('6.8')-3+D('6.8')
    assert '〔First DOWN 深度停損（150s 後 DOWN bid≤0.3×進場價；只記錄不賣）〕' in text
    assert (f'深度停損｜觀察 3｜觸發 2｜已結算 3｜停損PnL {stop:+.4f}｜持有PnL {hold:+.4f}｜'
            f'差 {stop-hold:+.4f} USDT') in text
    assert '觸發後原本會贏 1｜bid 深度不足 0｜待結算 0' in text
    assert '進場後盤口記錄｜市場 3｜快照 3' in text
    # Live headline comes from official settlements only.
    assert '本輪已知淨 PnL +10.6000 USDT' in text
    assert 'first DOWN｜成交 3｜已知WR 66.7%｜已知PnL +10.6000' in text


def test_report_skips_states_from_another_policy(world):
    world.position(winner='DOWN', pnl='6.8')
    world.book(160000, down_bid='.05')
    world.tick(161000)
    s = world.state()
    s['post_entry_fingerprint'] = 'other'
    world.feature.execute('UPDATE t69a_post_entry_states SET payload=?', (json.dumps(s),))
    world.feature.commit()
    text = format_live_report(world.root, now_ms=START+300000, profile_filter=T69A_PROFILE)
    assert '深度停損｜觀察 0｜觸發 0｜已結算 0' in text
    assert '進場後記錄待核對 1' in text


def test_empty_report_lists_the_deep_stop_line():
    from src.gridbot.prediction.regime_t69a_report import empty_report
    text = empty_report(START)
    assert '深度停損｜觀察 0｜觸發 0｜已結算 0｜停損PnL —｜持有PnL —' in text
    assert '進場後盤口記錄｜市場 0｜快照 0' in text


def test_producer_keeps_top_bid_levels_in_evidence(tmp_path):
    from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
    saved = []
    runtime = object.__new__(C180SignalRuntime)
    runtime._t67_active = lambda at: True
    runtime._t67_store = SimpleNamespace(book=saved.append)
    runtime._t67_book_persist_ms = 0
    runtime._current_market = dict(start=START, end=START+300000, topic='t', market_id='m',
                                   fee_bps=200, reference='1', identified_at=START)
    runtime.evidence = SimpleNamespace(tape=SimpleNamespace(books={'m': {'received_at_ms': START+1}}))
    bids = [(0.5-0.01*i, 6.0) for i in range(12)]
    quote = {side: dict(ask_levels=[(0.5, 10.0)], ask=0.5, bid=0.49, bid_levels=bids) for side in ('UP', 'DOWN')}
    quote.update(book_at_ms=START+1, received_at=START+1)
    runtime.logic = SimpleNamespace(normalize_book=lambda raw, market, at: quote)
    runtime._t67_raw_event(dict(kind='prediction_book', received_at=START+2))
    kept = saved[0]['quote']['DOWN']['bid_levels']
    assert kept[0] == ['0.5', '6.0'] and len(kept) == 5  # stops once 30 shares are covered
    assert saved[0]['quote']['UP']['ask_levels'] == [['0.5', '10.0']]
