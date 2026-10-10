"""Record-only market recorder: sampling, persistence, isolation and paper report."""
import json
import sqlite3
import zlib
from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.gridbot.prediction import market_recorder as mr
from src.gridbot.prediction.c180_signal_runtime import C180SignalRuntime
from scripts.market_recorder_report import build_report


S = 1791600000000


def meta(start=S, market_id='up', reference='100'):
    return dict(topic='topic', market_id=market_id, start=start, end=start+300000, reference=reference,
                yes='UP', fee_bps=200.0, identified_at=start+1000)


def quote(up='.6', down='.41', at=0):
    def side(ask, bid):
        return {'ask': float(ask), 'bid': float(bid), 'ask_levels': [(float(ask), 100.0), (float(ask)+.01, 50.0),
                (float(ask)+.02, 10.0), (float(ask)+.03, 5.0)], 'bid_levels': [(float(bid), 100.0)]}
    return {'book_at_ms': at, 'received_at': at, 'UP': side(up, D(1)-D(down)), 'DOWN': side(down, D(1)-D(up))}


def rows(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute('SELECT market_start_ms,market_id,samples,payload FROM market_samples '
                          'ORDER BY market_start_ms').fetchall()


def test_offsets_are_dense_only_around_the_entry_window():
    assert mr.OFFSETS_MS[0] == 0 and mr.OFFSETS_MS[-1] == 295000 and len(mr.OFFSETS_MS) == 73
    assert set(range(110000, 140000, 1000)) <= set(mr.OFFSETS_MS)
    assert list(mr.OFFSETS_MS) == sorted(set(mr.OFFSETS_MS))


def test_samples_are_causal_skip_missed_offsets_and_flush_once_at_market_end(tmp_path):
    path = tmp_path/'market-recorder.sqlite3'
    rec = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: S+400000)
    m = meta()
    rec.spot({'kind': 'binance_spot_aggTrade', 'received_at': S+500, 'body': {'p': '100.5', 'T': S+400}})
    calls = []

    def q(at):
        def get():
            calls.append(at)
            return quote(at=at)
        return get
    for t in (500, 9000, 10500, 10700, 125000, 125400, 128900):
        rec.tick(m, S+t, q(S+t))
    assert not path.exists()  # nothing is written while the market is open
    rec.tick(m, S+300000, q(S+300000))  # first event at/after end flushes
    [(start, market_id, n, raw)] = rows(path)
    data = json.loads(zlib.decompress(raw))
    assert (start, market_id, n) == (S, 'up', 4)
    # 125.0 s covers every offset up to 125 s in one sample; 128.9 s is due 128 s.
    assert [s['o'] for s in data['samples']] == [0, 10000, 125000, 128000]
    assert calls == [S+500, S+10500, S+125000, S+128900]  # books read only when a sample is due
    first = data['samples'][0]
    assert first['spot'] == ['100.5', S+400, S+500]
    assert first['q']['UP']['ask'] == '0.6' and len(first['q']['UP']['asks']) == mr.LEVELS
    assert data['market']['reference'] == '100' and data['symbol'] == 'BTCUSDT'
    assert mr.load_markets(path)[0]['samples'] == data['samples']


def test_market_switch_flushes_and_a_partial_row_never_replaces_a_fuller_one(tmp_path):
    path = tmp_path/'r.sqlite3'
    rec = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: S)
    for t in (0, 10000, 20000):
        rec.tick(meta(), S+t, lambda: quote())
    rec.tick(meta(S+300000), S+300100, lambda: quote())  # next market selected
    assert [r[:3] for r in rows(path)] == [(S, 'up', 3)]
    # A restarted sidecar with fewer samples for the same market keeps the old row.
    again = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: S)
    again.tick(meta(), S+30000, lambda: quote())
    again.close()
    assert [r[:3] for r in rows(path)][0] == (S, 'up', 3)
    # A fuller row for the same identity replaces it; another identity never does.
    fuller = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: S)
    for t in range(0, 50000, 10000):
        fuller.tick(meta(), S+t, lambda: quote())
    fuller.close()
    other = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: S)
    for t in range(0, 100000, 10000):
        other.tick(meta(market_id='other'), S+t, lambda: quote())
    other.close()
    assert [r[:3] for r in rows(path)][0] == (S, 'up', 5)
    rec.close()
    assert [r[:3] for r in rows(path)][1] == (S+300000, 'up', 1)


def test_unusable_or_stale_books_are_recorded_as_such(tmp_path):
    path = tmp_path/'r.sqlite3'
    rec = mr.MarketRecorder(path, symbol='BTCUSDT')
    rec.tick(meta(), S, lambda: None)
    rec.tick(meta(), S+10000, lambda: dict(quote(), stale=True))

    def boom():
        raise KeyError('x')
    rec.tick(meta(), S+20000, boom)
    rec.close()
    samples = mr.load_markets(path)[0]['samples']
    assert samples[0]['q'] is None
    assert samples[1]['stale'] is True and samples[1]['q']['DOWN']['ask'] == '0.41'
    assert samples[2] == {'o': 20000, 'at': S+20000, 'err': 'KeyError', 'q': None}


def test_identity_change_inside_a_market_drops_the_buffer(tmp_path):
    rec = mr.MarketRecorder(tmp_path/'r.sqlite3', symbol='BTCUSDT')
    rec.tick(meta(), S, lambda: quote())
    rec.tick(meta(market_id='other'), S+10000, lambda: quote())
    assert rec.dropped == 1
    rec.close()
    assert not (tmp_path/'r.sqlite3').exists()


def test_old_rows_expire_after_the_retention(tmp_path):
    path = tmp_path/'r.sqlite3'
    old = S - mr.RETENTION_MS - mr.SLOT_MS
    rec = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: old)
    rec.tick(meta(old), old, lambda: quote())
    rec.close()
    rec = mr.MarketRecorder(path, symbol='BTCUSDT', clock_ms=lambda: S+300000)
    rec.tick(meta(), S, lambda: quote())
    rec.close()
    assert [r[0] for r in rows(path)] == [S]


# ------------------------------------------------------------- sidecar hook

def runtime(tmp_path, books=None, normalize=None):
    rt = object.__new__(C180SignalRuntime)
    rt.symbol = 'BTCUSDT'
    rt.store = SimpleNamespace(path=tmp_path/'signals.sqlite3')
    rt.evidence = SimpleNamespace(tape=SimpleNamespace(books=books if books is not None else {'up': {'x': 1}}))
    rt.logic = SimpleNamespace(normalize_book=normalize or (lambda raw, market, at: quote(at=at)))
    rt._current_market = meta()
    return rt


def test_sidecar_hook_records_every_event_kind_into_its_own_file(tmp_path):
    rt = runtime(tmp_path)
    rt._record_market({'kind': 'binance_spot_aggTrade', 'received_at': S+100, 'body': {'p': '1', 'T': S+90}})
    rt._record_market({'kind': 'prediction_book', 'received_at': S+124000, 'body': {}})
    rt._current_market = meta(S+300000)
    rt._record_market({'kind': 'binance_futures_aggTrade', 'received_at': S+300050, 'body': {'p': '2', 'T': S+300040}})
    path = tmp_path/'market-recorder.sqlite3'
    assert path == mr.recorder_path(rt.store.path)
    [(start, _, n, _)] = rows(path)
    assert (start, n) == (S, 2)


def test_sidecar_hook_never_raises_and_never_touches_evidence(tmp_path, caplog):
    rt = runtime(tmp_path)
    with patch.object(mr.MarketRecorder, 'tick', side_effect=RuntimeError('disk')):
        rt._record_market({'kind': 'prediction_book', 'received_at': S, 'body': {}})
        rt._record_market({'kind': 'prediction_book', 'received_at': S+1, 'body': {}})
    assert caplog.text.count('Market recorder unavailable: RuntimeError') == 1  # rate limited
    rt._record_market({'kind': 'prediction_book'})  # malformed event: swallowed too


def test_sidecar_hook_can_be_switched_off(tmp_path, monkeypatch):
    monkeypatch.setenv('PREDICTION_MARKET_RECORDER', '0')
    rt = runtime(tmp_path)
    rt._record_market({'kind': 'prediction_book', 'received_at': S, 'body': {}})
    assert getattr(rt, '_recorder', None) is None and not (tmp_path/'market-recorder.sqlite3').exists()


def test_on_raw_event_still_persists_books_when_the_recorder_fails(tmp_path):
    rt = runtime(tmp_path)
    rt._t67_raw_event = lambda event: None
    rt._last_book_persist_ms = 0
    persisted = []
    rt.store.persist_book = persisted.append
    with patch.object(mr.MarketRecorder, 'tick', side_effect=OSError('full')):
        rt._on_raw_event({'kind': 'prediction_book', 'received_at': S+125000, 'body': {}})
    assert [p['captured_at_ms'] for p in persisted] == [S+125000]


def test_quiet_book_is_kept_and_flagged_stale(tmp_path):
    raw = {'received_at': S+1000, 'book_at_ms': S+900, 'received_at_ms': S+1000}

    def normalize(book, market, at):
        return quote(at=at) if at - book['received_at'] <= 2000 else None
    rt = runtime(tmp_path, books={'up': raw}, normalize=normalize)
    assert 'stale' not in rt._recorder_quote(meta(), S+2500)
    assert rt._recorder_quote(meta(), S+10000)['stale'] is True
    rt.evidence.tape.books = {}
    assert rt._recorder_quote(meta(), S+10000) is None


# ------------------------------------------------------------- paper report

def test_report_scores_favourite_band_chase_and_masked_quotes(tmp_path):
    rec_path = tmp_path/'rec.sqlite3'
    rec = mr.MarketRecorder(rec_path, symbol='BTCUSDT')
    refs = ['100', '101', '100.5', '100.5']  # UP, DOWN, DRAW by the reference chain
    books = [('.6', '.41'), ('.36', '.65'), ('.8', '.21'), ('.6', '.41')]
    for i, (ref, (up, down)) in enumerate(zip(refs, books)):
        start = S + i*300000
        for t in (124000, 125000, 128000):
            rec.tick(meta(start, reference=ref), start+t, lambda up=up, down=down: quote(up, down))
    rec.close()
    pred = tmp_path/'prediction.sqlite3'
    with closing(sqlite3.connect(pred)) as db, db:
        db.execute('CREATE TABLE prediction_campaigns(campaign_id TEXT,slug TEXT,start_time_ms INTEGER,payload_json TEXT)')
        db.execute('CREATE TABLE prediction_settlements(campaign_id TEXT,winner TEXT,net_pnl TEXT)')
        db.execute('CREATE TABLE prediction_fills(campaign_id TEXT,order_side TEXT,shares TEXT,gross_amount TEXT)')
        cid = f'btc-updown-5m-{(S+300000)//1000}'
        db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?)',
                   (cid, cid, S+300000, json.dumps({'market': {'reference_price': '101'}})))
        # Official DOWN agrees with the chain; the fill chased .65 to .68.
        db.execute('INSERT INTO prediction_settlements VALUES(?,?,?)', (cid, 'DOWN', '0.45'))
        db.execute('INSERT INTO prediction_fills VALUES(?,?,?,?)', (cid, 'BUY', '1.47', '0.9996'))
    feat = tmp_path/'features.sqlite3'
    with closing(sqlite3.connect(feat)) as db, db:
        db.execute('CREATE TABLE t69a_decisions(start INTEGER PRIMARY KEY,payload TEXT)')
        db.execute('INSERT INTO t69a_decisions VALUES(?,?)', (S+300000, json.dumps(
            dict(selected=True, side='DOWN', branch='core_c_down', selected_at_ms=S+300000+125500))))
        db.execute('INSERT INTO t69a_decisions VALUES(?,?)', (S, json.dumps(dict(selected=False, rejected_branches=[
            dict(branch='core_c_down', reason='loop_lane_masked', side='DOWN', would_cash='0.994',
                 would_net_shares='1.41')]))))
    report = build_report(rec_path, pred, feat)
    assert report['markets'] == 4
    assert report['winners'] == {'official': 1, 'reference_chain': 2, 'pending': 1}
    fav = report['favourite']['128s']
    # Market 0: UP .6 in band, UP wins. Market 1: DOWN .65 in band (live same side), DOWN wins.
    # Market 2: UP .8 is out of band. Market 3: in band, winner pending.
    assert fav['signals'] == 3 and fav['all']['n'] == 2
    assert fav['no_live_entry']['n'] == 1 and fav['live_same_side']['n'] == 1
    assert D(fav['all']['pnl']) > 0
    assert report['chase']['flagged']['n'] == 1 and D(report['chase']['flagged']['pnl']) == D('0.45')
    # Masked C DOWN at market 0 lost: UP won.
    assert report['masked']['n'] == 1 and D(report['masked']['pnl']) == D('-0.994')
