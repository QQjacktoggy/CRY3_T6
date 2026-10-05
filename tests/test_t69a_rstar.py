"""T6.9a R* late favourite chase Shadow: paper quote only, separate fingerprint."""
import json
import math
import sqlite3
from decimal import Decimal as D

import pytest

from src.gridbot.prediction import regime_t69a_rstar_shadow as rstar
from src.gridbot.prediction import regime_t69a_shadow as shadow
from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT, POLICY
from test_t63 import S, book
from test_t67 import spot

# T6.9a policy fingerprint as merged in PR #29; R* must not move it.
T69A_FINGERPRINT = '6d78adb8ca95fb84fa2c207aa5fa1a0db7bc57e2f29d7547db2b606e2694aba0'


def identity():
    return dict(fingerprint=FINGERPRINT, loop_id='loop', market_topic='topic',
                market_id='up', market_start_ms=S, market_end_ms=S+300000,
                end_ms=S+300000, unit_usdt='1', fee_bps='200')


def snap(t, up='.95', down='.06', qty='100'):
    value = {**book(up, down, t), 'reference': '100', 'reference_received_ms': S-60000}
    value['quote']['UP']['ask_levels'] = [[up, qty]]
    value['quote']['DOWN']['ask_levels'] = [[down, qty]]
    return value


def history(step='0.01'):
    """Pre-open tape alternating by ``step``: 1s log-return sigma ~ step/100."""
    return [spot(k, D(100)+(D(step) if (k//1000) % 2 else 0)) for k in range(-900000, 1, 1000)]


SIGMA = rstar.sigma_1s(rstar._spots(history(), S), S)[0]


def price_at_z(z, t):
    return D(str(100*math.exp(z*SIGMA*math.sqrt(t/1000))))


class Run:
    def __init__(self, tmp_path, tape=None, decision=None):
        self.db = sqlite3.connect(tmp_path/'features.sqlite3')
        shadow.schema(self.db)
        self.tape = history() if tape is None else tape
        self.decision = decision
        self.history_reads = 0

    def history(self, start):
        self.history_reads += 1
        return self.tape

    def tick(self, t, z=3.5, up='.95', down='.06', books=None, price=None):
        books = [snap(t, up, down)] if books is None else books
        spots = [spot(t, price if price is not None else price_at_z(z, t))]
        return rstar.observe(self.db, identity(), books, spots, S+t, self.decision, self.history)

    def quotes(self):
        return {b: json.loads(p) for b, p in self.db.execute('SELECT branch,payload FROM t69a_shadow_quotes')}

    def state(self):
        return json.loads(self.db.execute('SELECT payload FROM t69a_rstar_states').fetchone()[0])


def test_live_policy_and_fingerprint_are_untouched():
    assert FINGERPRINT == T69A_FINGERPRINT
    assert rstar.BRANCH not in POLICY['shadow_branches']
    assert rstar.RSTAR_POLICY['base_fingerprint'] == FINGERPRINT != rstar.RSTAR_FINGERPRINT
    assert POLICY['shadow_retired'] == ('external_lead_lag', 'reference_value')
    text = json.dumps(rstar.RSTAR_POLICY)
    assert 'external_lead' not in text and 'reference_value' not in text
    assert rstar.RSTAR_POLICY['markets'] == ('BTCUSDT',)
    assert rstar.RSTAR_POLICY['price_band'] == ['0.90', '0.98'] and rstar.RSTAR_POLICY['z_min'] == '3'


def test_sigma_and_z_math():
    sigma, count, coverage = rstar.sigma_1s(rstar._spots(history(), S), S)
    step = math.log(100.01/100)
    assert count == 900 and coverage == 1
    assert sigma == pytest.approx(step, rel=.01)
    assert rstar.z_score(price_at_z(3, 100000), D(100), sigma, 100) == pytest.approx(3, rel=1e-6)
    assert rstar.z_score(D(100), D(100), None, 100) is None
    # Sparse tapes are refused rather than guessed.
    sparse = [s for i, s in enumerate(history()) if i % 3 == 0]
    assert rstar.sigma_1s(rstar._spots(sparse, S), S)[0] is None


def test_quotes_first_qualifying_tick_and_never_replaces_it(tmp_path):
    run = Run(tmp_path)
    run.tick(269000, z=5)
    assert run.db.execute('SELECT count(*) FROM t69a_rstar_states').fetchone()[0] == 1
    assert not run.quotes() and run.history_reads == 0
    run.tick(270000, z=2.9)
    assert not run.quotes() and run.state()['near_misses'] == {'z_below_min': 1}
    run.tick(271000, z=3.2)
    q = run.quotes()[rstar.BRANCH]
    assert q['side'] == 'UP' and q['quoted_at_ms'] == S+271000 and q['fill_status'] == 'PAPER_QUOTE_ONLY'
    assert q['fingerprint'] == FINGERPRINT and q['rstar_fingerprint'] == rstar.RSTAR_FINGERPRINT
    assert q['unit_usdt'] == '1' and D(q['cash']) <= 1 and q['best_ask'] == '0.95'
    assert q['ask_depth'] == [['0.95', '100']] and q['open_price'] == '100'
    assert float(q['z']) == pytest.approx(3.2, rel=1e-3) and q['live_branch'] is None
    assert run.state()['reason'] == 'quoted' and run.history_reads == 1
    run.tick(272000, z=-6, up='.03', down='.97')
    assert run.quotes()[rstar.BRANCH] == q


def test_down_favourite(tmp_path):
    run = Run(tmp_path, decision=dict(selected=True, branch='core_c_down'))
    run.tick(280000, z=-4, up='.06', down='.93')
    q = run.quotes()[rstar.BRANCH]
    assert q['side'] == 'DOWN' and q['live_branch'] == 'core_c_down' and float(q['z']) < -3


@pytest.mark.parametrize('up,miss', [('.89', 'price_below_lower'), ('.99', 'insufficient_depth')])
def test_ask_outside_band_keeps_watching_then_ends(tmp_path, up, miss):
    run = Run(tmp_path)
    run.tick(270000, z=4, up=up)
    assert not run.quotes() and run.state()['near_misses'] == {miss: 1}
    run.tick(295500, z=4)
    assert not run.quotes() and run.state()['reason'] == 'no_signal'


def test_thin_depth_is_not_quoted(tmp_path):
    run = Run(tmp_path)
    thin = snap(270000)
    thin['quote']['UP']['ask_levels'] = [['.95', '.5']]
    run.tick(270000, z=4, books=[thin])
    assert not run.quotes() and run.state()['near_misses'] == {'insufficient_depth': 1}


def test_stale_book_is_not_quoted(tmp_path):
    run = Run(tmp_path)
    run.tick(272000, z=4, books=[snap(270000)])
    assert not run.quotes() and run.state()['near_misses'] == {'book_missing_or_stale': 1}


@pytest.mark.parametrize('tape,reason', [
    (lambda h: [s for s in h if s['event_ms'] < S-1500], 'open_spot_missing'),
    (lambda h: [s for i, s in enumerate(h) if i % 3 == 0 or s['event_ms'] == S], 'sigma_unavailable'),
])
def test_missing_open_or_sigma_is_terminal(tmp_path, tape, reason):
    run = Run(tmp_path, tape=tape(history()))
    run.tick(270000, z=5)
    assert run.state()['reason'] == reason and run.state()['terminal'] is True
    run.tick(271000, z=5)
    assert not run.quotes() and run.history_reads == 1


def test_observer_routes_the_late_window_to_rstar(tmp_path):
    pred = tmp_path/'prediction.sqlite3'
    with sqlite3.connect(pred) as db:
        db.executescript("""
            CREATE TABLE prediction_loops(loop_id TEXT,strategy_profile TEXT,mode TEXT,state TEXT);
            CREATE TABLE prediction_regime_slots(loop_id TEXT,market_start_ms INTEGER,market_topic_id TEXT,market_id TEXT,verified_at_ms INTEGER);
            CREATE TABLE prediction_runtime_config(config_key TEXT,config_value_json TEXT);
        """)
        db.execute("INSERT INTO prediction_loops VALUES('loop',?,'LIVE','RUNNING')", (POLICY['profile'],))
        db.execute("INSERT INTO prediction_regime_slots VALUES('loop',?,'topic','up',?)", (S, S+1))
        db.execute("INSERT INTO prediction_runtime_config VALUES('prediction_selected_order_unit',?)",
                   (json.dumps(dict(order_unit_usdt=2)),))
    from src.gridbot.prediction.regime_t67_evidence import EvidenceStore, evidence_path
    signals = tmp_path/'signals.sqlite3'
    t = 271000
    tape = [s for s in history() if s['event_ms'] >= S+t-900000]+[spot(t, price_at_z(4, t))]
    from contextlib import closing
    with closing(EvidenceStore(evidence_path(signals))) as store:
        store.book(snap(t))
        with store.db:
            store.db.executemany('INSERT OR IGNORE INTO spot VALUES(?,?,?,?,?)',
                                 [(s['source'], s['generation'], s['event_ms'], s['received_ms'], s['price'])
                                  for s in history()+tape])
    from src.gridbot.prediction.regime_feature_service import connect
    db = connect(tmp_path/'features.sqlite3')
    shadow.schema(db)
    assert shadow.observe(db, pred, signals, S+59000, 'BTCUSDT') == 'outside_shadow_window'
    assert shadow.observe(db, pred, signals, S+296000, 'BTCUSDT') == 'outside_shadow_window'
    assert shadow.observe(db, pred, signals, S+t, 'ETHUSDT') == 'outside_shadow_window'
    assert shadow.observe(db, pred, signals, S+t, 'BTCUSDT') == 'rstar_observed'
    q = json.loads(db.execute('SELECT payload FROM t69a_shadow_quotes WHERE branch=?', (rstar.BRANCH,)).fetchone()[0])
    # Paper stake stays 1U whatever the Live unit is.
    assert q['side'] == 'UP' and q['unit_usdt'] == '1' and q['loop_id'] == 'loop'
    # No Flat state or Live decision row was written by the late window.
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert 't69a_flat_shadow_states' not in tables and 't69a_decisions' not in tables


def test_report_counts_rstar_quotes_in_their_own_window(tmp_path, monkeypatch):
    from src.gridbot.prediction import regime_t69a_report as report
    from src.gridbot.prediction import loop_market
    path = tmp_path/'features.sqlite3'
    run = Run(tmp_path)
    run.tick(271000, z=4)
    q = run.quotes()[rstar.BRANCH]
    run.db.execute('INSERT INTO t69a_shadow_outcomes VALUES(?,?)', (S, json.dumps(dict(
        fingerprint=FINGERPRINT, loop_id='loop', market_topic='topic', market_id='up',
        market_start_ms=S, market_end_ms=S+300000, end_ms=S+300000, complete=True, winner='UP',
        final_side='UP', official_status='RESOLVED', known_at_ms=S+301000))))
    run.db.commit()
    monkeypatch.setattr(loop_market, 'report_feature_path', lambda root, loop_id: path)
    monkeypatch.setattr(report, '_official_winners', lambda *a: {})
    slots = [dict(loop_id='loop', market_start_ms=S, verified_at_ms=S+1, market_id='up', market_topic_id='topic')]
    m = report.shadow_metrics(tmp_path, now=S+400000, loop_id='loop', slots=slots, fingerprint=FINGERPRINT)
    r = m[rstar.BRANCH]
    assert (r['quoted'], r['known'], r['wins'], r['losses'], r['unverified']) == (1, 1, 1, 0, 0)
    assert r['pnl'] == D(q['net_shares'])-D(q['cash']) > 0
    assert all(m[b]['quoted'] == 0 for b in report.SHADOW_LABELS if b != rstar.BRANCH)
