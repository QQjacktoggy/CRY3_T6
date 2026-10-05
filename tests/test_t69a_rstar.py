"""T6.9a R* late near-certain favourite Shadow: backtest spec, paper quote only."""
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
REF = D(60000)
RV60 = 10.0  # bp per minute


def klines(rv=RV60, count=60, ref=REF):
    """Bars alternating +rv/-rv bp open-to-close, so rv60 == rv exactly."""
    rows = []
    for n in range(count):
        opened = S-3600000+n*60000
        o = float(ref)
        c = o*math.exp((rv if n % 2 else -rv)/1e4)
        rows.append([opened, str(o), str(max(o, c)), str(min(o, c)), str(c), '1', opened+59999])
    return rows


def price_at(z, o, fav='UP', rv=RV60, ref=REF):
    tau = (300000-o)/1000
    d_bp = z*rv/math.sqrt(60)*math.sqrt(tau)*(1 if fav == 'UP' else -1)
    return D(str(float(ref)*math.exp(d_bp/1e4)))


def snap(t, up='.95', down='.06', qty='100', ref=REF):
    value = {**book(up, down, t), 'reference': str(ref), 'reference_received_ms': S-60000}
    value['quote']['UP'].update(ask_levels=[[up, qty]], bid=str(D(up)-D('.01')))
    value['quote']['DOWN'].update(ask_levels=[[down, qty]], bid=str(D(down)-D('.01')))
    return value


def identity():
    return dict(fingerprint=FINGERPRINT, loop_id='loop', market_topic='topic',
                market_id='up', market_start_ms=S, market_end_ms=S+300000,
                end_ms=S+300000, unit_usdt='1', fee_bps='200')


class Run:
    def __init__(self, tmp_path, bars=None, decision=None):
        self.db = sqlite3.connect(tmp_path/'features.sqlite3')
        shadow.schema(self.db)
        self.bars = klines() if bars is None else bars
        self.decision = decision
        self.fetches = 0

    def klines(self, start):
        self.fetches += 1
        if isinstance(self.bars, Exception):
            raise self.bars
        return self.bars

    def tick(self, t, z=3.5, up='.95', down='.06', books=None, price=None, fav='UP'):
        books = [snap(t, up, down)] if books is None else books
        spots = [spot(t-200, price if price is not None else price_at(z, t, fav))]
        return rstar.observe(self.db, identity(), books, spots, S+t, self.decision, self.klines)

    def quotes(self):
        return {b: json.loads(p) for b, p in self.db.execute('SELECT branch,payload FROM t69a_shadow_quotes')}

    def arm(self, name=rstar.BRANCH):
        return json.loads(self.db.execute('SELECT payload FROM t69a_rstar_states').fetchone()[0])['arms'][name]


def test_live_policy_and_fingerprint_are_untouched():
    assert FINGERPRINT == T69A_FINGERPRINT
    assert rstar.BRANCH not in POLICY['shadow_branches']
    assert rstar.RSTAR_POLICY['base_fingerprint'] == FINGERPRINT != rstar.RSTAR_FINGERPRINT
    assert POLICY['shadow_retired'] == ('external_lead_lag', 'reference_value')
    text = json.dumps(rstar.RSTAR_POLICY)
    assert 'external_lead' not in text and 'reference_value' not in text
    assert rstar.RSTAR_POLICY['markets'] == ('BTCUSDT',)
    arms = rstar.RSTAR_POLICY['arms']
    assert arms[rstar.BRANCH] == dict(enabled=True, ask_min='0.90', ask_max='0.98', z_min='3')
    assert arms[rstar.BRANCH_99] == dict(enabled=True, ask_min='0.99', ask_max='0.99', z_min='5')


def test_spec_math():
    assert rstar.rv60_bp(klines(), S) == pytest.approx(RV60)
    # Bars outside [start-3600s, start-60s] are ignored; fewer than 60 is refused.
    assert rstar.rv60_bp(klines()+[[S, '1', '1', '1', '2', '1', S+59999]], S) == pytest.approx(RV60)
    with pytest.raises(ValueError):
        rstar.rv60_bp(klines(count=59), S)
    z, d_bp, tau = rstar.z_fav(price_at(3, 280000), REF, RV60, 280000, 'UP')
    assert z == pytest.approx(3) and tau == 20
    assert rstar.z_fav(price_at(3, 280000), REF, RV60, 280000, 'DOWN')[0] == pytest.approx(-3)
    assert rstar.cost_per_share(D('.97')) == D('.97')/(1-D('.02')*D('.03')/D('.97'))


def test_quotes_first_qualifying_book_with_backtest_cost(tmp_path):
    run = Run(tmp_path)
    run.tick(269000, z=5)
    assert not run.quotes() and run.fetches == 0
    run.tick(270000, z=2.9)
    assert not run.quotes() and run.arm()['near_misses'] == {'z_below_min': 1}
    run.tick(271000, z=3.2, up='.97', down='.04')
    q = run.quotes()[rstar.BRANCH]
    cost = rstar.cost_per_share(D('.97'))
    assert q['side'] == 'UP' and q['o_ms'] == 271000 and q['quoted_at_ms'] == S+271000
    assert q['fill_status'] == 'PAPER_QUOTE_ONLY' and q['unit_usdt'] == '1' and q['cash'] == '1'
    assert D(q['net_shares']) == 1/cost and q['ask'] == '0.97' and q['bid'] == '0.96'
    assert q['fingerprint'] == FINGERPRINT and q['rstar_fingerprint'] == rstar.RSTAR_FINGERPRINT
    assert q['fillable'] is True and q['ask_levels'] == [['0.97', '100']] and q['other_ask'] == '0.04'
    assert float(q['z_fav']) == pytest.approx(3.2, rel=1e-6) and q['tau_s'] == '29.000'
    assert float(q['rv60_bp']) == pytest.approx(RV60) and q['live_branch'] is None
    assert q['spot_event_ms'] == S+270800 and q['eval_latency_ms'] == 0
    assert run.arm()['reason'] == 'quoted' and run.fetches == 1
    run.tick(272000, z=6, up='.03', down='.97', fav='DOWN')
    assert run.quotes()[rstar.BRANCH] == q


def test_favourite_is_higher_ask(tmp_path):
    run = Run(tmp_path, decision=dict(selected=True, branch='core_c_down'))
    run.tick(280000, z=4, up='.06', down='.93', fav='DOWN')
    q = run.quotes()[rstar.BRANCH]
    assert q['side'] == 'DOWN' and q['live_branch'] == 'core_c_down' and float(q['z_fav']) > 3


def test_spot_against_the_favourite_never_fires(tmp_path):
    run = Run(tmp_path)
    run.tick(280000, z=4, up='.06', down='.93', fav='UP')
    assert not run.quotes() and run.arm()['near_misses'] == {'z_below_min': 1}


@pytest.mark.parametrize('up,ok', [('.89', False), ('.90', True), ('.98', True), ('.99', False)])
def test_band_on_top_of_book(tmp_path, up, ok):
    run = Run(tmp_path)
    run.tick(270000, z=4, up=up, down='.02')
    assert (rstar.BRANCH in run.quotes()) is ok
    if not ok:
        assert run.arm()['near_misses'] == {'ask_outside_band': 1}
        run.tick(295000, z=4, books=[])
        assert run.arm()['reason'] == 'no_signal'


def test_99_arm_needs_z5(tmp_path):
    run = Run(tmp_path)
    run.tick(270000, z=4.5, up='.99', down='.02')
    assert not run.quotes()
    run.tick(271000, z=5.1, up='.99', down='.02')
    assert set(run.quotes()) == {rstar.BRANCH_99}
    assert run.arm(rstar.BRANCH_99)['reason'] == 'quoted'


def test_thin_depth_is_still_a_signal_but_not_fillable(tmp_path):
    run = Run(tmp_path)
    thin = snap(270000)
    thin['quote']['UP']['ask_levels'] = [['.95', '.5'], ['.96', '100']]
    run.tick(270000, z=4, books=[thin])
    q = run.quotes()[rstar.BRANCH]
    assert q['fillable'] is False and q['fill_reason'] == 'insufficient requested depth'
    assert run.arm()['fillable'] is False


@pytest.mark.parametrize('kind,miss', [
    ('stale_spot', 'spot_stale'), ('far_spot', 'spot_ref_too_far'), ('low_ref', 'ref_invalid'),
])
def test_spot_and_ref_filters(tmp_path, kind, miss):
    run = Run(tmp_path)
    if kind == 'stale_spot':
        rows = [spot(268000, price_at(4, 270000))]
        rstar.observe(run.db, identity(), [snap(270000)], rows, S+270000, None, run.klines)
    elif kind == 'far_spot':
        run.tick(270000, price=REF*D('1.011'))
    else:
        run.tick(270000, books=[snap(270000, ref=D(9000))], price=D(9000)*D('1.003'))
    assert not run.quotes() and miss in run.arm()['near_misses']


def test_rv60_fetch_retries_then_ends(tmp_path):
    run = Run(tmp_path, bars=OSError('down'))
    for t in (270000, 271000):
        run.tick(t, z=5)
        assert 'terminal' not in run.arm()
    run.tick(272000, z=5)
    assert run.arm()['reason'] == 'rv60_unavailable' and run.fetches == 3
    run.tick(273000, z=5)
    assert run.fetches == 3 and not run.quotes()


def test_each_book_is_judged_once_and_window_end_is_exclusive(tmp_path):
    run = Run(tmp_path)
    books = [snap(293000, up='.5', down='.5'), snap(294000, up='.5', down='.5')]
    run.tick(294000, books=books)
    assert run.arm()['near_misses'] == {'ask_outside_band': 2}
    run.tick(295000, books=books+[snap(295000)], z=4)
    assert not run.quotes() and run.arm()['reason'] == 'no_signal'


def test_observer_routes_the_late_window_to_rstar(tmp_path, monkeypatch):
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
    from contextlib import closing
    from src.gridbot.prediction.regime_t67_evidence import EvidenceStore, evidence_path
    signals = tmp_path/'signals.sqlite3'
    t = 271000
    with closing(EvidenceStore(evidence_path(signals))) as store:
        store.book(snap(t))
        s = spot(t-300, price_at(4, t))
        with store.db:
            store.db.execute('INSERT INTO spot VALUES(?,?,?,?,?)',
                             (s['source'], s['generation'], s['event_ms'], s['received_ms'], s['price']))
    monkeypatch.setattr(rstar, 'fetch_klines', lambda start: klines())
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


@pytest.mark.parametrize('winner', ['UP', 'DOWN', 'DRAW'])
def test_report_pnl_matches_backtest_formula(tmp_path, monkeypatch, winner):
    from src.gridbot.prediction import regime_t69a_report as report
    from src.gridbot.prediction import loop_market
    path = tmp_path/'features.sqlite3'
    run = Run(tmp_path)
    run.tick(271000, z=4, up='.97', down='.04')
    run.db.execute('INSERT INTO t69a_shadow_outcomes VALUES(?,?)', (S, json.dumps(dict(
        fingerprint=FINGERPRINT, loop_id='loop', market_topic='topic', market_id='up',
        market_start_ms=S, market_end_ms=S+300000, end_ms=S+300000, complete=True, winner=winner,
        final_side=winner, official_status='RESOLVED', known_at_ms=S+301000))))
    run.db.commit()
    monkeypatch.setattr(loop_market, 'report_feature_path', lambda root, loop_id: path)
    monkeypatch.setattr(report, '_official_winners', lambda *a: {})
    slots = [dict(loop_id='loop', market_start_ms=S, verified_at_ms=S+1, market_id='up', market_topic_id='topic')]
    m = report.shadow_metrics(tmp_path, now=S+400000, loop_id='loop', slots=slots, fingerprint=FINGERPRINT)
    r = m[rstar.BRANCH]
    payout = {'UP': 1, 'DOWN': 0, 'DRAW': D('.5')}[winner]
    assert (r['quoted'], r['known'], r['unverified']) == (1, 1, 0)
    assert float(r['pnl']) == pytest.approx(float(payout/rstar.cost_per_share(D('.97'))-1))
    assert all(m[b]['quoted'] == 0 for b in report.SHADOW_LABELS if b != rstar.BRANCH)
