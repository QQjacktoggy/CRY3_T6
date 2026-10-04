"""T6.9 Flat F2-F4 Shadow: first observed checkpoints, verified empty core, paper only."""
import json
import sqlite3
from contextlib import closing
from decimal import Decimal as D

import pytest

from src.gridbot.prediction import regime_t69_flat_shadow as flat
from src.gridbot.prediction import regime_t69_shadow as shadow
from src.gridbot.prediction.regime_t69_policy import FINGERPRINT, SHADOW_BRANCHES
from test_t63 import S, feature
from test_t67 import snap, spot


def identity():
    return dict(fingerprint=FINGERPRINT, loop_id='loop', market_topic='topic',
                market_id='up', market_start_ms=S, market_end_ms=S+300000,
                end_ms=S+300000, unit_usdt='1', fee_bps='200')


def core(first='.7', last='-.3', prior=2, empty=True, **changes):
    value = dict(identity(), selected=False, core_guard=dict(
        verified=True, empty=empty, candidates=[] if empty else [dict(branch='core_stall_down')],
        frozen_at_ms=S+124000, initial_captured_at_ms=S+124000, initial_book_at_ms=S+124000,
        fee_bps='200', features=feature(first, last, prior)))
    value.update(changes)
    return value


def flat_spots(t, third='100'):
    return [spot(k) for k in range(0, min(t, 179000)+1, 1000)] + ([spot(180000, third)] if t >= 180000 else [])


class Run:
    def __init__(self, tmp_path, decision):
        self.db = sqlite3.connect(tmp_path/'features.sqlite3')
        shadow.schema(self.db)
        self.decision = decision

    def tick(self, t, up='.7', down='.3', spots=None, book=True):
        books = [snap(t, up=up, down=down)] if book else []
        return flat.observe(self.db, identity(), books, flat_spots(t) if spots is None else spots,
                            S+t, D(1), self.decision)

    def quotes(self):
        return {b: json.loads(p) for b, p in self.db.execute('SELECT branch,payload FROM t69_shadow_quotes')}

    def reason(self, branch):
        row = self.db.execute('SELECT payload FROM t69_flat_shadow_states WHERE start=?', (S,)).fetchone()
        return json.loads(row[0])['routes'][branch].get('reason')


def test_branches_are_appended_to_existing_shadow_inventory():
    assert SHADOW_BRANCHES[-3:] == ('flat_quiet_favorite', 'flat_cheap_prior', 'flat_hold_180')


def test_f2_quotes_stable_favorite_at_confirmation(tmp_path):
    run = Run(tmp_path, core('.7', '-.3', 2))
    run.tick(124000)
    assert not run.quotes()
    run.tick(128000)
    q = run.quotes()['flat_quiet_favorite']
    assert q['side'] == 'UP' and q['quoted_at_ms'] == S+128000 and q['fingerprint'] == FINGERPRINT
    assert q['fill_status'] == 'PAPER_QUOTE_ONLY' and D(q['cash']) <= 1 and q['live_branch'] is None
    assert run.reason('flat_quiet_favorite') == 'quoted'
    # One quote per market: later books never replace it.
    run.tick(129000, up='.65', down='.35')
    assert run.quotes()['flat_quiet_favorite'] == q
    assert run.reason('flat_cheap_prior') == 'cheaper_side_not_aligned_with_prior'
    assert run.reason('flat_hold_180') == 'not_flat'


@pytest.mark.parametrize('first,last,prior,reason', [
    ('.2', '.1', 2, 'flat_favorite_state'),
    ('1', '.1', 2, 'not_quiet'),
    ('.7', '.6', 2, 'not_quiet'),
    ('.7', '-.3', 5, 'not_quiet'),
    ('.7', '-.3', -5, 'not_quiet'),
])
def test_f2_boundaries_and_no_overlap_with_live_flat(tmp_path, first, last, prior, reason):
    run = Run(tmp_path, core(first, last, prior))
    run.tick(124000)
    run.tick(128000)
    assert 'flat_quiet_favorite' not in run.quotes()
    assert run.reason('flat_quiet_favorite') == reason


@pytest.mark.parametrize('confirm,reason', [
    (('.3', '.7'), 'favorite_tie_or_changed'),
    (('.81', '.19'), 'checkpoint_rejected:insufficient_depth'),
    (('.61', '.39'), 'checkpoint_rejected:price_below_lower'),
])
def test_f2_first_confirmation_is_judged_once(tmp_path, confirm, reason):
    run = Run(tmp_path, core())
    run.tick(124000)
    run.tick(128000, *confirm)
    run.tick(128500)
    assert 'flat_quiet_favorite' not in run.quotes()
    assert run.reason('flat_quiet_favorite') == reason


def test_f2_missing_confirmation_never_uses_later_book(tmp_path):
    run = Run(tmp_path, core())
    run.tick(124000)
    run.tick(129600)
    assert run.reason('flat_quiet_favorite') == 'checkpoint_missing'
    assert not run.quotes()


def test_f3_quotes_first_executable_cheaper_side_aligned_with_prior(tmp_path):
    run = Run(tmp_path, core('.3', '.2', 2))
    run.tick(124000, up='.3', down='.7')
    run.tick(128000, up='.45', down='.55')
    assert 'flat_cheap_prior' not in run.quotes()
    run.tick(129000, up='.35', down='.65')
    q = run.quotes()['flat_cheap_prior']
    assert q['side'] == 'UP' and q['quoted_at_ms'] == S+129000 and q['initial_captured_at_ms'] == S+124000
    run.tick(130000, up='.26', down='.74')
    assert run.quotes()['flat_cheap_prior'] == q


def test_f3_down_and_no_executable_quote(tmp_path):
    run = Run(tmp_path, core('-.3', '-.2', -2))
    run.tick(124000)
    run.tick(128000, up='.55', down='.45')
    run.tick(134600)
    assert run.reason('flat_cheap_prior') == 'no_executable_quote'
    assert 'flat_cheap_prior' not in run.quotes()


def test_f3_requires_quiet_net(tmp_path):
    run = Run(tmp_path, core('.7', '.6', 2))
    run.tick(124000, up='.3', down='.7')
    assert run.reason('flat_cheap_prior') == 'not_quiet'


def test_f4_quotes_when_three_minutes_stay_flat(tmp_path):
    run = Run(tmp_path, core('.2', '.1', 2))
    run.tick(120000, up='.75', down='.25')
    run.tick(124000)
    run.tick(180000, up='.8', down='.2')
    q = run.quotes()['flat_hold_180']
    assert q['side'] == 'UP' and q['quoted_at_ms'] == S+180000 and D(q['third_bp']) == 0


@pytest.mark.parametrize('third,at180,reason', [
    ('100.01', ('.8', '.2'), 'third_minute_moved'),
    ('100', ('.2', '.8'), 'favorite_tie_or_changed'),
    ('100', ('.86', '.14'), 'checkpoint_rejected:insufficient_depth'),
])
def test_f4_rejections(tmp_path, third, at180, reason):
    run = Run(tmp_path, core('.2', '.1', 2))
    run.tick(120000, up='.75', down='.25')
    run.tick(124000)
    run.tick(180000, *at180, spots=flat_spots(180000, third))
    assert 'flat_hold_180' not in run.quotes()
    assert run.reason('flat_hold_180') == reason


def test_f4_requires_both_spot_boundaries(tmp_path):
    run = Run(tmp_path, core('.2', '.1', 2))
    run.tick(120000, up='.75', down='.25')
    run.tick(124000)
    run.tick(180000, up='.8', down='.2', spots=[spot(k) for k in range(0, 110000, 1000)])
    assert run.reason('flat_hold_180') == 'third_minute_spot_missing'


def test_nonempty_core_never_quotes(tmp_path):
    run = Run(tmp_path, core(empty=False))
    run.tick(124000)
    run.tick(128000)
    assert not run.quotes()
    assert {run.reason(b) for b in flat.DEADLINES} == {'core_nonempty'}


def test_missing_core_waits_then_terminal(tmp_path):
    run = Run(tmp_path, None)
    run.tick(124000)
    assert run.reason('flat_quiet_favorite') is None
    run.decision = None
    run.tick(127000)
    assert run.reason('flat_quiet_favorite') == 'core_decision_missing'
    run.decision = core()
    run.tick(128000)
    assert not run.quotes()


def test_identity_mismatch_and_live_overlap_is_recorded(tmp_path):
    run = Run(tmp_path, core(loop_id='other'))
    run.tick(124000)
    assert run.reason('flat_quiet_favorite') == 'core_identity_mismatch'
    path = tmp_path/'second'
    path.mkdir()
    run = Run(path, core(selected=True, branch='c_mirror_up_prior'))
    run.tick(124000)
    run.tick(128000)
    assert run.quotes()['flat_quiet_favorite']['live_branch'] == 'c_mirror_up_prior'


def test_stale_or_wrong_market_books_are_not_checkpoints(tmp_path):
    run = Run(tmp_path, core())
    stale = snap(124000)
    stale['book_at_ms'] = S+122000
    other = snap(124500)
    other['market_id'] = 'other'
    flat.observe(run.db, identity(), [stale, other], flat_spots(124500), S+124500, D(1), run.decision)
    run.tick(126100)
    assert run.reason('flat_quiet_favorite') == 'initial_missing'


def test_shadow_observer_runs_flat_routes(tmp_path):
    from test_t69 import paper_db
    from src.gridbot.prediction.regime_t67_evidence import EvidenceStore, evidence_path
    from src.gridbot.prediction.regime_feature_service import connect
    pred, sig = tmp_path/'pred', tmp_path/'signals'
    paper_db(pred)
    before = pred.read_bytes()
    with closing(connect(tmp_path/'features')) as db:
        db.execute('CREATE TABLE IF NOT EXISTS t69_decisions(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
        db.execute('INSERT INTO t69_decisions VALUES(?,?)', (S, json.dumps(core(loop_id='testloop'))))
        db.commit()
        for t in (124000, 128000):
            with closing(EvidenceStore(evidence_path(sig))) as evidence:
                evidence.book(snap(t))
                with evidence.db:
                    evidence.db.executemany('INSERT OR IGNORE INTO spot VALUES(?,?,?,?,?)',
                        [(s['source'], s['generation'], s['event_ms'], s['received_ms'], s['price']) for s in flat_spots(t)])
            shadow.observe(db, pred, sig, S+t)
        branches = {r[0] for r in db.execute('SELECT branch FROM t69_shadow_quotes')}
        assert 'flat_quiet_favorite' in branches
    assert pred.read_bytes() == before


def test_initial_not_yet_observed_stays_pending(tmp_path):
    run = Run(tmp_path, core())
    run.tick(124000, book=False)
    assert run.reason('flat_quiet_favorite') is None
    run.tick(125000)
    run.tick(128000)
    assert run.reason('flat_quiet_favorite') == 'quoted'


def test_f1_flat_favorite_is_a_shadow_quote(tmp_path):
    run = Run(tmp_path, core('.2', '-.1', 2))
    run.tick(124000)
    run.tick(128000)
    q = run.quotes()['flat_favorite']
    assert q['side'] == 'UP' and q['quoted_at_ms'] == S+128000 and q['fill_status'] == 'PAPER_QUOTE_ONLY'
    assert (q['lower'], q['upper']) == ('0.65', '0.80')
    assert run.reason('flat_quiet_favorite') == 'flat_favorite_state'


@pytest.mark.parametrize('first,last,confirm,reason', [
    ('.5', '0', ('.7', '.3'), 'not_flat'),
    ('.1', '-.5', ('.7', '.3'), 'not_flat'),
    ('.2', '.1', ('.3', '.7'), 'favorite_tie_or_changed'),
    ('.2', '.1', ('.64', '.36'), 'checkpoint_rejected:price_below_lower'),
])
def test_f1_boundaries_and_single_judgement(tmp_path, first, last, confirm, reason):
    run = Run(tmp_path, core(first, last, 2))
    run.tick(124000)
    run.tick(128000, *confirm)
    run.tick(128500)
    assert 'flat_favorite' not in run.quotes()
    assert run.reason('flat_favorite') == reason
