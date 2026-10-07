"""T6.9a shallow retracement counter-trend floor: report lines for taken and skipped entries."""
import json

import pytest

from test_live_report import START
from test_t69a_post_entry import World
from src.gridbot.prediction import regime_t69a_shallow_filter as shallow
from src.gridbot.prediction.live_report import T69A_PROFILE, format_live_report
from src.gridbot.prediction.regime_t69a_bridge import shallow_prior_against
from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT, POLICY


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


def decision(world, start, **extra):
    d = dict(fingerprint=FINGERPRINT, loop_id='current', market_topic='topic'+str(start),
             market_id='up'+str(start), market_start_ms=start, end_ms=start+300000, **extra)
    world.feature.execute('INSERT OR REPLACE INTO t69a_decisions VALUES(?,?)', (start, json.dumps(d)))
    world.feature.commit()


def taken(world, start, *, side, winner, shares='1.5'):
    world.position(start, side=side, branch='shallow_retracement', price='.66', shares=shares,
                   winner=winner, pnl=str(float(shares)-1 if winner == side else -1))
    signal = json.dumps(dict(entry=dict(action=side, reason='regime_entry', side=side, stake_usdt='1',
                                        expected_shares=shares, cost_after_ev_usdt=None)))
    decision(world, start, selected=True, branch='shallow_retracement', side=side, signal=signal)


def skipped(world, start, *, side, winner, prior='2'):
    """A market Live skipped: registered and officially settled, with no order."""
    cid = 'c'+str(start)
    world.db.execute('INSERT INTO prediction_regime_slots (loop_id,market_start_ms,run_ordinal,verified_at_ms,'
                     'empty_attested_at_ms,market_topic_id,market_id) VALUES(?,?,?,?,NULL,?,?)',
                     ('current', start, (start-START)//300000+1, start, 'topic'+str(start), 'up'+str(start)))
    world.db.execute('INSERT INTO prediction_campaigns VALUES(?,?,?,?,?,?,?,?,?)',
                     (cid, 'current', start, 0, 'topic'+str(start), 'up'+str(start), start+300000, None, '{}'))
    world.db.execute("INSERT INTO prediction_settlements VALUES(?,?,'SETTLED',?,?)", ('s'+cid, cid, '0', winner))
    world.db.commit()
    decision(world, start, selected=False, rejected_branches=[dict(
        branch='shallow_retracement', reason='shallow_prior_not_against_5bp', prior_bp=prior, side=side)])


@pytest.mark.parametrize('side,prior,expected', [
    ('UP', '-5', True), ('UP', '-4.99', False), ('UP', '8', False),
    ('DOWN', '5', True), ('DOWN', '4.99', False), ('DOWN', '-12', False),
])
def test_floor_needs_prior_against_the_bet_by_5bp(side, prior, expected):
    assert POLICY['shallow_retracement']['prior_against_min_bp'] == '5'
    assert shallow_prior_against(side, prior) is expected


def test_report_shows_taken_and_skipped_shallow(world):
    a, b, c, d, e = (START+i*300000 for i in range(5))
    taken(world, a, side='UP', winner='UP')
    taken(world, b, side='DOWN', winner='UP')
    skipped(world, c, side='UP', winner='DOWN')
    skipped(world, d, side='DOWN', winner='DOWN')
    skipped(world, e, side='UP', winner='DOWN')
    text = format_live_report(world.root, now_ms=START+1500000, profile_filter=T69A_PROFILE)
    assert '〔淺回撤逆勢條件（前15分逆向≥5bp 才進 Live）〕' in text
    assert '通過（Live）｜選中 2｜成交 2｜已知WR 50.0%（1勝/1負）｜假設1U PnL -0.5000｜待結算 0' in text
    assert '被擋（不下單）｜3 場｜若做會贏 1｜會輸 2｜待結算 0' in text
    # Live lines still come from official settlements.
    assert '淺回撤｜成交 2｜已知WR 50.0%｜已知PnL -0.5000' in text


def test_unsettled_and_unverified_decisions_are_not_results(world):
    taken(world, START, side='UP', winner=None)
    text = format_live_report(world.root, now_ms=START+200000, profile_filter=T69A_PROFILE)
    assert '通過（Live）｜選中 1｜成交 1｜已知WR —（0勝/0負）｜假設1U PnL —｜待結算 1' in text
    row = json.loads(world.feature.execute('SELECT payload FROM t69a_decisions').fetchone()[0])
    row['fingerprint'] = 'other'
    world.feature.execute('UPDATE t69a_decisions SET payload=?', (json.dumps(row),))
    world.feature.commit()
    text = format_live_report(world.root, now_ms=START+400000, profile_filter=T69A_PROFILE)
    assert '淺回撤決策待核對 1；未核對不列收益。' in text


def test_other_skip_reasons_are_ignored(world):
    world.position(START, side='UP', branch='core_first_up', winner='UP', pnl='1')
    decision(world, START, selected=False, rejected_branches=[dict(
        branch='core_first_up', reason='first_up_prior_below_5bp', prior_bp='2')])
    m = shallow.metrics(world.root, now=START+400000, loop_id='current',
                        slots=[dict(loop_id='current', market_start_ms=START, verified_at_ms=START,
                                    market_id='up'+str(START), market_topic_id='topic'+str(START))],
                        official={})
    assert m['skipped']['count'] == 0 and m['taken']['selected'] == 0 and m['unverified'] == 0


def test_empty_report_lists_the_floor_lines():
    from src.gridbot.prediction.regime_t69a_report import empty_report
    text = empty_report(START)
    assert '通過（Live）｜選中 0｜成交 0｜已知WR —（0勝/0負）｜假設1U PnL —｜待結算 0' in text
    assert '被擋（不下單）｜0 場｜若做會贏 0｜會輸 0｜待結算 0' in text
