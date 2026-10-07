"""T6.9a shallow retracement counter-trend filter: Shadow report only."""
import json

import pytest

from test_live_report import START
from test_t69a_post_entry import T69A_FINGERPRINT, World
from src.gridbot.prediction import regime_t69a_shallow_filter as shallow
from src.gridbot.prediction.live_report import T69A_PROFILE, format_live_report
from src.gridbot.prediction.regime_t69a_policy import FINGERPRINT, POLICY


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


def shallow_decision(world, start, *, side, prior, shares='1.5', selected=True, branch='shallow_retracement'):
    """Overwrite the position's decision with a shallow selection carrying its frozen prior."""
    signal = json.dumps(dict(entry=dict(action=side, reason='regime_entry', side=side, stake_usdt='1',
                                        expected_shares=shares, cost_after_ev_usdt=None)))
    decision = dict(fingerprint=FINGERPRINT, loop_id='current', market_topic='topic'+str(start),
                    market_id='up'+str(start), market_start_ms=start, end_ms=start+300000,
                    selected=selected, branch=branch, side=side, signal=signal,
                    core_guard=dict(features=dict(prior_bp=prior)))
    world.feature.execute('INSERT OR REPLACE INTO t69a_decisions VALUES(?,?)', (start, json.dumps(decision)))
    world.feature.commit()


def test_live_policy_and_fingerprint_are_untouched():
    assert FINGERPRINT == T69A_FINGERPRINT
    assert 'prior_bp_against_side' not in json.dumps(POLICY)
    assert POLICY['shallow_retracement'] == dict(opposite_minute_sign=True, first_abs_min_bp='1',
                                                 first_abs_to_last_abs_min='2', side='compounded_net',
                                                 price_band=['0.10', '0.75'])
    assert shallow.SHALLOW_FILTER_POLICY['base_fingerprint'] == FINGERPRINT != shallow.SHALLOW_FILTER_FINGERPRINT


@pytest.mark.parametrize('side,prior,expected', [
    ('UP', '-5', True), ('UP', '-4.99', False), ('UP', '8', False),
    ('DOWN', '5', True), ('DOWN', '4.99', False), ('DOWN', '-12', False),
])
def test_filter_needs_prior_against_the_bet_by_5bp(side, prior, expected):
    assert shallow.passes(side, prior) is expected


def test_report_splits_shallow_by_filter_and_never_changes_live(world):
    a, b, c, d = (START+i*300000 for i in range(4))
    # a: UP after a falling prior (passes) and wins; b: DOWN after a falling prior (fails) and loses;
    # c: UP after a rising prior (fails) and wins; d: other lane, ignored.
    world.position(a, side='UP', branch='shallow_retracement', price='.66', shares='1.5', winner='UP', pnl='0.5')
    world.position(b, side='DOWN', branch='shallow_retracement', price='.66', shares='1.5', winner='UP', pnl='-1')
    world.position(c, side='UP', branch='shallow_retracement', price='.66', shares='1.5', winner='UP', pnl='0.5')
    world.position(d, side='DOWN', branch='core_c_down', price='.70', shares='1.4', winner='DOWN', pnl='0.4')
    shallow_decision(world, a, side='UP', prior='-7.2')
    shallow_decision(world, b, side='DOWN', prior='-9')
    shallow_decision(world, c, side='UP', prior='3')
    text = format_live_report(world.root, now_ms=START+1200000, profile_filter=T69A_PROFILE)
    assert '〔淺回撤逆勢條件（前15分逆向≥5bp；只記錄不改Live）〕' in text
    assert '逆勢≥5bp（通過）｜選中 1｜Live成交 1｜已知WR 100.0%（1勝/0負）｜假設1U PnL +0.5000｜待結算 0' in text
    assert '其他（不通過）｜選中 2｜Live成交 2｜已知WR 50.0%（1勝/1負）｜假設1U PnL -0.5000｜待結算 0' in text
    # Live lines still come from official settlements, unchanged by the filter.
    assert '淺回撤｜成交 3｜已知WR 66.7%｜已知PnL +0.0000' in text


def test_unsettled_and_unverified_shallow_decisions_are_not_results(world):
    world.position(START, side='UP', branch='shallow_retracement')
    shallow_decision(world, START, side='UP', prior='-6')
    text = format_live_report(world.root, now_ms=START+200000, profile_filter=T69A_PROFILE)
    assert '逆勢≥5bp（通過）｜選中 1｜Live成交 1｜已知WR —（0勝/0負）｜假設1U PnL —｜待結算 1' in text
    row = json.loads(world.feature.execute('SELECT payload FROM t69a_decisions').fetchone()[0])
    row['fingerprint'] = 'other'
    world.feature.execute('UPDATE t69a_decisions SET payload=?', (json.dumps(row),))
    world.feature.commit()
    text = format_live_report(world.root, now_ms=START+400000, profile_filter=T69A_PROFILE)
    assert '淺回撤決策待核對 1；未核對不列收益。' in text


def test_empty_report_lists_the_filter_lines():
    from src.gridbot.prediction.regime_t69a_report import empty_report
    text = empty_report(START)
    assert '逆勢≥5bp（通過）｜選中 0｜Live成交 0｜已知WR —（0勝/0負）｜假設1U PnL —｜待結算 0' in text
