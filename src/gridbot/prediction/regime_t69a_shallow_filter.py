"""T6.9a shallow retracement counter-trend filter: Shadow, report only.

Splits this loop's selected 淺回撤 decisions by whether the prior 15-minute move
(the frozen ``prior_bp`` feature) ran against the bet by at least 5bp, then shows
each group's official result. Nothing is written and Live selection is unchanged:
this module only reads the decision table and saved official winners.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from decimal import Decimal

from .regime_lane import dec
from .regime_t69a_policy import FINGERPRINT

SHALLOW_FILTER = dict(
    branch='shallow_retracement', feature='core_guard.features.prior_bp',
    rule='prior_bp_against_side', against_min_bp='5',
    mode='report_only_no_writes_no_live_effect',
)
SHALLOW_FILTER_POLICY = dict(base_fingerprint=FINGERPRINT, version=1, shallow_filter=SHALLOW_FILTER)
SHALLOW_FILTER_FINGERPRINT = hashlib.sha256(json.dumps(SHALLOW_FILTER_POLICY, sort_keys=True).encode()).hexdigest()
GROUPS = (('pass', '逆勢≥5bp（通過）'), ('fail', '其他（不通過）'))


def passes(side, prior_bp):
    """True when the prior move ran against ``side`` by at least the threshold."""
    floor = dec(SHALLOW_FILTER['against_min_bp'])
    prior = dec(prior_bp)
    if side == 'UP':
        return prior <= -floor
    if side == 'DOWN':
        return prior >= floor
    raise ValueError('shallow side invalid')


def _paper_pnl(d, winner):
    """Per 1U result at the selected quote's fee-net shares."""
    entry = json.loads(d['signal'])['entry']
    stake, shares = Decimal(entry['stake_usdt']), Decimal(entry['expected_shares'])
    if stake <= 0 or shares <= 0:
        raise ValueError('shallow paper amounts invalid')
    payout = shares/2 if winner == 'DRAW' else shares if winner == d['side'] else Decimal(0)
    return (payout-stake)/stake


def metrics(root, *, now, loop_id, slots, official, filled_starts=frozenset()):
    from .loop_market import report_feature_path
    result = {g: dict(candidates=0, fills=0, settled=0, wins=0, losses=0, pending=0, pnl=Decimal(0))
              for g, _ in GROUPS}
    result['unverified'] = 0
    slots = {int(s['market_start_ms']): s for s in slots if s.get('loop_id') == loop_id
             and s.get('verified_at_ms') is not None and int(s['market_start_ms']) <= now}
    path = report_feature_path(root, loop_id)
    if not path.is_file() or not slots:
        return result
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t69a_decisions'").fetchone():
            return result
        starts = tuple(slots)
        rows = [(int(s), p) for s, p in db.execute(
            'SELECT start,payload FROM t69a_decisions WHERE start IN ('+','.join('?' for _ in starts)+')', starts)]
    for start, raw in rows:
        try:
            d = json.loads(raw)
            if not isinstance(d, dict) or d.get('selected') is not True or d.get('branch') != SHALLOW_FILTER['branch']:
                continue
            slot = slots[start]
            if (d.get('fingerprint') != FINGERPRINT or d.get('loop_id') != loop_id
                    or d.get('market_start_ms') != start
                    or str(d.get('market_id')) != str(slot.get('market_id'))
                    or str(d.get('market_topic')) != str(slot.get('market_topic_id'))):
                raise ValueError('shallow decision identity mismatch')
            group = result['pass' if passes(d['side'], d['core_guard']['features']['prior_bp']) else 'fail']
            group['candidates'] += 1
            group['fills'] += start in filled_starts
            winners = official.get((str(slot['market_topic_id']), str(slot['market_id']), start, start+300000), set())
            if start+300000 > now or len(winners) != 1:
                group['pending'] += 1
                continue
            pnl = _paper_pnl(d, next(iter(winners)))
            group['settled'] += 1
            group['wins'] += pnl > 0
            group['losses'] += pnl < 0
            group['pnl'] += pnl
        except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError):
            result['unverified'] += 1
    return result


def report_lines(root, *, now, loop_id, slots, official, filled_starts=frozenset()):
    m = metrics(root, now=now, loop_id=loop_id, slots=slots, official=official, filled_starts=filled_starts)
    lines = ['〔淺回撤逆勢條件（前15分逆向≥5bp；只記錄不改Live）〕']
    for key, label in GROUPS:
        g = m[key]
        decisive = g['wins']+g['losses']
        wr = f'{g["wins"]/decisive:.1%}' if decisive else '—'
        pnl = f'{g["pnl"]:+.4f}' if g['settled'] else '—'
        lines.append(f'{label}｜選中 {g["candidates"]}｜Live成交 {g["fills"]}｜已知WR {wr}'
                     f'（{g["wins"]}勝/{g["losses"]}負）｜假設1U PnL {pnl}｜待結算 {g["pending"]}')
    if m['unverified']:
        lines.append(f'  淺回撤決策待核對 {m["unverified"]}；未核對不列收益。')
    return lines


def empty_lines():
    lines = ['〔淺回撤逆勢條件（前15分逆向≥5bp；只記錄不改Live）〕']
    lines += [f'{label}｜選中 0｜Live成交 0｜已知WR —（0勝/0負）｜假設1U PnL —｜待結算 0' for _, label in GROUPS]
    return lines
