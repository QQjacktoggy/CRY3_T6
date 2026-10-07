"""T6.9a shallow retracement counter-trend floor: report lines only.

Live takes 淺回撤 only when the frozen prior 15-minute move ran against the bet by
at least 5bp (``POLICY['shallow_retracement']['prior_against_min_bp']``). This
module reads the decision table and saved official winners to show both sides of
that floor: the shallow entries Live selected, and the shallow candidates it
skipped with whether they would have won. Nothing is written.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from decimal import Decimal

from .regime_t69a_policy import FINGERPRINT, POLICY

BRANCH = 'shallow_retracement'
SKIP_REASON = 'shallow_prior_not_against_5bp'


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
    result = dict(taken=dict(selected=0, fills=0, settled=0, wins=0, losses=0, pending=0, pnl=Decimal(0)),
                  skipped=dict(count=0, settled=0, would_win=0, would_lose=0, pending=0),
                  unverified=0)
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
            if not isinstance(d, dict):
                raise ValueError('decision payload invalid')
            taken = d.get('selected') is True and d.get('branch') == BRANCH
            skips = [r for r in d.get('rejected_branches') or ()
                     if r.get('branch') == BRANCH and r.get('reason') == SKIP_REASON]
            if not taken and not (d.get('selected') is not True and skips):
                continue
            slot = slots[start]
            if (d.get('fingerprint') != FINGERPRINT or d.get('loop_id') != loop_id
                    or d.get('market_start_ms') != start
                    or str(d.get('market_id')) != str(slot.get('market_id'))
                    or str(d.get('market_topic')) != str(slot.get('market_topic_id'))):
                raise ValueError('shallow decision identity mismatch')
            winners = official.get((str(slot['market_topic_id']), str(slot['market_id']), start, start+300000), set())
            known = start+300000 <= now and len(winners) == 1
            if taken:
                group = result['taken']
                group['selected'] += 1
                group['fills'] += start in filled_starts
                if not known:
                    group['pending'] += 1
                    continue
                pnl = _paper_pnl(d, next(iter(winners)))
                group['settled'] += 1
                group['wins'] += pnl > 0
                group['losses'] += pnl < 0
                group['pnl'] += pnl
                continue
            side = skips[0]['side']
            if side not in ('UP', 'DOWN'):
                raise ValueError('shallow skip side invalid')
            group = result['skipped']
            group['count'] += 1
            if not known:
                group['pending'] += 1
                continue
            winner = next(iter(winners))
            group['settled'] += 1
            group['would_win'] += winner == side
            group['would_lose'] += winner not in (side, 'DRAW')
        except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError, IndexError):
            result['unverified'] += 1
    return result


def _title():
    floor = POLICY['shallow_retracement']['prior_against_min_bp']
    return f'〔淺回撤逆勢條件（前15分逆向≥{floor}bp 才進 Live）〕'


def report_lines(root, *, now, loop_id, slots, official, filled_starts=frozenset()):
    m = metrics(root, now=now, loop_id=loop_id, slots=slots, official=official, filled_starts=filled_starts)
    t, s = m['taken'], m['skipped']
    decisive = t['wins']+t['losses']
    wr = f'{t["wins"]/decisive:.1%}' if decisive else '—'
    pnl = f'{t["pnl"]:+.4f}' if t['settled'] else '—'
    lines = [_title(),
             f'通過（Live）｜選中 {t["selected"]}｜成交 {t["fills"]}｜已知WR {wr}'
             f'（{t["wins"]}勝/{t["losses"]}負）｜假設1U PnL {pnl}｜待結算 {t["pending"]}',
             f'被擋（不下單）｜{s["count"]} 場｜若做會贏 {s["would_win"]}｜會輸 {s["would_lose"]}｜待結算 {s["pending"]}']
    if m['unverified']:
        lines.append(f'  淺回撤決策待核對 {m["unverified"]}；未核對不列收益。')
    return lines


def empty_lines():
    return [_title(),
            '通過（Live）｜選中 0｜成交 0｜已知WR —（0勝/0負）｜假設1U PnL —｜待結算 0',
            '被擋（不下單）｜0 場｜若做會贏 0｜會輸 0｜待結算 0']
