"""Current-loop T6.9a Live ledger and separate verified Shadow quotes."""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from decimal import Decimal

LIVE_LABELS = {
    'core_first_down': 'first DOWN',
    'core_first_up': 'first UP（前15分≥5bp）',
    'core_stall_down': 'stall DOWN',
    'core_c_down': '原 C DOWN',
    'core_continuation_original': 'continuation Original',
    'c_mirror_up_prior': 'C-UP 前趨勢鏡像',
    'shallow_retracement': '淺回撤',
}
SHADOW_LABELS = {
    'flat_favorite': 'F1 Flat 熱門方',
    'flat_quiet_favorite': 'F2 安靜熱門方',
    'flat_cheap_prior': 'F3 便宜方順前趨勢',
    'flat_hold_180': 'F4 180s 續橫盤熱門方',
    'late_favourite_chase': 'R* 尾盤熱門方（.90-.98，z≥3）',
    'late_favourite_chase_99': 'R*-99 尾盤熱門方（.99，z≥5）',
}
LIVE_GROUPS = (
    ('核心', ('core_first_down', 'core_first_up', 'core_stall_down', 'core_c_down', 'core_continuation_original')),
    ('增量', ('c_mirror_up_prior', 'shallow_retracement')),
)
SHADOW_GROUPS = (
    ('Flat F1–F4', ('flat_favorite', 'flat_quiet_favorite', 'flat_cheap_prior', 'flat_hold_180')),
    ('尾盤 R*', ('late_favourite_chase', 'late_favourite_chase_99')),
)
# Paper quote clock window per Shadow branch, relative to market start.
SHADOW_QUOTE_MS = {'late_favourite_chase': (270000, 295000), 'late_favourite_chase_99': (270000, 295000)}
HEADER = '📊 T6.9a Report｜七路 Live（T6.7c＋First UP 5bp）＋四路 Flat Shadow＋R* Shadow'
LIVE_SIDES = {
    'core_first_down': {'DOWN'}, 'core_first_up': {'UP'}, 'core_stall_down': {'DOWN'},
    'core_c_down': {'DOWN'}, 'core_continuation_original': {'UP', 'DOWN'},
    'c_mirror_up_prior': {'UP'}, 'shallow_retracement': {'UP', 'DOWN'},
}


def _identity(payload, slot, loop_id, fingerprint):
    start = int(slot['market_start_ms'])
    return (payload.get('fingerprint') == fingerprint
            and payload.get('loop_id') == loop_id
            and slot.get('verified_at_ms') is not None
            and bool(slot.get('market_id')) and bool(slot.get('market_topic_id'))
            and str(payload.get('market_topic')) == str(slot['market_topic_id'])
            and str(payload.get('market_id')) == str(slot['market_id'])
            and payload.get('market_start_ms') == start
            and payload.get('end_ms', payload.get('market_end_ms')) == start+300000)


def _campaign_up_id(campaign):
    if campaign.get('market_id'):
        return str(campaign['market_id'])
    market = json.loads(campaign.get('payload_json') or '{}').get('market', {})
    if (str(market.get('market_topic_id')) != str(campaign.get('market_topic_id'))
            or market.get('start_time_ms') != campaign.get('start_time_ms')
            or market.get('end_time_ms') != campaign.get('end_time_ms')):
        return None
    return str(market.get('up_market_id') or market.get('upMarketId') or '') or None


def branch_metrics(root, campaigns, current_ids, fill_ids, events, *, fingerprint, slots, loop_id):
    from .live_report import _metrics
    result = {b: dict(fills=0, pending=0, events=[]) for b in LIVE_LABELS}
    result['unattributed'] = 0
    filled = fill_ids & current_ids
    decisions = {}
    fill_sides = {}
    if filled:
        from .loop_market import report_feature_path
        path = report_feature_path(root, loop_id)
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            starts = tuple(int(campaigns[cid]['start_time_ms']) for cid in filled)
            decisions = {int(r[0]): json.loads(r[1]) for r in db.execute(
                'SELECT start,payload FROM t69a_decisions WHERE start IN ('+','.join('?' for _ in starts)+')', starts)}
        with closing(sqlite3.connect((root/'prediction/data/prediction.sqlite3').resolve().as_uri()+'?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            for cid, side in db.execute("SELECT campaign_id,outcome FROM prediction_fills WHERE order_side='BUY'"):
                if cid in filled:
                    fill_sides.setdefault(cid, set()).add(side)
    admissions = {int(s['market_start_ms']): s for s in slots}
    known = {e['cid']: e for e in events}
    for cid in filled:
        campaign = campaigns[cid]
        start = int(campaign['start_time_ms'])
        decision = decisions.get(start, {})
        branch = decision.get('branch')
        slot = admissions.get(start)
        if (slot is None or not _identity(decision, slot, loop_id, fingerprint)
                or decision.get('selected') is not True or branch not in LIVE_LABELS
                or decision.get('side') not in LIVE_SIDES[branch]
                or str(campaign.get('market_topic_id')) != str(slot['market_topic_id'])
                or campaign.get('end_time_ms') != start+300000
                or _campaign_up_id(campaign) != str(slot['market_id'])
                or fill_sides.get(cid) != {decision['side']}
                or (campaign.get('initial_outcome') and campaign['initial_outcome'] != decision['side'])):
            result['unattributed'] += 1
            continue
        result[branch]['fills'] += 1
        if cid in known:
            result[branch]['events'].append(known[cid])
        else:
            result[branch]['pending'] += 1
    for branch in LIVE_LABELS:
        result[branch].update(_metrics(result[branch].pop('events')))
    return result


def _official_winners(root, loop_id, slots):
    """Independent saved official winners, keyed by complete market identity."""
    official = {}
    slots = [s for s in slots if s.get('verified_at_ms') is not None and s.get('loop_id') == loop_id]
    starts = tuple(int(s['market_start_ms']) for s in slots)
    if not starts:
        return official
    path = root/'prediction/data/prediction.sqlite3'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        settlement_columns = {r[1] for r in db.execute('PRAGMA table_info(prediction_settlements)')}
        campaign_columns = {r[1] for r in db.execute('PRAGMA table_info(prediction_campaigns)')}
        if 'winner' in settlement_columns and {'market_topic_id', 'market_id', 'end_time_ms'} <= campaign_columns:
            for row in db.execute(
                    "SELECT c.*,s.winner "
                    "FROM prediction_campaigns c JOIN prediction_settlements s ON s.campaign_id=c.campaign_id "
                    "WHERE c.loop_id=? AND s.status='SETTLED' AND c.start_time_ms IN ("
                    +','.join('?' for _ in starts)+')', (loop_id, *starts)):
                if row['winner'] in ('UP', 'DOWN', 'DRAW'):
                    up_id = _campaign_up_id(dict(row))
                    if not up_id:
                        raise ValueError('official campaign UP identity unavailable')
                    key = (str(row['market_topic_id']), up_id, int(row['start_time_ms']), int(row['end_time_ms']))
                    official.setdefault(key, set()).add(row['winner'])
        columns = {r[1] for r in db.execute('PRAGMA table_info(prediction_shadow_observer_markets)')}
        starts = tuple(int(s['market_start_ms']) for s in slots
                       if s.get('verified_at_ms') is not None and s.get('loop_id') == loop_id)
        if starts and {'market_topic_id', 'market_id', 'start_time_ms', 'end_time_ms', 'winner', 'payload_json'} <= columns:
            for row in db.execute("SELECT * FROM prediction_shadow_observer_markets WHERE state='SETTLED' "
                                  "AND start_time_ms IN ("+','.join('?' for _ in starts)+')', starts):
                if row['winner'] not in ('UP', 'DOWN', 'DRAW'):
                    continue
                up_id = row['market_id']
                if not up_id:
                    from .models import MarketInfo
                    from .worker import PredictionWorker
                    detail = json.loads(row['payload_json'])
                    market = MarketInfo.from_api(detail)
                    detail_winner = PredictionWorker._official_shadow_resolution(detail)
                    if (str(market.market_topic_id) != str(row['market_topic_id'])
                            or market.start_time_ms != row['start_time_ms']
                            or market.end_time_ms != row['end_time_ms']
                            or detail_winner not in (None, row['winner'])):
                        raise ValueError('official observer identity conflict')
                    up_id = market.up_market_id
                key = (str(row['market_topic_id']), str(up_id), int(row['start_time_ms']), int(row['end_time_ms']))
                official.setdefault(key, set()).add(row['winner'])
    return official


def shadow_metrics(root, *, now, loop_id, slots, fingerprint):
    from .live_report import _decimal
    result = {b: dict(quoted=0, known=0, unknown=0, wins=0, losses=0, flats=0,
                      unverified=0, pnl=Decimal(0), wr='—', cash=Decimal(0), shares=Decimal(0),
                      breakeven='—') for b in SHADOW_LABELS}
    slots = [s for s in slots if s.get('loop_id') == loop_id
             and s.get('verified_at_ms') is not None and int(s['verified_at_ms']) <= now
             and int(s['market_start_ms']) <= now]
    starts = tuple(int(s['market_start_ms']) for s in slots)
    from .loop_market import report_feature_path
    path = report_feature_path(root, loop_id)
    if not path.is_file() or not starts:
        return result
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 't69a_shadow_quotes' not in tables:
            return result
        selected_starts = ' WHERE start IN ('+','.join('?' for _ in starts)+')'
        quotes = [(int(s), b, json.loads(p)) for s, b, p in db.execute('SELECT start,branch,payload FROM t69a_shadow_quotes'+selected_starts, starts)]
        outcomes = ({int(s): json.loads(p) for s, p in db.execute('SELECT start,payload FROM t69a_shadow_outcomes'+selected_starts, starts)}
                    if 't69a_shadow_outcomes' in tables else {})
    admissions = {int(s['market_start_ms']): s for s in slots}
    official = _official_winners(root, loop_id, slots)
    for start, branch, quote in quotes:
        if branch not in SHADOW_LABELS or quote.get('loop_id') != loop_id:
            continue
        metric = result[branch]
        try:
            slot = admissions[start]
            if not _identity(quote, slot, loop_id, fingerprint) or quote.get('side') not in ('UP', 'DOWN'):
                raise ValueError('paper quote identity mismatch')
            at, book_at, received = (int(quote[k]) for k in ('quoted_at_ms', 'book_at_ms', 'source_receive_ms'))
            low, high = SHADOW_QUOTE_MS.get(branch, (60000, 270000))
            if (not start+low <= at < start+high or at > now
                    or not book_at <= received <= at or not 0 <= at-book_at <= 1000):
                raise ValueError('paper quote clock invalid')
            cash, shares, unit = (_decimal(quote[k]) for k in ('cash', 'net_shares', 'unit_usdt'))
            if (unit not in (1, 2, 3) or not 0 < cash <= unit or shares <= 0
                    or not 0 <= _decimal(quote['fee_bps']) <= 10000):
                raise ValueError('paper quote amounts invalid')
            metric['quoted'] += 1
            metric['cash'] += cash
            metric['shares'] += shares
            outcome = outcomes.get(start)
            if not outcome or outcome.get('complete') is not True or int(outcome.get('known_at_ms', now+1)) > now:
                metric['unknown'] += 1
                continue
            if (not _identity(outcome, slot, loop_id, fingerprint)
                    or outcome.get('winner') not in ('UP', 'DOWN', 'DRAW')
                    or outcome.get('final_side') != outcome['winner']
                    or outcome.get('official_status') not in ('CLOSED', 'RESOLVED', 'SETTLED')
                    or int(outcome['known_at_ms']) < start+300000):
                raise ValueError('paper official outcome identity mismatch')
            winner = outcome['winner']
            key = (str(slot['market_topic_id']), str(slot['market_id']), start, start+300000)
            evidence = official.get(key, set())
            if evidence and evidence != {winner}:
                raise ValueError('conflicting official paper winner')
            pnl = (shares/2 if winner == 'DRAW' else shares if winner == quote['side'] else Decimal(0))-cash
            metric['known'] += 1
            metric['wins'] += pnl > 0
            metric['losses'] += pnl < 0
            metric['flats'] += pnl == 0
            metric['pnl'] += pnl
        except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError):
            metric['unverified'] += 1
    for metric in result.values():
        metric['unknown'] = metric['quoted']-metric['known']
        decisive = metric['wins']+metric['losses']
        metric['wr'] = f"{metric['wins']/decisive:.1%}" if decisive else '—'
        # Fee-net cost per share is the win rate at which paper PnL is zero.
        metric['breakeven'] = f"{metric['cash']/metric['shares']:.1%}" if metric['shares'] > 0 else '—'
    return result


def _grouped(groups, line):
    result = []
    for title, branches in groups:
        result.append(f'〔{title}〕')
        result += [line(branch) for branch in branches]
    return result


def empty_report(now):
    from .live_report import TZ
    clock = datetime.fromtimestamp(now/1000, TZ).strftime('%m/%d %H:%M:%S')
    lines = [HEADER, f'截至 {clock}（台灣時間）',
             '尚未建立 T6.9a Live 輪次；尚未開跑。',
             'Live fill rate／WR／PnL：—（尚無本輪資料）', '', 'Live 子策略（本輪）：']
    lines += _grouped(LIVE_GROUPS, lambda branch: f'{LIVE_LABELS[branch]}｜成交 0｜已知WR —｜已知PnL —｜待結算 0')
    lines += ['', 'Shadow（本輪報價研究）：']
    lines += _grouped(SHADOW_GROUPS, lambda branch: f'{SHADOW_LABELS[branch]}｜報價 0｜已知paper WR —｜假設paper PnL —｜未知 0')
    lines.append('Shadow報價不等於實際成交；假設收益不併入Live。')
    return '\n'.join(lines)


def _shared_block_events(root, *, start, end, now):
    """Read fee-net Live observations across the existing shared risk lane."""
    from .live_report import RISK_PROFILES, SLOT, TERMINAL, _decimal
    from collections import defaultdict
    path = root/'prediction/data/prediction.sqlite3'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        terminal = ','.join("'"+s+"'" for s in sorted(TERMINAL))
        intent_exposure = ("EXISTS(SELECT 1 FROM prediction_order_intents i WHERE i.campaign_id=c.campaign_id "
                           "AND (i.unknown=1 OR COALESCE(i.status,'') NOT IN ("+terminal+")))")
        order_exposure = ("EXISTS(SELECT 1 FROM prediction_orders o WHERE o.campaign_id=c.campaign_id "
                          "AND COALESCE(o.status,'') NOT IN ("+terminal+"))")
        buy_fill = "EXISTS(SELECT 1 FROM prediction_fills f WHERE f.campaign_id=c.campaign_id AND f.order_side='BUY')"
        campaigns = [dict(r) for r in db.execute(
            "SELECT c.*,"+buy_fill+" AS has_buy_fill,"+intent_exposure+" AS unresolved_intent,"+
            order_exposure+" AS unresolved_order FROM prediction_campaigns c JOIN prediction_loops l ON l.loop_id=c.loop_id "
            "WHERE l.mode='LIVE' AND l.strategy_profile IN ("+','.join('?' for _ in RISK_PROFILES)+") "
            "AND c.start_time_ms>=? AND c.start_time_ms<? AND ("+buy_fill+" OR c.pending_unknown=1 OR "+
            intent_exposure+" OR "+order_exposure+")",
            (*RISK_PROFILES, start, end))]
        claims, settlements = defaultdict(list), defaultdict(list)
        for c in campaigns:
            claims[c['campaign_id']] = [dict(r) for r in db.execute(
                'SELECT * FROM prediction_regime_entry_claims WHERE campaign_id=?', (c['campaign_id'],))]
            settlements[c['campaign_id']] = [dict(r) for r in db.execute(
                "SELECT s.*,o.net_pnl AS observed_net,o.known_at_ms FROM prediction_settlements s "
                "LEFT JOIN prediction_regime_settlement_observations o ON o.settlement_id=s.settlement_id "
                "AND o.campaign_id=s.campaign_id WHERE s.campaign_id=? AND s.status='SETTLED'",
                (c['campaign_id'],))]
    events, pending, unverified = [], {}, {}
    for c in campaigns:
        cid = c['campaign_id']
        q, s = claims[cid], settlements[cid]
        try:
            if (c['pending_unknown'] or c['unresolved_intent'] or c['unresolved_order'] or not c['has_buy_fill']):
                raise ValueError('unresolved shared order exposure')
            if (len(q) != 1 or q[0]['loop_id'] != c['loop_id']
                    or q[0]['market_start_ms'] != c['start_time_ms'] or not q[0]['intent_id']
                    or (c['start_time_ms']-start) % SLOT or len(s) > 1):
                raise ValueError('invalid shared risk provenance')
            if not s:
                pending[cid] = c['start_time_ms']
                continue
            row = s[0]
            pnl, unit = _decimal(row['net_pnl']), _decimal(q[0]['unit_usdt'])
            if (pnl != _decimal(row['observed_net']) or unit not in (1, 2, 3)
                    or row['known_at_ms'] is None or not c['start_time_ms']+SLOT <= int(row['known_at_ms']) <= now):
                raise ValueError('unconfirmed shared risk observation')
            events.append(dict(cid=cid, start=c['start_time_ms'], id=row['settlement_id'],
                               known=int(row['known_at_ms']), pnl=pnl/unit))
        except (ValueError, TypeError, KeyError, ArithmeticError):
            unverified[cid] = c['start_time_ms']
    return events, pending, unverified


def _current_report_scope(*, now, slots, campaigns, current_ids, fill_ids, events):
    """Unverified registrations and impossible settlement times are not results."""
    from .live_report import SLOT
    verified = {int(s['market_start_ms']): s for s in slots
                if s['verified_at_ms'] is not None and int(s['verified_at_ms']) <= now
                and int(s['market_start_ms']) <= now}
    actual = fill_ids & current_ids
    matched = set()
    for cid in actual:
        c = campaigns[cid]
        s = verified.get(int(c['start_time_ms']))
        if (s is not None and s['loop_id'] == c['loop_id']
                and s.get('market_topic_id') == c.get('market_topic_id')
                and s.get('market_id') and str(s['market_id']) == _campaign_up_id(c)
                and int(c['end_time_ms']) == int(c['start_time_ms'])+SLOT):
            matched.add(cid)
    valid = [e for e in events if e['cid'] in matched
             and e['start'] == int(campaigns[e['cid']]['start_time_ms'])
             and e['start']+SLOT <= e['known'] <= now]
    return valid, matched, actual-matched, {e['cid'] for e in events}-{e['cid'] for e in valid}


def scheduled_run_summary(root, *, now, slots, campaigns, current_ids, fill_ids, events, gate):
    """Keep fixed risk boundaries while showing only this loop's performance."""
    from .live_report import SLOT, TZ, _metrics, _value
    from .regime_lane import FINGERPRINT as RISK_FP
    lines = ['', '固定每20 run總結（含跳過場；跨loop沿用風控起點）：']
    try:
        events, filled, unmatched, _ = _current_report_scope(
            now=now, slots=slots, campaigns=campaigns, current_ids=current_ids,
            fill_ids=fill_ids, events=events)
        if not isinstance(gate, dict) or gate.get('fingerprint') != RISK_FP:
            raise ValueError('risk epoch unavailable')
        anchor = int(gate['first_market_start_ms'])
        if anchor <= 0 or anchor % SLOT:
            raise ValueError('invalid risk epoch')
        registered = {int(s['market_start_ms']) for s in slots
                      if s['verified_at_ms'] is not None and int(s['verified_at_ms']) <= now
                      and int(s['market_start_ms']) <= now}
        if not registered:
            return lines+[f'尚無已驗證登錄市場｜未驗證成交待核對{len(unmatched)}；不隱藏實際成交。']
        if any(s < anchor or (s-anchor) % SLOT for s in registered):
            raise ValueError('market off risk grid')
        first, last = min(registered), max(registered)
        first_block, last_block = (first-anchor)//SLOT//20, (last-anchor)//SLOT//20
        begin = max(first_block, last_block-4)
        if begin > first_block:
            lines.append(f'共{last_block-first_block+1}段；顯示最近5段。')
        start, end = anchor+begin*20*SLOT, anchor+(last_block+1)*20*SLOT
        shared, shared_pending, unverified = _shared_block_events(root, start=start, end=end, now=now)
        known = {e['cid'] for e in events}
        def in_block(cid, a, b):
            return a <= int(campaigns[cid]['start_time_ms']) < b
        for block in range(begin, last_block+1):
            a, b = anchor+block*20*SLOT, anchor+(block+1)*20*SLOT
            clock = '–'.join(datetime.fromtimestamp(t/1000, TZ).strftime('%m/%d %H:%M') for t in (a, b))
            elapsed = min(20, max(0, (now-a)//SLOT))
            phase = '時段已結束' if now >= b else '進行中'
            lines.append(f'第{block*20+1}–{(block+1)*20} run｜{clock}｜{phase} {elapsed}/20')
            ended = {s for s in registered if a <= s < b and s+SLOT <= now}
            batch_fills = {cid for cid in filled if in_block(cid, a, b)}
            batch_unmatched = sum(in_block(cid, a, b) for cid in unmatched)
            closed_fills = sum(int(campaigns[cid]['start_time_ms']) in ended for cid in batch_fills)
            fill_text = f'{closed_fills/len(ended):.1%}（{closed_fills}/{len(ended)}）' if ended else '—'
            batch = [e for e in events if a <= e['start'] < b]
            pending = len(batch_fills-known)
            metric = _metrics(batch)
            pnl_text = _value(metric,batch,pending) if ended or batch_fills else '—（無本輪登錄）'
            mdd_text = _value(metric,batch,pending,'mdd') if ended or batch_fills else '—'
            lines.append(f'  本輪登錄已結{len(ended)}場｜成交{len(batch_fills)}｜Fill {fill_text}｜待結算/核對{pending}')
            if batch_unmatched:
                lines.append(f'  未驗證成交待核對{batch_unmatched}；不計入區段績效或成交率')
            lines.append(f'  WR {metric["wr"]}（{metric["wins"]}勝/{metric["losses"]}負）｜'
                         f'已知PnL {pnl_text} USDT｜MDD {mdd_text} USDT')
            risk_batch = [e for e in shared if a <= e['start'] < b]
            bad = sum(a <= t < b for t in unverified.values())
            risk_pending = sum(a <= t < b for t in shared_pending.values())
            if bad:
                lines.append('  共用風控MDD 待核對 / 3.5U；不推定通過')
            else:
                rm = _metrics(risk_batch)
                lines.append(f'  共用風控已知MDD {_value(rm,risk_batch,risk_pending,"mdd")} / 3.5U（1U等值）')
        lines.append('本輪績效僅含T6.9a此loop；共用風控MDD含同區段其他T6 Live，Shadow不計入。')
        if shared_pending or unverified:
            lines.append(f'區段共用風控待結算{len(shared_pending)}｜待核對{len(unverified)}；未知結果不補零。')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        lines.append('20 run區段待核對；不推定為已通過。')
    return lines


def format_summary(root, *, now, loop, slots, campaigns, current_ids, fill_ids,
                   events, claims, pending, inflight, unknown, gate, loop_guard, hs, issues,
                   selected_unit=None):
    from .live_report import SLOT, TZ, _metrics, _value
    from .regime_lane import FINGERPRINT as RISK_FP
    from .regime_t69a_policy import FINGERPRINT
    events, verified_fills, unmatched, rejected_events = _current_report_scope(
        now=now, slots=slots, campaigns=campaigns, current_ids=current_ids,
        fill_ids=fill_ids, events=events)
    if unmatched:
        issues.add(f'成交市場登錄待核對{len(unmatched)}')
    if rejected_events-unmatched:
        issues.add(f'結算觀測時間待核對{len(rejected_events-unmatched)}')
    pending = len((fill_ids & current_ids)-{e['cid'] for e in events})
    metric = _metrics(events)
    clock = datetime.fromtimestamp(now/1000, TZ).strftime('%m/%d %H:%M:%S')
    units = '/'.join(sorted({str(q['unit_usdt']) for q in claims})) or '待成交確認'
    if not claims and isinstance(selected_unit, dict):
        chosen = str(selected_unit.get('order_unit_usdt') or '')
        if chosen in ('1', '2', '3'):
            units = chosen+'（目前設定；待成交確認）'
    ended = {int(s['market_start_ms']) for s in slots
             if s['verified_at_ms'] is not None and int(s['verified_at_ms']) <= now
             and int(s['market_start_ms'])+SLOT <= now}
    filled = {int(campaigns[cid]['start_time_ms']) for cid in verified_fills}
    closed_fills = len(filled & ended)
    fill_text = f'{closed_fills/len(ended):.1%}（{closed_fills}/{len(ended)} 已結束登錄市場）' if ended else '—（尚無已結束登錄市場）'
    pnl = _value(metric, events, pending) if events or pending else '—'
    mdd = _value(metric, events, pending, 'mdd') if events or pending else '—'
    from .loop_market import report_asset
    asset = report_asset(root, loop['loop_id'])
    lines = [HEADER, '市場：'+(asset or '歷史未綁定'), f'截至 {clock}（台灣時間）｜Loop {loop["loop_id"]}',
             f'狀態 {loop["state"]}｜完成 {loop["completed"]}/{loop["target"]} 場｜本輪每筆 {units} USDT',
             f'Live fill rate {fill_text}',
             f'本輪 WR {metric["wr"]}（{metric["wins"]}勝/{metric["losses"]}負/{metric["flats"]}平；已結算成交 {len(events)}）',
             f'本輪已知淨 PnL {pnl} USDT｜MDD {mdd} USDT',
             f'入場intent {len(claims)}｜送單嘗試 {sum(q["submission_at_ms"] is not None or q["order_id"] is not None for q in claims)}｜成交市場 {len(filled)}',
             f'待結算/核對 {pending}｜未終結intent {inflight}｜UNKNOWN市場 {unknown}',
             '', 'Live 子策略（本輪）：']
    try:
        branches = branch_metrics(root, campaigns, current_ids, verified_fills, events,
                                  fingerprint=FINGERPRINT, slots=slots, loop_id=loop['loop_id'])
        def live_line(branch):
            m = branches[branch]
            branch_pnl = f'{m["pnl"]:+.4f}' if m['wins']+m['losses']+m['flats'] else '—'
            return f'{LIVE_LABELS[branch]}｜成交 {m["fills"]}｜已知WR {m["wr"]}｜已知PnL {branch_pnl} USDT｜待結算 {m["pending"]}'
        lines += _grouped(LIVE_GROUPS, live_line)
        if branches['unattributed']:
            lines.append(f'子策略歸因待核對 {branches["unattributed"]} 筆；保留官方Live總PnL。')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        lines += [f'{title}｜成交待核對｜已知WR —｜已知PnL —' for title in LIVE_LABELS.values()]
        lines.append('T6.9a 子策略歸因待核對；保留官方Live總PnL。')
    lines += scheduled_run_summary(root, now=now, slots=slots, campaigns=campaigns,
                                   current_ids=current_ids, fill_ids=fill_ids, events=events, gate=gate)
    lines += ['', 'Shadow（本輪報價研究）：']
    try:
        shadows = shadow_metrics(root, now=now, loop_id=loop['loop_id'], slots=slots, fingerprint=FINGERPRINT)
        for group, members in SHADOW_GROUPS:
            lines.append(f'〔{group}〕')
            for branch in members:
                m = shadows[branch]
                paper_pnl = f'{m["pnl"]:+.4f} USDT' if m['known'] else '—'
                lines.append(f'{SHADOW_LABELS[branch]}｜報價 {m["quoted"]}｜已知paper WR {m["wr"]}｜假設paper PnL {paper_pnl}｜未知 {m["unknown"]}')
                if m['quoted']:
                    lines.append(f'  兩平WR {m["breakeven"]}（含費平均成本）')
                if m['unverified']:
                    lines.append(f'  報價/官方勝方待核對 {m["unverified"]}；未核對結果不列收益。')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        lines += [f'{title}｜報價待核對｜已知paper WR —｜假設paper PnL —' for title in SHADOW_LABELS.values()]
    lines.append('Shadow報價不等於實際成交；假設收益不併入Live。')
    lines.append('')
    hs_known = isinstance(hs, dict) and isinstance(hs.get('latched'), bool)
    stopped = bool(loop['hard_stop_latched']) or bool(hs.get('latched')) if hs_known else bool(loop['hard_stop_latched'])
    lines.append('HS：'+('已鎖定' if stopped else '未鎖定' if hs_known else '全域狀態待核對'))
    lines.append('新進場：'+('本輪已停止' if loop['new_entries_stopped'] else '仍須通過原有風控'))
    if isinstance(loop_guard, dict) and loop_guard.get('loop_id') == loop['loop_id'] and loop_guard.get('fingerprint') == FINGERPRINT:
        lines.append(f'本輪風控1U等值｜MDD {loop_guard.get("mdd_1u")} / 3.5｜停單 {loop_guard.get("halt_reason") or "未觸發"}')
    else:
        lines.append('本輪風控尚無可核對狀態；進場仍須通過即時檢查。')
    if isinstance(gate, dict) and gate.get('fingerprint') == RISK_FP:
        lines.append('持久停單：'+str(gate.get('halt_reason') or '未觸發'))
    else:
        lines.append('持久風控狀態待核對；不推定為已通過。')
    if issues:
        lines.append('⚠️ 資料待核對：'+'、'.join(sorted(issues))+'；績效僅含可核對結算。')
    lines.append('Fill只計實際BUY；WR=勝/(勝+負)。Live PnL依官方費後結算；未結算不補零。')
    return '\n'.join(lines)
