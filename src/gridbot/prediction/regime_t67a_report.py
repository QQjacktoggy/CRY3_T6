"""Current-loop T6.7a Live ledger and separate verified Shadow quotes."""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from decimal import Decimal

LIVE_LABELS = {
    'core_first_down': 'first DOWN',
    'core_first_up': 'first UP',
    'core_stall_down': 'stall DOWN',
    'core_c_down': '原 C DOWN',
    'core_continuation_original': 'continuation Original',
    'c_mirror_up_prior': 'C-UP 前趨勢鏡像',
    'shallow_retracement': '淺回撤',
}
SHADOW_LABELS = {
    'external_lead_lag': '外部先行',
    'reference_value': 'Reference 校正',
}
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
        path = root/'prediction/data/regime-target6/features.sqlite3'
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            starts = tuple(int(campaigns[cid]['start_time_ms']) for cid in filled)
            decisions = {int(r[0]): json.loads(r[1]) for r in db.execute(
                'SELECT start,payload FROM t67a_decisions WHERE start IN ('+','.join('?' for _ in starts)+')', starts)}
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
                    "WHERE c.loop_id=? AND s.status='SETTLED'", (loop_id,)):
                if row['winner'] in ('UP', 'DOWN', 'DRAW'):
                    up_id = _campaign_up_id(dict(row))
                    if not up_id:
                        raise ValueError('official campaign UP identity unavailable')
                    key = (str(row['market_topic_id']), up_id, int(row['start_time_ms']), int(row['end_time_ms']))
                    official.setdefault(key, set()).add(row['winner'])
        columns = {r[1] for r in db.execute('PRAGMA table_info(prediction_shadow_observer_markets)')}
        starts = tuple(int(s['market_start_ms']) for s in slots)
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
                      unverified=0, pnl=Decimal(0), wr='—') for b in SHADOW_LABELS}
    starts = tuple(int(s['market_start_ms']) for s in slots)
    path = root/'prediction/data/regime-target6/features.sqlite3'
    if not path.is_file() or not starts:
        return result
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 't67a_shadow_quotes' not in tables:
            return result
        selected_starts = ' WHERE start IN ('+','.join('?' for _ in starts)+')'
        quotes = [(int(s), b, json.loads(p)) for s, b, p in db.execute('SELECT start,branch,payload FROM t67a_shadow_quotes'+selected_starts, starts)]
        outcomes = ({int(s): json.loads(p) for s, p in db.execute('SELECT start,payload FROM t67a_shadow_outcomes'+selected_starts, starts)}
                    if 't67a_shadow_outcomes' in tables else {})
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
            if (not start+60000 <= at < start+270000 or at > now
                    or not book_at <= received <= at or not 0 <= at-book_at <= 1000):
                raise ValueError('paper quote clock invalid')
            cash, shares, unit = (_decimal(quote[k]) for k in ('cash', 'net_shares', 'unit_usdt'))
            if (unit not in (1, 2, 3) or not 0 < cash <= unit or shares <= 0
                    or not 0 <= _decimal(quote['fee_bps']) <= 10000):
                raise ValueError('paper quote amounts invalid')
            metric['quoted'] += 1
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
        except (ValueError, KeyError, TypeError, ArithmeticError):
            metric['unverified'] += 1
    for metric in result.values():
        metric['unknown'] = metric['quoted']-metric['known']
        decisive = metric['wins']+metric['losses']
        metric['wr'] = f"{metric['wins']/decisive:.1%}" if decisive else '—'
    return result


def empty_report(now):
    from .live_report import TZ
    clock = datetime.fromtimestamp(now/1000, TZ).strftime('%m/%d %H:%M:%S')
    lines = ['📊 T6.7a Report｜七路 Live＋兩路 Shadow', f'截至 {clock}（台灣時間）',
             '尚未建立 T6.7a Live 輪次；尚未開跑。',
             'Live fill rate／WR／PnL：—（尚無本輪資料）', '', 'Live 子策略（本輪）：']
    lines += [f'{title}｜成交 0｜已知WR —｜已知PnL —｜待結算 0' for title in LIVE_LABELS.values()]
    lines += ['', 'Shadow（本輪報價研究）：']
    lines += [f'{title}｜報價 0｜已知paper WR —｜假設paper PnL —｜未知 0' for title in SHADOW_LABELS.values()]
    lines.append('Shadow報價不等於實際成交；假設收益不併入Live。')
    return '\n'.join(lines)


def format_summary(root, *, now, loop, slots, campaigns, current_ids, fill_ids,
                   events, claims, pending, inflight, unknown, gate, loop_guard, hs, issues,
                   selected_unit=None):
    from .live_report import SLOT, TZ, _metrics, _value
    from .regime_lane import FINGERPRINT as RISK_FP
    from .regime_t67a_policy import FINGERPRINT
    metric = _metrics(events)
    clock = datetime.fromtimestamp(now/1000, TZ).strftime('%m/%d %H:%M:%S')
    units = '/'.join(sorted({str(q['unit_usdt']) for q in claims})) or '待成交確認'
    if not claims and isinstance(selected_unit, dict):
        chosen = str(selected_unit.get('order_unit_usdt') or '')
        if chosen in ('1', '2', '3'):
            units = chosen+'（目前設定；待成交確認）'
    ended = {int(s['market_start_ms']) for s in slots
             if s['verified_at_ms'] is not None and int(s['market_start_ms'])+SLOT <= now}
    filled = {int(campaigns[cid]['start_time_ms']) for cid in fill_ids & current_ids}
    closed_fills = len(filled & ended)
    fill_text = f'{closed_fills/len(ended):.1%}（{closed_fills}/{len(ended)} 已結束登錄市場）' if ended else '—（尚無已結束登錄市場）'
    pnl = _value(metric, events, pending) if events or pending else '—'
    mdd = _value(metric, events, pending, 'mdd') if events or pending else '—'
    lines = ['📊 T6.7a Report｜七路 Live＋兩路 Shadow', f'截至 {clock}（台灣時間）｜Loop {loop["loop_id"]}',
             f'狀態 {loop["state"]}｜完成 {loop["completed"]}/{loop["target"]} 場｜本輪每筆 {units} USDT',
             f'Live fill rate {fill_text}',
             f'本輪 WR {metric["wr"]}（{metric["wins"]}勝/{metric["losses"]}負/{metric["flats"]}平；已結算成交 {len(events)}）',
             f'本輪已知淨 PnL {pnl} USDT｜MDD {mdd} USDT',
             f'入場intent {len(claims)}｜送單嘗試 {sum(q["submission_at_ms"] is not None or q["order_id"] is not None for q in claims)}｜成交市場 {len(filled)}',
             f'待結算/核對 {pending}｜未終結intent {inflight}｜UNKNOWN市場 {unknown}',
             '', 'Live 子策略（本輪）：']
    try:
        branches = branch_metrics(root, campaigns, current_ids, fill_ids, events,
                                  fingerprint=FINGERPRINT, slots=slots, loop_id=loop['loop_id'])
        for branch, title in LIVE_LABELS.items():
            m = branches[branch]
            branch_pnl = f'{m["pnl"]:+.4f}' if m['wins']+m['losses']+m['flats'] else '—'
            lines.append(f'{title}｜成交 {m["fills"]}｜已知WR {m["wr"]}｜已知PnL {branch_pnl} USDT｜待結算 {m["pending"]}')
        if branches['unattributed']:
            lines.append(f'子策略歸因待核對 {branches["unattributed"]} 筆；保留官方Live總PnL。')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        lines += [f'{title}｜成交待核對｜已知WR —｜已知PnL —' for title in LIVE_LABELS.values()]
        lines.append('T6.7a 子策略歸因待核對；保留官方Live總PnL。')
    lines += ['', 'Shadow（本輪報價研究）：']
    try:
        shadows = shadow_metrics(root, now=now, loop_id=loop['loop_id'], slots=slots, fingerprint=FINGERPRINT)
        for branch, title in SHADOW_LABELS.items():
            m = shadows[branch]
            paper_pnl = f'{m["pnl"]:+.4f} USDT' if m['known'] else '—'
            lines.append(f'{title}｜報價 {m["quoted"]}｜已知paper WR {m["wr"]}｜假設paper PnL {paper_pnl}｜未知 {m["unknown"]}')
            if m['unverified']:
                lines.append(f'  報價/官方勝方待核對 {m["unverified"]}；未核對結果不列收益。')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError, ArithmeticError):
        lines += [f'{title}｜報價待核對｜已知paper WR —｜假設paper PnL —' for title in SHADOW_LABELS.values()]
    lines += ['Shadow報價不等於實際成交；假設收益不併入Live。', '']
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
