"""Attribute verified Live fills and official PnL, without paper returns."""
import json
import sqlite3
from contextlib import closing

from .regime_t67_policy import BRANCHES


def branch_metrics(root, campaigns, current_ids, fill_ids, events, *, fingerprint, slots):
    from .live_report import _metrics
    result = {b: dict(fills=0, pending=0, events=[]) for b in BRANCHES}
    result['unattributed'] = 0
    if not fill_ids & current_ids:
        for branch in BRANCHES:
            result[branch].update(_metrics(result[branch].pop('events')))
        return result
    path = root/'prediction/data/regime-target6/features.sqlite3'
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        decisions = {int(r[0]): json.loads(r[1]) for r in db.execute('SELECT start,payload FROM t67_decisions')}
    by_cid = {e['cid']: e for e in events}
    admissions = {(str(s['loop_id']), int(s['market_start_ms'])): s for s in slots}
    for cid in fill_ids & current_ids:
        campaign = campaigns[cid]
        d = decisions.get(int(campaign['start_time_ms']), {})
        branch = d.get('branch')
        start = int(campaign['start_time_ms'])
        slot = admissions.get((str(campaign['loop_id']), start), {})
        up_id = str(slot.get('market_id') or '')
        if (d.get('fingerprint') != fingerprint or not d.get('selected') or branch not in BRANCHES
                or str(d.get('market_topic')) != str(campaign.get('market_topic_id'))
                or slot.get('verified_at_ms') is None or not up_id
                or str(slot.get('market_topic_id')) != str(d.get('market_topic'))
                or str(d.get('market_id')) != up_id
                or d.get('market_start_ms') != start or d.get('market_end_ms') != start+300000
                or campaign.get('end_time_ms', start+300000) != start+300000
                or (campaign.get('market_id') and str(campaign['market_id']) != up_id)):
            result['unattributed'] += 1
            continue
        result[branch]['fills'] += 1
        if cid in by_cid:
            result[branch]['events'].append(by_cid[cid])
        else:
            result[branch]['pending'] += 1
    for branch in BRANCHES:
        result[branch].update(_metrics(result[branch].pop('events')))
    return result


BRANCH_LABELS = {
    'external_lead_lag': '外部先行',
    'reference_value': 'Reference 校正',
    'shallow_retracement': '淺回撤',
    'c_mirror_up_prior': 'C-UP 前趨勢鏡像',
}


def empty_report(now):
    from datetime import datetime
    from .live_report import TZ
    clock = datetime.fromtimestamp(now/1000, TZ).strftime('%m/%d %H:%M:%S')
    return '\n'.join(['📊 T6.7 Live Report｜四策略', f'截至 {clock}（台灣時間）',
                      '尚未建立 T6.7 Live 輪次；尚未開跑。',
                      '子策略：'+'／'.join(BRANCH_LABELS[b] for b in BRANCHES),
                      'Live fill rate／WR／PnL：—（尚無本輪資料）'])


def format_summary(root, *, now, loop, slots, campaigns, current_ids, fill_ids,
                   events, claims, pending, inflight, unknown, gate, loop_guard, hs, issues):
    """T6.7 current-loop performance plus shared stop state, never old PnL."""
    from datetime import datetime
    from .live_report import SLOT, TZ, _metrics, _value
    from .regime_lane import FINGERPRINT as RISK_FP
    from .regime_t67_policy import FINGERPRINT
    metric = _metrics(events)
    clock = datetime.fromtimestamp(now/1000, TZ).strftime('%m/%d %H:%M:%S')
    units = '/'.join(sorted({str(q['unit_usdt']) for q in claims})) or '待成交確認'
    ended = {int(s['market_start_ms']) for s in slots
             if s['verified_at_ms'] is not None and int(s['market_start_ms'])+SLOT <= now}
    filled = {int(campaigns[cid]['start_time_ms']) for cid in fill_ids & current_ids}
    closed_fills = len(filled & ended)
    fill_text = f'{closed_fills/len(ended):.1%}（{closed_fills}/{len(ended)} 已結束登錄市場）' if ended else '—（尚無已結束登錄市場）'
    pnl = _value(metric, events, pending) if events or pending else '—'
    mdd = _value(metric, events, pending, 'mdd') if events or pending else '—'
    lines = ['📊 T6.7 Live Report｜四策略', f'截至 {clock}（台灣時間）｜Loop {loop["loop_id"]}',
             f'狀態 {loop["state"]}｜完成 {loop["completed"]}/{loop["target"]} 場｜每筆 {units} USDT',
             f'Live fill rate {fill_text}',
             f'本輪 WR {metric["wr"]}（{metric["wins"]}勝/{metric["losses"]}負/{metric["flats"]}平；已結算成交 {len(events)}）',
             f'本輪已知淨 PnL {pnl} USDT｜MDD {mdd} USDT',
             f'成交市場 {len(filled)}｜待結算/核對 {pending}｜未終結intent {inflight}｜UNKNOWN市場 {unknown}', '']
    try:
        metrics = branch_metrics(root, campaigns, current_ids, fill_ids, events, fingerprint=FINGERPRINT, slots=slots)
        for branch in BRANCHES:
            m = metrics[branch]
            branch_pnl = f'{m["pnl"]:+.4f}' if m['wins']+m['losses']+m['flats'] else '—'
            lines += [f'{BRANCH_LABELS[branch]}｜成交 {m["fills"]}｜已知WR {m["wr"]}｜已知PnL {branch_pnl} USDT｜待結算 {m["pending"]}']
        if metrics['unattributed']:
            lines.append(f'子策略歸因待核對 {metrics["unattributed"]} 筆；保留官方Live總PnL。')
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        lines.append('T6.7 子策略歸因待核對；保留官方Live總PnL。')
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
    lines.append('Fill只計實際BUY；WR=勝/(勝+負)。PnL依官方費後結算；未結算不補零。')
    return '\n'.join(lines)
