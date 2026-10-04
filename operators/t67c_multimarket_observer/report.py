"""Bounded independent 20/40/100-market report; missing evidence stays missing."""
import json
from decimal import Decimal as D
from datetime import datetime
from zoneinfo import ZoneInfo
from .engine import VERSION,FINGERPRINT,LIVE_BRANCHES,SHADOW_BRANCHES
SYMBOLS=('BTCUSDT','ETHUSDT','BNBUSDT')
LABELS=dict(zip(LIVE_BRANCHES,('First DOWN','First UP','Stall DOWN','C DOWN','Continuation Original','C-UP鏡像','淺回撤')),
            external_lead_lag='外部先行',reference_value='Reference')


def metrics(quotes,total):
    settled=sorted([q for q in quotes if 'pnl' in q],key=lambda q:(q['known_at_ms'],q['at_ms']))
    w=sum(q['winner']==q['side'] for q in settled);d=sum(q['winner']=='DRAW' for q in settled);l=len(settled)-w-d
    equity=peak=mdd=D(0)
    for q in settled:
        equity+=D(q['pnl']);peak=max(peak,equity);mdd=max(mdd,peak-equity)
    return dict(candidates=len(quotes),settled=len(settled),pending=len(quotes)-len(settled),wins=w,losses=l,draws=d,
        wr=w/(w+l) if w+l else None,pnl=str(equity) if settled else None,mdd=str(mdd) if settled else None,
        candidate_rate=len(quotes)/total if total else None)


def snapshot(rows,at,epoch,health):
    ended=[r for r in rows if r['end']<=at]
    starts=sorted({r['start'] for r in ended})
    result=dict(version=VERSION,policy=FINGERPRINT,at_ms=at,epoch=epoch,health=health,
        mode='QUOTE_SIMULATION_NOT_FILL',risk_applied=False,rolling={})
    for n in (20,40,100):
        chosen=set(starts[-n:]);group=dict(start=min(chosen) if chosen else None,end=max(chosen)+300000 if chosen else None,count=len(chosen),markets={})
        for symbol in SYMBOLS:
            rs=[r for r in ended if r['symbol']==symbol and r['start'] in chosen];qs=[r['quote'] for r in rs if r.get('quote')]
            m=dict(total=len(rs),features=sum(bool(r.get('features_present')) for r in rs),
                originals=sum(bool(r.get('original_present')) for r in rs),guards=sum('guard' in r for r in rs),
                selected=sum(bool(r.get('selected')) for r in rs),missing=sum('guard' not in r for r in rs),
                all=metrics(qs,len(rs)),branches={},shadow={})
            for b in LIVE_BRANCHES:m['branches'][b]=metrics([q for q in qs if q['branch']==b],len(rs))
            for b in SHADOW_BRANCHES:m['shadow'][b]=metrics([r['shadow'][b] for r in rs if b in r.get('shadow',{})],len(rs))
            reasons={}
            for r in rs:reasons[r['reason']]=reasons.get(r['reason'],0)+1
            m['reasons']=reasons;group['markets'][symbol]=m
        result['rolling'][str(n)]=group
    return result


def render(p,window=20,asset=None,now_ms=None):
    if p['version']!=VERSION or p['policy']!=FINGERPRINT or p['mode']!='QUOTE_SIMULATION_NOT_FILL':raise ValueError('observer_provenance')
    if window not in (20,40,100) or asset not in (None,*SYMBOLS):raise ValueError('observer_arguments')
    if now_ms is not None and p['at_ms']>now_ms+1000:raise ValueError('observer_future')
    fmt=lambda t:datetime.fromtimestamp(t/1000,ZoneInfo('Asia/Taipei')).strftime('%m/%d %H:%M')
    group=p['rolling'][str(window)];lines=[f'T6.7c 三市場 Shadow｜最近{window}場',f"更新：{fmt(p['at_ms'])}（台灣）",
        '1U報價模擬；非fill、未套Live風控／送單延遲。']
    if group['start'] is not None:lines.append(f"區間：{fmt(group['start'])}–{fmt(group['end'])}｜實際{group['count']}/{window}場")
    if now_ms is not None and now_ms-min(p['at_ms'],p.get('health',{}).get('at_ms',0))>120000:lines.append('⚠ 資料過期，不能視為目前市況。')
    def stats(m):
        wr='—' if m['wr'] is None else f"{m['wr']*100:.0f}%";net='—' if m['pnl'] is None else f"{D(m['pnl']):+.4f}U"
        return f"候選{m['candidates']}｜{m['wins']}勝{m['losses']}負{m['draws']}平｜待{m['pending']}｜WR {wr}｜{net}"
    for symbol in ((asset,) if asset else SYMBOLS):
        m=group['markets'][symbol];a=m['all'];rate='—' if a['candidate_rate'] is None else f"{a['candidate_rate']*100:.1f}%"
        mdd='—' if a['mdd'] is None else f"{D(a['mdd']):.4f}U"
        lines += ['',f"【{symbol[:-4]}】K線{m['features']}/{m['total']}｜Original{m['originals']}｜判定完整{m['guards']}",
            f"七路合計：候選率 {rate}｜MDD {mdd}",stats(a)]
        for b in LIVE_BRANCHES:lines.append(LABELS[b]+'：'+stats(m['branches'][b]))
        lines.append('獨立Shadow（不加入七路合計）：')
        for b in SHADOW_BRANCHES:lines.append(LABELS[b]+'：'+stats(m['shadow'][b]))
        if m['missing']:lines.append(f"⚠ {m['missing']}場未完成初始判定，保留分母；不視為策略無訊號。")
    lines+=['','核心優先、補充讓位；每幣每場最多一次七路候選。','選中後須在原期限內獲得下一筆新鮮盤口；不代表真實成交。',
        '官方勝方；扣費一次，WR排除平局，PnL包含平局。','新觀測自啟用開始；無樣本的 — 不是零收益。不自動選幣／開單。']
    text='\n'.join(lines)
    if len(text)>3900:raise ValueError('report_length')
    return text


def load_render(root,window=20,asset=None,now_ms=None):
    path=root/'prediction/data/t67c-multimarket-observer/latest.json'
    with path.open('rb') as stream:raw=stream.read(2*1024*1024+1)
    if len(raw)>2*1024*1024:raise ValueError('report_size')
    return render(json.loads(raw),window,asset,now_ms)
