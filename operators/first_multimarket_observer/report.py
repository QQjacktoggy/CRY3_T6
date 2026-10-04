"""As-of quote simulations; explicit denominators, missing data and no fill claims."""
from collections import Counter
from decimal import Decimal
from datetime import datetime
import html
import json
from pathlib import Path
from zoneinfo import ZoneInfo
from policy import SYMBOLS, SLOT, FINGERPRINT, recheck


REASON_LABELS = {
    'not_first_reversal':'無First反轉', 'prior_trend_filter':'前趨勢不符',
    'price_band':'價格帶不符', 'initial_book_missing':'初始盤口缺漏',
    'initial_inputs_missing':'初始資料缺漏', 'feature_missing':'K線取樣失敗',
    'missed_feature_window':'錯過取樣窗口', 'insufficient_1u_depth':'初始深度不足',
    'price_above_frozen_cap':'高於凍結限價', 'insufficient_frozen_share_depth':'限價內深度不足',
    'quote_age':'報價過期', 'recheck_window':'超過重檢期限',
    'recheck_data_unavailable':'重檢資料缺漏', 'recheck_not_attempted':'重檢未執行',
    'book_stale_or_future':'盤口時間不符', 'recheck_unknown':'舊紀錄原因不足',
}

def recheck_reason(row):
    """Explain stored evidence only. Never create a simulated fill or edit history."""
    if not row.get('initial_quote') or row.get('sim_quote'):return None
    if not row.get('recheck_attempted'):return 'recheck_not_attempted'
    q=row.get('recheck_book')
    if not q:return 'recheck_data_unavailable'
    try:
        if Decimal(q['levels'][0][0])>Decimal(row['initial_quote']['limit']):
            return 'price_above_frozen_cap'
        recheck(row['meta'],row['initial_quote'],q,q['received_at_ms'])
    except ValueError as exc:
        return str(exc) if str(exc) in REASON_LABELS else 'recheck_unknown'
    except (KeyError,TypeError,ArithmeticError):return 'recheck_unknown'
    return 'recheck_unknown'


def rejection_summary(m):
    counts=m.get('recheck_reasons',{})
    return '、'.join(f"{REASON_LABELS.get(k,'其他資料/條件')} {n}" for k,n in sorted(counts.items()))


def metrics(rows,side=None,key='sim_quote',pnl_key='sim_pnl'):
    frows=[r for r in rows if r.get('features')]
    signal=[r for r in frows if r['features'].get('reversal') and (side is None or r['features']['side']==side)]
    trend=[r for r in signal if r['features']['trend_pass']]
    initial=[r for r in trend if r.get('initial_quote')]
    candidates=[r for r in trend if r.get(key)]
    settled=sorted([r for r in candidates if r.get(pnl_key) is not None],key=lambda r:(r['known_at_ms'],r['start']))
    wins=sum(r['winner']==r['features']['side'] for r in settled)
    draws=sum(r['winner']=='DRAW' for r in settled);losses=len(settled)-wins-draws
    equity=peak=mdd=Decimal(0)
    for r in settled:
        equity+=Decimal(r[pnl_key]);peak=max(peak,equity);mdd=max(mdd,peak-equity)
    return dict(scheduled_windows=len(rows),feature_complete=len(frows),initial_books_complete=sum(len(r.get('initial_books',{}))==2 for r in rows),
                signal=len(signal),trend_pass=len(trend),initial_quote_eligible=len(initial),quote_candidates=len(candidates),settled=len(settled),
                recheck_attempted=sum(bool(r.get('recheck_attempted')) for r in initial),
                recheck_reasons=dict(Counter(recheck_reason(r) for r in initial if not r.get('sim_quote'))),
                pending=len(candidates)-len(settled),wins=wins,losses=losses,draws=draws,
                wr=round(wins/(wins+losses),6) if wins+losses else None,
                quote_candidate_rate=round(len(candidates)/len(rows),6) if rows else None,
                net_pnl=str(equity),mdd=str(mdd),mdd_complete=len(candidates)==len(settled),
                mean_entry_price=str(sum(Decimal(r[key]['cash'])/Decimal(r[key]['gross_shares']) for r in candidates)/len(candidates)) if candidates else None,
                missing_features=len(rows)-len(frows),reasons=dict(Counter(r['reason'] for r in rows)))


def snapshot(db,at):
    rows=[json.loads(r[0]) for r in db.execute('SELECT payload FROM windows ORDER BY start,symbol')]
    ended=[r for r in rows if r['end']<=at]
    starts=sorted({r['start'] for r in ended})
    health=db.execute('SELECT at_ms,payload FROM health WHERE id=1').fetchone()
    epoch=int(db.execute("SELECT value FROM config WHERE key='epoch'").fetchone()[0])
    out=dict(at_ms=at,policy=FINGERPRINT,mode='QUOTE_SIMULATION_NO_REAL_ORDERS',unit='1U',selector_enabled=False,
             risk_applied=False,settlement_source='official_prediction_detail',epoch=epoch,
             caveats=['Displayed depth is assumed executable; no queue/claim/wallet/POST/actual fills modeled.',
                      'Standalone First rules; excludes other core reservations and Live risk stops.',
                      'Fixed T+128..129.5 recheck is a research checkpoint, not an exact replay of Live execution.',
                      'WR excludes official draws; PnL includes 0.5 payout draws and share fees.',
                      'ETH/BNB use transferred BTC First thresholds; not validated for Live.'],
             health={'at_ms':health[0],**json.loads(health[1])} if health else None,
             rolling={},rolling_ranges={},blocks20=[],current=[])
    for window in (20,40,100):
        selected=set(starts[-window:]);out['rolling'][str(window)]={}
        out['rolling_ranges'][str(window)]=dict(count=len(selected),start=min(selected) if selected else None,end=max(selected)+SLOT if selected else None)
        for symbol in SYMBOLS:
            sr=[r for r in ended if r['symbol']==symbol and r['start'] in selected]
            out['rolling'][str(window)][symbol]={s:metrics(sr,None if s=='ALL' else s) for s in ('ALL','UP','DOWN')}
            out['rolling'][str(window)][symbol]['INITIAL_ONLY']=metrics(sr,key='initial_quote',pnl_key='initial_sim_pnl')
    groups={}
    for row in ended:groups.setdefault((row['start']-epoch)//SLOT//20,[]).append(row)
    for block,rs in sorted(groups.items()):
        if block<0:continue
        entry=dict(block=block+1,start=epoch+block*20*SLOT,completed_windows=len({r['start'] for r in rs}),target=20,markets={})
        for symbol in SYMBOLS:
            sr=[r for r in rs if r['symbol']==symbol]
            entry['markets'][symbol]={s:metrics(sr,None if s=='ALL' else s) for s in ('ALL','UP','DOWN')}
        out['blocks20'].append(entry)
    for symbol in SYMBOLS:
        recent=[r for r in rows if r['symbol']==symbol]
        if recent:out['current'].append(recent[-1])
    return out


def write_reports(db,directory,at):
    p=snapshot(db,at);directory=Path(directory)
    def atomic(name,text):
        temp=directory/(name+'.tmp');temp.write_text(text,encoding='utf-8');temp.replace(directory/name)
    atomic('latest.json',json.dumps(p,ensure_ascii=False,indent=2))
    stamp=datetime.fromtimestamp(at/1000,ZoneInfo('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')
    md=[f'# First 三市場觀測 — {stamp} 台灣時間','',
        '**唯讀研究／1U 報價模擬。無真實送單，候選率不是 fill rate，未套 Live 風控；不自動選幣。**','',
        '兩階段：T+124–126 初始全深度凍結 cap/數量；T+128–129.5 固定重檢。官方勝方結算；費用扣一次；WR排除DRAW。','',
        'ETH／BNB沿用BTC First門檻，尚未證明適用。資料不足與未結算不可視為0收益或推薦開單。','']
    def table(group):
        lines=['|市場／方向|已結束場|K線完整|訊號|趨勢通過|初始合格|重檢候選|已結算／待結|WR|模擬PnL U|MDD U|',
               '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
        for symbol in SYMBOLS:
            for side in ('ALL','UP','DOWN'):
                m=group[symbol][side];wr='—' if m['wr'] is None else f"{m['wr']*100:.1f}%"
                mdd=f"{Decimal(m['mdd']):.4f}"+('（未完整）' if not m['mdd_complete'] else '')
                net=f"{Decimal(m['net_pnl']):+.4f}" if m['settled'] else '—'
                lines.append(f"|{symbol[:-4]} {side}|{m['scheduled_windows']}|{m['feature_complete']}|{m['signal']}|{m['trend_pass']}|{m['initial_quote_eligible']}|{m['quote_candidates']}|{m['settled']}／{m['pending']}|{wr}|{net}|{mdd}|")
        return lines
    for window,group in p['rolling'].items():md += [f'## 最近 {window} 場（不足時顯示實際場數）','']+table(group)+['']
    md+=['## 固定每20場摘要','']
    for b in p['blocks20'][-5:]:md += [f"### 批次 {b['block']}：{b['completed_windows']}/20",'']+table(b['markets'])+['']
    md+=['## 最新市場／阻擋原因','']
    for r in p['current']:md.append(f"- {r['symbol'][:-4]}：{r['reason']}；market start {r['start']}")
    md+=['','## 健康狀態','',f"`{json.dumps(p['health'],ensure_ascii=False)}`"]
    atomic('latest.md','\n'.join(md)+'\n')
    # Small local dashboard, no web server or public publication.
    trs=[]
    for window,group in p['rolling'].items():
        for symbol in SYMBOLS:
            for side in ('ALL','UP','DOWN'):
                m=group[symbol][side]
                cells=[window,symbol[:-4],side,m['scheduled_windows'],m['feature_complete'],m['signal'],m['trend_pass'],m['initial_quote_eligible'],m['quote_candidates'],f"{m['settled']}/{m['pending']}", '—' if m['wr'] is None else f"{m['wr']*100:.1f}%",'—' if not m['settled'] else f"{Decimal(m['net_pnl']):+.4f}",f"{Decimal(m['mdd']):.4f}"]
                trs.append('<tr>'+''.join('<td>'+html.escape(str(c))+'</td>' for c in cells)+'</tr>')
    headers=['最近場數','市場','方向','實際場數','K線完整','訊號','趨勢通過','初始合格','重檢候選','結算/待結','WR','模擬PnL U','MDD U']
    doc='<!doctype html><html lang="zh-Hant"><meta charset="utf-8"><meta http-equiv="refresh" content="15"><title>First 三市場觀測</title><style>body{font:16px system-ui;margin:24px;background:#101827;color:#edf2f7}table{border-collapse:collapse}td,th{padding:10px;border-bottom:1px solid #334155;text-align:right}th{color:#93c5fd}small{color:#cbd5e1}</style>'
    doc+='<h1>First BTC／ETH／BNB 觀測</h1><p>'+html.escape(stamp)+' 台灣時間</p><p>1U報價模擬；沒有真實fill、不套Live風控、不自動選幣。ETH／BNB規則尚未驗證。</p><table><thead><tr>'+''.join('<th>'+h+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join(trs)+'</tbody></table><p>候選按初始盤口與固定128秒重檢；官方勝方結算。待結算不是0收益；WR排除DRAW，PnL包含DRAW。</p><small>數據由VM收集；下載的本地副本不會自動同步VM。</small></html>'
    atomic('latest.html',doc)

if __name__=='__main__':
    import argparse,sqlite3,time
    parser=argparse.ArgumentParser();parser.add_argument('--data',required=True);args=parser.parse_args()
    directory=Path(args.data)
    db=sqlite3.connect((directory/'first-observer.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
    write_reports(db,directory,time.time_ns()//1000000)
