"""Report primary candidate and a same-entry HOLD control before legacy lanes."""
import csv
from report import summarize as baseline_summarize,metrics,cost
from candidate_engine import ENTRY,ACCOUNT,HOLD
from logic import dumps

def summarize(store,runtime):
    report=baseline_summarize(store,runtime)
    orders={o['id']:o for o in store.rows('orders')}
    decisions={d['id']:d for d in store.rows('decisions')}
    resolutions={r['topic']:dict(r) for r in store.db.execute('SELECT * FROM resolutions')}
    rows=[]
    for i in range(store.get('target')):
        start=store.get('start')+i*300000
        d=decisions.get(f'{start}:C180',{});xdecision=decisions.get(f'{start}:C90',{})
        e=orders.get(f'{start}:{ENTRY}')
        for account in (ACCOUNT,HOLD):
            x=orders.get(f'{start}:C90:{ACCOUNT}') if account==ACCOUNT else None
            fee=cost(d,'Original')
            if account==ACCOUNT and e and e['shares']>0: fee+=cost(xdecision,'A')
            row=dict(run=i+1,start=start,account=account,side=e['side'] if e else None,status='scheduled',entry_status=e['status'] if e else None,
                     exit_status=x['status'] if x else None,entry_cash=e['cash'] if e else None,entry_shares=e['shares'] if e else None,
                     exit_cash=x['cash'] if x else 0.,exit_shares=x['shares'] if x else 0.,fee_pnl=None,gross_pnl=None,after_model_cost=None,
                     model_cost=fee,action=xdecision.get('branches',{}).get(ACCOUNT,{}).get('action','HOLD') if account==ACCOUNT else 'HOLD',direction_correct=None,
                     missing_exit_probability=bool(account==ACCOUNT and xdecision.get('branches',{}).get(ACCOUNT,{}).get('missing_exit_probability')))
            entry_action=d.get('branches',{}).get(ACCOUNT,{}).get('action')
            if e is None and d.get('status')=='complete':
                row['status']='skip' if entry_action=='SKIP' else 'unknown'
            elif e is None and start+124000<runtime.get('asof_ms',0):
                row['status']='unknown'
            if e:
                row['status']=e['status']
                unknown=(e['status']=='unknown' or (x and x['status']=='unknown') or (e['shares']>0 and account==ACCOUNT and start+214000<runtime.get('asof_ms',0)
                         and (xdecision.get('status')!='complete' or xdecision.get('branches',{}).get(ACCOUNT,{}).get('action') in (None,'UNKNOWN'))))
                if unknown:row['status']='unknown'
                elif e['shares']>0 and e['topic'] in resolutions and (not x or x['status']!='open'):
                    outcome=resolutions[e['topic']]['outcome'];payout=.5 if outcome=='TIE' else float(outcome==e['side'])
                    gross=row['exit_cash']+(e['shares']-row['exit_shares'])*payout-e['cash']
                    fees=(e['cash']+row['exit_cash'])*e['fee_bps']/10000
                    row.update(status='known',gross_pnl=gross,fee_pnl=gross-fees,after_model_cost=gross-fees-fee,direction_correct=None if outcome=='TIE' else outcome==e['side'])
            rows.append(row)
    report['rows']=rows+report['rows']
    report['primary_accounts']=[ACCOUNT,HOLD]
    report['accounts']={**{a:metrics([r for r in rows if r['account']==a]) for a in (ACCOUNT,HOLD)},**report['accounts']}
    blocks=[]
    elapsed=min(store.get('target'),max(0,(runtime.get('asof_ms',0)-store.get('start'))//300000))
    for lo in range(1,store.get('target')+1,20):
        hi=min(lo+19,store.get('target'));rr=[r for r in rows if r['account']==ACCOUNT and lo<=r['run']<=hi]
        m=metrics(rr);complete=elapsed>=hi and all(r['status'] in ('known','skip','no_fill','value_gone') for r in rr)
        blocks.append(dict(start_run=lo,end_run=hi,complete=complete,**m))
    report['primary_20run_blocks']=blocks
    mature=[r for r in rows if r['account']==ACCOUNT and r['run']<=elapsed]
    m=metrics(mature)
    report['primary_cohort_summary']=dict(ended_market_windows=elapsed,**m,
        net_per20_known=m['known_PNL_less_all_model_cost']*20/elapsed if elapsed else None,
        economically_complete=bool(elapsed) and all(r['status'] in ('known','skip','no_fill','value_gone') for r in mature))
    report['primary_fee_pnl_less_total_research_api_cost']=report['accounts'][ACCOUNT]['fee_PNL']-report['actual_provider_cost_or_reserve']
    report['strategy']='C180 Original favorite + cost-after EV; C90 75% Market + 25% A value exit'
    report['limitations'].append('Primary is a research-selected paper candidate; 65-market retrospective 74.5% WR is not prospective proof. Retained legacy lanes preserve Original/A input context.')
    return report

def write_report(store,directory,runtime):
    report=summarize(store,runtime)
    lines=['# C180 Original + C90 Mix75 paper shadow',f"Phase: {runtime.get('phase')}; resolved: {report['resolved_markets']}/{report['target']}",report['strategy'],
           f"All research API cost/reserve: {report['actual_provider_cost_or_reserve']:.6f} USD",'','|Account|Known|W/L|WR|Fee PNL|Required AI costs|Net known|MDD known|','|---|---:|---:|---:|---:|---:|---:|---:|']
    for a,m in report['accounts'].items():
        wr=f"{m['WR']:.1%}" if m['WR'] is not None else '—'
        lines.append(f"|{a}|{m['known_filled']}|{m['wins']}/{m['losses']}|{wr}|{m['fee_PNL']:.6f}|{m['model_cost']:.6f}|{m['known_PNL_less_all_model_cost']:.6f}|{m['max_drawdown_known_less_all_costs']:.6f}|")
    lines+=['','|Market runs|Complete|WR|Net known|','|---|---|---:|---:|']
    for b in report['primary_20run_blocks']:
        wr=f"{b['WR']:.1%}" if b['WR'] is not None else '—'
        lines.append(f"|{b['start_run']}-{b['end_run']}|{b['complete']}|{wr}|{b['known_PNL_less_all_model_cost']:.6f}|")
    lines+=['','SKIP counts toward market windows, not WR. Unknown is not zero.','']+report['limitations']
    for name,body in (('report.json',dumps(report)),('REPORT.md','\n'.join(lines)),('heartbeat.json',dumps(runtime))):
        tmp=directory/(name+'.tmp');tmp.write_text(body,encoding='utf-8');tmp.replace(directory/name)
    if report['rows']:
        keys=sorted(set().union(*(r.keys() for r in report['rows'])))
        with (directory/'trades.csv.tmp').open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(report['rows'])
        (directory/'trades.csv.tmp').replace(directory/'trades.csv')
