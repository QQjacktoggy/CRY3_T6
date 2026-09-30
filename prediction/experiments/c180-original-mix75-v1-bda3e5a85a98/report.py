"""Probability quality, paired economics and full deployment costs by account."""
import csv
from collections import Counter
from calibration import probability_metrics
from logic import ACCOUNTS, ENTRY_LANE, dumps


def cost(decision, lane):
    return decision.get('calls', {}).get(lane, {}).get('cost', 0.)


def metrics(rows):
    known = [r for r in rows if r['fee_pnl'] is not None]
    pnl = sum(r['fee_pnl'] for r in known)
    model_cost = sum(r['model_cost'] for r in rows)
    wins = sum(r['fee_pnl'] > 1e-9 for r in known)
    losses = sum(r['fee_pnl'] < -1e-9 for r in known)
    curve = peak = mdd = net_curve = net_peak = net_mdd = 0.
    for r in rows:
        curve += r['fee_pnl'] if r['fee_pnl'] is not None else 0.
        peak = max(peak, curve); mdd = max(mdd, peak-curve)
        net_curve += (r['fee_pnl'] if r['fee_pnl'] is not None else 0.)-r['model_cost']
        net_peak = max(net_peak, net_curve); net_mdd = max(net_mdd, net_peak-net_curve)
    return {'known_filled': len(known), 'wins': wins, 'losses': losses, 'flat': len(known)-wins-losses,
            'WR': wins/len(known) if known else None, 'fee_PNL': pnl, 'model_cost': model_cost,
            'known_PNL_less_all_model_cost': pnl-model_cost, 'max_drawdown_known': mdd,
            'max_drawdown_known_less_all_costs': net_mdd, 'statuses': dict(Counter(r['status'] for r in rows))}


def summarize(store, runtime):
    orders = {o['id']: o for o in store.rows('orders')}
    decisions = {d['id']: d for d in store.rows('decisions')}
    packets = {p['id']: p for p in store.rows('packets')}
    resolutions = {r['topic']: dict(r) for r in store.db.execute('SELECT * FROM resolutions')}
    rows = []
    for i in range(store.get('target')):
        slot = store.get('start')+i*300000
        entry_d = decisions.get(f'{slot}:E10', {})
        for account in ACCOUNTS:
            lane = ENTRY_LANE[account]
            phase = 'C180' if account == 'C180_OBSERVE' else 'C90'
            cp = decisions.get(f'{slot}:{phase}', {})
            e = orders.get(f'{slot}:{lane}')
            x = orders.get(f'{slot}:{phase}:{account}')
            fee = cost(entry_d, 'Original') if lane == 'E10' else cost(entry_d, 'Meta') if lane == 'JEV' else 0.
            fee += cost(cp, 'Original') if account == 'ORIGINAL_VALUE' else cost(cp, 'Meta') if account in ('JEV_FEATURE_VALUE', 'JEV_ENTRY_VALUE') else 0.
            row = {'run': i+1, 'start': slot, 'account': account, 'side': e['side'] if e else None,
                   'status': 'scheduled', 'entry_status': e['status'] if e else None,
                   'exit_status': x['status'] if x else None, 'entry_cash': e['cash'] if e else None,
                   'entry_shares': e['shares'] if e else None, 'exit_cash': x['cash'] if x else 0.,
                   'exit_shares': x['shares'] if x else 0., 'fee_pnl': None, 'gross_pnl': None,
                   'after_model_cost': None, 'model_cost': fee,
                   'model_status': cp.get('model_status', entry_d.get('model_status')),
                   'action': cp.get('branches', {}).get(account, {}).get('action', 'HOLD'),
                   'direction_correct': None}
            if e is None and entry_d.get('status') == 'complete' and lane != 'E10':
                row['status'] = 'unknown' if entry_d.get('missed') else 'skip'
            if e:
                row['status'] = e['status']
                missing_cp = account != 'E10_HOLD' and slot+(120000 if phase == 'C180' else 210000) < runtime.get('asof_ms', 0) and (cp.get('status') != 'complete' or cp.get('missed'))
                if e['status'] == 'unknown' or (x and x['status'] == 'unknown') or missing_cp:
                    row['status'] = 'unknown'
                elif e['shares'] > 0 and e['topic'] in resolutions and (not x or x['status'] != 'open'):
                    outcome = resolutions[e['topic']]['outcome']
                    payout = .5 if outcome == 'TIE' else float(outcome == e['side'])
                    gross = row['exit_cash']+(e['shares']-row['exit_shares'])*payout-e['cash']
                    fees = (e['cash']+row['exit_cash'])*e['fee_bps']/10000 if e['fee_bps'] is not None else None
                    row.update(status='known' if fees is not None else 'unknown_fee', gross_pnl=gross,
                               fee_pnl=gross-fees if fees is not None else None,
                               after_model_cost=gross-fees-fee if fees is not None else None,
                               direction_correct=None if outcome == 'TIE' else outcome == e['side'])
            rows.append(row)
    accounts = {a: metrics([r for r in rows if r['account'] == a]) for a in ACCOUNTS}
    pairs = {}
    for a, b in (('JEV_FEATURE_VALUE', 'NUMERIC_VALUE'), ('JEV_ENTRY_VALUE', 'NUMERIC_ENTRY_VALUE'),
                 ('ORIGINAL_VALUE', 'E10_HOLD'), ('C180_OBSERVE', 'E10_HOLD')):
        aa = {r['run']: r for r in rows if r['account'] == a}
        bb = {r['run']: r for r in rows if r['account'] == b}
        # Confirmed skips/no-fills are zero trading exposure, but remain outside WR.
        def comparable(r):
            return r['fee_pnl'] is not None or r['status'] in ('skip', 'no_fill', 'value_gone')
        shared = [i for i in aa if comparable(aa[i]) and comparable(bb[i])]
        attribution = Counter()
        delta = net = 0.
        changed = 0
        for i in shared:
            x, y = aa[i], bb[i]
            difference = (x['fee_pnl'] or 0.)-(y['fee_pnl'] or 0.)
            delta += difference; net += difference-x['model_cost']+y['model_cost']
            if x['action'] != y['action'] or x['side'] != y['side'] or x['entry_status'] != y['entry_status']:
                changed += 1
                key = ('saved_loss' if (y['fee_pnl'] or 0.) < 0 else 'preserved_profit') if difference > 0 else ('killed_winner' if (y['fee_pnl'] or 0.) > 0 else 'missed_loss_reduction') if difference < 0 else 'no_pnl_change'
                attribution[key] += difference
        pairs[a+' minus '+b] = {'n': len(shared), 'fee_PNL_delta': delta, 'after_cost_delta': net,
                                'action_or_entry_changed': changed, 'attribution_fee_PNL': dict(attribution),
                                'excluded': len(aa)-len(shared)}
    quality = {}; quality_pairs = {}
    for phase in ('E10', 'C180', 'C90'):
        own = sorted((d for d in decisions.values() if d.get('phase') == phase and d.get('status') == 'complete' and not d.get('missed')), key=lambda d: d['start'])
        for lane in ('Original', 'A', 'B', 'Market', 'Numeric', 'Jev'):
            pairs_p = []
            for d in own:
                packet = packets[d['id']]; res = resolutions.get(packet['market']['topic'])
                p = d['probabilities'].get(lane)
                if p is not None and res and res['outcome'] != 'TIE':
                    pairs_p.append((p, int(res['outcome'] == 'UP')))
            item = probability_metrics(pairs_p)
            item['predictions'] = sum(d['probabilities'].get(lane) is not None for d in own)
            item['overconfidence_flags'] = sum(d.get('overconfidence', {}).get(lane, False) for d in own)
            item['rolling'] = {}
            for size in (20, 50):
                # Last N scheduled observations, never last N successful responses.
                recent = [d['probabilities'].get(lane) for d in own[-size:]]
                ps = [p for p in recent if p is not None]
                item['rolling'][str(size)] = {'n': len(ps), 'missing': len(recent)-len(ps),
                    'UP': sum(p > .5 for p in ps), 'DOWN': sum(p < .5 for p in ps),
                    'neutral': sum(p == .5 for p in ps), 'mean_UP': sum(ps)/len(ps) if ps else None}
            quality[phase+':'+lane] = item
        for candidate, baseline in (('A', 'Original'), ('B', 'A'), ('Jev', 'Numeric'), ('Numeric', 'Market')):
            aa, bb = [], []
            for d in own:
                packet = packets[d['id']]; res = resolutions.get(packet['market']['topic'])
                p, q = d['probabilities'].get(candidate), d['probabilities'].get(baseline)
                if p is not None and q is not None and res and res['outcome'] != 'TIE':
                    y = int(res['outcome'] == 'UP'); aa.append((p, y)); bb.append((q, y))
            quality_pairs[phase+':'+candidate+' minus '+baseline] = {'candidate': probability_metrics(aa), 'baseline': probability_metrics(bb)}
    return {'runtime': runtime, 'target': store.get('target'), 'resolved_markets': len(resolutions),
            'actual_provider_cost_or_reserve': store.spent(), 'model_hash': store.get('model_hash'),
            'call_statuses': dict(Counter(r[0] for r in store.db.execute('SELECT status FROM calls'))),
            'accounts': accounts, 'pairs': pairs, 'probability_quality': quality, 'probability_pairs': quality_pairs, 'rows': rows,
            'limitations': ['Paper execution; fees are cash sensitivity, not verified live fees.',
                           'Conditional non-tie probabilities; realized ties pay 0.5.',
                           'Known-only PNL/MDD omit unknown exposure; all actual research API costs shown separately.',
                           'Untrained Numeric/Jev are identical market baseline; no demonstrated JEV gain.']}


def write_report(store, directory, runtime):
    report = summarize(store, runtime)
    lines = ['# Early10 JEV v2 paper shadow', f"Phase: {runtime.get('phase')}; resolved: {report['resolved_markets']}/{report['target']}",
             f"Frozen model: {report['model_hash']}; total actual research API cost/reserve: {report['actual_provider_cost_or_reserve']:.6f}",
             '|Account|Known|WR|Fee PNL|Full model costs|Net known|MDD known|', '|---|---:|---:|---:|---:|---:|---:|']
    for account, m in report['accounts'].items():
        wr = f"{m['WR']:.1%}" if m['WR'] is not None else '—'
        lines.append(f"|{account}|{m['known_filled']}|{wr}|{m['fee_PNL']:.6f}|{m['model_cost']:.6f}|{m['known_PNL_less_all_model_cost']:.6f}|{m['max_drawdown_known']:.6f}|")
    lines += ['', *report['limitations'], '', 'Paired results:', '```json', dumps(report['pairs']), '```',
              '', 'Probability quality by phase/lane: see report.json (Brier, log loss, bins, bias and coverage).']
    for name, body in (('report.json', dumps(report)), ('heartbeat.json', dumps(runtime)), ('REPORT.md', '\n'.join(lines))):
        tmp = directory/(name+'.tmp'); tmp.write_text(body, encoding='utf-8'); tmp.replace(directory/name)
    with (directory/'trades.csv.tmp').open('w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(report['rows'][0])); writer.writeheader(); writer.writerows(report['rows'])
    (directory/'trades.csv.tmp').replace(directory/'trades.csv')
