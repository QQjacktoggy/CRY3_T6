"""Paired paper ledgers; model latency and execution observation remain causal."""
import asyncio
import json
from calibration import ProbabilityModel
from features import meta_features, overconfidence
from logic import (ACCOUNTS, ENTRY_LANE, CONFIG, QUESTIONS, AB_QUESTIONS, META_QUESTIONS,
                   model_state, parse_response, choose, entry_ev, value_exit, walk)
from store import now


class Engine:
    def __init__(self, store, model=None):
        self.store = store
        self.model = model or ProbabilityModel()
        if store.get('model_hash') not in (None, self.model.hash):
            raise ValueError('cohort model hash mismatch')
        store.set('model_hash', self.model.hash)
        self.orders = {o['id']: o for o in store.rows('orders')}
        for o in self.orders.values():
            if o['status'] == 'open' and now() > o['ready']:
                o.update(status='unknown', recovery_reason='process_gap')
                store.put('orders', o['id'], o)

    def entry(self, start, lane='E10'):
        return self.orders.get(f'{start}:{lane}')

    def freeze(self, packet, phase):
        packet.update(phase=phase, id=f"{packet['market']['start']}:{phase}")
        if phase != 'E10':
            holdings = {}
            for lane in ('E10', 'NUMERIC', 'JEV'):
                o = self.entry(packet['market']['start'], lane)
                q = packet['state']['quote']
                if not o or o['status'] not in ('filled', 'partial'):
                    continue
                cash, qty = walk(q[o['side']]['bid_levels'], o['shares']) if q else (0., 0.)
                net = cash*(1-o['fee_bps']/10000) if o['fee_bps'] is not None and q else None
                loss = net-o['cash']*(1+o['fee_bps']/10000) if net is not None and qty >= o['shares']-1e-9 else None
                holdings[lane] = {'side': o['side'], 'shares': o['shares'], 'entry_cash': o['cash'],
                                  'sale_qty': qty, 'sale_net': net, 'L': loss}
            packet['state']['holdings'] = holdings
            packet['state']['holding'] = holdings.get('E10')
        self.store.put('packets', packet['id'], packet)
        return packet

    def order(self, packet, side, kind, account, amount, p=None, guard=False):
        ident = f"{packet['market']['start']}:{account}" if kind == 'entry' else packet['id']+':'+account
        return {'id': ident, 'topic': packet['market']['topic'], 'start': packet['market']['start'],
                'phase': packet['phase'], 'account': account, 'kind': kind, 'side': side,
                'amount': amount, 'ready': packet['cutoff']+4000,
                'expires': min(packet['cutoff']+16000, packet['market']['end']-1),
                'fee_bps': packet['market']['fee_bps'], 'p_side': p, 'ev_guard': guard,
                'status': 'open', 'cash': 0., 'shares': 0., 'seen': 0, 'observations': []}

    async def decide(self, packet, session, key, recover=False):
        ident, phase, cutoff = packet['id'], packet['phase'], packet['cutoff']
        self.store.put('decisions', ident, {'id': ident, 'status': 'inflight', 'phase': phase, 'start': packet['market']['start']})
        missed = recover or 'missed_cutoff' in packet['reasons'] or 'identity_not_known' in packet['reasons']
        async def request(lane, questions):
            if missed or now() > cutoff+3000:
                return {'status': 'missed_cutoff', 'cost': 0, 'completed': cutoff+3000}, None
            call = await self.store.infer(session, key, model_state(packet, lane), questions)
            if call['status'] == 'ok' and call.get('completed', now()) <= cutoff+3000:
                return call, parse_response(json.loads(call['response']), questions)
            return call, None
        names = ('Original', 'A', 'B', 'Meta')
        replies = await asyncio.gather(*(request(lane, questions) for lane, questions in
                                      zip(names, (QUESTIONS, AB_QUESTIONS, AB_QUESTIONS, META_QUESTIONS))))
        calls = {lane: reply[0] for lane, reply in zip(names, replies)}
        answers = {lane: reply[1] for lane, reply in zip(names, replies)}
        probabilities = {lane: answers[lane]['direction']['probabilities']['UP'] if answers[lane] else None for lane in names[:3]}
        numeric = packet['state']['evidence_v2']['numeric']
        meta = meta_features(answers['Meta']) if answers['Meta'] else None
        predicted = self.model.probabilities(phase, cutoff, numeric, meta)
        probabilities.update(Numeric=predicted['numeric'], Jev=predicted['jev'], Market=numeric['market_up'])
        decision = {'id': ident, 'phase': phase, 'start': packet['market']['start'], 'status': 'complete',
                    'calls': calls, 'probabilities': probabilities, 'numeric': numeric, 'meta': meta,
                    'model_status': predicted['status'], 'model_hash': self.model.hash,
                    'origin': 'JEV' if probabilities['Original'] is not None else 'FALLBACK',
                    'overconfidence': {lane: overconfidence(probabilities[lane], numeric) for lane in names[:3]},
                    'completed': cutoff+3000, 'branches': {}, 'missed': missed}
        orders = []
        if phase == 'E10':
            p = probabilities['Original']
            side, reason = choose({'UP': p, 'DOWN': 1-p} if p is not None else None, packet['state']['quote'], packet['market']['start'])
            orders.append(self.order(packet, side, 'entry', 'E10', 1.))
            decision.update(side=side, reason=reason)
            for lane, source in (('NUMERIC', 'Numeric'), ('JEV', 'Jev')):
                p = probabilities[source]
                side, reason = entry_ev(p, packet['state']['quote'], packet['market']['fee_bps'])
                decision['branches'][lane] = {'action': side or 'SKIP', 'reason': reason}
                if side:
                    orders.append(self.order(packet, side, 'entry', lane, 1., p if side == 'UP' else 1-p, True))
        else:
            holdings = packet['state']['holdings']
            accounts = ['C180_OBSERVE'] if phase == 'C180' else ACCOUNTS[1:-1]
            for account in accounts:
                holding = holdings.get(ENTRY_LANE[account])
                if holding is None:
                    decision['branches'][account] = {'action': 'HOLD', 'reason': 'no_known_position'}
                    continue
                source = 'Original' if account == 'ORIGINAL_VALUE' else 'Jev' if account in ('JEV_FEATURE_VALUE', 'JEV_ENTRY_VALUE') else 'Numeric'
                p = probabilities[source]
                p_side = p if holding['side'] == 'UP' or p is None else 1-p
                guard = account in ('NUMERIC_VALUE', 'JEV_FEATURE_VALUE', 'NUMERIC_ENTRY_VALUE', 'JEV_ENTRY_VALUE')
                if account == 'C180_OBSERVE':
                    leave = True
                elif account == 'E10_S1_LOSS_EXIT':
                    leave = holding['L'] is not None and holding['L'] < 0
                elif account == 'ORIGINAL_VALUE':
                    leave = p_side is not None and holding['sale_net'] is not None and holding['sale_qty'] > 0 and holding['sale_net'] > holding['sale_qty']*p_side
                else:
                    leave = value_exit(p_side, holding)
                decision['branches'][account] = {'action': 'EXIT' if leave else 'HOLD', 'p_side': p_side, **holding}
                if leave:
                    o = self.order(packet, holding['side'], 'exit', account, holding['shares'], p_side, guard)
                    # Preserve original S4 execution-time value recheck without its old account name.
                    if account == 'ORIGINAL_VALUE':
                        o['original_value_guard'] = True
                    orders.append(o)
        if missed or now() > cutoff+4000:
            for o in orders:
                o.update(status='unknown', recovery_reason='missed_cutoff_or_execution_ready')
        self.store.put_many([('decisions', ident, decision)]+[('orders', o['id'], o) for o in orders])
        self.orders.update({o['id']: o for o in orders})

    def resolve(self, topic, outcome, known_at):
        if outcome in ('UP', 'DOWN', 'TIE'):
            self.store.db.execute('INSERT OR IGNORE INTO resolutions VALUES(?,?,?)', (topic, outcome, known_at))
            self.store.db.commit()
