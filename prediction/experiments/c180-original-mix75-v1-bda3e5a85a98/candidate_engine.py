"""Add a fixed paper candidate without changing Original/A model input context."""
from engine import Engine as BaselineEngine
from logic import entry_ev,value_exit,walk
from store import now

ENTRY='C180_FAVORITE'
ACCOUNT='C180_MIX75'
HOLD='C180_FAVORITE_HOLD'

class Engine(BaselineEngine):
    async def decide(self,packet,session,key,recover=False):
        # Retain all frozen baseline decisions and holdings, so JEV questions/state
        # remain comparable to the research cohort. No extra provider requests.
        previous=next((d for d in self.store.rows('decisions') if d['id']==packet['id']),None)
        if previous and previous.get('status')=='complete' and (packet['phase']=='E10' or ACCOUNT in previous.get('branches',{})):
            return
        if not previous or previous.get('status')!='complete':
            await super().decide(packet,session,key,recover)
        if packet['phase'] not in ('C180','C90'): return
        ident=packet['id'];phase=packet['phase'];market=packet['market']
        d=next(x for x in self.store.rows('decisions') if x['id']==ident)
        q=packet['state']['quote'];fee=market.get('fee_bps');orders=[]
        branch={'action':'HOLD','reason':'no_position','entry_lane':ENTRY}
        missed=recover or d.get('missed') or now()>packet['cutoff']+4000
        if phase=='C180':
            p=d['probabilities'].get('Original')
            invalid=(missed or not market.get('reference') or market.get('market_id')=='missing'
                     or fee is None or q is None or (market.get('identified_at') or 0)>packet['cutoff'] or p is None)
            if invalid:
                branch.update(action='UNKNOWN',reason='missing_entry_data_or_cutoff')
            else:
                side,reason=entry_ev(p,q,fee)
                if side and (p==.5 or side!=('UP' if p>.5 else 'DOWN')):
                    side=None;reason='not_forecast_favorite'
                branch.update(action=side or 'SKIP',reason=reason,p_up=p)
                if side:
                    orders.append(self.order(packet,side,'entry',ENTRY,1.,p if side=='UP' else 1-p,True))
        else:
            e=self.orders.get(f"{market['start']}:{ENTRY}")
            if e and e['shares']>0 and e['status'] in ('filled','partial'):
                pa=d['probabilities'].get('A');pm=d['probabilities'].get('Market')
                p=.75*pm+.25*pa if pa is not None and pm is not None else None
                if missed or e['topic']!=market['topic'] or fee is None or fee!=e['fee_bps']:
                    branch.update(action='UNKNOWN',reason='exit_observation_unknown')
                elif p is None or q is None:
                    branch.update(action='HOLD',reason='missing_exit_probability_or_quote',missing_exit_probability=True)
                else:
                    cash,qty=walk(q[e['side']]['bid_levels'],e['shares'])
                    ps=p if e['side']=='UP' else 1-p
                    holding=dict(sale_net=cash*(1-fee/10000),sale_qty=qty,shares=e['shares'])
                    leave=value_exit(ps,holding)
                    branch.update(action='EXIT' if leave else 'HOLD',reason='value_exit' if leave else 'hold_value',p_up=p,p_side=ps,**holding)
                    if leave: orders.append(self.order(packet,e['side'],'exit',ACCOUNT,e['shares'],ps,True))
        d.setdefault('branches',{})[ACCOUNT]=branch
        self.store.put_many([('decisions',ident,d)]+[('orders',o['id'],o) for o in orders])
        self.orders.update({o['id']:o for o in orders})
