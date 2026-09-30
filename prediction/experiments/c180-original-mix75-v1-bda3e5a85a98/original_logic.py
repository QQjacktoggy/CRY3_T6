"""Frozen early-entry experiment rules; all orders are hypothetical."""
import json
from legacy_features import dumps,digest,number,Tape as BaseTape,stats as base_stats,normalize_book as base_book
SLOT=300000
MODEL='typesafe/jev-1.13-20260917'
ACCOUNTS=['E10_HOLD','E10_S1_LOSS_EXIT','E10_S2_PROFIT_RISK','E10_S3_DIRECTION','E10_S4_VALUE','L60_HOLD']
CONFIG=dict(version='early10-check90-v1',stake=1.,timeout_ms=3000,delay_ms=1000,ttl_ms=12000,max_book_age_ms=2000,input_bytes=24000,target=200,accounts=ACCOUNTS)
QUESTIONS={'direction':{'type':'choice','instructions':
 'Estimate the FINAL official BTC five-minute settlement direction relative to the contract reference. '
 'Use only supplied evidence received by observed_at; lookback windows may include pre-open trades. '
 'Spot and futures are proxies, not the official Chainlink settlement. Quotes are market evidence. '
 'Return UP/DOWN probabilities conditional on non-tie settlement. Express uncertainty near 0.5. '
 'Never invent missing information; do not recommend SKIP or make an exit decision.',
 'criteria':{'UP':'Official end price strictly above starting reference','DOWN':'Official end price strictly below starting reference'}}}
DEFINITION=digest({'config':CONFIG,'model':MODEL,'questions':QUESTIONS})

def parse_response(raw):
    if raw.get('model')!=MODEL: raise ValueError('model')
    a=raw.get('answers',{}).get('direction',{});ps=a.get('probabilities',{})
    if a.get('type')!='choice' or a.get('choice') not in ('UP','DOWN') or set(ps)!= {'UP','DOWN'}: raise ValueError('choice')
    if any(number(v) is None or not isinstance(v,(int,float)) or not 0<=v<=1 for v in ps.values()): raise ValueError('probability')
    if abs(sum(ps.values())-1)>1e-6 or ps[a['choice']]<max(ps.values())-1e-6: raise ValueError('distribution')
    if number(a.get('confidence')) is None or not 0<=a['confidence']<=1: raise ValueError('confidence')
    return raw['answers']

def stats(rows,ref):
    x=base_stats(rows,ref or 1)
    if x and not ref:
        for k in ('distance_bps','range_bps','crossings'): x[k]=None
    return x

def normalize_book(book,market,at,allow_empty=True):
    q=base_book(book,market,at,allow_empty)
    if q is None: return None
    yes=market['yes'];no='DOWN' if yes=='UP' else 'UP'
    q[yes]['bid_levels']=sorted([(float(p),float(n)) for p,n in book['bids_levels']],reverse=True)
    q[no]['bid_levels']=sorted([(round(1-float(p),10),float(n)) for p,n in book['asks_levels']],reverse=True)
    return q

def walk(levels,amount,buy=False):
    cash=qty=0.
    for price,size in levels:
        take=min(size,(amount-cash)/price if buy else amount-qty)
        cash+=take*price;qty+=take
        if (amount-cash if buy else amount-qty)<1e-10: break
    return cash,qty

class Tape(BaseTape):
    def packet(self,market,cutoff):
        features={};reasons=[]
        for source,history in self.trades.items():
            rows=[r for r in history if r[0]<=cutoff]
            recent={str(s):stats([r for r in rows if r[0]>=cutoff-s*1000],market['reference']) for s in (5,10,30,60)}
            complete=(cutoff-market['start'])//60000
            minutes=[stats([r for r in rows if market['start']+i*60000<=r[0]<market['start']+(i+1)*60000],market['reference']) for i in range(max(0,complete))]
            age=cutoff-rows[-1][0] if rows else None
            features[source]={'recent':recent,'completed_contract_minutes':minutes,'last_age_ms':age,
                              'preopen_in_lookback':cutoff-60000<market['start'],'trade_id_gaps':sum(r[5] for r in rows if r[0]>=cutoff-60000)}
            if age is None or age>2000: reasons.append(source+'_stale')
        q=normalize_book(self.books.get(market['market_id']),market,cutoff)
        if q is None: reasons.append('book_missing')
        if not market.get('reference'): reasons.append('reference_missing')
        if market.get('fee_bps') is None: reasons.append('fee_unknown')
        state={'contract':{k:market[k] for k in ('topic','start','end','reference')},'observed_at':cutoff,
               'remaining_seconds':(market['end']-cutoff)/1000,'features':features,'quote':q,'stake_usdt':1.,
               'fee_bps_cash_sensitivity':market.get('fee_bps'),'data_warnings':reasons}
        for side in ('UP','DOWN'):
            if q: state.setdefault('buy_1u',{})[side]=dict(zip(('spent','shares'),walk(q[side]['ask_levels'],1.,True)))
        a=features['spot']['recent']['5'];b=features['futures']['recent']['5']
        state['basis_bps']=(b['close']/a['close']-1)*10000 if a and b else None
        return {'market':market,'cutoff':cutoff,'state':state,'reasons':reasons}

def model_state(packet):
    state=json.loads(dumps(packet['state']))
    if state['quote']:
        for side in ('UP','DOWN'):
            for k in ('bid_levels','ask_levels'): state['quote'][side][k]=state['quote'][side][k][:3]
    return state

def choose(ps,quote,start):
    if ps and ps['UP']!=ps['DOWN']: return max(ps,key=ps.get),'probability'
    if quote and all(quote[s][k] is not None for s in ('UP','DOWN') for k in ('bid','ask')):
        mids={s:(quote[s]['bid']+quote[s]['ask'])/2 for s in ('UP','DOWN')}
        if mids['UP']!=mids['DOWN']: return max(mids,key=mids.get),'market_midpoint'
    return ('UP' if start//SLOT%2==0 else 'DOWN'),'slot_parity'

def exit_rule(account,p,L,sell_net=None,sell_qty=0):
    if account=='E10_S1_LOSS_EXIT': return (L is not None and L<0),'loss' if L is not None else 'pnl_unavailable'
    if p is None: return False,'probability_unavailable'
    if account=='E10_S2_PROFIT_RISK': return (L is not None and p<(.60 if L>0 else .50)),'profit_risk' if L is not None else 'pnl_unavailable'
    if account=='E10_S3_DIRECTION': return p<.50,'direction'
    if account=='E10_S4_VALUE': return sell_net is not None and sell_qty>0 and sell_net>sell_qty*p,'value'
    return False,'hold_baseline'

def advance(order,book,market,at):
    if order['status']!='open' or at<order['ready']: return
    if at>order['expires']:
        stamps=[order['ready']]+order['observations']+[order['expires']]
        gap=max(b-a for a,b in zip(stamps,stamps[1:]))
        order.update(status='no_fill' if gap<=2000 else 'unknown',max_gap_ms=gap);return
    q=normalize_book(book,market,at)
    if q is None or min(q['book_at_ms'],q['received_at'])<order['ready'] or q['book_at_ms']<=order['seen']: return
    order['seen']=q['book_at_ms'];order['observations'].append(at)
    levels=q[order['side']]['ask_levels' if order['kind']=='entry' else 'bid_levels']
    if not levels: return
    cash,qty=walk(levels,order['amount'],order['kind']=='entry')
    if order['kind']=='exit' and order['account']=='E10_S4_VALUE':
        if order['fee_bps'] is None or cash*(1-order['fee_bps']/10000)<=qty*order['p_side']:
            order.update(status='value_gone',cash=0.,shares=0.);return
    order.update(cash=cash,shares=qty,filled_at=at,avg_price=cash/qty,
                 status='filled' if (cash if order['kind']=='entry' else qty)>=order['amount']-1e-9 else 'partial')
