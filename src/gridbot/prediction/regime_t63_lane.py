"""T6.3 frozen candidates: JEV conflict, late momentum, net-down reversal."""
import hashlib
import json
from decimal import Decimal
from .regime_lane import dec, walk, state_of
from .regime_t62_lane import select_primary, FINGERPRINT as PARENT
from .regime_t61_lane import select_fallback

PROFILE = 'regime_target6_3_v1'
TIER = 'REGIME_T63'
POLICY = dict(profile=PROFILE, parent=PARENT, A=['p_up<0.5','DOWN','0.20','0.40'],
              B=['late_move','abs_last>=1','aligned_prior>=1','0.25','0.65'],
              C=['reversal','net<=-1','DOWN','0.65','0.75'],
              units=[1,2,3], decision_ms=[124000,126000], last_selection_ms=134500,
              expiry_ms=136000, risk_state_key='regime_target6_risk_v1', version=1)
FINGERPRINT = hashlib.sha256(json.dumps(POLICY,sort_keys=True).encode()).hexdigest()

def eligible_execution(candidate, snapshot, amount):
    side=candidate['side']; levels=snapshot['quote'][side]['ask_levels']
    if not levels or not dec(candidate['lower'])<=dec(levels[0][0])<=dec(candidate['upper']):
        raise ValueError('candidate_price_band')
    execution=walk(levels,snapshot['fee_bps'],dec(candidate['cap']),amount)
    if candidate['action']=='original' and dec(candidate['probability'])*execution['net_shares']-execution['cash']<=Decimal('.005')*amount:
        raise ValueError('candidate_ev')
    return execution

def candidates(features, original, snapshot, amount):
    """No outcomes; baseline T6.2 eligibility evaluated at initial book."""
    features=dict(features)
    first,last,prior=(dec(features[k]) for k in ('first_bp','last_bp','prior_bp'))
    features.setdefault('net_bp',str(((1+first/10000)*(1+last/10000)-1)*10000))
    base=select_primary(features,original);base['branch']='T6'
    def validate(d):
        if not d['allowed']: return d
        try:
            ex=walk(snapshot['quote'][d['side']]['ask_levels'],snapshot['fee_bps'],amount=amount)
            if not dec(d['lower'])<=ex['limit']<=dec(d['upper']):
                return {**d,'allowed':False,'reason':'price_band'}
            d={**d,'cap':str(ex['limit'])}
            eligible_execution(d,snapshot,amount)
            return d
        except (ValueError,KeyError,TypeError,ArithmeticError):
            return {**d,'allowed':False,'reason':'initial_depth_or_fee_invalid'}
    base=validate(base)
    if not base['allowed'] and base['reason']!='initial_depth_or_fee_invalid':
        base=validate(select_fallback(features,original,base))
    out=[]
    def add(branch,action,side,lo,hi):
        out.append(dict(branch=branch,action=action,side=side,lower=lo,upper=hi,cap=hi,
                        state=state_of(first,last),allowed=True,reason='t63_candidate'))
    if base['allowed']:
        start=int(features['market_start_ms'])
        valid=(original and original.get('market_start_ms')==start
               and original.get('cutoff_ms')==start+120000
               and start+120000<=int(original.get('completed_at_ms',0))<=start+123000)
        if base['action']=='net_up' and valid and 0<=dec(original['original_p_up'])<Decimal('.5'):
            add('A_jev_conflict','jev_conflict','DOWN','.20','.40')
            out[-1]['jev_p_up']=str(original['original_p_up'])
        out.append(base)
    elif base['reason']!='initial_depth_or_fee_invalid':
        state=state_of(first,last)
        if state=='late_move' and abs(last)>=1 and abs(prior)>=1 and last*prior>0:
            add('B_late_momentum','late_momentum','UP' if last>0 else 'DOWN','.25','.65')
        elif state=='reversal' and dec(features['net_bp'])<=-1:
            add('C_reversal_netdown','net_down','DOWN','.65','.75')
    return out
