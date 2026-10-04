"""Frozen quote-only First research policy; no trading or network dependencies."""
from decimal import Decimal, ROUND_DOWN
import hashlib
import json

SYMBOLS = ('BTCUSDT', 'ETHUSDT', 'BNBUSDT')
SLOT = 300000
POLICY = dict(version=1, symbols=SYMBOLS, epsilon_bp='0.5', prior_bp='1',
              lower='0.15', upper='0.45', unit='1', feature_window=[120000,123000],
              initial_window=[124000,126000], recheck_window=[128000,129500],
              max_book_age_ms=1000, share_step='0.01',
              simulation='full_displayed_depth_at_fixed_recheck_assumed_execution',
              real_fills=False, risk_applied=False, selector_enabled=False)
FINGERPRINT = hashlib.sha256(json.dumps(POLICY, sort_keys=True).encode()).hexdigest()

def dec(x):
    d = Decimal(str(x))
    if not d.is_finite(): raise ValueError('nonfinite')
    return d

def features(symbol, start, candles, received):
    if symbol not in SYMBOLS or start % SLOT or not start+120000 <= received <= start+123000:
        raise ValueError('feature_window_or_identity')
    if len(candles) != 17: raise ValueError('candle_count')
    for i,c in enumerate(candles):
        opening = start-900000+i*60000
        if int(c[0]) != opening or int(c[6]) != opening+59999 or min(dec(c[1]),dec(c[4]))<=0:
            raise ValueError('candle_identity')
    bp=lambda a,b: str((dec(b)/dec(a)-1)*10000)
    a,b=dec(bp(candles[15][1],candles[15][4])),dec(bp(candles[16][1],candles[16][4]))
    prior=dec(bp(candles[0][1],candles[14][4]))
    reversal=abs(a)>=dec('.5') and abs(b)>=dec('.5') and a*b<0
    side=('UP' if a>0 else 'DOWN') if reversal else None
    trend=bool(reversal and (prior>=1 if side=='UP' else prior<=-1))
    return dict(symbol=symbol,start=start,received_at_ms=received,first_bp=str(a),last_bp=str(b),
                prior_bp=str(prior),reversal=reversal,side=side,trend_pass=trend,
                reason='trend_pass' if trend else 'prior_trend_filter' if reversal else 'not_first_reversal')

def metadata(raw, symbol, start):
    d=raw.get('data',raw)
    if (d.get('symbol')!=symbol or symbol not in SYMBOLS or int(d['startDate'])!=start
            or int(d['endDate'])!=start+SLOT or not str(d.get('slug','')).startswith(symbol[:-4].lower()+'-updown-5m-')):
        raise ValueError('market_identity')
    v=d.get('variantData',{})
    if v.get('priceFeedSymbol')!=symbol or v.get('type')!='CRYPTO_UP_DOWN' or dec(v['startPrice'])<=0:
        raise ValueError('reference_identity')
    fee=dec(d['feeRateBps'])
    if not 0<=fee<=10000: raise ValueError('fee')
    nodes=d['markets']
    if len(nodes)!=1: raise ValueError('binary_market_count')
    node=nodes[0];outcomes=node['outcomes']
    if len(outcomes)!=2: raise ValueError('outcome_count')
    tokens={}
    for o in outcomes:
        side=str(o.get('name','')).upper()
        if side not in ('UP','DOWN') or side in tokens or not str(o.get('tokenId','')).isdigit():
            raise ValueError('outcome_identity')
        tokens[side]=str(o['tokenId'])
    if tokens['UP']==tokens['DOWN']: raise ValueError('duplicate_token')
    topic=str(d['marketTopicId']);mid=str(node['marketId'])
    if not topic.isdigit() or not mid.isdigit(): raise ValueError('topic_market')
    return dict(symbol=symbol,start=start,end=start+SLOT,topic=topic,market_id=mid,tokens=tokens,
                reference=str(v['startPrice']),fee_bps=str(fee))

def book(raw, meta, side, received):
    d=raw.get('data',raw)
    if str(d.get('tokenId'))!=meta['tokens'][side] or str(d.get('outcome','')).upper()!=side:
        raise ValueError('book_token_side')
    if d.get('marketId') is not None and str(d['marketId'])!=meta['market_id']:raise ValueError('book_market')
    ts=int(d['timestamp'])
    if not 0<=received-ts<=POLICY['max_book_age_ms']:raise ValueError('book_stale_or_future')
    levels=[]
    for x in d['asks']:
        p,q=dec(x['price']),dec(x['size'])
        if not 0<p<1 or q<=0: raise ValueError('ask_invalid')
        levels.append([str(p),str(q)])
    if not levels or levels!=sorted(levels,key=lambda x:dec(x[0])):raise ValueError('ask_sort_or_empty')
    bids=d.get('bids',[])
    if bids and max(dec(x['price']) for x in bids)>dec(levels[0][0]):raise ValueError('crossed_book')
    return dict(side=side,book_at_ms=ts,received_at_ms=received,levels=levels)

def walk(levels, fee_bps, cap='0.90', gross_shares=None):
    """Same 1U depth walk / 0.01 gross-share floor / share fee as T6."""
    fee=dec(fee_bps)/10000;cap=dec(cap)
    if not 0<=fee<=1:raise ValueError('fee')
    parsed=[(dec(p),dec(q)) for p,q in levels]
    if not parsed or parsed!=sorted(parsed) or any(not 0<p<1 or q<=0 for p,q in parsed):raise ValueError('depth')
    if gross_shares is None:
        cash=gross=Decimal(0);worst=None
        for p,q in parsed:
            if p>cap:break
            take=min(q,(1-cash)/p);cash+=take*p;gross+=take;worst=p
            if cash>=1-dec('.0000000001'):break
        if cash<1-dec('.0000000001'):raise ValueError('insufficient_1u_depth')
        gross=gross.quantize(dec('.01'),rounding=ROUND_DOWN)
        initial_limit=worst
    else:
        gross=dec(gross_shares);worst=None
        if gross<=0 or gross!=gross.quantize(dec('.01')):raise ValueError('share_step')
    initial_limit=worst if gross_shares is not None else initial_limit
    remaining=gross;cash=net=Decimal(0)
    for p,q in parsed:
        if p>cap:break
        take=min(q,remaining);cash+=take*p;net+=take*(1-fee*min(p,1-p)/p)
        if take>0:worst=p
        remaining-=take
        if remaining<=0:break
    if remaining>0 or cash<=0 or cash>1:raise ValueError('insufficient_frozen_share_depth')
    return dict(cash=str(cash),gross_shares=str(gross),net_shares=str(net),limit=str(worst if gross_shares is not None else initial_limit),fee_bps=str(fee_bps))

def initial_quote(meta, features_, quote, at):
    if not meta['start']+124000<=at<=meta['start']+126000:raise ValueError('initial_window')
    if not features_['trend_pass'] or quote['side']!=features_['side']:raise ValueError('first_ineligible')
    if not 0<=at-quote['book_at_ms']<=1000:raise ValueError('quote_age')
    ex=walk(quote['levels'],meta['fee_bps'])
    if not dec('.15')<=dec(ex['limit'])<=dec('.45'):raise ValueError('price_band')
    if not dec('.15')<=dec(quote['levels'][0][0])<=dec('.45'):raise ValueError('price_band')
    return ex

def recheck(meta, initial, quote, at):
    if not meta['start']+128000<=at<=meta['start']+129500:raise ValueError('recheck_window')
    if not 0<=at-quote['book_at_ms']<=1000:raise ValueError('quote_age')
    if not dec('.15')<=dec(quote['levels'][0][0])<=dec('.45'):raise ValueError('price_band')
    return walk(quote['levels'],meta['fee_bps'],initial['limit'],initial['gross_shares'])

def resolution(raw, saved, at):
    current=metadata(raw,saved['symbol'],saved['start'])
    for key in ('topic','market_id','tokens','reference','fee_bps'):
        if current[key]!=saved[key]:raise ValueError('resolution_identity_'+key)
    if at<saved['end']:raise ValueError('resolution_future')
    d=raw.get('data',raw);node=d['markets'][0]
    terminal=str(node.get('status') or d.get('status','')).upper() in ('RESOLVED','SETTLED')
    if not terminal:return None
    winners=[]
    for o in node['outcomes']:
        if o.get('winner') is True or o.get('isWinner') is True:winners.append(str(o['name']).upper())
    nested=winners[0] if len(winners)==1 else None
    if len(winners)==2 and all(dec(o.get('price','-1'))==dec('.5') for o in node['outcomes']):nested='DRAW'
    for key in ('resolvedOutcome','finalOutcome','winner','outcome','resolvedSide','result'):
        direct=str(d.get(key,'')).upper()
        if direct in ('UP','DOWN','DRAW') and direct!=nested:raise ValueError('winner_conflict')
    return nested

def pnl(ex,winner,side):
    if winner not in ('UP','DOWN','DRAW'):raise ValueError('unresolved')
    payout=dec('.5') if winner=='DRAW' else dec(1 if winner==side else 0)
    return str(payout*dec(ex['net_shares'])-dec(ex['cash']))
