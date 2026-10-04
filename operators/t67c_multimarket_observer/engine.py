"""Causal seven-lane quote simulator. Never creates a trading repository/client.

Core freezing and execution use production functions. A selected quote needs a
subsequent fresh source book before its unchanged deadline; this is a research
checkpoint, not a model of queue position, POST latency or actual fills.
"""
from copy import deepcopy
from decimal import Decimal as D
from types import SimpleNamespace
from src.gridbot.prediction.regime_t67a_bridge import freeze_core, additions
from src.gridbot.prediction.regime_t67c_bridge import _refusal
from src.gridbot.prediction.regime_t67c_policy import FINGERPRINT, LIVE_BRANCHES, SHADOW_BRANCHES
from src.gridbot.prediction.regime_t63_lane import eligible_execution
from src.gridbot.prediction.regime_t67_lane import execution, candidates
from src.gridbot.prediction.regime_t67c_shadow import _usable_depth
from src.gridbot.prediction.regime_worker_bridge import RegimeWorkerBridge

VERSION='t67c-three-market-quote-v1'
SLOT=300000


def same_fee(left,right):
    """Compare validated basis points, independent of JSON numeric spelling."""
    try:
        a,b=D(str(left)),D(str(right))
        return a.is_finite() and b.is_finite() and 0<=a<=10000 and a==b
    except (ValueError,TypeError,ArithmeticError):
        return False


def new_state(symbol,start):
    return dict(version=VERSION,policy=FINGERPRINT,symbol=symbol,start=start,end=start+SLOT,
                reason='awaiting',diagnostics={},shadow={},selected=None,quote=None)


def reject(state,reason):
    state['reason']=reason
    state['diagnostics'][reason]=state['diagnostics'].get(reason,0)+1


def quote(candidate,ex,book,at):
    return dict(branch=candidate['branch'],side=candidate['side'],at_ms=at,
                book_at_ms=book['book_at_ms'],source_received_ms=book['received_at_ms'],
                cash=str(ex['cash']),net_shares=str(ex['net_shares']),limit=str(ex['limit']),
                fee_bps=str(book['fee_bps']),status='QUOTE_SIMULATION_NOT_FILL')


def evaluate(state,bridge,market,features,book,at):
    start=state['start'];unit=D(1)
    if not start+124000<=at<start+136000 or state.get('quote'):return
    try:
        if not book:
            reject(state,'book_missing');return
        stamp=bridge._book(book,market,at)
        if at-stamp>1000:
            reject(state,'book_stale');return
        state['valid_book_seen']=True
        if 'guard' not in state:
            if at>start+126000:
                reject(state,'initial_window_missed');return
            if not features:
                reject(state,'features_missing');return
            state['features_present']=True
            guard=freeze_core(bridge,market,features,at,unit)
            if D(guard['fee_bps'])!=D(book['fee_bps']):raise ValueError('initial_fee_changed')
            state['guard']=guard
            state['initial_book_evidence']=deepcopy(bridge._first_book(market,at))
            state['market_identity']=[str(market.market_topic_id),str(market.up_market_id)]
        guard=state['guard']
        if state['market_identity']!=[str(market.market_topic_id),str(market.up_market_id)] or D(guard['fee_bps'])!=D(book['fee_bps']):
            raise ValueError('frozen_identity_unit_fee_mismatch')
        selected=state['selected']
        if selected:
            if at>=selected['expires_at_ms']:
                reject(state,'selected_expired');return
            # Do not call the same observed book a second confirmation.
            if stamp<=selected['book_at_ms'] or at<=selected['at_ms']:return
            c=selected['candidate'];ex=eligible_execution(c,book,unit) if c['branch'].startswith('core_') else execution(book,c['side'],unit,lower=c['lower'],cap=c['cap'])
            state['quote']=quote(c,ex,book,at);state['confirmation_book_evidence']=deepcopy(book);state['reason']='simulated_quote';return
        if at>start+134500:return
        choices=additions(book,guard['features'],unit) if guard['empty'] else guard['candidates']
        state['eligible_branches']=[c['branch'] for c in choices]
        for c in choices:
            try:
                ex=eligible_execution(c,book,unit) if c['branch'].startswith('core_') else execution(book,c['side'],unit,lower=c['lower'],cap=c['cap'])
            except (ValueError,KeyError,TypeError,ArithmeticError) as exc:
                reject(state,_refusal(exc,book,at));continue
            state['selected']=dict(candidate=deepcopy(c),at_ms=at,book_at_ms=stamp,
                expires_at_ms=min(start+136000,at+2000) if guard['empty'] else start+136000,
                initial_quote=quote(c,ex,book,at))
            state['reason']='selected_waiting_new_book';return
        if not choices:reject(state,'no_candidate_core_empty' if guard['empty'] else 'core_reserved')
    except (ValueError,KeyError,TypeError,ArithmeticError) as exc:
        known={'feature_provenance','feature_asset_mismatch','initial_book_missing','initial_book_clock',
               'original_identity','original_missing_or_invalid_for_empty_core'}
        reason=str(exc) if isinstance(exc,ValueError) and str(exc) in known else _refusal(exc,book,at)
        reject(state,reason)


def observe_shadow(state,market,books,spots,at):
    """Two independent reference lanes, excluded from seven-lane portfolio."""
    start=state['start']
    if not start+60000<=at<start+270000 or not books:return
    book=books[-1]
    try:
        stamp=RegimeWorkerBridge._book(book,market,at)
        if at-stamp>1000 or int(book['reference_received_ms'])>at or D(book['reference'])<=0 or not _usable_depth(book):return
        if not same_fee(book['fee_bps'],state['meta']['fee_bps']):return
        memory=state.setdefault('shadow_state',{})
        choices=candidates(book,spots,None,memory,at,D(1),[b for b in books[:-1] if _usable_depth(b)])
        for c in choices:
            if c['branch'] not in SHADOW_BRANCHES or c['branch'] in state['shadow']:continue
            ex=execution(book,c['side'],D(1),c['probability'],cap=c['cap'],lower=c['lower'])
            state['shadow'][c['branch']]=quote(c,ex,book,at)
        state['shadow_evaluated_at_ms']=at
    except (ValueError,KeyError,TypeError,ArithmeticError):
        state['shadow_data_error']=True


def settle(state,official,at):
    """Only verified First-observer official evidence, no candle-derived winners."""
    if state.get('winner') or state['end']>at or not official.get('winner'):return
    meta=official.get('meta',{});own=state.get('meta',{})
    keys=('symbol','start','end','topic','market_id','tokens','fee_bps','reference')
    if any(meta.get(k)!=own.get(k) for k in keys) or official.get('known_at_ms',at+1)>at:
        state['settlement_error']='official_identity_or_time';return
    winner=official['winner']
    if winner not in ('UP','DOWN','DRAW'):return
    state['winner']=winner;state['known_at_ms']=official['known_at_ms']
    for q in [state.get('quote'),*state['shadow'].values()]:
        if q:
            payout=D('.5') if winner=='DRAW' else D(int(winner==q['side']))
            q['pnl']=str(payout*D(q['net_shares'])-D(q['cash']))
            q['winner']=winner;q['known_at_ms']=official['known_at_ms']
