"""Causal T6.7 candidates. Only actual Live claims/orders are performance data."""
import math
from decimal import Decimal as D

from .regime_lane import dec, walk
from .regime_t67_policy import POLICY, FINGERPRINT


def probability(book, spots, at_ms):
    start = int(book['market_start_ms'])
    current = [s for s in spots if s['source'] == 'binance_spot'
               and s['received_ms'] <= at_ms and 0 <= at_ms-s['event_ms'] <= 1500]
    if not current:
        raise ValueError('spot missing/stale')
    latest = max(current, key=lambda s: (s['event_ms'], s['received_ms']))
    generation = latest['generation']
    history = [s for s in spots if s['source'] == latest['source'] and s['generation'] == generation
               and at_ms-900000 <= s['event_ms'] <= latest['event_ms'] and s['received_ms'] <= at_ms]
    # The official start reference anchors the proxy's level, not its source.
    # Constant proxy/oracle basis is an explicit experimental assumption.
    anchors = [s for s in history if start <= s['event_ms'] <= start+1500
               and start <= s['received_ms'] <= start+1500]
    if not anchors:
        raise ValueError('opening proxy anchor missing or generation changed')
    history = sorted({s['event_ms']: s for s in history}.values(), key=lambda s: s['event_ms'])
    if len(history) < 21 or history[-1]['event_ms']-history[0]['event_ms'] < 60000:
        raise ValueError('volatility history incomplete')
    values = [float(dec(s['price'])) for s in history]
    if any(x <= 0 or not math.isfinite(x) for x in values):
        raise ValueError('spot price invalid')
    variance = sum(math.log(b/a)**2 for a, b in zip(values, values[1:]))
    seconds = (history[-1]['event_ms']-history[0]['event_ms'])/1000
    sigma = max(float(POLICY['sigma_floor_bp_sqrt_second'])/10000, math.sqrt(variance/seconds))
    remaining = (start+300000-at_ms)/1000
    if remaining <= 0 or dec(book['reference']) <= 0 or int(book['reference_received_ms']) > at_ms:
        raise ValueError('reference or remaining time invalid')
    opening = dec(min(anchors, key=lambda s: (s['event_ms'], s['received_ms']))['price'])
    spot = dec(latest['price'])
    z = max(-8, min(8, math.log(float(spot/opening))/(sigma*math.sqrt(remaining))))
    p = D(str(.5*(1+math.erf(z/math.sqrt(2)))))
    return p, latest, {'opening_proxy': str(opening), 'reference': str(book['reference']),
                       'sigma': str(sigma), 'samples': len(history),
                       'source': latest['source'], 'generation': generation,
                       'basis_assumption': 'constant_proxy_vs_settlement_basis'}


def execution(book, side, amount, probability_up=None, minimum_ev='.005', cap='.75'):
    if dec(book['quote'][side]['ask_levels'][0][0]) < D('.10'):
        raise ValueError('best ask below price band')
    result = walk(book['quote'][side]['ask_levels'], book['fee_bps'], cap=dec(cap), amount=amount)
    if not D('.10') <= result['limit'] <= dec(cap):
        raise ValueError('price band')
    if probability_up is not None:
        p = dec(probability_up) if side == 'UP' else 1-dec(probability_up)
        if p*result['net_shares']-result['cash'] < dec(minimum_ev)*dec(amount):
            raise ValueError('fee net EV')
    return result


def model_candidate(branch, book, side, p_up, amount, model):
    result = execution(book, side, amount, p_up, '.03')
    shifted = {**book, 'quote': {**book['quote'], side: {'ask_levels':
        [[str(dec(p)+D('.02')), q] for p, q in book['quote'][side]['ask_levels'] if dec(p)+D('.02') < 1]}}}
    execution(shifted, side, amount, p_up, '.005')
    return dict(branch=branch, side=side, action=branch, probability=str(p_up),
                cap=str(min(D('.75'), result['limit']+D('.02'))), lower='.10',
                model=model, fingerprint=FINGERPRINT)


def confirmation_book(snapshot, current, at_ms):
    for key in ('market_start_ms', 'market_topic', 'market_id', 'reference', 'fee_bps'):
        if snapshot[key] != current[key]:
            raise ValueError('confirmation identity changed')
    stamp = int(snapshot['book_at_ms'])
    if snapshot.get('full_depth') is not True or not 0 <= at_ms-stamp <= 1000:
        raise ValueError('confirmation depth/clock')
    for key in ('received_at', 'received_at_ms', 'captured_at_ms'):
        if not stamp <= int(snapshot[key]) <= at_ms:
            raise ValueError('confirmation source/receipt clock')


def confirm_lead_lag(trigger, book, spots, prior_books, at_ms, amount, p, latest, model):
    side = trigger['side']
    if p is None or latest['generation'] != trigger['generation'] or at_ms-trigger['at_ms'] > 2000:
        raise ValueError('confirmation source/window')
    if (dec(latest['price'])-dec(trigger['spot']))*(1 if side == 'UP' else -1) < 0:
        raise ValueError('external move reversed')
    if dec(book['quote'][side]['ask_levels'][0][0])-dec(trigger['ask']) >= D('.01'):
        raise ValueError('prediction price caught up')
    conservative = min(p, dec(trigger['p_up'])) if side == 'UP' else max(p, dec(trigger['p_up']))
    for offset in POLICY['lead_lag']['confirm_ms']:
        key = str(offset)
        confirmations = trigger.setdefault('confirmations', {})
        if key not in confirmations:
            # Reconstruct both checkpoints from immutable public evidence so
            # worker cadence does not pretend a one-second sample was T+300ms.
            beginning = trigger['at_ms']+offset
            ending = beginning+POLICY['lead_lag']['confirmation_grace_ms']
            snapshots = [s for s in (*prior_books, book)
                         if beginning <= int(s['captured_at_ms']) <= min(ending, at_ms)
                         and int(s['book_at_ms']) >= beginning]
            if not snapshots:
                if at_ms > ending:
                    raise ValueError('confirmation checkpoint missed')
                return None
            sample = min(snapshots, key=lambda s: (s['captured_at_ms'], s['book_at_ms']))
            cutoff = int(sample['captured_at_ms'])
            confirmation_book(sample, book, cutoff)
            cp, spot, _ = probability(sample, spots, cutoff)
            if spot['generation'] != trigger['generation']:
                raise ValueError('confirmation source changed')
            if (dec(spot['price'])-dec(trigger['spot']))*(1 if side == 'UP' else -1) < 0:
                raise ValueError('checkpoint reversed')
            if dec(sample['quote'][side]['ask_levels'][0][0])-dec(trigger['ask']) >= D('.01'):
                raise ValueError('checkpoint caught up')
            model_candidate('external_lead_lag', sample, side, cp, amount, model)
            confirmations[key] = {'at_ms': cutoff, 'p_up': str(cp)}
        cp = dec(confirmations[key]['p_up'])
        conservative = min(conservative, cp) if side == 'UP' else max(conservative, cp)
    return model_candidate('external_lead_lag', book, side, conservative, amount, model)


def candidates(book, spots, features, state, at_ms, amount, prior_books=()):
    """Mutates durable routing state; never receives a winner or PnL."""
    start = int(book['market_start_ms'])
    result = []
    p = latest = model = None
    try:
        p, latest, model = probability(book, spots, at_ms)
    except (ValueError, KeyError, TypeError, ArithmeticError):
        pass

    trigger = state.get('lead_lag')
    if trigger and trigger.get('status') == 'PENDING':
        try:
            choice = confirm_lead_lag(trigger, book, spots, prior_books, at_ms, amount, p, latest, model)
            if choice is not None:
                result.append(choice)
                trigger['status'] = 'CONFIRMED'
        except (ValueError, KeyError, TypeError, ArithmeticError):
            trigger['status'] = 'REJECTED'
    elif trigger is None and p is not None:
        before = [s for s in spots if s['source'] == latest['source'] and s['generation'] == latest['generation']
                  and latest['received_ms']-1500 <= s['received_ms'] <= latest['received_ms']-1000]
        old_books = [b for b in prior_books if at_ms-1500 <= int(b['captured_at_ms']) <= at_ms-1000]
        if before and old_books:
            delta = (dec(latest['price'])/dec(before[-1]['price'])-1)*10000
            side = 'UP' if delta > 0 else 'DOWN'
            try:
                confirmation_book(old_books[-1], book, int(old_books[-1]['captured_at_ms']))
                ask = dec(book['quote'][side]['ask_levels'][0][0])
                previous = dec(old_books[-1]['quote'][side]['ask_levels'][0][0])
                if abs(delta) >= 2 and ask-previous < D('.01'):
                    state['lead_lag'] = dict(status='PENDING', side=side, at_ms=at_ms,
                                            ask=str(ask), spot=latest['price'], p_up=str(p),
                                            generation=latest['generation'])
            except (ValueError, KeyError, TypeError, ArithmeticError):
                pass

    for offset in POLICY['reference_value_ms']:
        if start+offset <= at_ms <= start+offset+1500 and str(offset) not in state.setdefault('value_seen', []):
            state['value_seen'].append(str(offset))
            if p is not None:
                choices = []
                for side in ('UP', 'DOWN'):
                    try:
                        c = model_candidate('reference_value', book, side, p, amount, model)
                        e = execution(book, side, amount, p)
                        prob = p if side == 'UP' else 1-p
                        choices.append((prob*e['net_shares']-e['cash'], c))
                    except (ValueError, KeyError, TypeError, ArithmeticError):
                        pass
                if choices:
                    result.append(max(choices, key=lambda x: x[0])[1])

    if start+124000 <= at_ms <= start+134500 and features is not None:
        first, last = (dec(features[k]) for k in ('first_bp', 'last_bp'))
        if first*last < 0 and abs(first) >= 1 and abs(first) >= 2*abs(last):
            net = (1+first/10000)*(1+last/10000)-1
            side = 'UP' if net > 0 else 'DOWN'
            try:
                execution(book, side, amount)
                result.append(dict(branch='shallow_retracement', side=side, action='shallow_retracement',
                                   probability=None, lower='.10', cap='.75', fingerprint=FINGERPRINT))
            except (ValueError, KeyError, TypeError, ArithmeticError):
                pass
    return result
