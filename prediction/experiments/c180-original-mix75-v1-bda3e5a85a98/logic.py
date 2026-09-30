"""Versioned paper-only policy and prompt ablations."""
import copy
from original_logic import (SLOT, MODEL, Tape as OriginalTape, QUESTIONS as ORIGINAL_QUESTIONS,
                            model_state as original_state, normalize_book, walk, choose,
                            advance as original_advance, dumps, digest, number)
from features import build, META_OPTIONS, SCHEMA

ACCOUNTS = ['E10_HOLD', 'E10_S1_LOSS_EXIT', 'ORIGINAL_VALUE', 'NUMERIC_VALUE',
            'JEV_FEATURE_VALUE', 'NUMERIC_ENTRY_VALUE', 'JEV_ENTRY_VALUE', 'C180_OBSERVE']
ENTRY_LANE = {a: 'E10' for a in ACCOUNTS}
ENTRY_LANE.update(NUMERIC_ENTRY_VALUE='NUMERIC', JEV_ENTRY_VALUE='JEV')
CONFIG = dict(version='early10-jev-v2', stake=1., timeout_ms=3000, delay_ms=1000,
              ttl_ms=12000, max_book_age_ms=2000, input_bytes=30000, target=200,
              exit_buffer_per_share=.005, entry_buffer_usdt=.005, accounts=ACCOUNTS,
              phases=[('E10', 10000), ('C180', 120000), ('C90', 210000)])
GUIDANCE = ('Predict FINAL official settlement conditional on non-tie, never the current side of reference. '
            'Futures discount/basis is not DOWN evidence; correlated windows are not independent votes. '
            'Explicitly account for conflicts and missing official live price. Market mid is a fallible anchor. ')
QUESTIONS = copy.deepcopy(ORIGINAL_QUESTIONS)
AB_QUESTIONS = copy.deepcopy(QUESTIONS)
AB_QUESTIONS['direction']['instructions'] = GUIDANCE + QUESTIONS['direction']['instructions']
META_QUESTIONS = {
    'edge': {'type': 'choice', 'instructions': GUIDANCE + 'Which direction has evidence for an edge relative to market baseline? Do not output final settlement probability.',
             'criteria': {'UP': 'Evidence favors UP relative to market', 'DOWN': 'Evidence favors DOWN relative to market', 'NONE': 'No defensible deviation from market'}},
    'conflict': {'type': 'choice', 'instructions': 'Do spot momentum, futures momentum, basis changes and prediction book give materially conflicting evidence?',
                 'criteria': {'LOW': 'Broadly consistent evidence', 'HIGH': 'Contradictory or missing evidence'}},
    'regime': {'type': 'choice', 'instructions': 'Classify the observed price path using supplied evidence only; do not predict an action.',
               'criteria': {'TREND': 'Persistent directional pressure', 'REVERSAL': 'Prior pressure exhausting or reversing', 'CHOP': 'No stable direction or insufficient evidence'}},
    'uncertainty': {'type': 'choice', 'instructions': 'How uncertain is evidence for deviating from the market anchor? Missing official prices and conflicting data increase uncertainty.',
                    'criteria': {'LOW': 'Strong consistent evidence', 'HIGH': 'Weak, conflicting or incomplete evidence'}}}
DEFINITION = digest({'config': CONFIG, 'model': MODEL, 'original': QUESTIONS,
                     'ab': AB_QUESTIONS, 'meta': META_QUESTIONS, 'schema': SCHEMA})
CANDIDATE = dict(version='c180-original-mix75-v1',entry_phase='C180',entry_source='Original',
                 favorite_only=True,entry_ev_buffer=.005,stake=1.,exit_phase='C90',
                 exit_market_weight=.75,exit_A_weight=.25,exit_buffer_per_share=.005,
                 retain_baseline_context=True)
DEFINITION = digest({'baseline_definition':DEFINITION,'candidate':CANDIDATE})


def parse_response(raw, questions=None):
    questions = questions or QUESTIONS
    if raw.get('model') != MODEL or not isinstance(raw.get('answers'), dict):
        raise ValueError('model/answers')
    for name, spec in questions.items():
        a = raw['answers'].get(name, {}); ps = a.get('probabilities', {})
        if a.get('type') != 'choice' or set(ps) != set(spec['criteria']) or a.get('choice') not in ps:
            raise ValueError('choice')
        if any(type(v) not in (int, float) or number(v) is None or not 0 <= v <= 1 for v in ps.values()):
            raise ValueError('probability')
        if abs(sum(ps.values())-1) > 1e-6 or ps[a['choice']] < max(ps.values())-1e-6:
            raise ValueError('distribution')
        if number(a.get('confidence')) is None or not 0 <= a['confidence'] <= 1:
            raise ValueError('confidence')
    return raw['answers']


class Tape(OriginalTape):
    def packet(self, market, cutoff):
        packet = super().packet(market, cutoff)
        if market.get('identified_at') is not None and market['identified_at'] > cutoff:
            packet['reasons'].append('identity_not_known')
        packet['state']['evidence_v2'] = build(packet['state'], self.trades)
        return packet


def model_state(packet, lane='Original'):
    state = original_state(packet)
    evidence = state.pop('evidence_v2')
    # Extra holdings are internal execution data, not extra evidence in Original.
    state.pop('holdings', None)
    if lane == 'Original':
        return state
    state.pop('basis_bps', None)  # one basis observation, not two copies of evidence
    allowed = {'return_bps', 'volume', 'signed_volume_ratio', 'aggregate_trades'}
    future = state['features']['futures']
    for key in ('recent', 'completed_contract_minutes'):
        values = future[key]
        def clean(x):
            return {k: v for k, v in x.items() if k in allowed} if x else None
        future[key] = {k: clean(v) for k, v in values.items()} if isinstance(values, dict) else [clean(v) for v in values]
    state['evidence_v2'] = evidence
    if lane == 'A':
        for key in ('normalized_distance', 'sigma_remaining_bps', 'diffusion_up'):
            evidence['numeric'].pop(key)
        evidence.pop('volatility')
    return state


def entry_ev(p, quote, fee_bps):
    if p is None or quote is None or fee_bps is None:
        return None, 'missing_probability_book_or_fee'
    options = []
    for side in ('UP', 'DOWN'):
        cash, qty = walk(quote[side]['ask_levels'], 1., True)
        if cash < 1.-1e-9:
            continue
        value = qty*(p if side == 'UP' else 1-p)-cash*(1+fee_bps/10000)
        options.append((value, side))
    if not options or max(options)[0] <= CONFIG['entry_buffer_usdt']:
        return None, 'nonpositive_cost_after_ev'
    return max(options)[1], 'positive_cost_after_ev'


def value_exit(p, holding):
    if p is None or holding['sale_net'] is None or holding['sale_qty'] < holding['shares']-1e-9:
        return False
    return holding['sale_net'] > holding['shares']*(p+CONFIG['exit_buffer_per_share'])


def advance(order, book, market, at):
    if order['status'] != 'open' or at < order['ready']:
        return
    if at <= order['expires'] and order.get('ev_guard'):
        q = normalize_book(book, market, at)
        if q is None or min(q['book_at_ms'], q['received_at']) < order['ready'] or q['book_at_ms'] <= order['seen']:
            return
        levels = q[order['side']]['ask_levels' if order['kind'] == 'entry' else 'bid_levels']
        if levels:
            cash, qty = walk(levels, order['amount'], order['kind'] == 'entry')
            fee = order['fee_bps']/10000
            good = (qty*order['p_side']-cash*(1+fee) > CONFIG['entry_buffer_usdt'] and cash >= order['amount']-1e-9) if order['kind'] == 'entry' else cash*(1-fee) > qty*(order['p_side']+CONFIG['exit_buffer_per_share'])
            if not good:
                order.update(status='value_gone', cash=0., shares=0.)
                return
    if order.get('original_value_guard'):
        account = order['account']
        order['account'] = 'E10_S4_VALUE'
        try:
            original_advance(order, book, market, at)
        finally:
            order['account'] = account
    else:
        original_advance(order, book, market, at)
