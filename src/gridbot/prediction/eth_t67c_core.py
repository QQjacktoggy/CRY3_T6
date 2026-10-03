"""Pure ETH validation and T6.7c candidate routing. No client or database imports."""
from decimal import Decimal

from .eth_t67c_policy import FINGERPRINT, SLOT_MS, SYMBOL, digest
from .models import MarketInfo
from .regime_lane import dec, freeze_features, state_of, walk
from .regime_t63_lane import eligible_execution
from .regime_t65_lane import candidates as btc_core_candidates
from .regime_t67a_bridge import additions, core_name
from .regime_t67_lane import execution as additive_execution


def validate_spec(spec):
    """Operator-reviewed evidence is required; there are no default ETH specifications."""
    if not isinstance(spec, dict) or spec.get('verified') is not True:
        raise ValueError('eth_spec_unverified')
    if (spec.get('symbol') != SYMBOL or spec.get('underlying') != 'ETH'
            or spec.get('duration_ms') != SLOT_MS):
        raise ValueError('eth_spec_asset_or_duration')
    sha = spec.get('evidence_sha256', '')
    if not isinstance(sha, str) or len(sha) != 64 or any(c not in '0123456789abcdef' for c in sha):
        raise ValueError('eth_spec_evidence_missing')
    for key in ('oracle_provider', 'oracle_feed_id', 'vendor', 'chain_id'):
        if not isinstance(spec.get(key), str) or not spec[key].strip():
            raise ValueError('eth_spec_identity_missing')
    for key in ('tick_size', 'min_cash_usdt', 'min_shares', 'share_step'):
        if dec(spec[key]) <= 0:
            raise ValueError('eth_spec_size_invalid')
    if (not 0 <= dec(spec['fee_bps']) <= 10000 or dec(spec['tick_size']) >= 1
            or dec(spec['share_step']) != Decimal('.01')):
        raise ValueError('eth_spec_unsupported_precision_or_fee')
    return digest(spec)


def validate_market(raw, spec, start):
    validate_spec(spec)
    if not isinstance(raw, dict) or raw.get('symbol') != SYMBOL or raw.get('underlying') != 'ETH':
        raise ValueError('eth_market_asset_mismatch')
    if raw.get('l1Category') != 'crypto' or raw.get('l2Category') != 'up-down':
        raise ValueError('eth_market_category_unverified')
    oracle = raw.get('settlementOracle', {})
    if (not isinstance(oracle, dict) or oracle.get('provider') != spec['oracle_provider']
            or oracle.get('feedId') != spec['oracle_feed_id']):
        raise ValueError('eth_oracle_unverified_or_mismatched')
    for key, setting in (('tickSize', 'tick_size'), ('minOrderAmount', 'min_cash_usdt'),
                         ('minShares', 'min_shares'), ('shareStep', 'share_step'),
                         ('feeRateBps', 'fee_bps')):
        if key not in raw or dec(raw[key]) != dec(spec[setting]):
            raise ValueError('eth_official_spec_missing_or_changed')
    if raw.get('vendor') != spec['vendor'] or str(raw.get('chainId')) != spec['chain_id']:
        raise ValueError('eth_vendor_or_chain_mismatch')
    market = MarketInfo.from_api(raw)
    nodes = raw.get('markets')
    if not isinstance(nodes, list) or len(nodes) != 1 or not isinstance(nodes[0], dict):
        raise ValueError('eth_binary_market_unverified')
    node = nodes[0]
    if str(node.get('status', node.get('tradingStatus', ''))).upper() not in ('OPEN', 'RESOLVED', 'SETTLED'):
        raise ValueError('eth_market_status_unverified')
    if raw.get('status') is not None and str(raw['status']).upper() not in ('OPEN', 'CLOSED', 'RESOLVED', 'SETTLED'):
        raise ValueError('eth_market_status_unverified')
    outcomes = node.get('outcomes', [])
    if (not isinstance(outcomes, list) or len(outcomes) != 2
            or any(not isinstance(o, dict) or not isinstance(o.get('name'), str) for o in outcomes)):
        raise ValueError('eth_binary_outcomes_unverified')
    if ({str(o.get('index')) for o in outcomes} != {'0', '1'}
            or {o.get('name', '').upper() for o in outcomes} != {'UP', 'DOWN'}
            or market.up_market_id != market.down_market_id
            or str(node.get('marketId', '')) != market.up_market_id
            or {str(o.get('tokenId')) for o in outcomes} != {market.up_token_id, market.down_token_id}):
        raise ValueError('eth_binary_orientation_unverified')
    if (type(start) is not int or start <= 0 or start % SLOT_MS
            or market.start_time_ms != start or market.end_time_ms != start+SLOT_MS
            or not market.market_topic_id or not market.up_market_id or not market.down_market_id
            or not market.up_token_id or not market.down_token_id
            or market.up_token_id == market.down_token_id
            or market.reference_price is None or dec(market.reference_price) <= 0):
        raise ValueError('eth_market_identity_or_reference_invalid')
    return market


def identity(raw, spec, start):
    market = validate_market(raw, spec, start)
    # Canonicalize exactly, without Decimal context rounding or exponent expansion.
    reference = dec(market.reference_price).as_tuple()
    digits, exponent = list(reference.digits), reference.exponent
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    canonical_reference = ''.join(str(d) for d in digits)+'e'+str(exponent)
    return dict(symbol=SYMBOL, fingerprint=FINGERPRINT, spec_sha256=digest(spec),
                market_start_ms=start, market_end_ms=start+SLOT_MS,
                market_topic=market.market_topic_id, market_id=market.up_market_id,
                down_market_id=market.down_market_id, up_token_id=market.up_token_id,
                down_token_id=market.down_token_id, reference=canonical_reference,
                oracle_provider=spec['oracle_provider'], oracle_feed_id=spec['oracle_feed_id'],
                fee_bps=str(dec(spec['fee_bps'])))


def freeze_eth_features(start, candles, received_ms):
    # Reuse the frozen BTC math only. Its BTC provenance is never accepted by ETH.
    value = freeze_features(start, candles, received_ms)
    value.update(symbol=SYMBOL, fingerprint=FINGERPRINT, source='Binance Spot ETHUSDT 1m')
    return value


def validate_book(book, expected, spec, at_ms):
    if any(book.get(k) != value for k, value in expected.items()):
        raise ValueError('eth_book_identity_mismatch')
    start = expected['market_start_ms']
    if not start <= book['book_at_ms'] <= book['received_at_ms'] <= book['captured_at_ms'] <= at_ms:
        raise ValueError('eth_book_future_or_reversed_clock')
    if at_ms-book['book_at_ms'] > 1000 or book.get('full_depth') is not True:
        raise ValueError('eth_book_stale_or_incomplete')
    for side in ('UP', 'DOWN'):
        levels = book['quote'][side]['ask_levels']
        if not isinstance(levels, list) or not levels or len(levels) > 100:
            raise ValueError('eth_book_levels_invalid')
        for price, shares in levels:
            if not 0 < dec(price) < 1 or dec(price) % dec(spec['tick_size']) or dec(shares) <= 0:
                raise ValueError('eth_book_precision_invalid')
        if [(dec(p), dec(q)) for p, q in levels] != sorted((dec(p), dec(q)) for p, q in levels):
            raise ValueError('eth_book_levels_unsorted')


def choose(state, features, book, expected, spec, at_ms):
    """Sticky parent routing with explicit unavailable-original and late-input states."""
    start = expected['market_start_ms']
    value = dict(state or {})
    if value.get('candidate') or value.get('terminal'):
        return value
    validate_book(book, expected, spec, at_ms)
    if (features.get('symbol') != SYMBOL or features.get('fingerprint') != FINGERPRINT
            or features.get('market_start_ms') != start or features.get('cutoff_ms') != start+120000
            or not start+120000 <= features['received_at_ms'] <= min(at_ms, start+123000)):
        raise ValueError('eth_feature_provenance_invalid')
    if 'core_guard' not in value:
        if not start+124000 <= at_ms <= start+126000:
            return dict(value, terminal=at_ms > start+126000, reason='initial_window_missed' if at_ms > start+126000 else 'before_initial_window')
        # Never invent an ETH Original probability or treat missing core as empty.
        state_name = state_of(features['first_bp'], features['last_bp'])
        choices, _ = btc_core_candidates(features, None, book, dec(1))
        for candidate in choices:
            candidate['source_branch'] = candidate['branch']
            candidate['branch'] = core_name(candidate)
        if not choices and state_name in ('continuation', 'flat', 'stall'):
            return dict(value, terminal=True, reason='eth_original_probability_unavailable')
        if not choices:
            # Parent T6.7c requires two-sided depth only to prove empty core.
            for side in ('UP', 'DOWN'):
                walk(book['quote'][side]['ask_levels'], book['fee_bps'], cap=dec('.99'), amount=dec(1))
        value['core_guard'] = dict(empty=not choices, candidates=choices, features=features,
                                   frozen_at_ms=at_ms, initial_book_at_ms=book['book_at_ms'])
    guard = value['core_guard']
    if at_ms > start+134500:
        return dict(value, terminal=True, reason='selection_deadline_missed')
    choices = additions(book, guard['features'], dec(1)) if guard['empty'] else guard['candidates']
    for candidate in choices:
        try:
            result = (additive_execution(book, candidate['side'], dec(1), lower=candidate['lower'], cap=candidate['cap'])
                      if guard['empty'] else eligible_execution(candidate, book, dec(1)))
            if result['cash'] < dec(spec['min_cash_usdt']) or result['net_shares'] < dec(spec['min_shares']):
                continue
        except (ValueError, KeyError, TypeError, ArithmeticError):
            continue
        value.update(candidate=dict(candidate, **{k: str(v) for k, v in result.items()},
                                    selected_at_ms=at_ms, book_at_ms=book['book_at_ms'],
                                    expires_at_ms=min(start+136000, at_ms+2000) if guard['empty'] else start+136000,
                                    fill_status='PAPER_QUOTE_ONLY', mode='SHADOW', **expected),
                     reason='paper_quote_selected')
        break
    value.setdefault('reason', 'core_reserved' if not guard['empty'] else 'verified_empty_core')
    return value


def official_winner(raw):
    """Only terminal official named payouts; no spot/futures-derived resolution."""
    markets = raw.get('markets', [])
    if len(markets) != 1:
        return None
    if str(markets[0].get('status') or raw.get('status') or '').upper() not in ('RESOLVED', 'SETTLED'):
        return None
    outcomes = markets[0].get('outcomes', [])
    if len(outcomes) != 2 or {o.get('name', '').upper() for o in outcomes} != {'UP', 'DOWN'}:
        return None
    winners = [o['name'].upper() for o in outcomes if o.get('winner') is True or o.get('isWinner') is True]
    if len(winners) == 1:
        return winners[0]
    if all(o.get('price', o.get('payout')) is not None and dec(o.get('price', o.get('payout'))) == Decimal('.5') for o in outcomes):
        return 'DRAW'
    return None
