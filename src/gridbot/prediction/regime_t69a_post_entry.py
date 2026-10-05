"""T6.9a post-entry book recorder and First DOWN deep-stop Shadow.

Record only. The trading DB is opened read-only to learn which market holds a
T6.9a Live BUY; this module never places, cancels or sells an order and never
writes Live claims, risk, MDD or HS. Its writes go to the feature DB only.

After a Live BUY fills, one public book snapshot per second (from the evidence
store the signal producer already fills at 100 ms) is copied into a durable
table until ``end_ms``. For ``core_first_down`` the same snapshots drive a
hypothetical "deep stop": the first fresh snapshot at or after ``after_ms``
whose DOWN best bid is at most ``bid_ratio_max`` x the entry fill price is
frozen with the bid depth and the sale it could absorb. The report compares
stopping there with holding to the official settlement.
"""
import hashlib
import json
import sqlite3
from contextlib import closing
from decimal import Decimal
from pathlib import Path

from .regime_lane import dec
from .regime_t69a_policy import FINGERPRINT, PROFILE

POST_ENTRY_POLICY = dict(
    base_fingerprint=FINGERPRINT, version=1,
    scope='record_only; feature_db_writes; no_order_no_claim_no_live_risk_writes',
    recorder=dict(source='t67_evidence_books', start='first_live_buy_fill', end_ms=290000,
                  sample_ms=1000, levels=5, retention_days=21),
    deep_stop=dict(branch='core_first_down', side='DOWN', after_ms=150000, bid_ratio_max='0.3',
                   book_max_age_ms=1000, trigger='first_fresh_snapshot_only',
                   sell='walk_bid_levels; fee_bps*min(p,1-p) per share; remainder held',
                   pnl='stop_vs_hold_to_official_settlement'),
)
POST_ENTRY_FINGERPRINT = hashlib.sha256(json.dumps(POST_ENTRY_POLICY, sort_keys=True).encode()).hexdigest()
RECORDER = POST_ENTRY_POLICY['recorder']
DEEP_STOP = POST_ENTRY_POLICY['deep_stop']
_LAST_RUN = {}


def schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS t69a_post_entry_states(start INTEGER PRIMARY KEY,payload TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS t69a_post_entry_books(start INTEGER,offset_ms INTEGER,payload TEXT NOT NULL,'
               'PRIMARY KEY(start,offset_ms))')


def _levels(levels, count, reverse):
    parsed = sorted(((dec(p), dec(q)) for p, q in levels or ()), reverse=reverse)
    if any(not 0 < p < 1 or q <= 0 for p, q in parsed):
        raise ValueError('post_entry_level_invalid')
    return [[str(p), str(q)] for p, q in parsed[:count]]


def compact(book, start):
    """Best bid/ask and top levels for both sides; the evidence row stays the source."""
    captured, stamp = int(book['captured_at_ms']), int(book['book_at_ms'])
    if not stamp <= captured or book['market_start_ms'] != start:
        raise ValueError('post_entry_book_clock_invalid')
    value = dict(offset_ms=captured-start, captured_at_ms=captured, book_at_ms=stamp,
                 age_ms=captured-stamp, fee_bps=str(book['fee_bps']))
    for side in ('UP', 'DOWN'):
        quote = book['quote'][side]
        asks = _levels(quote.get('ask_levels'), RECORDER['levels'], False)
        bids = _levels(quote.get('bid_levels'), RECORDER['levels'], True) if 'bid_levels' in quote else None
        bid = quote.get('bid')
        value[side] = dict(ask=asks[0][0] if asks else None,
                           bid=str(dec(bid)) if bid is not None else (bids[0][0] if bids else None),
                           ask_levels=asks, bid_levels=bids)
    return value


def sell(levels, shares, fee_bps):
    """Walk bid depth for ``shares``; fee is fee_bps x min(p,1-p) per share sold."""
    rate = dec(fee_bps)/10000
    sold = proceeds = fee = Decimal(0)
    for p, q in ((dec(p), dec(q)) for p, q in levels):
        take = min(q, shares-sold)
        if take <= 0:
            break
        sold += take
        fee += take*rate*min(p, 1-p)
        proceeds += take*p
    return sold, proceeds-fee, fee


def net_shares(state):
    """Held shares after the buy fee taken in shares, as the entry walk models it."""
    rate, price = dec(state['fee_bps'])/10000, dec(state['entry_price'])
    return str(dec(state['gross_shares'])*(1-rate*min(price, 1-price)/price))


def _position(p, start, symbol):
    """Current T6.9a Live BUY for this market, or a reason string."""
    row = p.execute("SELECT l.loop_id,s.market_topic_id,s.market_id FROM prediction_loops l "
                    "JOIN prediction_regime_slots s ON s.loop_id=l.loop_id "
                    "WHERE l.strategy_profile=? AND l.mode='LIVE' AND s.market_start_ms=? "
                    "AND s.verified_at_ms IS NOT NULL ORDER BY l.created_at_ms DESC LIMIT 1",
                    (PROFILE, start)).fetchone()
    if row is None:
        return 'no_verified_live_market'
    bound = p.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='prediction_loop_market_bindings'").fetchone()
    bound = p.execute('SELECT symbol FROM prediction_loop_market_bindings WHERE loop_id=?', (row[0],)).fetchone() if bound else None
    if (bound[0] if bound else 'BTCUSDT') != symbol:
        return 'other_asset_loop'
    campaigns = [r[0] for r in p.execute(
        'SELECT campaign_id FROM prediction_campaigns WHERE loop_id=? AND start_time_ms=?', (row[0], start))]
    if not campaigns:
        return 'no_live_position'
    fills = p.execute("SELECT outcome,shares,price,gross_amount,event_time_ms FROM prediction_fills "
                      "WHERE order_side='BUY' AND campaign_id IN ("+','.join('?' for _ in campaigns)+') '
                      'ORDER BY event_time_ms', campaigns).fetchall()
    if not fills:
        return 'no_live_position'
    sides = {f[0] for f in fills}
    if len(sides) != 1 or not sides <= {'UP', 'DOWN'}:
        raise ValueError('post_entry_fill_side_invalid')
    shares = sum(dec(f[1]) for f in fills)
    cash = sum(dec(f[3]) if dec(f[3]) > 0 else dec(f[1])*dec(f[2]) for f in fills)
    if shares <= 0 or cash <= 0:
        raise ValueError('post_entry_fill_amount_invalid')
    first = int(fills[0][4] or 0)
    return dict(loop_id=row[0], market_topic=str(row[1]), market_id=str(row[2]), campaign_ids=sorted(campaigns),
                side=sides.pop(), fills=len(fills), gross_shares=str(shares), cash=str(cash),
                entry_price=str(cash/shares), first_fill_ms=first if start <= first < start+300000 else None)


def _branch(db, start, loop_id):
    try:
        row = db.execute('SELECT payload FROM t69a_decisions WHERE start=?', (start,)).fetchone()
    except sqlite3.Error:
        return None
    decision = json.loads(row[0]) if row else {}
    if (decision.get('fingerprint') != FINGERPRINT or decision.get('loop_id') != loop_id
            or decision.get('selected') is not True):
        return None
    return decision.get('branch')


def _trigger(snapshot, state):
    """Freeze the first fresh deep-stop snapshot; the rest stay recorded only."""
    stop = state['deep_stop']
    if stop['trigger'] is not None or snapshot['offset_ms'] < DEEP_STOP['after_ms']:
        return
    if snapshot['age_ms'] > DEEP_STOP['book_max_age_ms'] or snapshot['DOWN']['bid'] is None:
        stop['skipped_snapshots'] += 1
        return
    stop['checked_snapshots'] += 1
    entry, bid = dec(state['entry_price']), dec(snapshot['DOWN']['bid'])
    ratio = bid/entry
    if stop['min_bid_ratio'] is None or ratio < dec(stop['min_bid_ratio']):
        stop['min_bid_ratio'] = str(ratio)
    if ratio > dec(DEEP_STOP['bid_ratio_max']):
        return
    fee_bps = dec(snapshot['fee_bps'])
    held = dec(state['net_shares'])
    levels = snapshot['DOWN']['bid_levels']
    depth_known = levels is not None
    if not depth_known:
        levels = [[snapshot['DOWN']['bid'], str(held)]]
    sold, proceeds, fee = sell(levels, held, fee_bps)
    stop['trigger'] = dict(offset_ms=snapshot['offset_ms'], captured_at_ms=snapshot['captured_at_ms'],
                           book_at_ms=snapshot['book_at_ms'], bid=str(bid), bid_ratio=str(ratio),
                           bid_levels=levels, depth_known=depth_known, fee_bps=str(fee_bps),
                           held_shares=str(held), sell_shares=str(sold), unsold_shares=str(held-sold),
                           proceeds=str(proceeds), fee=str(fee))


def hypothetical(state, winner):
    """Stop vs hold PnL in USDT for one position; unsold shares ride to settlement."""
    cash, held = dec(state['cash']), dec(state['net_shares'])
    payout = {state['side']: Decimal(1), 'DRAW': Decimal('0.5')}.get(winner, Decimal(0))
    hold = held*payout-cash
    trigger = state['deep_stop']['trigger']
    if trigger is None:
        return hold, hold
    stop = dec(trigger['proceeds'])+dec(trigger['unsold_shares'])*payout-cash
    return stop, hold


def observe(db, prediction_db, signal_db, at_ms, symbol='BTCUSDT'):
    """Copy 1/s public books for a held T6.9a Live market; record-only."""
    start = at_ms//300000*300000
    if not start+124000 <= at_ms < start+300000:
        return 'outside_post_entry_window'
    if at_ms-_LAST_RUN.get(start, 0) < RECORDER['sample_ms']:
        return 'throttled'
    _LAST_RUN.clear()
    _LAST_RUN[start] = at_ms
    schema(db)
    db.commit()
    stored = db.execute('SELECT payload FROM t69a_post_entry_states WHERE start=?', (start,)).fetchone()
    state = json.loads(stored[0]) if stored else None
    if state and state.get('complete'):
        return 'post_entry_complete'
    with closing(sqlite3.connect(Path(prediction_db).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as p:
        p.execute('PRAGMA query_only=ON')
        p.execute('BEGIN')
        position = _position(p, start, symbol)
    if isinstance(position, str):
        return position
    try:
        return _record(db, signal_db, at_ms, start, state, position)
    except BaseException:
        db.rollback()
        raise


def _record(db, signal_db, at_ms, start, state, position):
    from .regime_t67_evidence import evidence_path
    end = start+RECORDER['end_ms']
    if state is None:
        db.execute('DELETE FROM t69a_post_entry_books WHERE start<?',
                   (at_ms-RECORDER['retention_days']*86400000,))
        branch = _branch(db, start, position['loop_id'])
        state = dict(fingerprint=FINGERPRINT, post_entry_fingerprint=POST_ENTRY_FINGERPRINT,
                     market_start_ms=start, end_ms=start+300000, branch=branch,
                     record_from_ms=position['first_fill_ms'] or at_ms, last_offset_ms=None,
                     snapshots=0, invalid_books=0, complete=False,
                     deep_stop=dict(eligible=branch == DEEP_STOP['branch'] and position['side'] == DEEP_STOP['side'],
                                    trigger=None, checked_snapshots=0, skipped_snapshots=0, min_bid_ratio=None))
    # Later partial fills update the held amount; the recording window never moves.
    state.update(position, record_from_ms=state['record_from_ms'])
    if state.get('fee_bps') is not None:
        state['net_shares'] = net_shares(state)
    since = max(state['record_from_ms'], start+(state['last_offset_ms'] if state['last_offset_ms'] is not None else -1)+1)
    with closing(sqlite3.connect(evidence_path(signal_db).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as e:
        rows = e.execute('SELECT payload FROM books WHERE start=? AND captured_ms BETWEEN ? AND ? '
                         'ORDER BY captured_ms,book_ms', (start, since, min(at_ms, end))).fetchall()
    sample = RECORDER['sample_ms']
    bucket = None if state['last_offset_ms'] is None else state['last_offset_ms']//sample
    added = 0
    for (raw,) in rows:
        try:
            book = json.loads(raw)
            if str(book['market_id']) != state['market_id'] or str(book['market_topic']) != state['market_topic']:
                continue
            snapshot = compact(book, start)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            state['invalid_books'] += 1
            continue
        if snapshot['offset_ms']//sample == bucket:
            continue
        bucket = snapshot['offset_ms']//sample
        if state.get('fee_bps') is None:
            state['fee_bps'] = snapshot['fee_bps']
            state['net_shares'] = net_shares(state)
        db.execute('INSERT OR IGNORE INTO t69a_post_entry_books VALUES(?,?,?)',
                   (start, snapshot['offset_ms'], json.dumps(snapshot, sort_keys=True)))
        state['snapshots'] += 1
        state['last_offset_ms'] = snapshot['offset_ms']
        added += 1
        if state['deep_stop']['eligible']:
            _trigger(snapshot, state)
    state['complete'] = at_ms >= end
    state['last_checked_at_ms'] = at_ms
    db.execute('INSERT INTO t69a_post_entry_states VALUES(?,?) ON CONFLICT(start) DO UPDATE SET payload=excluded.payload',
               (start, json.dumps(state, sort_keys=True)))
    db.commit()
    return 'post_entry_recorded' if added else 'post_entry_no_new_book'


def metrics(root, *, now, loop_id, slots, official):
    """Current-loop recorder coverage and deep-stop stop-vs-hold paper PnL."""
    from .loop_market import report_feature_path
    result = dict(markets=0, snapshots=0, observed=0, triggered=0, settled=0, pending=0, unverified=0,
                  thin=0, flipped=0, stop=Decimal(0), hold=Decimal(0))
    slots = {int(s['market_start_ms']): s for s in slots if s.get('loop_id') == loop_id
             and s.get('verified_at_ms') is not None and int(s['market_start_ms']) <= now}
    path = report_feature_path(root, loop_id)
    if not path.is_file() or not slots:
        return result
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t69a_post_entry_states'").fetchone():
            return result
        starts = tuple(slots)
        states = [json.loads(r[0]) for r in db.execute(
            'SELECT payload FROM t69a_post_entry_states WHERE start IN ('+','.join('?' for _ in starts)+')', starts)]
    for state in states:
        start = state.get('market_start_ms')
        slot = slots.get(start)
        if (slot is None or state.get('loop_id') != loop_id or state.get('fingerprint') != FINGERPRINT
                or state.get('post_entry_fingerprint') != POST_ENTRY_FINGERPRINT
                or str(state.get('market_id')) != str(slot.get('market_id'))
                or str(state.get('market_topic')) != str(slot.get('market_topic_id'))):
            result['unverified'] += 1
            continue
        result['markets'] += 1
        result['snapshots'] += int(state.get('snapshots', 0))
        stop = state.get('deep_stop') or {}
        if not stop.get('eligible') or 'net_shares' not in state:
            continue
        result['observed'] += 1
        trigger = stop.get('trigger')
        if trigger is not None:
            result['triggered'] += 1
            result['thin'] += dec(trigger['unsold_shares']) > 0 or not trigger['depth_known']
        winners = official.get((str(slot['market_topic_id']), str(slot['market_id']), start, start+300000), set())
        if start+300000 > now or len(winners) != 1:
            result['pending'] += 1
            continue
        winner = next(iter(winners))
        try:
            stopped, held = hypothetical(state, winner)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            result['unverified'] += 1
            continue
        result['settled'] += 1
        result['stop'] += stopped
        result['hold'] += held
        result['flipped'] += trigger is not None and winner == state['side']
    return result


def report_lines(root, *, now, loop_id, slots, official):
    m = metrics(root, now=now, loop_id=loop_id, slots=slots, official=official)
    ratio, after = DEEP_STOP['bid_ratio_max'], DEEP_STOP['after_ms']//1000
    lines = [f'〔First DOWN 深度停損（{after}s 後 DOWN bid≤{ratio}×進場價；只記錄不賣）〕']
    if m['settled']:
        lines.append(f'深度停損｜觀察 {m["observed"]}｜觸發 {m["triggered"]}｜已結算 {m["settled"]}｜'
                     f'停損PnL {m["stop"]:+.4f}｜持有PnL {m["hold"]:+.4f}｜差 {m["stop"]-m["hold"]:+.4f} USDT')
    else:
        lines.append(f'深度停損｜觀察 {m["observed"]}｜觸發 {m["triggered"]}｜已結算 0｜停損PnL —｜持有PnL —')
    if m['triggered']:
        lines.append(f'  觸發後原本會贏 {m["flipped"]}｜bid 深度不足 {m["thin"]}｜待結算 {m["pending"]}')
    lines.append(f'進場後盤口記錄｜市場 {m["markets"]}｜快照 {m["snapshots"]}（每秒一筆至 {RECORDER["end_ms"]//1000}s）')
    if m['unverified']:
        lines.append(f'  進場後記錄待核對 {m["unverified"]}；未核對不列收益。')
    return lines


def empty_lines():
    ratio, after = DEEP_STOP['bid_ratio_max'], DEEP_STOP['after_ms']//1000
    return [f'〔First DOWN 深度停損（{after}s 後 DOWN bid≤{ratio}×進場價；只記錄不賣）〕',
            '深度停損｜觀察 0｜觸發 0｜已結算 0｜停損PnL —｜持有PnL —',
            f'進場後盤口記錄｜市場 0｜快照 0（每秒一筆至 {RECORDER["end_ms"]//1000}s）']
