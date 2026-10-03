"""Standalone finite ETH T6.7c Shadow collector/replay. Never runs a trading worker."""
import argparse
import asyncio
import fcntl
import hashlib
import io
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from .eth_t67c_core import choose, freeze_eth_features, identity, official_winner, validate_market, validate_spec
from .eth_t67c_policy import FINGERPRINT, PROFILE, SLOT_MS, SYMBOL, digest
from .eth_t67c_store import ShadowStore, encode


def now_ms():
    return time.time_ns()//1_000_000


class EthShadow:
    mode = 'SHADOW'

    def __init__(self, store, spec=None):
        self.store, self.spec = store, spec
        self.latest, self.anchors = {}, {}
        self.watermarks = {r[0]: json.loads(r[1]) for r in store.db.execute('SELECT source,payload FROM eth_shadow_feed_watermarks')}
        if store.namespace['input_mode'] == 'replay':
            for source, value in self.watermarks.items():
                if value.get('connected'):
                    self.latest[source] = value
                    if value.get('anchor'):
                        anchor = value['anchor']
                        self.anchors[(source, anchor['start'], value['generation'])] = anchor
        else:
            for value in self.watermarks.values():
                value['connected'] = False  # Process restart starts a fresh generation.

    def preflight(self, *, require_live=False):
        return dict(passed=not require_live, reasons=['ETH Shadow is permanently non-trading'] if require_live else [])

    def set_shadow_mode(self, enabled):
        if enabled is not True:
            raise PermissionError('eth_live_forbidden')
        return self.status()

    def status(self):
        report = self.store.report()
        return {k: report[k] for k in ('profile', 'symbol', 'mode', 'fingerprint', 'observed_windows',
                                      'paper_quotes', 'official_outcomes', 'limitations')}

    def diagnostic(self, code, at_ms):
        self.store.diagnostic(at_ms//SLOT_MS*SLOT_MS, code, at_ms)

    def disconnect(self, source, at_ms):
        if source not in ('spot', 'futures'):
            raise ValueError('eth_feed_source_invalid')
        self.latest.pop(source, None)
        self.anchors = {k: v for k, v in self.anchors.items() if k[0] != source}
        if source in self.watermarks:
            value = dict(self.watermarks[source], connected=False, anchor=None)
            self.watermarks[source] = value
            self.store.feed_state(source, value)
        self.diagnostic('eth_'+source+'_disconnected', at_ms)

    def trade(self, source, generation, packet, received_ms):
        prior = self.watermarks.get(source)
        if prior and (generation < prior['generation'] or
                      (not prior.get('connected') and generation <= prior['generation']) or
                      (generation == prior['generation'] and
                       (packet['T'] < prior['event_ms'] or packet['a'] <= prior['trade_id']))):
            raise ValueError('eth_trade_sequence_invalid')
        persisted = self.store.tape(source, generation, packet, received_ms)
        if prior and generation != prior['generation']:
            self.anchors = {k: v for k, v in self.anchors.items() if k[0] != source}
        value = dict(generation=generation, event_ms=packet['T'], received_ms=received_ms,
                     trade_id=packet['a'], connected=True, packet_sha256=digest(packet))
        start = packet['T']//SLOT_MS*SLOT_MS
        if start <= packet['T'] <= received_ms <= start+1500:
            self.anchors.setdefault((source, start, generation), dict(value, price=packet['p']))
        anchor = self.anchors.get((source, start, generation))
        value['anchor'] = dict(anchor, start=start) if anchor else None
        self.latest[source] = value
        self.watermarks[source] = value
        if persisted or self.store.namespace['input_mode'] == 'replay':
            self.store.feed_state(source, value)

    def _admit(self, start, at_ms):
        prior = self.store.db.execute('SELECT max(start) FROM eth_shadow_windows').fetchone()[0]
        if prior is not None and start > prior+SLOT_MS:
            missing = prior+SLOT_MS
            while missing < start:
                value = self.store.admit(missing)
                if value is None:
                    return None
                value.update(status='SKIPPED', state={'terminal': True, 'reason': 'worker_window_gap'})
                self.store.save_window(missing, value)
                self.store.diagnostic(missing, 'worker_window_gap', at_ms)
                missing += SLOT_MS
        return self.store.admit(start)

    def observe(self, start, at_ms, *, raw=None, candles=None, feature_received_ms=None, book=None):
        if type(at_ms) is not int or not start <= at_ms < start+SLOT_MS:
            raise ValueError('eth_observation_clock_invalid')
        window = self._admit(start, at_ms)
        if window is None:
            return
        try:
            if candles is not None and self.store.get('features', start) is None:
                received = at_ms if feature_received_ms is None else feature_received_ms
                if received > at_ms:
                    raise ValueError('eth_feature_future')
                if not start+120000 <= received <= start+123000:
                    raise ValueError('eth_feature_freeze_deadline_missed')
                self.store.append('features', start, freeze_eth_features(start, candles, received))
            if raw is not None:
                expected = identity(raw, self.spec, start)
                if window.get('identity') and window['identity'] != expected:
                    raise ValueError('eth_market_identity_changed')
                market = validate_market(raw, self.spec, start)
                if market.status != 'OPEN' or any(str(m.get('status', m.get('tradingStatus', ''))).upper() != 'OPEN'
                                                for m in raw.get('markets', [])):
                    raise ValueError('eth_market_not_open')
                window['identity'] = expected
            if at_ms > start+126000 and 'core_guard' not in window['state']:
                window['state'].update(terminal=True, reason='initial_window_missed')
                self.store.diagnostic(start, 'initial_window_missed', at_ms)
            if not window['state'].get('candidate') and not window['state'].get('terminal') and at_ms >= start+124000:
                if 'identity' not in window:
                    raise ValueError('eth_market_unverified')
                features = self.store.get('features', start)
                if features is None:
                    raise ValueError('eth_features_unavailable')
                spot = self.latest.get('spot')
                if not spot or not 0 <= at_ms-spot['event_ms'] <= 1500 or not 0 <= at_ms-spot['received_ms'] <= 1500:
                    raise ValueError('eth_spot_stale_or_disconnected')
                if ('spot', start, spot['generation']) not in self.anchors:
                    raise ValueError('eth_opening_anchor_or_generation_unavailable')
                if book is None:
                    raise ValueError('eth_book_unavailable')
                if not self.latest.get('futures') or at_ms-self.latest['futures']['received_ms'] > 1500:
                    self.store.diagnostic(start, 'eth_futures_optional_missing', at_ms)
                window['state'] = choose(window['state'], features, book, window['identity'], self.spec, at_ms)
                if window['state'].get('candidate'):
                    self.store.diagnostic(start, 'paper_quote_selected', at_ms)
            window['status'] = 'PAPER_QUOTE_ONLY' if window['state'].get('candidate') else 'SKIPPED' if window['state'].get('terminal') else 'OBSERVING'
        except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
            code = str(exc) if str(exc).startswith('eth_') and str(exc).replace('_', '').isalnum() else 'eth_input_rejected_'+type(exc).__name__
            self.store.diagnostic(start, code, at_ms)
        finally:
            self.store.save_window(start, window)

    def resolve(self, start, raw, at_ms):
        window = self.store.get('windows', start)
        if not window or not window.get('identity') or at_ms < start+SLOT_MS:
            raise ValueError('eth_resolution_window_or_clock_invalid')
        if identity(raw, self.spec, start) != window['identity']:
            raise ValueError('eth_resolution_identity_mismatch')
        winner = official_winner(raw)
        if winner is None:
            self.store.diagnostic(start, 'eth_official_resolution_pending', at_ms)
            return
        prior = self.store.get('outcomes', start)
        if prior:
            if prior['winner'] != winner:
                raise ValueError('eth_official_resolution_changed')
            return
        value = dict(window['identity'], winner=winner, known_at_ms=at_ms,
                     source='official_market_detail', mode='SHADOW', fill_status='PAPER_QUOTE_ONLY')
        quote = self.store.get('quotes', start)
        if quote:
            from .regime_lane import dec
            payout = dec('.5') if winner == 'DRAW' else dec(1) if winner == quote['side'] else dec(0)
            value['hypothetical_quote_pnl_usdt'] = str(payout*dec(quote['net_shares'])-dec(quote['cash']))
        self.store.append('outcomes', start, value)


def replay(engine, path):
    if engine.store.namespace['input_mode'] != 'replay':
        raise ValueError('eth_replay_namespace_mode_mismatch')
    with Path(path).open('rb') as stream:
        snapshot = stream.read(256*1024*1024+1)
    if len(snapshot) > 256*1024*1024:
        raise ValueError('eth_replay_file_too_large')
    source_sha = hashlib.sha256(snapshot).hexdigest()
    row = engine.store.db.execute('SELECT payload FROM eth_shadow_replay_cursor WHERE id=1').fetchone()
    cursor = json.loads(row[0]) if row else dict(source_sha256=source_sha, ordinal=0, received_ms=0)
    if cursor['source_sha256'] != source_sha:
        raise ValueError('eth_replay_source_changed')
    # Bind the source before any feature/window/trade side effect, including crashes.
    with engine.store.db:
        engine.store.db.execute('INSERT OR IGNORE INTO eth_shadow_replay_cursor VALUES(1,?)', (encode(cursor),))
    ordinal = 0
    with io.BytesIO(snapshot) as stream:
        while line := stream.readline(262145):
            if len(line) > 262144:
                raise ValueError('eth_replay_line_too_large')
            ordinal += 1
            if ordinal <= cursor['ordinal']:
                continue
            event = json.loads(line)
            if event.get('symbol') != SYMBOL:
                raise ValueError('eth_replay_asset_invalid')
            kind, at = event['kind'], event['received_at_ms']
            if type(at) is not int or at < cursor['received_ms']:
                raise ValueError('eth_replay_clock_reversed')
            if kind == 'trade':
                prior = engine.watermarks.get(event['source'], {})
                if not (prior.get('connected') and prior.get('packet_sha256') == digest(event['packet'])
                        and prior.get('generation') == event['generation'] and prior.get('received_ms') == at):
                    engine.trade(event['source'], event['generation'], event['packet'], at)
            elif kind == 'disconnect':
                engine.disconnect(event['source'], at)
            elif kind == 'observe':
                engine.observe(event['start'], at, raw=event.get('market'), candles=event.get('candles'),
                               feature_received_ms=event.get('feature_received_ms'), book=event.get('book'))
            elif kind == 'resolution':
                engine.resolve(event['start'], event['market'], at)
            else:
                raise ValueError('eth_replay_kind_invalid')
            cursor.update(ordinal=ordinal, received_ms=at)
            with engine.store.db:
                engine.store.db.execute('INSERT INTO eth_shadow_replay_cursor VALUES(1,?) '
                    'ON CONFLICT(id) DO UPDATE SET payload=excluded.payload', (encode(cursor),))


@contextmanager
def process_lock(root):
    path = Path(root)/'service.lock'
    if path.is_symlink():
        raise ValueError('eth_namespace_lock_symlink')
    with path.open('a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('eth_namespace_already_running') from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def shared_budget(path):
    from .rate_limit import SharedRequestBudget
    value = Path(path).absolute()
    if value.name != 'request-weight.sqlite3' or any(p.is_symlink() for p in (value, *value.parents)):
        raise ValueError('eth_shared_budget_path_invalid')
    if value.exists():
        with sqlite3.connect(value.as_uri()+'?mode=ro', uri=True) as db:
            names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if names - {'weight_events', 'weight_meta', 'budget_security'}:
                raise ValueError('eth_shared_budget_foreign_database')
    return SharedRequestBudget(value)


def next_resolution(engine, last_checked, at_ms):
    rows = engine.store.db.execute('SELECT start,payload FROM eth_shadow_windows ORDER BY start').fetchall()
    rows.sort(key=lambda row: (row[0] <= last_checked, row[0]))
    for start, payload in rows:
        window = json.loads(payload)
        if start+SLOT_MS <= at_ms and window.get('identity') and not engine.store.get('outcomes', start):
            return start, window
    return None


async def collect(engine, budget_path, grace_seconds):
    if engine.store.namespace['input_mode'] != 'collect':
        raise ValueError('eth_collect_namespace_mode_mismatch')
    from .eth_t67c_data import EthCatalog, PublicTape, book_feed, book_snapshot, fetch_klines
    key, secret = os.environ.get('PREDICTION_BINANCE_API_KEY', ''), os.environ.get('PREDICTION_BINANCE_API_SECRET', '')
    catalog = EthCatalog(key, secret, shared_budget(budget_path))
    tape, feed = PublicTape(engine), book_feed(key, secret)
    stop = asyncio.Event()
    current = {'raw': None, 'start': None}

    async def discovery():
        while not stop.is_set():
            try:
                start = now_ms()//SLOT_MS*SLOT_MS
                data = await asyncio.to_thread(catalog.markets)
                topics = data.get('marketTopics', [])
                for topic in topics:
                    if topic.get('symbol') != SYMBOL:
                        continue
                    raw = await asyncio.to_thread(catalog.detail, topic['marketTopicId'])
                    if raw.get('symbol') != SYMBOL:
                        continue
                    from .models import MarketInfo
                    market = MarketInfo.from_api(raw)
                    if market.start_time_ms != start:
                        continue
                    current.update(raw=raw, start=start)
                    if engine.spec is not None:
                        validate_market(raw, engine.spec, start)
                        await feed.select(market.up_market_id)
                    break
            except Exception as exc:
                engine.diagnostic('eth_catalog_unavailable_'+type(exc).__name__, now_ms())
            await asyncio.sleep(20)

    async def features():
        while not stop.is_set():
            stamp = now_ms()
            start = stamp//SLOT_MS*SLOT_MS
            if stamp < start+120000:
                await asyncio.sleep(min(1, (start+120000-stamp)/1000))
                continue
            if stamp <= start+123000 and engine.store.get('features', start) is None:
                try:
                    candles = await asyncio.to_thread(fetch_klines, start, budget=catalog.budget)
                    received = now_ms()  # Never freeze using the pre-request timestamp.
                    engine.observe(start, received, candles=candles, feature_received_ms=received)
                except Exception as exc:
                    engine.diagnostic('eth_feature_collection_'+type(exc).__name__, now_ms())
            await asyncio.sleep(.1 if stamp < start+123000 else 1)

    async def resolutions():
        last_checked = -1
        while not stop.is_set():
            pending = next_resolution(engine, last_checked, now_ms())
            if pending:
                start, window = pending
                try:
                    raw = await asyncio.to_thread(catalog.detail, window['identity']['market_topic'])
                    engine.resolve(start, raw, now_ms())
                except Exception as exc:
                    engine.diagnostic('eth_resolution_read_'+type(exc).__name__, now_ms())
                last_checked = start
            await asyncio.sleep(20)

    tasks = []
    try:
        await tape.start()
        tasks = [asyncio.create_task(fn()) for fn in (discovery, features, resolutions)]
        while True:
            at = now_ms()
            start = at//SLOT_MS*SLOT_MS
            raw = current['raw'] if current['start'] == start else None
            book = None
            if raw is not None and engine.spec is not None:
                try:
                    book = book_snapshot(feed, raw, engine.spec, start, at)
                except (ValueError, KeyError, TypeError, ArithmeticError):
                    pass
            observed = now_ms()
            if observed//SLOT_MS*SLOT_MS != start:
                continue  # Recompute the new slot; never use the prior slot's metadata/book.
            engine.observe(start, observed, raw=raw, book=book)
            count, last = engine.store.db.execute('SELECT count(*),max(start) FROM eth_shadow_windows').fetchone()
            if count >= engine.store.namespace['windows'] and at >= last+SLOT_MS+grace_seconds*1000:
                break
            await asyncio.sleep(.1)
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await tape.close()
        await feed.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Experimental ETH T6.7c Shadow only; no trading capabilities')
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--replay', type=Path)
    source.add_argument('--collect', action='store_true')
    parser.add_argument('--root', type=Path, default=Path('prediction/data/eth-t67c-shadow'))
    parser.add_argument('--market-spec', type=Path)
    parser.add_argument('--windows', type=int, default=20)
    parser.add_argument('--shared-weight-db', type=Path)
    parser.add_argument('--resolution-grace-seconds', type=int, default=600)
    parser.add_argument('--poll-telegram', action='store_true')
    args = parser.parse_args(argv)
    if args.collect and not args.shared_weight_db:
        parser.error('--collect requires an explicit shared --shared-weight-db')
    if not 0 <= args.resolution_grace_seconds <= 3600 or (args.poll_telegram and not args.collect):
        parser.error('invalid grace or Telegram/replay combination')
    spec = json.loads(args.market_spec.read_text()) if args.market_spec else None
    if spec is not None:
        validate_spec(spec)
    store = ShadowStore(args.root, spec=spec, windows=args.windows, input_mode='collect' if args.collect else 'replay')
    try:
        with process_lock(store.root):
            engine = EthShadow(store, spec)
            if args.replay:
                replay(engine, args.replay)
            else:
                if args.poll_telegram:
                    from .eth_t67c_telegram import collect_with_telegram
                    asyncio.run(collect_with_telegram(engine, args.shared_weight_db, args.resolution_grace_seconds))
                else:
                    asyncio.run(collect(engine, args.shared_weight_db, args.resolution_grace_seconds))
            print(encode(engine.status()))
    finally:
        store.close()


if __name__ == '__main__':
    main()
