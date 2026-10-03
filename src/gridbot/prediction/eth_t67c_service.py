"""Standalone finite ETH T6.7c Shadow collector/replay. Never runs a trading worker."""
import argparse
import asyncio
import fcntl
import hashlib
import io
import json
import os
import signal
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


def parse_budget_identity(value):
    try:
        db_dev, db_inode, journal_dev, journal_inode = [int(v) for v in value.split(':')]
        if min(db_dev, journal_dev) < 0 or min(db_inode, journal_inode) <= 0:
            raise ValueError
        return {'database': [db_dev, db_inode], 'journal': [journal_dev, journal_inode]}
    except (ValueError, AttributeError):
        raise argparse.ArgumentTypeError('expected DBDEV:DBINODE:JOURNALDEV:JOURNALINODE') from None


def shared_budget(path, *, expected_identity=None):
    from .rate_limit import SharedRequestBudget
    value = Path(path).absolute()
    if value.name != 'request-weight.sqlite3' or any(p.is_symlink() for p in (value, *value.parents)):
        raise ValueError('eth_shared_budget_path_invalid')
    if not value.is_file():
        raise ValueError('eth_shared_budget_missing_database')
    with sqlite3.connect(value.as_uri()+'?mode=ro', uri=True) as db:
        names = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if names != {'weight_events', 'weight_meta', 'budget_security'}:
            raise ValueError('eth_shared_budget_foreign_database')
        if not db.execute('SELECT 1 FROM budget_security WHERE id=1').fetchone():
            raise ValueError('eth_shared_budget_journal_unverified')
        if not db.execute('SELECT 1 FROM weight_meta WHERE id=1').fetchone():
            raise ValueError('eth_shared_budget_metadata_missing')
        db.execute('SELECT at_ms,weight,priority,pid FROM weight_events LIMIT 0')
        db.execute('SELECT backoff_until_ms,headers_json FROM weight_meta LIMIT 0')
    journal = Path(str(value)+'.cooldown')
    try:
        ready = (journal/'ready').read_bytes()
    except OSError:
        raise ValueError('eth_shared_budget_journal_unverified') from None
    if journal.is_symlink() or (journal/'ready').is_symlink() or ready != b'cooldown-v1\n':
        raise ValueError('eth_shared_budget_journal_unverified')
    def current_identity():
        database, cooldown = value.stat(), journal.stat()
        return {'database': [database.st_dev, database.st_ino], 'journal': [cooldown.st_dev, cooldown.st_ino]}
    prior = current_identity()
    if expected_identity is not None and prior != expected_identity:
        raise ValueError('eth_shared_budget_identity_mismatch')
    budget = SharedRequestBudget(value)
    if current_identity() != prior:
        raise ValueError('eth_shared_budget_identity_changed')
    return budget


def next_resolution(engine, last_checked, at_ms):
    rows = engine.store.db.execute('SELECT start,payload FROM eth_shadow_windows ORDER BY start').fetchall()
    rows.sort(key=lambda row: (row[0] <= last_checked, row[0]))
    for start, payload in rows:
        window = json.loads(payload)
        if start+SLOT_MS <= at_ms and window.get('identity') and not engine.store.get('outcomes', start):
            return start, window
    return None


@contextmanager
def stop_signals(stop):
    """Keep stop handlers installed through asyncio.run's executor drain."""
    previous = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        for number in previous:
            signal.signal(number, lambda signum, frame: stop.set())
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


class StopFlag:
    """Polling-only, lock-free flag safe under repeated Python signal handlers."""
    def __init__(self):
        self.requested = False

    def set(self):
        self.requested = True

    def is_set(self):
        return self.requested


async def final_resolution_pass(engine, read_detail, stop):
    rows = engine.store.db.execute('SELECT start,payload FROM eth_shadow_windows ORDER BY start').fetchall()
    for start, payload in rows:
        if stop.is_set():
            break
        window = json.loads(payload)
        if start+SLOT_MS > now_ms() or not window.get('identity') or engine.store.get('outcomes', start):
            continue
        try:
            raw = await read_detail(window['identity']['market_topic'])
            engine.resolve(start, raw, now_ms())
        except Exception as exc:
            engine.diagnostic('eth_final_resolution_read_'+type(exc).__name__, now_ms())


async def collect(engine, budget_path, grace_seconds, *, stop=None, budget_identity=None):
    if engine.store.namespace['input_mode'] != 'collect':
        raise ValueError('eth_collect_namespace_mode_mismatch')
    from .eth_t67c_data import EthCatalog, PublicTape, book_feed, book_snapshot, fetch_klines
    key, secret = os.environ.get('PREDICTION_BINANCE_API_KEY', ''), os.environ.get('PREDICTION_BINANCE_API_SECRET', '')
    if budget_identity is None:
        raise ValueError('eth_shared_budget_expected_identity_required')
    catalog = EthCatalog(key, secret, shared_budget(budget_path, expected_identity=budget_identity))
    tape, feed = PublicTape(engine), book_feed(key, secret)
    stop = stop if stop is not None else StopFlag()
    current = {'raw': None, 'start': None}
    pending_reads = set()

    def read_done(task):
        pending_reads.discard(task)
        if not task.cancelled():
            task.exception()  # Retrieve failures even if its scheduling task was cancelled.

    async def read(function, *args, **kwargs):
        if stop.is_set():
            raise RuntimeError('eth_shutdown_requested')
        def dispatch():
            if stop.is_set():
                raise RuntimeError('eth_shutdown_requested')
            return function(*args, **kwargs)
        task = asyncio.create_task(asyncio.to_thread(dispatch))
        pending_reads.add(task)
        task.add_done_callback(read_done)
        # Cancel scheduling without cancelling the armed HTTP/journal completion.
        return await asyncio.shield(task)

    async def discovery():
        while not stop.is_set():
            try:
                start = now_ms()//SLOT_MS*SLOT_MS
                data = await read(catalog.markets)
                topics = data.get('marketTopics', [])
                for topic in topics:
                    if stop.is_set():
                        break
                    if topic.get('symbol') != SYMBOL:
                        continue
                    raw = await read(catalog.detail, topic['marketTopicId'])
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
                    candles = await read(fetch_klines, start, budget=catalog.budget)
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
                    raw = await read(catalog.detail, window['identity']['market_topic'])
                    engine.resolve(start, raw, now_ms())
                except Exception as exc:
                    engine.diagnostic('eth_resolution_read_'+type(exc).__name__, now_ms())
                last_checked = start
            await asyncio.sleep(20)

    tasks = []
    completed = False
    try:
        await tape.start()
        tasks = [asyncio.create_task(fn()) for fn in (discovery, features, resolutions)]
        while not stop.is_set():
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
                completed = True
                break
            await asyncio.sleep(.1)
    finally:
        if not completed:
            stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await tape.close()
            await feed.close()
        finally:
            # Never close the store/lock or exit with an armed journal HTTP thread.
            # Socket timeout is not a total request deadline: do not impose an unsafe
            # drain timeout which would abandon an unknown server response.
            if pending_reads:
                await asyncio.gather(*pending_reads, return_exceptions=True)
            try:
                if completed and not stop.is_set():
                    await final_resolution_pass(engine, lambda topic: read(catalog.detail, topic), stop)
            finally:
                stop.set()
                if pending_reads:
                    await asyncio.gather(*pending_reads, return_exceptions=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Experimental ETH T6.7c Shadow only; no trading capabilities')
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--replay', type=Path)
    source.add_argument('--collect', action='store_true')
    parser.add_argument('--root', type=Path, default=Path('prediction/data/eth-t67c-shadow'))
    parser.add_argument('--market-spec', type=Path)
    parser.add_argument('--windows', type=int, default=20)
    parser.add_argument('--shared-weight-db', type=Path)
    parser.add_argument('--expected-shared-budget-identity', type=parse_budget_identity)
    parser.add_argument('--resolution-grace-seconds', type=int, default=600)
    parser.add_argument('--poll-telegram', action='store_true')
    args = parser.parse_args(argv)
    if args.collect and (not args.shared_weight_db or not args.expected_shared_budget_identity):
        parser.error('--collect requires --shared-weight-db and independently verified --expected-shared-budget-identity')
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
                stop = StopFlag()
                with stop_signals(stop):
                    if args.poll_telegram:
                        from .eth_t67c_telegram import collect_with_telegram
                        asyncio.run(collect_with_telegram(engine, args.shared_weight_db, args.resolution_grace_seconds, stop=stop,
                                                        budget_identity=args.expected_shared_budget_identity))
                    else:
                        asyncio.run(collect(engine, args.shared_weight_db, args.resolution_grace_seconds, stop=stop,
                                            budget_identity=args.expected_shared_budget_identity))
            print(encode(engine.status()))
    finally:
        store.close()


if __name__ == '__main__':
    main()
