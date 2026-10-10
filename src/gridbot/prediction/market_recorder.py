"""Record-only book and spot samples for every followed market (edge research).

Public evidence only: this module has no order route and opens no connection.
The C180 sidecar hands it the book and aggTrade events it already receives.
Each market becomes one compressed row in its own SQLite file, written after
the market ends, so no-trade markets can be scored later (favourite band,
chase-filled entries, masked lanes) against the official or reference-chain
winner. Any failure here only loses research rows; callers must swallow it.
"""

from __future__ import annotations

import errno
import json
import shutil
import sqlite3
import time
import zlib
from contextlib import closing
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from .evidence_retention import MAX_DATABASE_BYTES, MIN_FREE_BYTES, EvidenceBudget


VERSION = 1
SLOT_MS = 300_000
# Sparse before the entry window, every second through the T6 decision and
# order window (124..136 s), then every 5 s for post-entry and exit research.
OFFSETS_MS = (tuple(range(0, 110_000, 10_000)) + tuple(range(110_000, 140_000, 1_000))
              + tuple(range(140_000, SLOT_MS, 5_000)))
LEVELS = 3
RETENTION_MS = 90 * 86_400_000
# 90 days of markets; also bounds the file when rows compress worse than expected.
MAX_ROWS = 90 * 288
PRUNE_EVERY = 48
# Stop far above the 128 MB floor the signal and evidence stores fail closed
# on, so research rows can never take the space trading needs.
RECORDER_MIN_FREE_BYTES = MIN_FREE_BYTES + 4 * MAX_DATABASE_BYTES
SPOT_SOURCES = {'binance_spot_aggTrade': 'spot', 'binance_futures_aggTrade': 'futures'}


def recorder_path(signal_db: str | Path) -> Path:
    return Path(signal_db).with_name('market-recorder.sqlite3')


def _require_recorder_space(path: Path) -> None:
    if shutil.disk_usage(path.parent).free < RECORDER_MIN_FREE_BYTES:
        raise OSError(errno.ENOSPC, 'market recorder disk reserve reached')


def _levels(levels: Any) -> list[list[str]]:
    return [[str(price), str(size)] for price, size in list(levels or ())[:LEVELS]]


def compact_quote(quote: Mapping[str, Any]) -> dict[str, Any]:
    """Top levels of a frozen ``normalize_book`` quote, as exact decimal strings."""

    out: dict[str, Any] = {}
    for side in ('UP', 'DOWN'):
        q = quote[side]
        out[side] = {
            'ask': None if q.get('ask') is None else str(q['ask']),
            'bid': None if q.get('bid') is None else str(q['bid']),
            'asks': _levels(q.get('ask_levels')),
            'bids': _levels(q.get('bid_levels')),
        }
    return out


class MarketRecorder:
    """Sample the latest book at fixed offsets; persist one row per market."""

    def __init__(self, path: str | Path, *, symbol: str,
                 clock_ms: Callable[[], int] | None = None) -> None:
        self.path = Path(path)
        self.symbol = symbol
        self._clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._db: sqlite3.Connection | None = None
        self._budget: EvidenceBudget | None = None
        self._market: dict[str, Any] | None = None
        self._samples: list[dict[str, Any]] = []
        self._next = 0
        self._spot: dict[str, list[Any]] = {}
        self._writes = 0
        self.written = 0
        self.dropped = 0

    def _open(self) -> sqlite3.Connection:
        if self._db is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            _require_recorder_space(self.path)
            db = sqlite3.connect(self.path, timeout=1)
            try:
                self._budget = EvidenceBudget(db, self.path)
                db.execute('PRAGMA journal_mode=WAL')
                db.execute(
                    'CREATE TABLE IF NOT EXISTS market_samples('
                    'market_start_ms INTEGER PRIMARY KEY, symbol TEXT NOT NULL, '
                    'market_topic TEXT NOT NULL, market_id TEXT NOT NULL, reference TEXT NOT NULL, '
                    'fee_bps TEXT, yes TEXT, identified_at_ms INTEGER, version INTEGER NOT NULL, '
                    'samples INTEGER NOT NULL, written_at_ms INTEGER NOT NULL, payload BLOB NOT NULL)')
                db.commit()
            except BaseException:
                db.close()
                self._budget = None
                raise
            self._db = db
        return self._db

    def spot(self, event: Mapping[str, Any]) -> None:
        """Remember the latest public trade price per source (no history kept)."""

        source = SPOT_SOURCES.get(str(event.get('kind')))
        body = event.get('body')
        if source is None or not isinstance(body, Mapping) or body.get('p') is None:
            return
        self._spot[source] = [str(body['p']), body.get('T', body.get('E')), int(event['received_at'])]

    def tick(self, market: Mapping[str, Any] | None, at_ms: int,
             quote: Callable[[], Mapping[str, Any] | None]) -> None:
        """Called on every evidence event; samples when an offset is due."""

        buffered = self._market
        if buffered is not None and (market is None or int(market['start']) != buffered['start']
                                     or at_ms >= buffered['end']):
            self.flush()
        if market is None:
            return
        start, end = int(market['start']), int(market['end'])
        if not start <= at_ms < end:
            return
        if self._market is None:
            self._market = dict(
                start=start, end=end, topic=str(market['topic']), market_id=str(market['market_id']),
                reference=str(market['reference']), fee_bps=None if market.get('fee_bps') is None else str(market['fee_bps']),
                yes=market.get('yes'), identified_at=market.get('identified_at'))
            self._samples, self._next = [], 0
        elif self._market['market_id'] != str(market['market_id']):
            # The sidecar stops on an identity change; never mix two books.
            self._market, self._samples, self.dropped = None, [], self.dropped + 1
            return
        offset = at_ms - start
        if self._next >= len(OFFSETS_MS) or offset < OFFSETS_MS[self._next]:
            return
        # Missed offsets are skipped, never backfilled from a later book.
        while self._next < len(OFFSETS_MS) and OFFSETS_MS[self._next] <= offset:
            due = OFFSETS_MS[self._next]
            self._next += 1
        sample: dict[str, Any] = {'o': due, 'at': at_ms}
        try:
            q = quote()
        except Exception as exc:
            q, sample['err'] = None, type(exc).__name__
        if q is None:
            sample['q'] = None
        else:
            sample['book_at'] = q.get('book_at_ms')
            sample['q'] = compact_quote(q)
            if q.get('stale'):
                sample['stale'] = True
        for source, value in self._spot.items():
            sample[source] = list(value)
        self._samples.append(sample)

    def flush(self) -> None:
        """Write the buffered market once; a fuller row replaces a partial one."""

        market, samples = self._market, self._samples
        self._market, self._samples, self._next = None, [], 0
        if market is None or not samples:
            return
        payload = zlib.compress(json.dumps(
            {'v': VERSION, 'symbol': self.symbol, 'market': market, 'offsets_ms': 'v1', 'samples': samples},
            sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8'), 9)
        try:
            db = self._open()
            _require_recorder_space(self.path)
            self._budget.prepare()
            now = self._clock()
            if self._writes % PRUNE_EVERY == 0:
                # Own transaction, before the insert: a full file can still free pages.
                with db:
                    db.execute('DELETE FROM market_samples WHERE market_start_ms<?', (now - RETENTION_MS,))
                    db.execute('DELETE FROM market_samples WHERE market_start_ms IN (SELECT market_start_ms '
                               'FROM market_samples ORDER BY market_start_ms DESC LIMIT -1 OFFSET ?)',
                               (MAX_ROWS - 1,))
            self._writes += 1
            with db:
                db.execute(
                    'INSERT INTO market_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '
                    'ON CONFLICT(market_start_ms) DO UPDATE SET samples=excluded.samples, '
                    'written_at_ms=excluded.written_at_ms, payload=excluded.payload '
                    'WHERE excluded.samples>market_samples.samples AND excluded.market_id=market_samples.market_id',
                    (market['start'], self.symbol, market['topic'], market['market_id'], market['reference'],
                     market['fee_bps'], market['yes'], market['identified_at'], VERSION, len(samples), now,
                     sqlite3.Binary(payload)))
        except BaseException:
            self.dropped += 1
            raise
        self.written += 1

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._db is not None:
                self._db.close()
                self._db = None


def iter_markets(path: str | Path, since_ms: int = 0) -> Iterator[dict[str, Any]]:
    """Read-only: decoded rows by market start, one at a time (months do not fit in memory)."""

    uri = Path(path).resolve().as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True, timeout=2)) as db:
        for (raw,) in db.execute('SELECT payload FROM market_samples WHERE market_start_ms>=? '
                                 'ORDER BY market_start_ms', (int(since_ms),)):
            yield json.loads(zlib.decompress(raw))


def load_markets(path: str | Path, since_ms: int = 0) -> list[dict[str, Any]]:
    return list(iter_markets(path, since_ms))
