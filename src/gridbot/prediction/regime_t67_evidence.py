"""Durable public evidence only. This module cannot place or cancel orders."""
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from .regime_t67_policy import PROFILE
from .evidence_retention import EvidenceBudget, bounded_payload, require_free_space


def selected_profile(prediction_db):
    with closing(sqlite3.connect(Path(prediction_db).resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as db:
        row = db.execute("SELECT config_value_json FROM prediction_runtime_config "
                         "WHERE config_key='prediction_selected_strategy'").fetchone()
    return json.loads(row[0]).get('profile') if row else None


def evidence_path(signal_db):
    return Path(signal_db).with_name('t67-evidence.sqlite3')


class EvidenceStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        require_free_space(self.path)
        self.db = sqlite3.connect(self.path, timeout=1)
        self.budget = EvidenceBudget(self.db, self.path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.budget.prepare()
        self.db.execute('CREATE TABLE IF NOT EXISTS spot(source TEXT,generation INTEGER,event_ms INTEGER,'
                        'received_ms INTEGER,price TEXT,PRIMARY KEY(source,generation,event_ms))')
        self.db.execute('CREATE INDEX IF NOT EXISTS spot_received ON spot(received_ms)')
        self.db.execute('CREATE TABLE IF NOT EXISTS books(start INTEGER,book_ms INTEGER,captured_ms INTEGER,'
                        'payload TEXT,PRIMARY KEY(start,book_ms))')
        self.db.execute('CREATE INDEX IF NOT EXISTS books_capture ON books(start,captured_ms)')
        self.db.execute('CREATE INDEX IF NOT EXISTS books_retention ON books(captured_ms)')
        self.db.commit()
        self.last_spot = {}

    def spot(self, event, generation):
        kind = event.get('kind')
        source = {'binance_spot_aggTrade': 'binance_spot',
                  'binance_futures_aggTrade': 'binance_futures'}.get(kind)
        if source is None:
            return
        from .regime_lane import dec
        body = event['body']
        received, stamp = int(event['received_at']), int(body.get('T', body.get('E', 0)))
        price = dec(body['p'])
        if not 0 <= received-stamp <= 1500 or price <= 0:
            raise ValueError('spot clock/price invalid')
        if received-self.last_spot.get(source, 0) < 100:
            return
        self.budget.prepare()
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO spot VALUES(?,?,?,?,?)',
                            (source, int(generation), stamp, received, bounded_payload(str(price))))
            # Keep the model's 15-minute horizon plus opening-anchor margin.
            self.budget.prune('spot', 'received_ms', received,
                              age_ms=1200000, rows=30000)
        self.last_spot[source] = received

    def book(self, snapshot):
        raw = bounded_payload(json.dumps(snapshot, sort_keys=True, allow_nan=False))
        self.budget.prepare()
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO books VALUES(?,?,?,?)',
                            (snapshot['market_start_ms'], snapshot['book_at_ms'], snapshot['captured_at_ms'],
                             raw))
            self.budget.prune('books', 'captured_ms', snapshot['captured_at_ms'],
                              age_ms=3600000, rows=36000)

    def close(self):
        self.db.close()


def read_inputs(signal_db, start, at_ms):
    path = evidence_path(signal_db)
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=1)) as db:
        books = [json.loads(r[0]) for r in db.execute(
            'SELECT payload FROM books WHERE start=? AND captured_ms BETWEEN ? AND ? ORDER BY captured_ms,book_ms',
            (start, at_ms-3500, at_ms))]
        spots = [dict(source=r[0], generation=r[1], event_ms=r[2], received_ms=r[3], price=r[4])
                 for r in db.execute('SELECT * FROM spot WHERE received_ms BETWEEN ? AND ? ORDER BY received_ms,event_ms',
                                     (min(start, at_ms-900000), at_ms))]
    return books, spots
