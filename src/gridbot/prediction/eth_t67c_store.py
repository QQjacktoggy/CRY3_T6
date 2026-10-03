"""Finite, isolated ETH paper namespace. No Prediction migrations or trading tables."""
import json
import sqlite3
from pathlib import Path

from .eth_t67c_policy import FINGERPRINT, PROFILE, SLOT_MS, SYMBOL, digest


def encode(value):
    data = json.dumps(value, sort_keys=True, allow_nan=False)
    if len(data.encode()) > 262144:
        raise ValueError('eth_evidence_too_large')
    return data


class ShadowStore:
    def __init__(self, root, *, spec=None, windows=20, input_mode='replay'):
        path = Path(root).absolute()
        if path.name != 'eth-t67c-shadow' or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('eth_namespace_path_invalid')
        if not 1 <= windows <= 50:
            raise ValueError('eth_windows_must_be_1_to_50')
        if input_mode not in ('replay', 'collect'):
            raise ValueError('eth_input_mode_invalid')
        namespace = dict(profile=PROFILE, symbol=SYMBOL, mode='SHADOW', fingerprint=FINGERPRINT,
                         spec_sha256=digest(spec) if spec else None, windows=windows, input_mode=input_mode)
        marker = path / 'namespace.json'
        if path.exists() and any(path.iterdir()) and not marker.is_file():
            raise ValueError('eth_namespace_unowned_directory')
        if marker.is_symlink():
            raise ValueError('eth_namespace_symlink')
        if marker.exists() and json.loads(marker.read_text()) != namespace:
            raise ValueError('eth_namespace_config_mismatch')
        path.mkdir(parents=True, exist_ok=True)
        if not marker.exists():
            with marker.open('x') as stream:
                stream.write(encode(namespace)+'\n')
        db_path = path / 'shadow.sqlite3'
        if db_path.is_symlink():
            raise ValueError('eth_namespace_database_symlink')
        # Refuse a BTC/foreign database before any schema or PRAGMA writes.
        if db_path.exists():
            with sqlite3.connect(db_path.as_uri()+'?mode=ro', uri=True) as probe:
                names = {r[0] for r in probe.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not names or any(not n.startswith('eth_shadow_') for n in names):
                    raise ValueError('eth_namespace_foreign_database')
                row = probe.execute('SELECT payload FROM eth_shadow_meta WHERE id=1').fetchone()
                if not row or json.loads(row[0]) != namespace:
                    raise ValueError('eth_namespace_database_mismatch')
        self.root, self.namespace = path, namespace
        self._tape_seconds = {}
        self.db = sqlite3.connect(db_path, timeout=.25)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS eth_shadow_meta(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS eth_shadow_windows(start INTEGER PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS eth_shadow_features(start INTEGER PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS eth_shadow_quotes(start INTEGER PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS eth_shadow_outcomes(start INTEGER PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS eth_shadow_diagnostics(start INTEGER,code TEXT,first_ms INTEGER,last_ms INTEGER,count INTEGER,PRIMARY KEY(start,code));
            CREATE TABLE IF NOT EXISTS eth_shadow_tape(source TEXT,generation INTEGER,second INTEGER,payload TEXT NOT NULL,PRIMARY KEY(source,generation,second));
            CREATE INDEX IF NOT EXISTS eth_shadow_tape_retention ON eth_shadow_tape(second);
            CREATE TABLE IF NOT EXISTS eth_shadow_feed_watermarks(source TEXT PRIMARY KEY,payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS eth_shadow_replay_cursor(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
        ''')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO eth_shadow_meta VALUES(1,?)', (encode(namespace),))

    def close(self):
        self.db.close()

    def get(self, table, start):
        if table not in ('windows', 'features', 'quotes', 'outcomes'):
            raise ValueError('eth_table_invalid')
        row = self.db.execute(f'SELECT payload FROM eth_shadow_{table} WHERE start=?', (start,)).fetchone()
        return json.loads(row[0]) if row else None

    def admit(self, start):
        if type(start) is not int or start <= 0 or start % SLOT_MS:
            raise ValueError('eth_window_invalid')
        with self.db:
            existing = self.get('windows', start)
            if existing is not None:
                return existing
            starts = [r[0] for r in self.db.execute('SELECT start FROM eth_shadow_windows ORDER BY start')]
            if len(starts) >= self.namespace['windows']:
                return None
            if starts and start != starts[-1]+SLOT_MS:
                raise ValueError('eth_window_schedule_discontinuous')
            value = dict(self.namespace, market_start_ms=start, state={}, status='OBSERVING')
            self.db.execute('INSERT INTO eth_shadow_windows VALUES(?,?)', (start, encode(value)))
        return value

    def save_window(self, start, value):
        if value.get('fingerprint') != FINGERPRINT or value.get('symbol') != SYMBOL:
            raise ValueError('eth_window_provenance_invalid')
        with self.db:
            candidate = value['state'].get('candidate')
            prior = self.get('quotes', start)
            if prior is not None and prior != candidate:
                raise ValueError('eth_frozen_quote_state_mismatch')
            if candidate is not None:
                self.db.execute('INSERT OR IGNORE INTO eth_shadow_quotes VALUES(?,?)', (start, encode(candidate)))
            self.db.execute('UPDATE eth_shadow_windows SET payload=? WHERE start=?', (encode(value), start))

    def feed_state(self, source, value):
        with self.db:
            self.db.execute('INSERT INTO eth_shadow_feed_watermarks VALUES(?,?) '
                            'ON CONFLICT(source) DO UPDATE SET payload=excluded.payload', (source, encode(value)))

    def append(self, table, start, value):
        if table not in ('features', 'quotes', 'outcomes') or self.get('windows', start) is None:
            raise ValueError('eth_evidence_table_or_window_invalid')
        if value.get('symbol') != SYMBOL or value.get('fingerprint') != FINGERPRINT:
            raise ValueError('eth_evidence_asset_invalid')
        prior = self.get(table, start)
        if prior is not None:
            if prior != value:
                raise ValueError('eth_immutable_evidence_changed')
            return
        with self.db:
            self.db.execute(f'INSERT INTO eth_shadow_{table} VALUES(?,?)', (start, encode(value)))

    def diagnostic(self, start, code, at_ms):
        # Codes are fixed by the program; exception strings and raw URLs never enter the DB.
        if not code.replace('_', '').isalnum() or len(code) > 100:
            raise ValueError('eth_diagnostic_invalid')
        with self.db:
            self.db.execute('INSERT INTO eth_shadow_diagnostics VALUES(?,?,?,?,1) '
                            'ON CONFLICT(start,code) DO UPDATE SET last_ms=excluded.last_ms,count=count+1',
                            (start, code, at_ms, at_ms))

    def tape(self, source, generation, packet, received_ms):
        from .regime_lane import dec
        if source not in ('spot', 'futures') or packet.get('s') != SYMBOL or packet.get('e') != 'aggTrade':
            raise ValueError('eth_tape_asset_invalid')
        stamp = packet['T']
        if (type(stamp) is not int or not 0 <= received_ms-stamp <= 1500
                or dec(packet['p']) <= 0 or type(generation) is not int or generation < 1):
            raise ValueError('eth_tape_clock_or_generation_invalid')
        value = dict(source=source, symbol=SYMBOL, generation=generation, event_ms=stamp,
                     received_ms=received_ms, price=str(dec(packet['p'])))
        bucket = (generation, stamp//1000)
        if self._tape_seconds.get(source) == bucket:
            return False
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO eth_shadow_tape VALUES(?,?,?,?)',
                            (source, generation, stamp//1000, encode(value)))
            self.db.execute('DELETE FROM eth_shadow_tape WHERE rowid IN '
                            '(SELECT rowid FROM eth_shadow_tape ORDER BY second DESC LIMIT -1 OFFSET 40000)')
        self._tape_seconds[source] = bucket
        return True

    def report(self):
        rows = [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM eth_shadow_windows ORDER BY start')]
        quotes = [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM eth_shadow_quotes ORDER BY start')]
        outcomes = [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM eth_shadow_outcomes ORDER BY start')]
        diagnostic = [dict(code=r[0], count=r[1]) for r in self.db.execute(
            'SELECT code,sum(count) FROM eth_shadow_diagnostics GROUP BY code ORDER BY code')]
        return dict(self.namespace, observed_windows=len(rows), paper_quotes=len(quotes),
                    official_outcomes=len(outcomes), windows=rows, quotes=quotes, outcomes=outcomes,
                    diagnostics=diagnostic, limitations='PAPER_QUOTE_ONLY; no fill rate, Live WR/PnL or promotion')
