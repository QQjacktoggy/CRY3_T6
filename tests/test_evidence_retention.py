"""Offline storage exhaustion and retention regressions (no market APIs)."""
import errno
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from src.gridbot.prediction import evidence_retention as limits
from src.gridbot.prediction.c180_signal_runtime import C180SignalStore
from src.gridbot.prediction.regime_t67_evidence import EvidenceStore


def book(at, start=None, padding=''):
    return dict(market_start_ms=at // 300000 * 300000 if start is None else start,
                market_topic='topic', market_id='id', book_at_ms=at,
                captured_at_ms=at, padding=padding)


def size(path):
    return sum(p.stat().st_size for p in path.parent.glob(path.name+'*'))


@pytest.mark.parametrize('store_type', [EvidenceStore, C180SignalStore])
def test_low_space_rejects_before_write_then_recovers(tmp_path, monkeypatch, store_type):
    with closing(store_type(tmp_path/'evidence')) as store:
        write = store.book if store_type is EvidenceStore else store.persist_book
        write(book(1000000))
        before = store.db.total_changes
        real_usage = limits.shutil.disk_usage
        monkeypatch.setattr(limits.shutil, 'disk_usage', lambda _: SimpleNamespace(free=1))
        with pytest.raises(OSError, match='reserve') as error:
            write(book(1000100))
        assert error.value.errno == errno.ENOSPC
        assert store.db.total_changes == before
        monkeypatch.setattr(limits.shutil, 'disk_usage', real_usage)
        write(book(1000200))
        assert store.db.total_changes > before


@pytest.mark.parametrize('store_type', [EvidenceStore, C180SignalStore])
def test_payload_cap_counts_utf8_bytes(tmp_path, store_type):
    with closing(store_type(tmp_path/'evidence')) as store:
        write = store.book if store_type is EvidenceStore else store.persist_book
        with pytest.raises(ValueError, match='payload'):
            write(book(1000000, padding='界' * limits.MAX_PAYLOAD_BYTES))
        assert store.db.total_changes == 0


def test_age_retention_preserves_signals_outcomes_and_other_audit(tmp_path):
    with closing(C180SignalStore(tmp_path/'signal')) as store:
        with store.db:
            store.db.execute("INSERT INTO c180_signals VALUES(1,'t','m',2,3,'SKIP','audit')")
            store.db.execute("INSERT INTO c180_recovery_outcomes VALUES(1,'UNKNOWN','audit',3)")
            store.db.execute('CREATE TABLE trading_ledger(payload TEXT)')
            store.db.execute("INSERT INTO trading_ledger VALUES('never delete')")
        store.persist_book(book(1000000))
        for n in range(limits.PRUNE_EVERY):
            store.persist_book(book(1000000 + 86400001 + n))
        assert store.db.execute('SELECT min(captured_at_ms) FROM c180_book_events').fetchone()[0] > 86400000
        assert store.db.execute('SELECT count(*) FROM c180_books').fetchone()[0] == 1
        assert store.db.execute('SELECT signal_json FROM c180_signals').fetchone()[0] == 'audit'
        assert store.db.execute('SELECT detail_json FROM c180_recovery_outcomes').fetchone()[0] == 'audit'
        assert store.db.execute('SELECT payload FROM trading_ledger').fetchone()[0] == 'never delete'


def test_t67_time_retention_preserves_model_horizon(tmp_path):
    with closing(EvidenceStore(tmp_path/'evidence')) as store:
        for at in range(1000000, 1000000+3700000, 10000):
            store.book(book(at))
            store.spot(dict(kind='binance_spot_aggTrade', received_at=at,
                            body={'T': at, 'p': '100'}), 1)
        # Force the next sweep to demonstrate the precise retention horizon.
        store.budget.counts.clear()
        at += 100
        store.book(book(at))
        store.spot(dict(kind='binance_spot_aggTrade', received_at=at,
                        body={'T': at, 'p': '100'}), 1)
        assert store.db.execute('SELECT min(received_ms) FROM spot').fetchone()[0] >= at-1200000
        assert store.db.execute('SELECT min(captured_ms) FROM books').fetchone()[0] >= at-3600000
        assert store.db.execute('SELECT count(*) FROM spot WHERE received_ms>=?', (at-900000,)).fetchone()[0] >= 90


def test_row_caps_apply_even_without_clock_progress(tmp_path):
    with closing(EvidenceStore(tmp_path/'evidence')) as store:
        with store.db:
            store.db.executemany('INSERT INTO books VALUES(?,?,?,?)',
                                 [(1, n, 1000000, '{}') for n in range(37000)])
            store.db.executemany('INSERT INTO spot VALUES(?,?,?,?,?)',
                                 [('binance_spot', 1, n, 1000000, '100') for n in range(31000)])
        store.book(book(1000000))
        store.spot(dict(kind='binance_spot_aggTrade', received_at=1000000,
                        body={'T': 1000000, 'p': '100'}), 1)
        assert store.db.execute('SELECT count(*) FROM books').fetchone()[0] == 36000
        assert store.db.execute('SELECT count(*) FROM spot').fetchone()[0] == 30000


def test_long_running_storage_converges_and_restart_reapplies_budget(tmp_path):
    path = tmp_path/'evidence'
    with closing(sqlite3.connect(path)) as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE evidence(at INTEGER,payload TEXT)')
        db.execute('CREATE INDEX evidence_at ON evidence(at)')
        budget = limits.EvidenceBudget(db, path)
        sizes = []
        # 30 complete retention windows, with bursts exceeding the row cap.
        for cycle in range(30):
            for n in range(128):
                budget.prepare()
                with db:
                    db.execute('INSERT INTO evidence VALUES(?,?)', (cycle*10000+n, 'x'*1024))
                    budget.prune('evidence', 'at', cycle*10000+n, age_ms=20000, rows=100)
            assert db.execute('SELECT count(*) FROM evidence').fetchone()[0] <= 100+limits.PRUNE_EVERY-1
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            sizes.append(size(path))
        assert max(sizes[10:])-min(sizes[10:]) <= 16384
    with closing(sqlite3.connect(path)) as db:
        limits.EvidenceBudget(db, path)
        assert db.execute('PRAGMA max_page_count').fetchone()[0]*db.execute('PRAGMA page_size').fetchone()[0] <= limits.MAX_DATABASE_BYTES


def test_pinned_wal_stops_collection_then_recovers(tmp_path, monkeypatch):
    path = tmp_path/'evidence'
    with closing(EvidenceStore(path)) as store, closing(sqlite3.connect(path)) as reader:
        store.book(book(1000000))
        store.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        reader.execute('BEGIN')
        reader.execute('SELECT * FROM books').fetchall()
        store.book(book(1000100))
        monkeypatch.setattr(limits, 'MAX_WAL_BYTES', 1)
        before = store.db.total_changes
        with pytest.raises(OSError, match='active reader'):
            store.book(book(1000200))
        assert store.db.total_changes == before
        reader.rollback()
        store.book(book(1000300))
        assert store.db.total_changes > before


def test_database_byte_cap_fails_closed_without_deleting_audit(tmp_path, monkeypatch):
    monkeypatch.setattr(limits, 'MAX_DATABASE_BYTES', 128*1024)
    with closing(C180SignalStore(tmp_path/'signal')) as store:
        with store.db:
            store.db.execute("INSERT INTO c180_signals VALUES(1,'t','m',2,3,'SKIP','audit')")
        with pytest.raises(sqlite3.OperationalError, match='full'):
            for n in range(100):
                store.persist_book(book(1000000+n, padding='x'*32000))
        assert store.db.execute('SELECT signal_json FROM c180_signals').fetchone()[0] == 'audit'
        assert store.path.stat().st_size <= limits.MAX_DATABASE_BYTES


def test_c180_10hz_entry_windows_retain_entire_recovery_span(tmp_path):
    # Runtime writes only T+124s..136s, inclusive: 121 quotes per five-minute
    # market. More than the maximum twenty-market/100-minute recovery span.
    with closing(C180SignalStore(tmp_path/'signal')) as store:
        for market in range(25):
            start = 1000000 + market*300000
            for offset in range(124000, 136001, 100):
                store.persist_book(book(start+offset, start=start))
        assert len(store.book_events(1000000)) == 121
        assert store.db.execute('SELECT count(*) FROM c180_book_events').fetchone()[0] == 25*121


def test_persistence_failure_reaches_collector_fail_closed_health(tmp_path, monkeypatch):
    from src.gridbot.prediction.c180_evidence_collector import C180EvidenceCollector
    with closing(C180SignalStore(tmp_path/'signal')) as store:
        collector = C180EvidenceCollector(
            tape=SimpleNamespace(ingest=lambda _: None), feed_factory=lambda _: None,
            on_raw_event=lambda event: store.persist_book(event))
        monkeypatch.setattr(limits.shutil, 'disk_usage', lambda _: SimpleNamespace(free=1))
        collector._ingest(book(1000000))
        assert collector._evidence_error == 'raw_persist:OSError'
        assert store.db.execute('SELECT count(*) FROM c180_book_events').fetchone()[0] == 0


def test_c180_row_caps_preserve_recent_recovery(tmp_path):
    with closing(C180SignalStore(tmp_path/'signal')) as store:
        with store.db:
            store.db.executemany('INSERT INTO c180_book_events VALUES(?,?,?,?)',
                                 [(1, n, 1000000, '{}') for n in range(41000)])
            store.db.executemany('INSERT INTO c180_books VALUES(?,?,?,?,?)',
                                 [(n, 'topic', 'id', n, '{}') for n in range(400)])
        store.persist_book(book(1000000))
        assert store.db.execute('SELECT count(*) FROM c180_book_events').fetchone()[0] == 40000
        assert store.db.execute('SELECT count(*) FROM c180_books').fetchone()[0] == 300
