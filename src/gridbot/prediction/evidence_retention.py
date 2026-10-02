"""Resource budgets for disposable public quote evidence, never trading ledgers.

Rows expire but SQLite reuses freed pages (no blocking VACUUM). Immutable signal,
recovery and trading audit rows are never pruned: exhaustion fails closed instead.
"""
import errno
import shutil
from pathlib import Path

MIN_FREE_BYTES = 128 * 1024 * 1024
MAX_DATABASE_BYTES = 256 * 1024 * 1024
MAX_WAL_BYTES = 8 * 1024 * 1024
MAX_PAYLOAD_BYTES = 64 * 1024
PRUNE_EVERY = 64


def require_free_space(path):
    if shutil.disk_usage(Path(path).parent).free < MIN_FREE_BYTES:
        raise OSError(errno.ENOSPC, 'evidence storage reserve reached')


def bounded_payload(raw):
    if len(raw.encode('utf-8')) > MAX_PAYLOAD_BYTES:
        raise ValueError('evidence payload exceeds storage budget')
    return raw


class EvidenceBudget:
    def __init__(self, db, path):
        self.db, self.path = db, Path(path)
        self.counts = {}
        page_size = db.execute('PRAGMA page_size').fetchone()[0]
        pages = MAX_DATABASE_BYTES // page_size
        # max_page_count is connection scoped; reapply at every restart.
        actual = db.execute(f'PRAGMA max_page_count={pages}').fetchone()[0]
        if actual > pages:
            raise OSError(errno.ENOSPC, 'existing evidence database exceeds budget')
        db.execute(f'PRAGMA journal_size_limit={MAX_WAL_BYTES}')

    def prepare(self):
        """Before a transaction: reserve space and bound WAL pinned by readers."""
        require_free_space(self.path)
        wal = Path(str(self.path) + '-wal')
        if wal.exists() and wal.stat().st_size >= MAX_WAL_BYTES:
            # PASSIVE never waits for a reader. A pinned WAL stops collection;
            # freshness/coverage checks then deny trading on stale evidence.
            busy, total, copied = self.db.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
            if busy or copied < total:
                raise OSError(errno.ENOSPC, 'evidence WAL retained by active reader')
            # Fully copied WAL can be reused by SQLite without growing it.

    def prune(self, table, timestamp, now_ms, *, age_ms, rows):
        """Only call with hard-coded evidence table/column names, in transaction.

        At most PRUNE_EVERY - 1 excess rows exist between sweeps. Timestamp
        indexes make expiration and selection of the newest bounded set cheap.
        """
        count = self.counts.get(table, 0)
        self.counts[table] = count + 1
        if count % PRUNE_EVERY:
            return
        self.db.execute(f'DELETE FROM {table} WHERE {timestamp}<?', (now_ms-age_ms,))
        self.db.execute(
            f'DELETE FROM {table} WHERE rowid IN ('
            f'SELECT rowid FROM {table} ORDER BY {timestamp} DESC,rowid DESC LIMIT -1 OFFSET ?)',
            (rows,),
        )
