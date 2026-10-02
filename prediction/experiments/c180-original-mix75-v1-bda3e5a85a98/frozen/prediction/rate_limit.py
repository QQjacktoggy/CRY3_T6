"""Small process-wide weight budget for Prediction REST calls.

The official API exposes a weight budget rather than a request-count limit.
Keeping the budget outside the HTTP client lets worker instances share one
process-wide gate while tests can inject a deterministic clock.
"""

from __future__ import annotations

import time
import hashlib
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from email.utils import parsedate_to_datetime
from dataclasses import dataclass
from threading import Lock
from typing import Callable


MAX_DEADLINE_MS = 2**63 - 1  # SQLite INTEGER; saturation remains fail-closed.
DEFAULT_BACKOFF_MS = 60_000
MAX_BLOCKING_WAIT_MS = 1_000


def retry_after_deadline(value, now_ms, *, milliseconds=False):
    """Never shorten a valid server ban; invalid hints get a safe fallback.

    Huge bans defer requests immediately instead of sleeping a worker. Keep
    their deadline durable, including across restart; do not cap a ban to a
    shorter interval merely to make the client responsive.
    """
    fallback = min(MAX_DEADLINE_MS, now_ms + DEFAULT_BACKOFF_MS)
    if value is None:
        return fallback
    text = str(value).strip()
    if len(text) > 128:
        return MAX_DEADLINE_MS  # Unparseable oversized hints require fail-closed review.
    try:
        delay = Decimal(text)
    except InvalidOperation:
        if not milliseconds:
            try:
                date = parsedate_to_datetime(text)
                if date.tzinfo is not None:
                    return min(MAX_DEADLINE_MS, max(now_ms + 1000, int(date.timestamp() * 1000)))
            except (ValueError, TypeError, OverflowError):
                pass
        return fallback
    if not delay.is_finite() or delay < 0:
        return fallback
    multiplier = 1 if milliseconds else 1000
    # Compare before multiplying/converting: Decimal exponents may be enormous.
    if delay >= Decimal(MAX_DEADLINE_MS - now_ms) / multiplier:
        return MAX_DEADLINE_MS
    return min(MAX_DEADLINE_MS, now_ms + max(1000, int((delay * multiplier).to_integral_value(rounding=ROUND_CEILING))))


@dataclass(frozen=True)
class RateLimitHealth:
    limit: int
    used: int
    remaining: int
    window_started_at_ms: int
    backoff_until_ms: int = 0
    last_error: str | None = None
    reserve: int = 0
    deferred: int = 0

    @property
    def healthy(self) -> bool:
        return (
            self.remaining > self.reserve
            and self.backoff_until_ms <= int(time.time() * 1000)
            and "deferred" not in str(self.last_error or "").lower()
        )

    def as_dict(self) -> dict[str, int | str | None | bool]:
        return {
            "limit": self.limit,
            "used": self.used,
            "remaining": self.remaining,
            "window_started_at_ms": self.window_started_at_ms,
            "backoff_until_ms": self.backoff_until_ms,
            "last_error": self.last_error,
            "reserve": self.reserve,
            "normal_remaining": max(0, self.remaining - self.reserve),
            "emergency_remaining": self.remaining,
            "deferred": self.deferred,
            "healthy": self.healthy,
        }


class PredictionRateLimiter:
    """Sliding one-minute weight budget with exponential backoff metadata."""

    def __init__(
        self,
        *,
        limit: int = 1_200,
        window_ms: int = 60_000,
        clock_ms: Callable[[], int] | None = None,
        sleep: Callable[[float], None] | None = None,
        reserve_weight: int | None = None,
    ) -> None:
        if int(limit) < 1:
            raise ValueError("rate-limit weight budget must be positive")
        self.limit = int(limit)
        self.window_ms = max(1, int(window_ms))
        self.reserve_weight = min(int(limit), max(0, int(reserve_weight if reserve_weight is not None else min(200, max(1, int(limit) // 5)))))
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.sleep = sleep or time.sleep
        self._window_started_at_ms = int(self.clock_ms())
        self._used = 0
        self._backoff_until_ms = 0
        self._last_error: str | None = None
        self._deferred = 0
        self._lock = Lock()

    def _roll(self, now: int) -> None:
        if now - self._window_started_at_ms >= self.window_ms:
            self._window_started_at_ms = now
            self._used = 0
            # A time-window rollover must not erase a server cooldown.

    def acquire(self, weight: int = 1, *, block: bool = True, emergency: bool = False, management: bool = False, headroom: int = 0) -> bool:
        amount = max(1, int(weight))
        if amount > self.limit:
            raise ValueError("request weight exceeds global budget")
        started = int(self.clock_ms())
        waited = False
        while True:
            with self._lock:
                now = int(self.clock_ms())
                self._roll(now)
                usable_limit = self.limit if emergency else max(0, self.limit - (min(200, self.reserve_weight) if management else self.reserve_weight))
                wait_until = max(self._backoff_until_ms, self._window_started_at_ms + self.window_ms if self._used + amount + max(0, int(headroom)) > usable_limit else now)
                if wait_until <= now:
                    self._used += amount
                    return True
                if not block or waited:
                    return False
                remaining_wait = MAX_BLOCKING_WAIT_MS - max(0, now - started)
                if wait_until - now > remaining_wait or remaining_wait <= 0:
                    return False
                delay = max(0.0, (wait_until - now) / 1000.0)
            self.sleep(delay)
            waited = True

    def can_send_prepaid(self) -> bool:
        """Check only the live server ban; a reservation never exempts a request."""
        with self._lock:
            return int(self.clock_ms()) >= self._backoff_until_ms

    def try_acquire(self, weight: int = 1, *, emergency: bool = False) -> bool:
        return self.acquire(weight, block=False, emergency=emergency)

    can_acquire = try_acquire

    @property
    def remaining(self) -> int:
        return self.health().remaining

    def reset(self) -> None:
        with self._lock:
            now = int(self.clock_ms())
            self._window_started_at_ms = now
            self._used = 0
            self._backoff_until_ms = 0
            self._last_error = None

    def note_deferred(self, weight: int = 1, *, error: str | None = None) -> None:
        """Record a local budget deferral without treating it as an API fault."""

        with self._lock:
            self._deferred += 1
            self._last_error = error or f"weight {max(1, int(weight))} deferred"

    def note_rate_limit(self, retry_after_ms: int | None = None, *, error: str | None = None) -> None:
        with self._lock:
            now = int(self.clock_ms())
            deadline = retry_after_deadline(retry_after_ms, now, milliseconds=True)
            self._backoff_until_ms = max(self._backoff_until_ms, deadline)
            self._last_error = error or "rate limited"

    def note_response(self, status, headers=None, *, error=None):
        if int(status) not in (418, 429):
            return
        headers = {str(k).lower(): v for k, v in (headers or {}).items()}
        with self._lock:
            deadline = retry_after_deadline(headers.get('retry-after'), int(self.clock_ms()))
            self._backoff_until_ms = max(self._backoff_until_ms, deadline)
            self._last_error = error or 'rate limited'

    def note_success(self) -> None:
        with self._lock:
            self._last_error = None
            if self._backoff_until_ms < int(self.clock_ms()):
                self._backoff_until_ms = 0

    def health(self) -> RateLimitHealth:
        with self._lock:
            now = int(self.clock_ms())
            self._roll(now)
            return RateLimitHealth(
                self.limit,
                self._used,
                max(0, self.limit - self._used),
                self._window_started_at_ms,
                self._backoff_until_ms,
                self._last_error,
                self.reserve_weight,
                self._deferred,
            )


# Friendly aliases used by lightweight adapters/tests.
WeightBudget = PredictionRateLimiter
RateBudget = PredictionRateLimiter


__all__ = ["PredictionRateLimiter", "RateLimitHealth", "RateBudget", "WeightBudget"]


"""Appended to rate_limit.py by the isolated candidate builder."""
import json
import os
import sqlite3
import fcntl
import tempfile
import stat
from pathlib import Path
from contextvars import ContextVar
from contextlib import contextmanager

REQUEST_PRIORITY = ContextVar('prediction_request_priority', default='normal')
REQUEST_PREPAID = ContextVar('prediction_request_prepaid', default=False)

@contextmanager
def request_budget_scope(priority='normal', prepaid=False):
    a=REQUEST_PRIORITY.set(priority);b=REQUEST_PREPAID.set(prepaid)
    try:yield
    finally:REQUEST_PREPAID.reset(b);REQUEST_PRIORITY.reset(a)

class SharedBudgetDeferred(RuntimeError):
    def __init__(self,health):
        self.health=health
        super().__init__('Prediction shared request budget deferred')

class _CooldownJournal:
    """Per-request write-ahead records, independent of the SQLite writer lock.

    A locked pending record represents a live HTTP request and permits parallel
    requests. An unlocked pending/corrupt record means persistence was interrupted
    and denies all new requests, including after restart. Only a completed,
    fsynced response replaces/removes it. No expiry guesses lift unknown bans.
    """
    def __init__(self, path, *, required):
        self.path = Path(str(path) + '.cooldown')
        if required:
            self._check_ready()
        else:
            self.path.mkdir(mode=0o700, exist_ok=True)
            ready = self.path / 'ready'
            if not ready.exists():
                with ready.open('xb') as stream:
                    stream.write(b'cooldown-v1\n')
                    stream.flush()
                    os.fsync(stream.fileno())
                self._sync_directory()
            self._check_ready()

    def _check_ready(self):
        ready = self.path / 'ready'
        if ready.is_symlink() or ready.read_bytes() != b'cooldown-v1\n':
            raise OSError('cooldown journal unavailable')

    def _sync_directory(self):
        fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def deadline(self, now):
        self._check_ready()
        deadline = 0
        for path in self.path.iterdir():
            if path.name == 'ready':
                continue
            if not path.name.startswith('request-'):
                return MAX_DEADLINE_MS
            fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    return MAX_DEADLINE_MS
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue  # A live request still owns this write-ahead record.
                data = os.read(fd, 128)
                parts = data.split(b':')
                if (len(parts) != 3 or parts[0] != b'v1' or not parts[1].isdigit()
                        or len(parts[1]) > 19 or parts[2] != hashlib.sha256(parts[1]).hexdigest().encode()):
                    return MAX_DEADLINE_MS
                value = int(parts[1])
                if not 0 <= value <= MAX_DEADLINE_MS:
                    return MAX_DEADLINE_MS
                deadline = max(deadline, value)
                if value <= now:
                    path.unlink()
            finally:
                os.close(fd)
        return deadline

    def begin(self, now):
        if self.deadline(now) > now:
            return None
        return self.begin_record()

    def begin_record(self):
        self._check_ready()
        fd, name = tempfile.mkstemp(prefix='request-', dir=self.path)
        stream = os.fdopen(fd, 'r+b')
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            stream.write(b'pending')
            stream.flush()
            os.fsync(stream.fileno())
            self._sync_directory()
            return stream, Path(name)
        except BaseException:
            stream.close()  # Leave the incomplete record: peers/restarts deny.
            raise

    def finish(self, token, deadline):
        stream, path = token
        try:
            # A failed write leaves pending/partial bytes, never an old zero.
            stream.seek(0)
            stream.truncate()
            value = str(deadline).encode('ascii')
            stream.write(b'v1:' + value + b':' + hashlib.sha256(value).hexdigest().encode())
            stream.flush()
            os.fsync(stream.fileno())
            if not deadline:
                path.unlink()
                self._sync_directory()
        finally:
            stream.close()


class SharedRequestBudget:
    """Cross-process rolling budget; separate file, never the trading database.

    1200 is a local safety ceiling, not a claim about the exchange limit.
    Normal work leaves 300, BUY reconciliation leaves 200, exits use the rest.
    No keys, wallet identifiers, orders, or request bodies are stored here.
    """
    def __init__(self,path,*,clock_ms=None,limit=1200,reserve=300,management_reserve=200):
        self._metadata_unavailable = False
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.clock_ms=clock_ms or (lambda:int(time.time()*1000))
        self.limit=int(limit);self.reserve=int(reserve);self.management_reserve=int(management_reserve)
        self._cooldowns = None
        with self._connect() as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('CREATE TABLE IF NOT EXISTS weight_events(at_ms INTEGER NOT NULL,weight INTEGER NOT NULL,priority TEXT NOT NULL,pid INTEGER NOT NULL DEFAULT 0)')
            c.execute('CREATE INDEX IF NOT EXISTS weight_events_time ON weight_events(at_ms)')
            c.execute('CREATE TABLE IF NOT EXISTS weight_meta(id INTEGER PRIMARY KEY CHECK(id=1),backoff_until_ms INTEGER NOT NULL,headers_json TEXT NOT NULL)')
            c.execute("INSERT OR IGNORE INTO weight_meta VALUES(1,0,'{}')")
            # Old/corrupt non-integer deadlines cannot silently lift a ban.
            c.execute("UPDATE weight_meta SET backoff_until_ms=? WHERE typeof(backoff_until_ms)!='integer' OR backoff_until_ms<0", (MAX_DEADLINE_MS,))
            c.execute('CREATE TABLE IF NOT EXISTS budget_security(id INTEGER PRIMARY KEY CHECK(id=1))')
            required = c.execute('SELECT 1 FROM budget_security WHERE id=1').fetchone() is not None
            try:
                self._cooldowns = _CooldownJournal(self.path, required=required)
            except (OSError, ValueError):
                self._metadata_unavailable = True
            else:
                c.execute('INSERT OR IGNORE INTO budget_security VALUES(1)')
    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(str(self.path),timeout=.5,isolation_level=None)
        try:
            yield connection
        finally:
            connection.close()
    def begin_request(self):
        """Durably arm the response record immediately before an actual HTTP call."""
        if self._metadata_unavailable:
            return None
        try:
            return self._cooldowns.begin(int(self.clock_ms()))
        except (OSError, ValueError):
            self._metadata_unavailable = True
            return None

    def transport_failed(self, token):
        # No HTTP status was obtained. Preserve usual transport-failure behavior.
        try:
            self._cooldowns.finish(token, 0)
        except (OSError, ValueError):
            self._metadata_unavailable = True

    def can_send_prepaid(self):
        if self._metadata_unavailable:
            return False
        try:
            with self._connect() as c:
                backoff=c.execute('SELECT backoff_until_ms FROM weight_meta WHERE id=1').fetchone()[0]
            now = int(self.clock_ms())
            return now >= max(backoff, self._cooldowns.deadline(now))
        except (sqlite3.Error, OSError, ValueError):
            return False
    def acquire(self,weight=1,*,priority='normal',headroom=0):
        if self._metadata_unavailable:
            return False
        now=int(self.clock_ms());weight=max(1,int(weight));headroom=max(0,int(headroom))
        ceiling=self.limit if priority=='exit' else self.limit-(self.management_reserve if priority=='management' else self.reserve)
        try:
            if self._cooldowns.deadline(now) > now:
                return False
            with self._connect() as c:
                c.execute('BEGIN IMMEDIATE')
                c.execute('DELETE FROM weight_events WHERE at_ms<=?',(now-60000,))
                used=c.execute('SELECT coalesce(sum(weight),0) FROM weight_events').fetchone()[0]
                backoff=c.execute('SELECT backoff_until_ms FROM weight_meta WHERE id=1').fetchone()[0]
                if now<backoff or used+weight+headroom>ceiling:
                    c.rollback();return False
                c.execute('INSERT INTO weight_events VALUES(?,?,?,?)',(now,weight,priority,os.getpid()));c.commit();return True
        except (sqlite3.Error, OSError, ValueError):
            return False  # Storage contention never grants unmetered requests.
    def note_response(self,status,headers,*,token=None):
        now=int(self.clock_ms())
        allowed={str(k).lower():str(v) for k,v in (headers or {}).items()
                 if str(k).lower()=='retry-after' or str(k).lower().startswith(('x-mbx-used-weight','x-sapi-used-ip-weight','x-sapi-used-uid-weight'))}
        deadline = retry_after_deadline(allowed.get('retry-after'), now) if int(status) in (418,429) else 0
        allowed = {key: value[:128] for key, value in allowed.items()}
        try:
            if token is None:
                # Also support callers recording an externally obtained response.
                # Actual client HTTP always arms its token BEFORE sending.
                token = self._cooldowns.begin(now)
            if token is not None:
                self._cooldowns.finish(token, deadline)
            elif deadline:
                # An existing journal ban must never be shortened by a new one.
                # Use a fresh record even when admission is currently blocked.
                token = self._cooldowns.begin_record()
                self._cooldowns.finish(token, deadline)
        except (OSError, ValueError):
            self._metadata_unavailable = True
        try:
            with self._connect() as c:
                c.execute('UPDATE weight_meta SET backoff_until_ms=max(backoff_until_ms,?),headers_json=? WHERE id=1',
                          (deadline,json.dumps({'at_ms':now,'status':status,'headers':allowed})))
        except sqlite3.Error:
            self._metadata_unavailable = True  # Preserve POST outcome; deny subsequent requests.
    def health(self):
        if self._metadata_unavailable:
            return dict(limit=self.limit, remaining=0, error='shared_budget_unavailable')
        now=int(self.clock_ms())
        try:
            with self._connect() as c:
                used=c.execute('SELECT coalesce(sum(weight),0) FROM weight_events WHERE at_ms>?',(now-60000,)).fetchone()[0]
                backoff,headers=c.execute('SELECT backoff_until_ms,headers_json FROM weight_meta WHERE id=1').fetchone()
                by_priority=dict(c.execute('SELECT priority,sum(weight) FROM weight_events WHERE at_ms>? GROUP BY priority',(now-60000,)))
                by_process=dict(c.execute('SELECT pid,sum(weight) FROM weight_events WHERE at_ms>? GROUP BY pid',(now-60000,)))
            backoff = max(backoff, self._cooldowns.deadline(now))
            return dict(limit=self.limit,used=used,remaining=max(0,self.limit-used),reserve=self.reserve,
                        management_reserve=self.management_reserve,backoff_until_ms=backoff,
                        by_priority=by_priority,by_process=by_process,last_response=json.loads(headers),scope='shared_prediction_clients')
        except (sqlite3.Error, OSError, ValueError):return dict(limit=self.limit,remaining=0,error='shared_budget_unavailable')
