"""Small process-wide weight budget for Prediction REST calls.

The official API exposes a weight budget rather than a request-count limit.
Keeping the budget outside the HTTP client lets worker instances share one
process-wide gate while tests can inject a deterministic clock.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock
from typing import Callable


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
        while True:
            with self._lock:
                now = int(self.clock_ms())
                self._roll(now)
                usable_limit = self.limit if emergency else max(0, self.limit - (min(200, self.reserve_weight) if management else self.reserve_weight))
                wait_until = max(self._backoff_until_ms, self._window_started_at_ms + self.window_ms if self._used + amount + max(0, int(headroom)) > usable_limit else now)
                if wait_until <= now:
                    self._used += amount
                    return True
                if not block:
                    return False
                delay = max(0.0, (wait_until - now) / 1000.0)
            self.sleep(delay)

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
            delay = int(retry_after_ms) if retry_after_ms is not None else max(250, min(10_000, (self._backoff_until_ms - now) * 2 or 250))
            self._backoff_until_ms = max(self._backoff_until_ms, now + max(0, delay))
            self._last_error = error or "rate limited"

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

class SharedRequestBudget:
    """Cross-process rolling budget; separate file, never the trading database.

    1200 is a local safety ceiling, not a claim about the exchange limit.
    Normal work leaves 300, BUY reconciliation leaves 200, exits use the rest.
    No keys, wallet identifiers, orders, or request bodies are stored here.
    """
    def __init__(self,path,*,clock_ms=None,limit=1200,reserve=300,management_reserve=200):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.clock_ms=clock_ms or (lambda:int(time.time()*1000))
        self.limit=int(limit);self.reserve=int(reserve);self.management_reserve=int(management_reserve)
        with self._connect() as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('CREATE TABLE IF NOT EXISTS weight_events(at_ms INTEGER NOT NULL,weight INTEGER NOT NULL,priority TEXT NOT NULL,pid INTEGER NOT NULL DEFAULT 0)')
            c.execute('CREATE INDEX IF NOT EXISTS weight_events_time ON weight_events(at_ms)')
            c.execute('CREATE TABLE IF NOT EXISTS weight_meta(id INTEGER PRIMARY KEY CHECK(id=1),backoff_until_ms INTEGER NOT NULL,headers_json TEXT NOT NULL)')
            c.execute("INSERT OR IGNORE INTO weight_meta VALUES(1,0,'{}')")
    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(str(self.path),timeout=.5,isolation_level=None)
        try:
            yield connection
        finally:
            connection.close()
    def can_send_prepaid(self):
        try:
            with self._connect() as c:
                backoff=c.execute('SELECT backoff_until_ms FROM weight_meta WHERE id=1').fetchone()[0]
            return int(self.clock_ms()) >= backoff
        except sqlite3.Error:
            return False
    def acquire(self,weight=1,*,priority='normal',headroom=0):
        now=int(self.clock_ms());weight=max(1,int(weight));headroom=max(0,int(headroom))
        ceiling=self.limit if priority=='exit' else self.limit-(self.management_reserve if priority=='management' else self.reserve)
        try:
            with self._connect() as c:
                c.execute('BEGIN IMMEDIATE')
                c.execute('DELETE FROM weight_events WHERE at_ms<=?',(now-60000,))
                used=c.execute('SELECT coalesce(sum(weight),0) FROM weight_events').fetchone()[0]
                backoff=c.execute('SELECT backoff_until_ms FROM weight_meta WHERE id=1').fetchone()[0]
                if now<backoff or used+weight+headroom>ceiling:
                    c.rollback();return False
                c.execute('INSERT INTO weight_events VALUES(?,?,?,?)',(now,weight,priority,os.getpid()));c.commit();return True
        except sqlite3.Error:
            return False  # Storage contention never grants unmetered requests.
    def note_response(self,status,headers):
        now=int(self.clock_ms())
        allowed={str(k).lower():str(v) for k,v in (headers or {}).items()
                 if str(k).lower()=='retry-after' or str(k).lower().startswith(('x-mbx-used-weight','x-sapi-used-ip-weight','x-sapi-used-uid-weight'))}
        delay=0
        if int(status) in (418,429):
            try:delay=max(1000,int(float(allowed.get('retry-after','60'))*1000))
            except (ValueError,OverflowError):delay=60000
        try:
            with self._connect() as c:
                c.execute('UPDATE weight_meta SET backoff_until_ms=max(backoff_until_ms,?),headers_json=? WHERE id=1',
                          (now+delay if delay else 0,json.dumps({'at_ms':now,'status':status,'headers':allowed})))
        except sqlite3.Error:
            pass  # Never mask a possibly executed POST with a metadata error.
    def health(self):
        now=int(self.clock_ms())
        try:
            with self._connect() as c:
                used=c.execute('SELECT coalesce(sum(weight),0) FROM weight_events WHERE at_ms>?',(now-60000,)).fetchone()[0]
                backoff,headers=c.execute('SELECT backoff_until_ms,headers_json FROM weight_meta WHERE id=1').fetchone()
                by_priority=dict(c.execute('SELECT priority,sum(weight) FROM weight_events WHERE at_ms>? GROUP BY priority',(now-60000,)))
                by_process=dict(c.execute('SELECT pid,sum(weight) FROM weight_events WHERE at_ms>? GROUP BY pid',(now-60000,)))
            return dict(limit=self.limit,used=used,remaining=max(0,self.limit-used),reserve=self.reserve,
                        management_reserve=self.management_reserve,backoff_until_ms=backoff,
                        by_priority=by_priority,by_process=by_process,last_response=json.loads(headers),scope='shared_prediction_clients')
        except sqlite3.Error:return dict(limit=self.limit,remaining=0,error='shared_budget_unavailable')
