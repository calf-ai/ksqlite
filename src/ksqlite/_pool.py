"""Connection layer (plan §3 ``_pool``; spec §11, Design B).

Connection factory (autocommit, WAL, ``busy_timeout``), the ``aiosqlitepool``
adapter behind the ``ConnectionPool`` seam, checkout hygiene, and the
``BEGIN IMMEDIATE`` transaction helper with bounded ``SQLITE_BUSY`` retry.
Grown red-first by C-01..C-10.
"""

import asyncio
import sqlite3
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import TypeVar

import aiosqlite
from aiosqlitepool import SQLiteConnectionPool

from .errors import StorageError

T = TypeVar("T")

# Bounded SQLITE_BUSY retry (plan §3 _pool; spec §11): attempts are capped by
# whichever bound trips first — the attempt count or the total deadline of
# BUSY_RETRY_DEADLINE_FACTOR x busy_timeout.
BUSY_RETRY_MAX_ATTEMPTS = 3
BUSY_RETRY_DEADLINE_FACTOR = 2.0
_BUSY_BACKOFF_BASE_SECONDS = 0.01


def _is_busy_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "busy" in message


async def open_connection(
    db_path: str | Path, busy_timeout: float
) -> aiosqlite.Connection:
    """Open one connection per spec §11: autocommit (``isolation_level=None``
    — all transactions are explicit), WAL, and the SQLite busy handler set to
    ``busy_timeout`` (seconds; SQLite takes milliseconds).
    """
    conn = await aiosqlite.connect(db_path, isolation_level=None)
    try:
        # Both PRAGMAs return a row; an un-reset statement holds a shared lock
        # that blocks other connections' writes (C-01b) — close each cursor.
        cursor = await conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout * 1000)}")
        await cursor.close()
        cursor = await conn.execute("PRAGMA journal_mode=WAL")
        await cursor.close()
    except BaseException:
        # A leaked connection keeps a non-daemon worker thread alive and
        # wedges the host interpreter at exit (C-01c).
        await conn.close()
        raise
    return conn


@asynccontextmanager
async def query_only(conn: aiosqlite.Connection) -> AsyncIterator[None]:
    """Hold ``PRAGMA query_only=ON`` for the scope; always reset in a finally
    (spec §9 read-only enforcement).
    """
    cursor = await conn.execute("PRAGMA query_only=ON")
    await cursor.close()
    try:
        yield
    finally:
        cursor = await conn.execute("PRAGMA query_only=OFF")
        await cursor.close()


class AiosqlitePoolAdapter:
    """The default ``ConnectionPool`` implementation (spec §4 "Types", §11):
    a uniform ``aiosqlitepool`` where every connection reads and writes.
    """

    def __init__(
        self, db_path: str | Path, pool_size: int, busy_timeout: float
    ) -> None:
        self._pool = SQLiteConnectionPool(
            connection_factory=lambda: open_connection(db_path, busy_timeout),
            pool_size=pool_size,
        )

    def connection(self) -> AbstractAsyncContextManager[aiosqlite.Connection]:
        return self._checked_out()

    @asynccontextmanager
    async def _checked_out(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._pool.connection() as conn:
            # Checkout hygiene (spec §11): the inner pool only rolls back on
            # RELEASE, so a transaction opened while the connection sat idle
            # reaches checkout — reset it here (C-07). Likewise a stale
            # query_only flag passes the pool's SELECT 1 health check (C-08).
            if conn.in_transaction:
                await conn.rollback()
            cursor = await conn.execute("PRAGMA query_only=OFF")
            await cursor.close()
            yield conn

    async def close(self) -> None:
        await self._pool.close()


class TransactionRunner:
    """Runs work inside ``BEGIN IMMEDIATE`` (spec §6/§11): the write lock is
    taken up front, so the ``SQLITE_BUSY_SNAPSHOT`` lock-upgrade path — which
    ``busy_timeout`` does not retry — can never occur.
    """

    def __init__(
        self,
        busy_timeout: float,
        backoff_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._busy_timeout = busy_timeout
        self._backoff_sleep = backoff_sleep

    async def run(
        self,
        conn: aiosqlite.Connection,
        work: Callable[[aiosqlite.Connection], Awaitable[T]],
    ) -> T:
        """Run ``work`` inside one ``BEGIN IMMEDIATE`` transaction.

        The bounded SQLITE_BUSY retry covers the WHOLE attempt (BEGIN + work
        + commit), not just BEGIN: observed live (macOS, SQLite 3.45.3, WAL,
        two same-process connections), a BEGIN IMMEDIATE retried immediately
        after a busy failure can spuriously succeed WITHOUT the write lock,
        and the transaction's first write then raises busy. Retrying the
        attempt makes that race harmless — ``work`` only ever re-runs after
        a full rollback, so nothing partial persists (spec §11 hygiene).
        """
        deadline = time.monotonic() + self._busy_timeout * BUSY_RETRY_DEADLINE_FACTOR
        attempt = 1
        while True:
            try:
                cursor = await conn.execute("BEGIN IMMEDIATE")
                await cursor.close()
                try:
                    result = await work(conn)
                    await conn.commit()
                except BaseException:
                    # Connection hygiene (spec §11): never return a connection
                    # to the pool holding an open transaction or write lock.
                    await conn.rollback()
                    raise
                return result
            except sqlite3.Error as exc:
                if not (
                    isinstance(exc, sqlite3.OperationalError) and _is_busy_error(exc)
                ):
                    # Boundary mapping (spec §4): SQLite failures inside the
                    # write transaction surface as StorageError.
                    raise StorageError(f"transaction failed: {exc}") from exc
                if conn.in_transaction:  # pragma: no cover - defensive
                    await conn.rollback()
                remaining = deadline - time.monotonic()
                if attempt >= BUSY_RETRY_MAX_ATTEMPTS or remaining <= 0:
                    raise StorageError(
                        f"database busy after {attempt} attempts "
                        f"(busy_timeout={self._busy_timeout}s)"
                    ) from exc
                # Per-attempt wait bounded by the remaining deadline.
                await self._backoff_sleep(
                    min(_BUSY_BACKOFF_BASE_SECONDS * attempt, remaining)
                )
                attempt += 1
