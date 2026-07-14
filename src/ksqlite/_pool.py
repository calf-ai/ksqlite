"""Connection layer (plan §3 ``_pool``; spec §11, Design B).

Connection factory (autocommit, WAL, ``busy_timeout``), the ``aiosqlitepool``
adapter behind the ``ConnectionPool`` seam, checkout hygiene, and the
``BEGIN IMMEDIATE`` transaction helper with bounded ``SQLITE_BUSY`` retry.
"""

import asyncio
import sqlite3
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import Any, TypeVar

import aiosqlite
from aiosqlitepool import (
    PoolClosedError,
    PoolConnectionAcquireTimeoutError,
    SQLiteConnectionPool,
)

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


# The three statement helpers below centralize one load-bearing rule: every
# cursor is closed as soon as its rows are consumed — PRAGMAs and DML return
# rows, and an un-reset statement holds a shared lock that blocks other
# connections' writes (C-01b).


async def execute(
    conn: aiosqlite.Connection,
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] = (),
) -> None:
    """Execute one statement and immediately close its cursor."""
    cursor = await conn.execute(sql, parameters)
    await cursor.close()


async def fetch_one(
    conn: aiosqlite.Connection,
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] = (),
) -> Any:
    """Execute, fetch the first row (or ``None``), close the cursor."""
    cursor = await conn.execute(sql, parameters)
    try:
        return await cursor.fetchone()
    finally:
        await cursor.close()


async def fetch_all(
    conn: aiosqlite.Connection,
    sql: str,
    parameters: Sequence[Any] | Mapping[str, Any] = (),
) -> list[Any]:
    """Execute, fetch every row, close the cursor."""
    cursor = await conn.execute(sql, parameters)
    try:
        return list(await cursor.fetchall())
    finally:
        await cursor.close()


async def open_connection(
    db_path: str | Path, busy_timeout: float
) -> aiosqlite.Connection:
    """Open one connection per spec §11: autocommit (``isolation_level=None``
    — all transactions are explicit), WAL, and the SQLite busy handler set to
    ``busy_timeout`` (seconds; SQLite takes milliseconds).
    """
    conn = await aiosqlite.connect(db_path, isolation_level=None)
    try:
        await execute(conn, f"PRAGMA busy_timeout = {int(busy_timeout * 1000)}")
        row = await fetch_one(conn, "PRAGMA journal_mode=WAL")
        # PRAGMA journal_mode does NOT raise on failure — it returns the
        # RESULTING mode (':memory:' DBs stay 'memory'; some network/overlay
        # filesystems keep the prior mode). The uniform read/write pool
        # (Design B, spec §11) is only safe under WAL: silently degraded,
        # readers and the writer serialize against each other with zero
        # signal pointing at the cause. Fail loud instead (C-01d).
        mode = None if row is None else str(row[0]).lower()
        if mode != "wal":
            raise StorageError(
                f"could not enable WAL on {str(db_path)!r}: journal_mode is"
                f" {mode!r} (WAL needs a regular file on a WAL-capable"
                " filesystem)"
            )
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
    await execute(conn, "PRAGMA query_only=ON")
    try:
        yield
    finally:
        await execute(conn, "PRAGMA query_only=OFF")


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
        try:
            async with self._pool.connection() as conn:
                # Checkout hygiene (spec §11): the inner pool only rolls back
                # on RELEASE, so a transaction opened while the connection sat
                # idle reaches checkout — reset it here (C-07). Likewise a
                # stale query_only flag passes the pool's SELECT 1 health
                # check (C-08).
                if conn.in_transaction:
                    await conn.rollback()
                await execute(conn, "PRAGMA query_only=OFF")
                yield conn
        except (PoolClosedError, PoolConnectionAcquireTimeoutError) as exc:
            # Boundary mapping (spec §4): pool-layer failures — acquisition
            # timeout under exhaustion, or the pool closed by a concurrent
            # stop() — surface as the documented StorageError (C-11). NB:
            # these exceptions carry their text in `.message` only; str(exc)
            # is the empty string.
            raise StorageError(f"connection pool failure: {exc.message}") from exc

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
                await execute(conn, "BEGIN IMMEDIATE")
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
