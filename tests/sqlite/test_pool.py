"""P3 `_pool` tests: C-01..C-10 (plan §5 P3). Real tmp-file SQLite; never
``:memory:`` (WAL and file-lock semantics don't hold there).
"""

import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path

import aiosqlite
import pytest
from aiosqlitepool import PoolClosedError, PoolConnectionAcquireTimeoutError

from ksqlite import _pool
from ksqlite.errors import StorageError

ConnPair = tuple[aiosqlite.Connection, aiosqlite.Connection]


async def test_c01_factory_pragmas(tmp_path: Path) -> None:
    conn = await _pool.open_connection(tmp_path / "c01.db", busy_timeout=5.0)
    try:
        # Autocommit: no implicit stdlib transactions (spec §11).
        assert conn.isolation_level is None

        cursor = await conn.execute("PRAGMA journal_mode")
        journal_mode = await cursor.fetchone()
        assert journal_mode is not None and journal_mode[0] == "wal"

        cursor = await conn.execute("PRAGMA busy_timeout")
        busy_ms = await cursor.fetchone()
        assert busy_ms is not None and busy_ms[0] == 5000
    finally:
        await conn.close()


async def test_c01b_factory_leaves_no_lingering_locks(tmp_path: Path) -> None:
    """The factory's own PRAGMA cursors must be closed: PRAGMA statements
    return rows, and an un-reset statement holds a shared lock that blocks
    every OTHER connection's writes (observed live: a second
    ``open_connection`` on the same file failed with 'database is locked').
    """
    db = tmp_path / "c01b.db"
    a = await _pool.open_connection(db, busy_timeout=0.05)
    try:
        b = await _pool.open_connection(db, busy_timeout=0.05)
        try:
            await b.execute("CREATE TABLE t (v TEXT)")  # a write from B works
        finally:
            await b.close()
    finally:
        await a.close()


async def test_c01c_factory_failure_leaks_no_connection_thread(
    tmp_path: Path,
) -> None:
    """A failed post-connect statement must close the half-open connection:
    a leaked aiosqlite connection keeps a non-daemon worker thread alive,
    which wedges the host interpreter at exit (observed live).
    """
    garbage = tmp_path / "not_a_db.bin"
    garbage.write_bytes(b"definitely not a sqlite database file")

    baseline = threading.active_count()
    with pytest.raises(sqlite3.DatabaseError):
        await _pool.open_connection(garbage, busy_timeout=0.05)
    assert threading.active_count() <= baseline


async def test_c01d_non_wal_journal_mode_raises_storage_error() -> None:
    """``PRAGMA journal_mode=WAL`` does NOT raise on failure — SQLite
    returns the RESULTING mode (a ``:memory:`` DB stays 'memory'; a network
    filesystem can stay 'delete'). The uniform read/write pool (Design B,
    spec §11) is only safe under WAL, so a silent fallback must fail loud —
    without leaking the half-open connection's worker thread.
    """
    baseline = threading.active_count()
    with pytest.raises(StorageError):
        conn = await _pool.open_connection(":memory:", busy_timeout=0.5)
        await conn.close()  # reached only if the WAL check is missing
    assert threading.active_count() <= baseline


@pytest.mark.parametrize(
    "pool_exc",
    [PoolConnectionAcquireTimeoutError(), PoolClosedError()],
    ids=["acquire-timeout", "pool-closed"],
)
async def test_c11_pool_layer_failures_map_to_storage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pool_exc: Exception
) -> None:
    """aiosqlitepool's failures are plain Exceptions whose ``str()`` is
    EMPTY (the text lives in ``.message``): unmapped, they'd bypass every
    ``except StorageError`` a host writes for storage pressure. The adapter
    maps them to the documented StorageError with the message preserved.
    """
    adapter = _pool.AiosqlitePoolAdapter(tmp_path / "c11.db", 1, 0.5)

    class ExhaustedPool:
        def connection(self) -> AbstractAsyncContextManager[aiosqlite.Connection]:
            return self._acquire()

        @asynccontextmanager
        async def _acquire(self) -> AsyncIterator[aiosqlite.Connection]:
            raise pool_exc
            yield  # pragma: no cover - never reached

        async def close(self) -> None:
            pass

    inner = adapter._pool  # noqa: SLF001 - the inner pool is not injectable
    monkeypatch.setattr(adapter, "_pool", ExhaustedPool())
    try:
        with pytest.raises(StorageError, match="pool"):
            async with adapter.connection():
                pass
    finally:
        await inner.close()


async def test_c02_txn_helper_commits(two_conns: ConnPair) -> None:
    conn, other = two_conns
    await conn.execute("CREATE TABLE t (v TEXT)")

    runner = _pool.TransactionRunner(busy_timeout=0.2)

    async def work(c: aiosqlite.Connection) -> int:
        await c.execute("INSERT INTO t VALUES ('x')")
        assert c.in_transaction  # BEGIN IMMEDIATE was issued
        return 42

    assert await runner.run(conn, work) == 42
    assert not conn.in_transaction

    # Visible from the second, independent connection => durably committed.
    cursor = await other.execute("SELECT v FROM t")
    rows = await cursor.fetchall()
    assert [r[0] for r in rows] == ["x"]


class Boom(Exception):
    """Test-owned marker exception."""


async def test_c03_txn_helper_rolls_back_on_any_exception(
    two_conns: ConnPair,
) -> None:
    conn, other = two_conns
    await conn.execute("CREATE TABLE t (v TEXT)")

    runner = _pool.TransactionRunner(busy_timeout=0.2)

    async def work(c: aiosqlite.Connection) -> None:
        await c.execute("INSERT INTO t VALUES ('doomed')")
        raise Boom()

    with pytest.raises(Boom):
        await runner.run(conn, work)

    assert not conn.in_transaction  # connection returned clean

    cursor = await other.execute("SELECT COUNT(*) FROM t")
    row = await cursor.fetchone()
    assert row is not None and row[0] == 0  # no partial writes


async def test_c04_sqlite_busy_bounded_retry_raises_storage_error(
    two_conns: ConnPair,
) -> None:
    conn, blocker = two_conns
    await conn.execute("CREATE TABLE t (v TEXT)")

    # The blocker holds the write lock for the whole test.
    cursor = await blocker.execute("BEGIN IMMEDIATE")
    await cursor.close()

    sleeps: list[float] = []

    async def recording_sleep(delay: float) -> None:
        sleeps.append(delay)  # records, never waits: deterministic backoff

    # The runner's deadline (2x ITS busy_timeout) is deliberately DECOUPLED
    # from the fixture connections' 0.2 s PRAGMA wait: on a slow CI runner a
    # single busy-blocked BEGIN can consume a tight deadline before any
    # backoff runs (observed on GitHub macOS runners: "after 1 attempts",
    # zero sleeps). With a generous deadline the ATTEMPT CAP is what binds —
    # deterministically, on any machine.
    runner = _pool.TransactionRunner(busy_timeout=30.0, backoff_sleep=recording_sleep)

    async def work(c: aiosqlite.Connection) -> None:
        await c.execute("INSERT INTO t VALUES ('x')")

    start = time.monotonic()
    with pytest.raises(StorageError, match="after 3 attempts"):
        await runner.run(conn, work)
    elapsed = time.monotonic() - start

    # Loose bound only (plan C-04): 3 attempts x the 0.2 s PRAGMA wait plus
    # scheduling jitter — proves boundedness, not timing.
    assert elapsed < 10
    assert sleeps, "bounded retry must back off between attempts"
    assert not conn.in_transaction

    await blocker.rollback()


async def test_c05_retry_succeeds_when_lock_frees(two_conns: ConnPair) -> None:
    conn, blocker = two_conns
    await conn.execute("CREATE TABLE t (v TEXT)")

    cursor = await blocker.execute("BEGIN IMMEDIATE")
    await cursor.close()

    async def releasing_sleep(delay: float) -> None:
        # Deterministically frees the lock during the backoff (plan C-05).
        if blocker.in_transaction:
            await blocker.rollback()

    # Generous deadline (see C-04): the backoff — which is what frees the
    # lock — must be guaranteed to run regardless of runner speed.
    runner = _pool.TransactionRunner(busy_timeout=30.0, backoff_sleep=releasing_sleep)

    async def work(c: aiosqlite.Connection) -> str:
        await c.execute("INSERT INTO t VALUES ('x')")
        return "ok"

    assert await runner.run(conn, work) == "ok"

    cursor = await blocker.execute("SELECT COUNT(*) FROM t")
    row = await cursor.fetchone()
    assert row is not None and row[0] == 1


async def test_c06_no_busy_snapshot_path(two_conns: ConnPair) -> None:
    """BEGIN IMMEDIATE takes the write lock up front, so the WAL
    snapshot-upgrade error (which ``busy_timeout`` does NOT retry) cannot
    occur. The deferred-BEGIN half is the oracle proving this test detects
    the failure mode it guards against (spec §11).
    """
    conn, other = two_conns
    await conn.execute("CREATE TABLE t (k INTEGER PRIMARY KEY, v INTEGER)")
    await conn.execute("INSERT INTO t VALUES (1, 0)")

    # Oracle: a DEFERRED read-then-write under a concurrent committed write
    # raises the immediate (non-retried) snapshot-upgrade "database is locked".
    cursor = await conn.execute("BEGIN")
    await cursor.close()
    cursor = await conn.execute("SELECT v FROM t WHERE k = 1")
    await cursor.fetchone()
    await cursor.close()
    await other.execute("UPDATE t SET v = v + 100 WHERE k = 1")  # commits (WAL)
    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        await conn.execute("UPDATE t SET v = -1 WHERE k = 1")
    await conn.rollback()

    # The runner's read-then-write under the same attack never hits it.
    runner = _pool.TransactionRunner(busy_timeout=0.2)

    async def work(c: aiosqlite.Connection) -> None:
        cursor = await c.execute("SELECT v FROM t WHERE k = 1")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None
        # The concurrent writer attacks mid-transaction. Because our write
        # lock is already held (IMMEDIATE), the attacker deterministically
        # exhausts ITS busy timeout — it can never sneak a commit under our
        # snapshot the way it did in the deferred oracle above.
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            await other.execute("UPDATE t SET v = v + 1000 WHERE k = 1")
        # ...so this in-transaction write upgrade can never raise SNAPSHOT.
        await c.execute("UPDATE t SET v = ? WHERE k = 1", (row[0] + 1,))

    await runner.run(conn, work)

    cursor = await other.execute("SELECT v FROM t WHERE k = 1")
    row = await cursor.fetchone()
    assert row is not None and row[0] == 101  # oracle's +100, then our +1


async def test_c07_checkout_hygiene_rolls_back_open_txn(tmp_path: Path) -> None:
    """A connection found with an open transaction at CHECKOUT is reset by
    the adapter itself — aiosqlitepool only rolls back on release (V4), so a
    transaction opened while the connection sat idle in the pool reaches the
    next checkout unless the adapter heals it (plan C-07 defense-in-depth).
    """
    pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "c07.db", pool_size=1, busy_timeout=0.2
    )
    try:
        async with pool.connection() as conn:
            await conn.execute("CREATE TABLE t (v TEXT)")
            raw = conn

        # Dirty the idle pooled connection outside any checkout (simulates a
        # prior holder whose transaction escaped release-rollback).
        cursor = await raw.execute("BEGIN IMMEDIATE")
        await cursor.close()
        await raw.execute("INSERT INTO t VALUES ('zombie')")
        assert raw.in_transaction

        async with pool.connection() as conn:
            assert conn is raw  # pool_size=1: same underlying connection
            assert not conn.in_transaction  # adapter rolled it back
            cursor = await conn.execute("SELECT COUNT(*) FROM t")
            row = await cursor.fetchone()
            await cursor.close()
            assert row is not None and row[0] == 0  # zombie write gone
            await conn.execute("INSERT INTO t VALUES ('ok')")  # and writable
    finally:
        await pool.close()


async def test_c08_checkout_hygiene_resets_stale_query_only(tmp_path: Path) -> None:
    """A stale ``query_only=ON`` passes the inner pool's ``SELECT 1`` health
    check (V4), so the reset provably lives in KSQLite's adapter (plan C-08).
    """
    pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "c08.db", pool_size=1, busy_timeout=0.2
    )
    try:
        async with pool.connection() as conn:
            await conn.execute("CREATE TABLE t (v TEXT)")
            raw = conn

        cursor = await raw.execute("PRAGMA query_only=ON")  # stale flag
        await cursor.close()

        async with pool.connection() as conn:
            assert conn is raw  # pool_size=1: same underlying connection
            await conn.execute("INSERT INTO t VALUES ('ok')")  # writable again
    finally:
        await pool.close()


async def test_c09_query_only_scope_helper(two_conns: ConnPair) -> None:
    conn, _ = two_conns
    await conn.execute("CREATE TABLE t (v TEXT)")
    await conn.execute("INSERT INTO t VALUES ('seed')")

    async with _pool.query_only(conn):
        cursor = await conn.execute("SELECT COUNT(*) FROM t")  # reads work
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and row[0] == 1

        for stmt in (
            "INSERT INTO t VALUES ('x')",
            "UPDATE t SET v = 'y'",
            "DELETE FROM t",
            "DROP TABLE t",
        ):
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                await conn.execute(stmt)

    await conn.execute("INSERT INTO t VALUES ('after')")  # OFF restored

    with pytest.raises(Boom):  # reset lives in a finally
        async with _pool.query_only(conn):
            raise Boom()
    await conn.execute("INSERT INTO t VALUES ('after-exception')")


async def test_c10_pool_seam(tmp_path: Path) -> None:
    """The default adapter honors the ConnectionPool protocol, and an
    injected fake satisfying the protocol is usable through the exact same
    surface (plan C-10).
    """
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager, asynccontextmanager

    from ksqlite.types import ConnectionPool

    adapter = _pool.AiosqlitePoolAdapter(
        tmp_path / "c10.db", pool_size=2, busy_timeout=0.2
    )
    try:
        assert isinstance(adapter, ConnectionPool)
    finally:
        await adapter.close()

    class FakePool:
        def __init__(self) -> None:
            self.closed = False
            self.checkouts = 0

        def connection(self) -> AbstractAsyncContextManager[aiosqlite.Connection]:
            self.checkouts += 1

            @asynccontextmanager
            async def cm() -> AsyncIterator[aiosqlite.Connection]:
                conn = await _pool.open_connection(tmp_path / "fake.db", 0.2)
                try:
                    yield conn
                finally:
                    await conn.close()

            return cm()

        async def close(self) -> None:
            self.closed = True

    fake = FakePool()
    assert isinstance(fake, ConnectionPool)
    async with fake.connection() as conn:
        await conn.execute("CREATE TABLE t (v TEXT)")
    await fake.close()
    assert fake.closed and fake.checkouts == 1
