"""RecordingPool (plan §4.2): wraps the real pool adapter through the
``pool=`` seam, proxying real aiosqlite connections. Capabilities: (a) fail
an execute matching a predicate, (b) record txn_begin/txn_commit/rollback
events into a shared event log, (c) expose every executed statement, and
(d) block an execute matching a predicate on a two-sided gate.

Test-owned; zero production hooks.
"""

import sqlite3
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import aiosqlite

from ksqlite.types import ConnectionPool

from .fakes import TwoSidedGate

Predicate = Callable[[str], bool]


class InjectedSqliteFailure(sqlite3.OperationalError):
    """Distinguishable injected failure (never matches the busy heuristics)."""


class RecordingConnection:
    """Proxy over a real aiosqlite connection; only the intercepted methods
    differ, everything else delegates verbatim.
    """

    def __init__(self, conn: aiosqlite.Connection, pool: "RecordingPool") -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_pool", pool)

    def __setattr__(self, name: str, value: Any) -> None:
        # Attribute writes (e.g. row_factory) go to the REAL connection.
        setattr(self._conn, name, value)

    async def execute(self, sql: str, parameters: Any = None) -> aiosqlite.Cursor:
        await self._pool.before_execute(sql)
        cursor: aiosqlite.Cursor
        if parameters is None:
            cursor = await self._conn.execute(sql)
        else:
            cursor = await self._conn.execute(sql, parameters)
        if sql.lstrip().upper().startswith("BEGIN"):
            self._pool.events.append("txn_begin")
        return cursor

    async def commit(self) -> None:
        self._pool.commit_attempts += 1
        if self._pool.fail_commit_at == self._pool.commit_attempts:
            raise InjectedSqliteFailure(
                f"injected commit failure #{self._pool.commit_attempts}"
            )
        await self._conn.commit()
        self._pool.events.append("txn_commit")

    async def rollback(self) -> None:
        await self._conn.rollback()
        self._pool.events.append("rollback")

    @property
    def in_transaction(self) -> bool:
        return bool(self._conn.in_transaction)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class RecordingPool:
    """A ConnectionPool implementation wrapping another pool (the seam)."""

    def __init__(self, inner: ConnectionPool) -> None:
        self.inner = inner
        self.events: list[str] = []
        self.executed: list[str] = []
        self.checkouts = 0
        self.commit_attempts = 0
        self.fail_commit_at: int | None = None
        self._fail_predicate: Predicate | None = None
        self._gate_predicate: Predicate | None = None
        self._gate: TwoSidedGate | None = None

    def fail_on(self, predicate: Predicate) -> None:
        """Fail the next execute whose SQL matches (one-shot)."""
        self._fail_predicate = predicate

    def gate_on(self, predicate: Predicate, gate: TwoSidedGate) -> None:
        """Park the next matching execute on a two-sided gate (one-shot)."""
        self._gate_predicate = predicate
        self._gate = gate

    async def before_execute(self, sql: str) -> None:
        self.executed.append(sql)
        if self._gate_predicate is not None and self._gate_predicate(sql):
            gate = self._gate
            self._gate_predicate, self._gate = None, None
            assert gate is not None
            await gate.pass_through()
        if self._fail_predicate is not None and self._fail_predicate(sql):
            self._fail_predicate = None
            raise InjectedSqliteFailure(f"injected failure on: {sql[:60]}")

    def connection(self) -> AbstractAsyncContextManager[aiosqlite.Connection]:
        return self._checked_out()

    @asynccontextmanager
    async def _checked_out(self) -> AsyncIterator[aiosqlite.Connection]:
        self.checkouts += 1
        async with self.inner.connection() as conn:
            yield RecordingConnection(conn, self)  # type: ignore[misc]

    async def close(self) -> None:
        await self.inner.close()
