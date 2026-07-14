"""Shared fixtures for real-SQLite tests (plan §4.2). All test-owned; zero
production hooks.
"""

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path

import aiosqlite
import pytest

from ksqlite import _pool


@pytest.fixture
async def two_conns(
    tmp_path: Path,
) -> AsyncIterator[tuple[aiosqlite.Connection, aiosqlite.Connection]]:
    """Two independent raw connections to the same tmp-file DB, with a small
    ``busy_timeout`` so lock-contention tests stay fast.

    Exception-safe teardown: an unclosed aiosqlite connection leaves a
    non-daemon worker thread that wedges the interpreter at exit.
    """
    db = tmp_path / "db.sqlite"
    async with contextlib.AsyncExitStack() as stack:
        a = await _pool.open_connection(db, busy_timeout=0.2)
        stack.push_async_callback(a.close)
        b = await _pool.open_connection(db, busy_timeout=0.2)
        stack.push_async_callback(b.close)
        yield a, b
