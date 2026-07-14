"""Shared fixtures (plan §4.1/§4.2): the migrated-pool `db` and the
session-scoped pinned Redpanda `broker` (e2e; auto-skips without Docker).
"""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from ksqlite import _pool, _schema


@pytest.fixture(scope="session")
def broker() -> Iterator[Any]:
    """Session-scoped pinned Redpanda container (plan §4.2). The image pin
    (v24.2.20) is the P0-recorded constant in tests/e2e/conftest.py.
    """
    from tests.e2e.conftest import REDPANDA_IMAGE

    try:
        from testcontainers.kafka import RedpandaContainer

        container = RedpandaContainer(REDPANDA_IMAGE)
        container.start(timeout=60)
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Docker/Redpanda unavailable: {exc}")
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture
async def db(tmp_path: Path) -> AsyncIterator[_pool.AiosqlitePoolAdapter]:
    """A migrated uniform pool on a fresh tmp-file DB (plan §4.2 ``db``)."""
    pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "state.db", pool_size=4, busy_timeout=0.5
    )
    async with pool.connection() as conn:
        await _schema.migrate(conn, generated_columns=(), indexes=())
    try:
        yield pool
    finally:
        await pool.close()
