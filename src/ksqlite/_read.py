"""The QueryRunner service (plan §3 ``_read``; spec §9). Grown red-first by
R-01..R-11.
"""

import dataclasses
import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from . import _pool
from .errors import HydrationError, StorageError
from .types import ConnectionPool


class QueryRunner:
    """Runs host SQL against the auto-scoped ``records`` view (spec §9)."""

    def __init__(self, *, pool: ConnectionPool) -> None:
        self._pool = pool

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        into: type[Any] | None = None,
    ) -> list[Any]:
        async with self._pool.connection() as conn:
            conn.row_factory = sqlite3.Row
            async with _pool.query_only(conn):
                try:
                    cursor = await conn.execute(sql, params)
                    rows = list(await cursor.fetchall())
                    await cursor.close()
                except sqlite3.Error as exc:
                    # Boundary mapping (spec §4/§9): a write under query_only
                    # ("attempt to write a readonly database"), SQLITE_BUSY
                    # past busy_timeout, etc. surface as StorageError.
                    raise StorageError(f"query failed: {exc}") from exc
        if into is None:
            return rows
        return [_hydrate(row, into) for row in rows]


def _hydrate(row: sqlite3.Row, into: type[Any]) -> Any:
    """Hydrate the row's ``payload`` column into the target (spec §9)."""
    if "payload" not in row.keys():
        raise HydrationError(
            "into= requires the query to select a `payload` column (spec §9)"
        )
    payload = row["payload"]
    validator = getattr(into, "model_validate_json", None)
    if validator is not None:  # Pydantic v2
        return validator(payload)
    if dataclasses.is_dataclass(into):
        return into(**json.loads(payload))
    # Exactly the two documented targets — no generic-kwargs fallback (§9).
    raise TypeError(f"into= must be a Pydantic v2 model or a dataclass, got {into!r}")
