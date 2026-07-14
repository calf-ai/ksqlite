"""P4 `_schema` tests: M-01..M-12 (plan §5 P4). Real tmp-file SQLite."""

import logging
import sqlite3
from pathlib import Path

import aiosqlite
import pytest

from ksqlite import _pool, _schema
from ksqlite.errors import ConfigError, SchemaMigrationError, SQLiteVersionError
from ksqlite.types import GeneratedColumn, Index


async def _fresh_conn(tmp_path: Path) -> aiosqlite.Connection:
    return await _pool.open_connection(tmp_path / "schema.db", busy_timeout=0.2)


async def test_m01_fresh_migrate_creates_full_schema(tmp_path: Path) -> None:
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(conn, generated_columns=(), indexes=())

        cursor = await conn.execute(
            "SELECT name, type FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
        )
        objects = {(r[0], r[1]) for r in await cursor.fetchall()}
        await cursor.close()
        assert ("_records", "table") in objects
        assert ("partition_checkpoint", "table") in objects
        assert ("_owned", "table") in objects
        assert ("records", "view") in objects
        assert ("ix_records_tp", "index") in objects
        assert ("ix_records_key", "index") in objects

        # The UNIQUE(message_id) dedup index exists (spec §5.1).
        cursor = await conn.execute("PRAGMA index_list(_records)")
        indexes = await cursor.fetchall()
        await cursor.close()
        assert any(row[2] == 1 for row in indexes), "no unique index on _records"

        # partition_checkpoint PK columns are NOT NULL (decisions log, nits).
        cursor = await conn.execute("PRAGMA table_info(partition_checkpoint)")
        cols = {r[1]: (r[3], r[5]) for r in await cursor.fetchall()}  # notnull, pk
        await cursor.close()
        assert cols["source_topic"] == (1, 1)
        assert cols["source_partition"] == (1, 2)
        assert cols["changelog_offset"][0] == 1
    finally:
        await conn.close()


async def test_m02_second_migrate_is_idempotent(tmp_path: Path) -> None:
    """Generated columns are invisible to ``PRAGMA table_info`` — a diff
    using it re-adds them every boot and crashes with 'duplicate column
    name'. The diff must use ``table_xinfo`` (spec §5.1, empirically pinned).
    """
    conn = await _fresh_conn(tmp_path)
    try:
        columns = (GeneratedColumn("task_id", "$.task_id"),)
        await _schema.migrate(conn, generated_columns=columns, indexes=())
        await _schema.migrate(conn, generated_columns=columns, indexes=())

        cursor = await conn.execute("PRAGMA table_xinfo(_records)")
        names = [r[1] for r in await cursor.fetchall()]
        await cursor.close()
        assert names.count("task_id") == 1
    finally:
        await conn.close()


async def test_m03_owned_cleared_on_migrate(tmp_path: Path) -> None:
    """Ownership is per-process, re-established by rebalances: a prior run's
    stale `_owned` rows must not serve un-owned partitions at boot (§5.1).
    """
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(conn, generated_columns=(), indexes=())
        await conn.execute("INSERT INTO _owned VALUES ('t', 0)")

        await _schema.migrate(conn, generated_columns=(), indexes=())

        cursor = await conn.execute("SELECT COUNT(*) FROM _owned")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and row[0] == 0
    finally:
        await conn.close()


async def test_m04_generated_column_added_and_indexed(tmp_path: Path) -> None:
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(
            conn,
            generated_columns=(GeneratedColumn("thread_id", "$.thread_id"),),
            indexes=(Index("ix_thread", ["thread_id"]),),
        )

        await conn.execute(
            "INSERT INTO _records VALUES ('m1', 't', 0, 0, 'k', json(?))",
            ('{"thread_id": "th-1"}',),
        )
        await conn.execute(
            "INSERT INTO _records VALUES ('m2', 't', 0, 1, 'k', json(?))",
            ('{"other": 1}',),  # missing payload field -> NULL, no error
        )

        cursor = await conn.execute(
            "SELECT thread_id FROM _records ORDER BY changelog_offset"
        )
        values = [r[0] for r in await cursor.fetchall()]
        await cursor.close()
        assert values == ["th-1", None]

        cursor = await conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM _records WHERE thread_id = ?", ("x",)
        )
        plan = " ".join(str(tuple(r)) for r in await cursor.fetchall())
        await cursor.close()
        assert "ix_thread" in plan
    finally:
        await conn.close()


async def test_m05_add_column_later_on_populated_table(tmp_path: Path) -> None:
    """VIRTUAL is alterable onto a populated table (STORED is not; §5.1);
    values are extracted for pre-existing rows.
    """
    conn = await _fresh_conn(tmp_path)
    try:
        v1_columns = (GeneratedColumn("thread_id", "$.thread_id"),)
        await _schema.migrate(conn, generated_columns=v1_columns, indexes=())
        await conn.execute(
            "INSERT INTO _records VALUES ('m1', 't', 0, 0, 'k', json(?))",
            ('{"thread_id": "th-1", "user_id": "u-9"}',),
        )

        v2_columns = (*v1_columns, GeneratedColumn("user_id", "$.user_id"))
        await _schema.migrate(conn, generated_columns=v2_columns, indexes=())

        cursor = await conn.execute("SELECT user_id FROM _records")
        values = [r[0] for r in await cursor.fetchall()]
        await cursor.close()
        assert values == ["u-9"]
    finally:
        await conn.close()


async def test_m06_drift_changed_json_path_fails_fast(tmp_path: Path) -> None:
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(
            conn,
            generated_columns=(GeneratedColumn("task_id", "$.task_id"),),
            indexes=(),
        )

        with pytest.raises(ConfigError) as exc_info:
            await _schema.migrate(
                conn,
                generated_columns=(GeneratedColumn("task_id", "$.meta.task_id"),),
                indexes=(),
            )

        message = str(exc_info.value)
        assert "task_id" in message
        assert "$.meta.task_id" in message  # the newly-declared path
        assert "$.task_id" in message  # the stored path
    finally:
        await conn.close()


async def test_m07_drift_changed_sql_type_fails_fast(tmp_path: Path) -> None:
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(
            conn,
            generated_columns=(GeneratedColumn("size", "$.size", "TEXT"),),
            indexes=(),
        )

        with pytest.raises(ConfigError, match="size"):
            await _schema.migrate(
                conn,
                generated_columns=(GeneratedColumn("size", "$.size", "INTEGER"),),
                indexes=(),
            )
    finally:
        await conn.close()


async def test_m08_index_drift_fails_fast(tmp_path: Path) -> None:
    """CREATE INDEX IF NOT EXISTS silently keeps a same-named index with
    different shape — drift must be detected explicitly (spec §5.1).
    """
    conn = await _fresh_conn(tmp_path)
    try:
        columns = (
            GeneratedColumn("thread_id", "$.thread_id"),
            GeneratedColumn("user_id", "$.user_id"),
        )
        await _schema.migrate(
            conn, generated_columns=columns, indexes=(Index("ix_x", ["thread_id"]),)
        )

        with pytest.raises(ConfigError, match="ix_x"):  # changed columns
            await _schema.migrate(
                conn, generated_columns=columns, indexes=(Index("ix_x", ["user_id"]),)
            )

        with pytest.raises(ConfigError, match="ix_x"):  # changed unique
            await _schema.migrate(
                conn,
                generated_columns=columns,
                indexes=(Index("ix_x", ["thread_id"], unique=True),),
            )

        # The unchanged shape still migrates cleanly.
        await _schema.migrate(
            conn, generated_columns=columns, indexes=(Index("ix_x", ["thread_id"]),)
        )
    finally:
        await conn.close()


async def test_m09_removal_not_automatic_but_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Removing a column/index from config leaves the orphan in place and
    logs it (spec §5.1). Assert logger + event + fields, never substrings
    (plan §3 logging convention).
    """
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(
            conn,
            generated_columns=(GeneratedColumn("gone_col", "$.gone"),),
            indexes=(Index("ix_gone", ["gone_col"]),),
        )

        with caplog.at_level(logging.INFO, logger="ksqlite.schema"):
            await _schema.migrate(conn, generated_columns=(), indexes=())

        events = {
            (r.name, getattr(r, "event", None), getattr(r, "orphan", None))
            for r in caplog.records
        }
        assert ("ksqlite.schema", "orphaned_column", "gone_col") in events
        assert ("ksqlite.schema", "orphaned_index", "ix_gone") in events

        cursor = await conn.execute("PRAGMA table_xinfo(_records)")
        column_names = {r[1] for r in await cursor.fetchall()}
        await cursor.close()
        assert "gone_col" in column_names  # still present

        cursor = await conn.execute("PRAGMA index_list(_records)")
        index_names = {r[1] for r in await cursor.fetchall()}
        await cursor.close()
        assert "ix_gone" in index_names  # still present
    finally:
        await conn.close()


async def test_m10_migration_ddl_failure_raises(tmp_path: Path) -> None:
    """Hook-free failure injection (plan M-10): tables and indexes share a
    namespace, so a pre-created table named like the index makes the
    CREATE INDEX genuinely fail.
    """
    conn = await _fresh_conn(tmp_path)
    try:
        await conn.execute("CREATE TABLE ix_collide (x)")

        with pytest.raises(SchemaMigrationError):
            await _schema.migrate(
                conn,
                generated_columns=(GeneratedColumn("thread_id", "$.thread_id"),),
                indexes=(Index("ix_collide", ["thread_id"]),),
            )
    finally:
        await conn.close()


def test_m11_version_fail_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """The >= 3.38 check is the ENTIRE in-code version story (spec §12)."""
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 37, 0))
    with pytest.raises(SQLiteVersionError):
        _schema.check_sqlite_version()

    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 38, 0))
    _schema.check_sqlite_version()  # exactly the floor passes


async def test_m12_view_exposes_later_added_generated_columns(
    tmp_path: Path,
) -> None:
    """The `records` view is created before generated columns exist; SQLite
    re-expands ``SELECT r.*`` at statement-prepare time, so later-added
    columns surface through it (spec §5.1, load-bearing, empirically pinned).
    """
    conn = await _fresh_conn(tmp_path)
    try:
        await _schema.migrate(conn, generated_columns=(), indexes=())
        await conn.execute(
            "INSERT INTO _records VALUES ('m1', 't', 0, 0, 'k', json(?))",
            ('{"user_id": "u-1"}',),
        )

        # A later boot declares the column; the view predates it.
        await _schema.migrate(
            conn,
            generated_columns=(GeneratedColumn("user_id", "$.user_id"),),
            indexes=(),
        )
        await conn.execute("INSERT INTO _owned VALUES ('t', 0)")

        cursor = await conn.execute("SELECT user_id FROM records")
        values = [r[0] for r in await cursor.fetchall()]
        await cursor.close()
        assert values == ["u-1"]
    finally:
        await conn.close()
