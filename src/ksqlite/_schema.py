"""Schema migration + drift detection (plan §3 ``_schema``; spec §5.1, §12).

Also the single home of shared DML: the materialization pair, the
checkpoint clamp, and the three ``_owned`` statements.
"""

import logging
import re
import sqlite3
from collections.abc import Sequence

import aiosqlite

from . import _pool, _sql
from .errors import ConfigError, SchemaMigrationError, SQLiteVersionError
from .types import GeneratedColumn, Index

_SQLITE_VERSION_FLOOR = (3, 38)


def check_sqlite_version() -> None:
    """``start()`` fail-fast (spec §12) — the entire in-code version story.

    Generated columns use ``->>``, which needs SQLite >= 3.38; for older
    runtimes the README documents the two-line pysqlite3 entrypoint swap.
    """
    if sqlite3.sqlite_version_info < _SQLITE_VERSION_FLOOR:
        raise SQLiteVersionError(
            f"KSQLite requires SQLite >= 3.38; found {sqlite3.sqlite_version_info}"
        )


logger = logging.getLogger("ksqlite.schema")

# table_xinfo "hidden" values marking generated columns.
_HIDDEN_VIRTUAL = 2
_HIDDEN_STORED = 3

_BASE_INDEX_NAMES = frozenset({"ix_records_tp", "ix_records_key"})

_CREATE_RECORDS = """\
CREATE TABLE IF NOT EXISTS _records (
    message_id       TEXT    NOT NULL,
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    changelog_offset INTEGER NOT NULL,
    entity_key       TEXT    NOT NULL,
    payload          TEXT    NOT NULL,
    UNIQUE (message_id)
)"""

_CREATE_CHECKPOINT = """\
CREATE TABLE IF NOT EXISTS partition_checkpoint (
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    changelog_offset INTEGER NOT NULL,
    PRIMARY KEY (source_topic, source_partition)
)"""

_CREATE_OWNED = """\
CREATE TABLE IF NOT EXISTS _owned (
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    PRIMARY KEY (source_topic, source_partition)
)"""

_CREATE_VIEW = """\
CREATE VIEW IF NOT EXISTS records AS
    SELECT r.* FROM _records r
    JOIN _owned o ON r.source_topic = o.source_topic
                 AND r.source_partition = o.source_partition"""

_BASE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_records_tp ON _records(source_topic, source_partition)",
    "CREATE INDEX IF NOT EXISTS ix_records_key ON _records(entity_key)",
)

# The materialization pair (spec §6) — the SINGLE home of this DML: _write
# and _lifecycle run byte-identical statements, which is what makes replay
# and write-path dedup provably equivalent (invariants I2/I4).
INSERT_RECORD_SQL = """\
INSERT INTO _records(message_id, source_topic, source_partition,
                     changelog_offset, entity_key, payload)
  VALUES (:message_id, :source_topic, :source_partition,
          :changelog_offset, :entity_key, json(:payload))
  ON CONFLICT (message_id) DO NOTHING"""

CHECKPOINT_UPSERT_SQL = """\
INSERT INTO partition_checkpoint(source_topic, source_partition, changelog_offset)
  VALUES (:source_topic, :source_partition, :changelog_offset)
  ON CONFLICT(source_topic, source_partition)
  DO UPDATE SET changelog_offset = MAX(changelog_offset, excluded.changelog_offset)"""

# The §7 step-2 clamp — the ONLY non-monotonic checkpoint write. It
# deliberately overrides the MAX() upsert above: after a truncation reset
# the checkpoint must be able to move DOWN.
CHECKPOINT_CLAMP_SQL = (
    "INSERT OR REPLACE INTO partition_checkpoint"
    "(source_topic, source_partition, changelog_offset)"
    " VALUES (?, ?, ?)"
)


# The three _owned statements (spec §5.3) — presence-only visibility rows.
OWNED_INSERT_SQL = (
    "INSERT OR IGNORE INTO _owned(source_topic, source_partition) VALUES (?, ?)"
)
OWNED_DELETE_SQL = "DELETE FROM _owned WHERE source_topic = ? AND source_partition = ?"
OWNED_DELETE_ALL_SQL = "DELETE FROM _owned"

SELECT_CHECKPOINT_SQL = (
    "SELECT changelog_offset FROM partition_checkpoint"
    " WHERE source_topic = ? AND source_partition = ?"
)


async def apply_materialization(
    conn: aiosqlite.Connection,
    *,
    message_id: str,
    source_topic: str,
    source_partition: int,
    changelog_offset: int,
    entity_key: str,
    payload: str,
) -> None:
    """Run the materialization pair inside the caller's open transaction."""
    params = {
        "message_id": message_id,
        "source_topic": source_topic,
        "source_partition": source_partition,
        "changelog_offset": changelog_offset,
        "entity_key": entity_key,
        "payload": payload,
    }
    await _pool.execute(conn, INSERT_RECORD_SQL, params)
    # sqlite3 ignores named-parameter keys a statement does not reference
    # (verified empirically), so the same dict feeds both statements.
    await _pool.execute(conn, CHECKPOINT_UPSERT_SQL, params)


async def migrate(
    conn: aiosqlite.Connection,
    generated_columns: Sequence[GeneratedColumn],
    indexes: Sequence[Index],
) -> None:
    """Idempotent schema migration at ``start()`` (spec §5.1)."""
    for ddl in (_CREATE_RECORDS, _CREATE_CHECKPOINT, _CREATE_OWNED, *_BASE_INDEXES):
        await _pool.execute(conn, ddl)
    # The view is created BEFORE generated columns exist — safe and
    # load-bearing: SQLite re-expands ``SELECT r.*`` at statement-prepare
    # time, so later-added columns surface through it (spec §5.1).
    await _pool.execute(conn, _CREATE_VIEW)

    # Ownership is per-process, re-established by rebalances (spec §5.1):
    # a prior run's stale rows must never serve un-owned partitions at boot.
    await _pool.execute(conn, OWNED_DELETE_ALL_SQL)

    # Generated-column reconciliation: diff via table_xinfo (generated
    # columns are INVISIBLE to table_info — using it re-adds them on every
    # boot and crashes with 'duplicate column name'; spec §5.1).
    xinfo_rows = await _pool.fetch_all(conn, "PRAGMA table_xinfo(_records)")
    existing_columns = {row[1] for row in xinfo_rows}
    existing_generated = {
        row[1] for row in xinfo_rows if row[6] in (_HIDDEN_VIRTUAL, _HIDDEN_STORED)
    }

    declared_names = {c.name for c in generated_columns}
    for orphan in sorted(existing_generated - declared_names):
        logger.info(
            "generated column %r removed from config; left in place",
            orphan,
            extra={"event": "orphaned_column", "orphan": orphan},
        )

    row = await _pool.fetch_one(
        conn, "SELECT sql FROM sqlite_schema WHERE name = '_records' AND type = 'table'"
    )
    assert row is not None  # _records was just created above
    stored_schema: str = row[0]

    for column in generated_columns:
        if column.name in existing_columns:
            _check_column_drift(column, stored_schema)
        else:
            await _execute_migration_ddl(conn, _sql.emit_add_column(column))

    index_rows = await _pool.fetch_all(conn, "PRAGMA index_list(_records)")
    existing_index_names = {
        r[1] for r in index_rows if not r[1].startswith("sqlite_autoindex")
    }
    declared_index_names = {i.name for i in indexes}
    for orphan in sorted(
        existing_index_names - declared_index_names - _BASE_INDEX_NAMES
    ):
        logger.info(
            "index %r removed from config; left in place",
            orphan,
            extra={"event": "orphaned_index", "orphan": orphan},
        )

    for index in indexes:
        existing_shape = await _existing_index_shape(conn, index.name)
        if existing_shape is None:
            await _execute_migration_ddl(conn, _sql.emit_create_index(index))
        elif existing_shape != (list(index.columns), index.unique):
            stored_columns, stored_unique = existing_shape
            raise ConfigError(
                f"index {index.name!r} drifted from the stored schema: declared "
                f"(columns={list(index.columns)}, unique={index.unique}), stored "
                f"(columns={stored_columns}, unique={stored_unique})"
            )


async def _execute_migration_ddl(conn: aiosqlite.Connection, ddl: str) -> None:
    """Execute an ``ALTER TABLE``/``CREATE INDEX`` migration statement; an
    execution failure raises ``SchemaMigrationError`` (spec §5.1).
    """
    try:
        await _pool.execute(conn, ddl)
    except sqlite3.Error as exc:
        raise SchemaMigrationError(f"migration DDL failed: {ddl!r}") from exc
    logger.info(
        "executed migration DDL",
        extra={"event": "migration_ddl", "ddl": ddl},
    )


async def _existing_index_shape(
    conn: aiosqlite.Connection, name: str
) -> tuple[list[str], bool] | None:
    """Return ``(key_columns, unique)`` for an existing ``_records`` index,
    or ``None`` when absent (spec §5.1: index drift via ``index_xinfo``).
    """
    index_rows = await _pool.fetch_all(conn, "PRAGMA index_list(_records)")
    entry = next((r for r in index_rows if r[1] == name), None)
    if entry is None:
        return None
    unique = bool(entry[2])

    xinfo = await _pool.fetch_all(conn, f"PRAGMA index_xinfo({name})")
    # Rows: (seqno, cid, name, desc, coll, key); key columns only, in order.
    key_columns = [r[2] for r in sorted(xinfo, key=lambda r: r[0]) if r[5] == 1]
    return key_columns, unique


def _check_column_drift(column: GeneratedColumn, stored_schema: str) -> None:
    """Drift = the canonically re-rendered clause is absent from the stored
    schema text (spec §5.1: no free-form SQL parsing for detection; the
    narrow regex below only decorates the error message).
    """
    clause = _sql.render_column_clause(column)
    if clause in stored_schema:
        return
    stored_match = re.search(
        rf"\b{re.escape(column.name)}\s+\w+\s+GENERATED ALWAYS AS"
        r"\s*\((.*?)\)\s*VIRTUAL",
        stored_schema,
    )
    stored_expr = stored_match.group(1) if stored_match else "<unrecognized>"
    raise ConfigError(
        f"generated column {column.name!r} drifted from the stored schema: "
        f"declared clause {clause!r}, stored extraction ({stored_expr})"
    )
