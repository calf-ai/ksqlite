"""P2 `_sql` tests that execute DDL/SQL against real tmp-file SQLite:
S-03, S-05, S-06 (plan §5 P2). Pure-validation S-tests live in
tests/unit/test_sql.py.
"""

import sqlite3
from pathlib import Path

from ksqlite import _sql
from ksqlite.types import GeneratedColumn


def test_s03_json_path_escaping_renders_and_executes(tmp_path: Path) -> None:
    literal = _sql.escape_string_literal("$.a'b")
    assert literal == "'$.a''b'"

    conn = sqlite3.connect(tmp_path / "s03.db")
    try:
        conn.execute("CREATE TABLE t (payload TEXT NOT NULL)")
        conn.execute("INSERT INTO t VALUES (json(?))", ('{"a\'b": 42}',))
        value = conn.execute(f"SELECT payload ->> {literal} FROM t").fetchone()[0]
        assert value == 42
    finally:
        conn.close()


def test_s05_emit_add_column_exact_and_executable(tmp_path: Path) -> None:
    ddl = _sql.emit_add_column(GeneratedColumn("x", "$.x"))
    assert ddl == (
        "ALTER TABLE _records ADD COLUMN x TEXT "
        "GENERATED ALWAYS AS (payload->>'$.x') VIRTUAL"
    )

    conn = sqlite3.connect(tmp_path / "s05.db")
    try:
        conn.execute("CREATE TABLE _records (payload TEXT NOT NULL)")
        conn.execute(ddl)
        conn.execute("INSERT INTO _records VALUES (json(?))", ('{"x": "v"}',))
        assert conn.execute("SELECT x FROM _records").fetchone()[0] == "v"
    finally:
        conn.close()


def test_s06_canonical_render_round_trip_is_the_drift_mechanism(
    tmp_path: Path,
) -> None:
    col = GeneratedColumn("task_id", "$.meta.task_id", "INTEGER")

    conn = sqlite3.connect(tmp_path / "s06.db")
    try:
        conn.execute("CREATE TABLE _records (payload TEXT NOT NULL)")
        conn.execute(_sql.emit_add_column(col))
        stored = conn.execute(
            "SELECT sql FROM sqlite_schema WHERE name = '_records'"
        ).fetchone()[0]
    finally:
        conn.close()

    clause = _sql.render_column_clause(col)
    assert clause == (
        "task_id INTEGER GENERATED ALWAYS AS (payload->>'$.meta.task_id') VIRTUAL"
    )
    # The re-rendered clause appears verbatim in the stored schema text —
    # spec §5.1: drift detection compares this rendering, no SQL parsing.
    assert clause in stored
