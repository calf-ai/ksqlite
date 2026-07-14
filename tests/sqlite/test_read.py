"""P7 `_read` tests: R-01..R-12 (plan §5 P7). Real tmp-file SQLite."""

from ksqlite import _pool, _read


async def _seed_record(
    pool: _pool.AiosqlitePoolAdapter,
    message_id: str,
    topic: str,
    partition: int,
    offset: int,
    entity_key: str,
    payload: str,
) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO _records VALUES (?, ?, ?, ?, ?, json(?))",
            (message_id, topic, partition, offset, entity_key, payload),
        )


async def _own(pool: _pool.AiosqlitePoolAdapter, topic: str, partition: int) -> None:
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO _owned VALUES (?, ?)", (topic, partition))


async def test_r01_scoping_matrix(db: _pool.AiosqlitePoolAdapter) -> None:
    """I1: the view returns a row only when its exact (topic, partition) has
    an `_owned` row — including across topics sharing a partition number
    (kills both dropped-JOIN-clause mutants).
    """
    await _seed_record(db, "m1", "tA", 0, 0, "a0", "{}")
    await _seed_record(db, "m2", "tA", 1, 0, "a1", "{}")
    await _seed_record(db, "m3", "tB", 0, 0, "b0", "{}")
    await _own(db, "tA", 0)

    runner = _read.QueryRunner(pool=db)
    rows = await runner.query(
        "SELECT source_topic, source_partition, entity_key FROM records"
        " ORDER BY entity_key"
    )

    assert [
        (r["source_topic"], r["source_partition"], r["entity_key"]) for r in rows
    ] == [("tA", 0, "a0")]


async def test_r02_into_pydantic_v2(db: _pool.AiosqlitePoolAdapter) -> None:
    import pydantic

    class Message(pydantic.BaseModel):
        text: str
        n: int

    await _seed_record(db, "m1", "t", 0, 0, "k", '{"text": "hi", "n": 1}')
    await _seed_record(db, "m2", "t", 0, 1, "k", '{"text": "yo", "n": 2}')
    await _own(db, "t", 0)

    runner = _read.QueryRunner(pool=db)
    models = await runner.query(
        "SELECT payload FROM records ORDER BY changelog_offset", into=Message
    )

    assert models == [Message(text="hi", n=1), Message(text="yo", n=2)]


async def test_r03_into_dataclass_and_typeerror_otherwise(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Targets are EXACTLY Pydantic-v2 models or dataclasses; a plain class
    whose kwargs would fit is still rejected — no generic-kwargs fallback
    (pinned, spec §9).
    """
    import dataclasses

    import pytest

    @dataclasses.dataclass
    class Msg:
        text: str
        n: int

    class PlainWithMatchingKwargs:
        def __init__(self, text: str, n: int) -> None:
            self.text = text
            self.n = n

    await _seed_record(db, "m1", "t", 0, 0, "k", '{"text": "hi", "n": 1}')
    await _own(db, "t", 0)
    runner = _read.QueryRunner(pool=db)

    models = await runner.query("SELECT payload FROM records", into=Msg)
    assert models == [Msg(text="hi", n=1)]

    with pytest.raises(TypeError):
        await runner.query("SELECT payload FROM records", into=PlainWithMatchingKwargs)


async def test_r04_hydration_error_without_payload_column(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    import dataclasses

    import pytest

    from ksqlite.errors import HydrationError

    @dataclasses.dataclass
    class Msg:
        text: str

    await _seed_record(db, "m1", "t", 0, 0, "k", '{"text": "hi"}')
    await _own(db, "t", 0)
    runner = _read.QueryRunner(pool=db)

    with pytest.raises(HydrationError):
        await runner.query("SELECT entity_key FROM records", into=Msg)


async def test_r05_write_via_view_raises_storage_error(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    import pytest

    from ksqlite.errors import StorageError

    runner = _read.QueryRunner(pool=db)
    with pytest.raises(StorageError):
        await runner.query("INSERT INTO records VALUES ('m','t',0,0,'k','{}')")


async def test_r06_write_via_internals_raises_storage_error(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I6: query() cannot mutate ANY DB state — the view protects only the
    name `records`; query_only protects everything (spec §9).
    """
    import pytest

    from ksqlite.errors import StorageError

    await _seed_record(db, "m1", "t", 0, 0, "k", "{}")
    await _own(db, "t", 0)
    runner = _read.QueryRunner(pool=db)

    for stmt in (
        "DELETE FROM _records",
        "UPDATE _owned SET source_partition = 9",
        "DROP VIEW records",
        "INSERT INTO _records VALUES ('mX','t',0,1,'k','{}')",
    ):
        with pytest.raises(StorageError):
            await runner.query(stmt)

    rows = await runner.query("SELECT COUNT(*) AS n FROM _records")
    assert rows[0]["n"] == 1  # nothing was mutated


async def test_r08_index_used_through_view(tmp_path: object) -> None:
    from pathlib import Path

    from ksqlite import _schema
    from ksqlite.types import GeneratedColumn, Index

    assert isinstance(tmp_path, Path)
    pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "r08.db", pool_size=2, busy_timeout=0.5
    )
    try:
        async with pool.connection() as conn:
            await _schema.migrate(
                conn,
                generated_columns=(GeneratedColumn("thread_id", "$.thread_id"),),
                indexes=(Index("ix_thread", ["thread_id"]),),
            )
        await _seed_record(pool, "m1", "t", 0, 0, "k", '{"thread_id": "th-1"}')
        await _own(pool, "t", 0)

        runner = _read.QueryRunner(pool=pool)
        plan_rows = await runner.query(
            "EXPLAIN QUERY PLAN SELECT * FROM records WHERE thread_id = ?", ("th-1",)
        )
        plan_text = " ".join(str(tuple(r)) for r in plan_rows)
        # SQLite inlines the view; index usage is preserved (spec §5.3/§9).
        assert "ix_thread" in plan_text
    finally:
        await pool.close()


async def test_r09_rows_are_sqlite3_rows(db: _pool.AiosqlitePoolAdapter) -> None:
    import sqlite3

    await _seed_record(db, "m1", "t", 0, 0, "k", '{"x": 1}')
    await _own(db, "t", 0)

    rows = await _read.QueryRunner(pool=db).query("SELECT * FROM records")
    assert isinstance(rows[0], sqlite3.Row)
    assert rows[0]["entity_key"] == "k"  # mapping access by column name


async def test_r10_direct_read_of_internals_is_allowed(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Documented footgun (spec §9): naming an `_`-prefixed internal bypasses
    ownership scoping — documents, not forbids.
    """
    await _seed_record(db, "m1", "t", 0, 0, "k", "{}")
    # NOT owned: invisible through the view, visible via the internal table.
    runner = _read.QueryRunner(pool=db)
    assert await runner.query("SELECT * FROM records") == []
    assert len(await runner.query("SELECT * FROM _records")) == 1


async def test_r12_query_resets_row_factory_on_the_pooled_connection(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """``query()`` sets sqlite3.Row for its own fetch; the pooled connection
    must come back clean — a leaked row_factory is cross-checkout state that
    every later (non-query) user silently inherits (spec §11 hygiene).
    """
    runner = _read.QueryRunner(pool=db)
    await runner.query("SELECT 1")

    async with db.connection() as conn:
        assert conn.row_factory is None
