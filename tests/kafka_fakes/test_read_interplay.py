"""P7 read/write interplay: R-07, R-11 (plan §5 P7). These need the Appender
(fakes tier): both pin that a query() — failed or cancelled — leaves the
single pooled connection writable (spec §9's two-layer contract: finally-
reset + checkout hygiene).
"""

import asyncio
from pathlib import Path

import pytest
from aiokafka import TopicPartition

from ksqlite import _pool, _read, _schema, _state, _write
from ksqlite.errors import StorageError
from tests.support.fakes import FakeProducer, InMemoryKafka, TwoSidedGate
from tests.support.recording_pool import RecordingPool

SOURCE = TopicPartition("t", 0)


class StubResolver:
    def changelog_name(self, source: TopicPartition) -> str:
        return f"{source.topic}.p{source.partition}.changelog"


async def _single_conn_pool(tmp_path: Path) -> _pool.AiosqlitePoolAdapter:
    pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "single.db", pool_size=1, busy_timeout=0.5
    )
    async with pool.connection() as conn:
        await _schema.migrate(conn, generated_columns=(), indexes=())
    return pool


def _appender_on(pool: object) -> _write.Appender:
    return _write.Appender(
        pool=pool,  # type: ignore[arg-type]
        producer=FakeProducer(InMemoryKafka()),
        name_resolver=StubResolver(),
        state=_state.PartitionStateMachine(),
        txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
    )


async def test_r07_query_only_resets_for_append(tmp_path: Path) -> None:
    pool = await _single_conn_pool(tmp_path)
    try:
        runner = _read.QueryRunner(pool=pool)
        for _ in range(2):
            with pytest.raises(StorageError):
                await runner.query("DELETE FROM _records")

        # pool_size=1: the very same connection must be writable again.
        await _appender_on(pool).append(source=SOURCE, entity_key="k", payload={})

        rows = await runner.query("SELECT COUNT(*) AS n FROM _records")
        assert rows[0]["n"] == 1
    finally:
        await pool.close()


async def test_r11_cancellation_mid_read_leaves_connection_clean(
    tmp_path: Path,
) -> None:
    """I6 under cancellation: a query parked mid-read then cancelled must
    not leave the connection read-only or in a transaction (spec §9)."""
    inner = await _single_conn_pool(tmp_path)
    recording = RecordingPool(inner)
    try:
        runner = _read.QueryRunner(pool=recording)
        gate = TwoSidedGate()
        recording.gate_on(lambda sql: sql.lstrip().upper().startswith("SELECT"), gate)

        task = asyncio.ensure_future(runner.query("SELECT * FROM records"))
        await asyncio.wait_for(gate.arrived.wait(), 5)  # parked mid-read
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The same (only) connection accepts a write transaction.
        await _appender_on(recording).append(source=SOURCE, entity_key="k", payload={})
        rows = await runner.query("SELECT COUNT(*) AS n FROM _records")
        assert rows[0]["n"] == 1
    finally:
        await recording.close()
