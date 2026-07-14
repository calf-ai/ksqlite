"""P6 `_write` tests: W-01..W-15 (plan §5 P6). Fakes + real SQLite.

(W-06 acks=0 validation lives at P9 F-01; W-12b self-heal lands with the P8
RH suite — both per plan.)
"""

import logging

import pytest
from aiokafka import TopicPartition

from ksqlite import _pool, _state, _write
from tests.support.fakes import FakeProducer, InMemoryKafka

SOURCE = TopicPartition("messages", 3)


class StubResolver:
    """The two-line ChangelogNameResolver stub the plan promises (§3)."""

    def changelog_name(self, source: TopicPartition) -> str:
        return f"{source.topic}.p{source.partition}.changelog"


def make_appender(
    db: _pool.AiosqlitePoolAdapter,
    kafka: InMemoryKafka,
    producer: FakeProducer,
) -> tuple[_write.Appender, _state.PartitionStateMachine]:
    state = _state.PartitionStateMachine()
    appender = _write.Appender(
        pool=db,
        producer=producer,
        name_resolver=StubResolver(),
        state=state,
        txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
    )
    return appender, state


async def test_w01_happy_path(db: _pool.AiosqlitePoolAdapter) -> None:
    kafka = InMemoryKafka()
    producer = FakeProducer(kafka)
    appender, state = make_appender(db, kafka, producer)

    await appender.append(source=SOURCE, entity_key="task-1", payload={"a": 1})

    # Produced to the resolved changelog, explicit partition 0.
    stored = kafka.records("messages.p3.changelog", 0)
    assert len(stored) == 1
    assert stored[0].key == b"task-1"

    async with db.connection() as conn:
        cursor = await conn.execute(
            "SELECT source_topic, source_partition, changelog_offset,"
            " entity_key, payload FROM _records"
        )
        rows = [tuple(r) for r in await cursor.fetchall()]
        await cursor.close()
        # payload stored AS JSON TEXT through json(?) => minified.
        assert rows == [("messages", 3, 0, "task-1", '{"a":1}')]

        cursor = await conn.execute(
            "SELECT source_topic, source_partition, changelog_offset"
            " FROM partition_checkpoint"
        )
        checkpoints = [tuple(r) for r in await cursor.fetchall()]
        await cursor.close()
        assert checkpoints == [("messages", 3, 0)]

    # The in-memory machine was pushed post-commit (spec §4 write-back).
    assert state.partition_states()[SOURCE].checkpoint_offset == 0


async def test_w02_entity_key_validation(db: _pool.AiosqlitePoolAdapter) -> None:
    """An invalid key would land in a NOT NULL column and on the record key,
    poisoning every future rehydrate of the partition (spec §6) — rejected
    with ValueError and NOTHING produced.
    """
    kafka = InMemoryKafka()
    appender, _ = make_appender(db, kafka, FakeProducer(kafka))

    bad_keys: tuple[object, ...] = (None, "", 42, b"bytes", "\ud800")
    for bad_key in bad_keys:
        with pytest.raises(ValueError):
            await appender.append(
                source=SOURCE,
                entity_key=bad_key,  # type: ignore[arg-type]
                payload={},
            )

    assert kafka.records("messages.p3.changelog", 0) == []  # nothing produced


async def test_w03_payload_validated_before_produce(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    kafka = InMemoryKafka()
    appender, _ = make_appender(db, kafka, FakeProducer(kafka))

    with pytest.raises(ValueError):
        await appender.append(source=SOURCE, entity_key="k", payload=object())
    with pytest.raises(ValueError):
        await appender.append(source=SOURCE, entity_key="k", payload="{not json")

    # Pre-produce (spec §6): the fake log is unchanged.
    assert kafka.records("messages.p3.changelog", 0) == []


async def test_w04_produce_failure(db: _pool.AiosqlitePoolAdapter) -> None:
    from aiokafka.errors import KafkaError

    from ksqlite.errors import ChangelogProduceError

    kafka = InMemoryKafka()
    appender, state = make_appender(
        db, kafka, FakeProducer(kafka, fail_with=KafkaError("broker down"))
    )

    with pytest.raises(ChangelogProduceError):
        await appender.append(source=SOURCE, entity_key="k", payload={})

    async with db.connection() as conn:
        for table in ("_records", "partition_checkpoint"):
            cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")
            row = await cursor.fetchone()
            await cursor.close()
            assert row is not None and row[0] == 0  # no SQLite write
    assert state.partition_states().get(SOURCE) is None  # checkpoint unchanged


async def test_w05_offset_guard(db: _pool.AiosqlitePoolAdapter) -> None:
    """acks=0 is rejected at start(), so the guard defends against the
    idempotent-producer retried-batch edge reporting offset == -1 (spec §6).
    """
    from ksqlite.errors import ChangelogProduceError

    kafka = InMemoryKafka()
    appender, _ = make_appender(db, kafka, FakeProducer(kafka, force_offset=-1))

    with pytest.raises(ChangelogProduceError):
        await appender.append(source=SOURCE, entity_key="k", payload={})

    async with db.connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM _records")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and row[0] == 0  # no SQLite write


async def _checkpoint_rows(
    db: _pool.AiosqlitePoolAdapter,
) -> list[tuple[str, int, int]]:
    async with db.connection() as conn:
        cursor = await conn.execute(
            "SELECT source_topic, source_partition, changelog_offset"
            " FROM partition_checkpoint ORDER BY source_partition"
        )
        rows = [(r[0], r[1], r[2]) for r in await cursor.fetchall()]
        await cursor.close()
    return rows


async def test_w07_checkpoint_max_monotonic(db: _pool.AiosqlitePoolAdapter) -> None:
    kafka = InMemoryKafka()
    state = _state.PartitionStateMachine()

    def appender_with(producer: FakeProducer) -> _write.Appender:
        return _write.Appender(
            pool=db,
            producer=producer,
            name_resolver=StubResolver(),
            state=state,
            txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
        )

    await appender_with(FakeProducer(kafka, force_offset=5)).append(
        source=SOURCE, entity_key="k", payload={}
    )
    await appender_with(FakeProducer(kafka, force_offset=3)).append(
        source=SOURCE, entity_key="k", payload={}
    )

    assert await _checkpoint_rows(db) == [("messages", 3, 5)]  # stays 5
    assert state.partition_states()[SOURCE].checkpoint_offset == 5


async def test_w08_checkpoint_per_partition(db: _pool.AiosqlitePoolAdapter) -> None:
    kafka = InMemoryKafka()
    p0 = TopicPartition("t", 0)
    p1 = TopicPartition("t", 1)
    appender, state = make_appender(db, kafka, FakeProducer(kafka))

    await appender.append(source=p0, entity_key="a", payload={})
    await appender.append(source=p1, entity_key="b", payload={})
    await appender.append(source=p0, entity_key="c", payload={})

    assert await _checkpoint_rows(db) == [("t", 0, 1), ("t", 1, 0)]
    assert state.partition_states()[p0].checkpoint_offset == 1
    assert state.partition_states()[p1].checkpoint_offset == 0


async def test_w09_duplicate_message_id_same_offset(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """The shared materialization DML applied twice with one message_id
    (a rehydrate re-read) => one row, checkpoint advances (I2/I3).
    """
    from ksqlite import _schema

    runner = _pool.TransactionRunner(busy_timeout=0.5)

    async def apply_once(offset: int) -> None:
        async with db.connection() as conn:

            async def work(c: object) -> None:
                await _schema.apply_materialization(
                    conn,
                    message_id="0189-fixed-id",
                    source_topic="t",
                    source_partition=0,
                    changelog_offset=offset,
                    entity_key="k",
                    payload="{}",
                )

            await runner.run(conn, work)

    await apply_once(4)
    await apply_once(4)  # re-read of the same record

    async with db.connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM _records")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and row[0] == 1  # deduped

    assert await _checkpoint_rows(db) == [("t", 0, 4)]


async def test_w10_record_insert_and_checkpoint_upsert_are_atomic(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I7: both or neither (spec §5.2/§6 — one BEGIN IMMEDIATE txn)."""
    from ksqlite.errors import StorageError
    from tests.support.recording_pool import RecordingPool

    recording = RecordingPool(db)
    kafka = InMemoryKafka()
    state = _state.PartitionStateMachine()
    appender = _write.Appender(
        pool=recording,
        producer=FakeProducer(kafka),
        name_resolver=StubResolver(),
        state=state,
        txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
    )

    recording.fail_on(lambda sql: "partition_checkpoint" in sql)
    with pytest.raises(StorageError):
        await appender.append(source=SOURCE, entity_key="k", payload={})

    async with db.connection() as conn:
        for table in ("_records", "partition_checkpoint"):
            cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")
            row = await cursor.fetchone()
            await cursor.close()
            assert row is not None and row[0] == 0, table  # NEITHER persisted

    assert state.partition_states().get(SOURCE) is None  # no write-back either


async def test_w11_non_ready_append_warns_but_proceeds(
    db: _pool.AiosqlitePoolAdapter, caplog: pytest.LogCaptureFixture
) -> None:
    """A host-contract violation (spec §6): warn, never raise — the write
    still converges (new offsets extend the log; message_id dedup keeps an
    in-flight replay idempotent).
    """
    kafka = InMemoryKafka()
    appender, state = make_appender(db, kafka, FakeProducer(kafka))

    with caplog.at_level(logging.WARNING, logger="ksqlite.write"):
        await appender.append(source=SOURCE, entity_key="k", payload={})

    events = [
        (r.name, getattr(r, "event", None), getattr(r, "tp", None))
        for r in caplog.records
    ]
    assert ("ksqlite.write", "append_to_non_ready", SOURCE) in events
    assert len(kafka.records("messages.p3.changelog", 0)) == 1  # proceeded

    # A READY partition appends without the warning.
    caplog.clear()
    state.mark_ready(SOURCE)
    with caplog.at_level(logging.WARNING, logger="ksqlite.write"):
        await appender.append(source=SOURCE, entity_key="k", payload={})
    assert not [
        r for r in caplog.records if getattr(r, "event", None) == "append_to_non_ready"
    ]


async def test_w12_produce_ok_apply_failed(db: _pool.AiosqlitePoolAdapter) -> None:
    """Spec §14 self-heal precondition: the record is durably in the
    changelog, the checkpoint did NOT advance — the next rehydrate
    materializes it (W-12b exercises that at P8). Do not retry the append.
    """
    from ksqlite.errors import StorageError
    from tests.support.recording_pool import RecordingPool

    recording = RecordingPool(db)
    kafka = InMemoryKafka()
    state = _state.PartitionStateMachine()
    appender = _write.Appender(
        pool=recording,
        producer=FakeProducer(kafka),
        name_resolver=StubResolver(),
        state=state,
        txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
    )

    recording.fail_on(lambda sql: "INSERT INTO _records" in sql)
    with pytest.raises(StorageError):
        await appender.append(source=SOURCE, entity_key="k", payload={"v": 1})

    assert len(kafka.records("messages.p3.changelog", 0)) == 1  # acked + durable
    assert await _checkpoint_rows(db) == []  # checkpoint unchanged
    assert state.partition_states().get(SOURCE) is None


async def test_w13_concurrent_appends(db: _pool.AiosqlitePoolAdapter) -> None:
    """Labeled sanity (plan W-13): the locking weight is carried by
    C-04..C-06."""
    import asyncio

    kafka = InMemoryKafka()
    appender, state = make_appender(db, kafka, FakeProducer(kafka))
    p0 = TopicPartition("t", 0)
    p1 = TopicPartition("t", 1)

    async def worker(source: TopicPartition, n: int) -> None:
        for i in range(n):
            await appender.append(source=source, entity_key=f"k{i}", payload={"i": i})

    await asyncio.gather(worker(p0, 100), worker(p1, 100))

    async with db.connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM _records")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None and row[0] == 200

    assert await _checkpoint_rows(db) == [("t", 0, 99), ("t", 1, 99)]
    assert state.partition_states()[p0].checkpoint_offset == 99
    assert state.partition_states()[p1].checkpoint_offset == 99


async def _replay_log_through_shared_dml(
    db: _pool.AiosqlitePoolAdapter, records: list[object]
) -> None:
    """The eventual rehydrate, reduced to its materialization: classify each
    log record and apply the SAME shared DML the write path used.
    """
    from ksqlite import _schema, _wire

    runner = _pool.TransactionRunner(busy_timeout=0.5)
    async with db.connection() as conn:

        async def work(c: object) -> None:
            for r in records:
                decoded = _wire.classify(
                    key=r.key,  # type: ignore[attr-defined]
                    value=r.value,  # type: ignore[attr-defined]
                    headers=list(r.headers),  # type: ignore[attr-defined]
                )
                assert isinstance(decoded, _wire.KsqliteRecord)
                await _schema.apply_materialization(
                    conn,
                    message_id=decoded.message_id,
                    source_topic="messages",
                    source_partition=3,
                    changelog_offset=r.offset,  # type: ignore[attr-defined]
                    entity_key=decoded.entity_key,
                    payload=decoded.payload,
                )

        await runner.run(conn, work)


async def _records_count(db: _pool.AiosqlitePoolAdapter) -> int:
    async with db.connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM _records")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None
        return int(row[0])


async def test_w14_host_retry_duplicate_is_the_documented_loss(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Spec §14: retrying append() after ChangelogProduceError mints a NEW
    message_id — when the first produce was a phantom, replay materializes
    the record TWICE. Pins the documented behavior; hosts must not retry.
    """
    from aiokafka.errors import KafkaError

    from ksqlite.errors import ChangelogProduceError

    kafka = InMemoryKafka()
    state = _state.PartitionStateMachine()

    def appender_with(producer: FakeProducer) -> _write.Appender:
        return _write.Appender(
            pool=db,
            producer=producer,
            name_resolver=StubResolver(),
            state=state,
            txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
        )

    with pytest.raises(ChangelogProduceError):
        await appender_with(
            FakeProducer(kafka, phantom_failure=KafkaError("ack lost"))
        ).append(source=SOURCE, entity_key="k", payload={"v": 1})

    # The host (wrongly) retries: a NEW message_id for the same logical write.
    await appender_with(FakeProducer(kafka)).append(
        source=SOURCE, entity_key="k", payload={"v": 1}
    )

    log = kafka.records("messages.p3.changelog", 0)
    assert len(log) == 2
    message_ids = [dict(r.headers)["message_id"] for r in log]
    assert message_ids[0] != message_ids[1]

    await _replay_log_through_shared_dml(db, list(log))
    assert await _records_count(db) == 2  # the genuine, documented duplicate


async def test_w15_producer_retry_duplicate_dedups(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I2: the SAME message_id at two offsets (producer retry; metadata =
    the LAST offset) => one row, checkpoint = the last offset. The kill
    transfers to replay because both paths share the §3 DML; RH-24 re-kills
    at the lifecycle level.
    """
    kafka = InMemoryKafka()
    appender, state = make_appender(
        db, kafka, FakeProducer(kafka, duplicate_on_retry=True)
    )

    await appender.append(source=SOURCE, entity_key="k", payload={"v": 1})

    log = kafka.records("messages.p3.changelog", 0)
    assert len(log) == 2
    assert dict(log[0].headers) == dict(log[1].headers)  # same message_id

    assert await _records_count(db) == 1
    assert await _checkpoint_rows(db) == [("messages", 3, 1)]  # the LAST offset

    # Replaying BOTH copies through the shared DML changes nothing (I2).
    await _replay_log_through_shared_dml(db, list(log))
    assert await _records_count(db) == 1
    assert await _checkpoint_rows(db) == [("messages", 3, 1)]
    assert state.partition_states()[SOURCE].checkpoint_offset == 1
