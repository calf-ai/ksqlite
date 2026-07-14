"""P8 `_lifecycle` tests: RH-01..RH-28 + T-07 + W-12b (plan §5 P8).
Fakes + real SQLite.
"""

import json
import uuid

from aiokafka import TopicPartition

from ksqlite import _lifecycle, _pool, _state, _topics, _wire
from tests.support.fakes import (
    FakeAdmin,
    FakeConsumer,
    FakeProducer,
    InMemoryKafka,
    MutableClock,
)
from tests.support.recording_pool import RecordingPool

SOURCE = TopicPartition("messages", 3)
CHANGELOG = "messages.p3.changelog"
TEMPLATE = "{source_topic}.p{partition}.changelog"


async def seed_changelog(
    kafka: InMemoryKafka, topic: str, n: int, *, start_index: int = 0
) -> None:
    """Produce n well-formed KSQLite records to a (registered) changelog."""
    if not kafka.topic_exists(topic):
        kafka.create_topic(
            topic, num_partitions=1, configs={"cleanup.policy": "delete"}
        )
    producer = FakeProducer(kafka)
    for i in range(start_index, start_index + n):
        encoded = _wire.encode(
            entity_key=f"key-{i}",
            payload={"i": i},
            # Deterministic AND unique across topics (message_id is the
            # global dedup key — cross-topic collisions would dedup rows).
            message_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{topic}:{i}")),
        )
        await producer.send_and_wait(
            topic,
            value=encoded.value,
            key=encoded.key,
            partition=0,
            headers=encoded.headers,
        )


class LifecycleHarness:
    def __init__(
        self,
        db: _pool.AiosqlitePoolAdapter,
        kafka: InMemoryKafka,
        *,
        rehydrate_timeout: float = 30.0,
        batch_size: int = 500,
        clock: object = None,
        verify: bool = True,
        retention_ms: int | None = None,
        pool: object = None,
        event_log: list[str] | None = None,
        on_poll: object = None,
        configure_consumer: object = None,
    ) -> None:
        self.kafka = kafka
        self.state = _state.PartitionStateMachine()
        self.admin = FakeAdmin(kafka)
        self.topics = _topics.ChangelogTopics(
            admin=self.admin,
            template=TEMPLATE,
            create_topics_retention_ms=retention_ms,
            verify_changelog_policy=verify,
        )
        self.consumers: list[FakeConsumer] = []

        def consumer_factory() -> FakeConsumer:
            consumer = FakeConsumer(
                kafka,
                group_id=None,
                enable_auto_commit=False,
                auto_offset_reset="none",
            )
            if event_log is not None:
                consumer.event_log = event_log
            if on_poll is not None:
                consumer.on_poll = on_poll
            if callable(configure_consumer):
                configure_consumer(consumer)
            self.consumers.append(consumer)
            return consumer

        kwargs: dict[str, object] = {}
        if clock is not None:
            kwargs["clock"] = clock
        self.lifecycle = _lifecycle.PartitionLifecycle(
            pool=pool if pool is not None else db,  # type: ignore[arg-type]
            consumer_factory=consumer_factory,
            topics=self.topics,
            state=self.state,
            txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
            rehydrate_timeout=rehydrate_timeout,
            batch_size=batch_size,
            **kwargs,  # type: ignore[arg-type]
        )


async def all_records(db: _pool.AiosqlitePoolAdapter) -> list[tuple[object, ...]]:
    async with db.connection() as conn:
        cursor = await conn.execute(
            "SELECT source_topic, source_partition, changelog_offset, entity_key,"
            " payload FROM _records ORDER BY changelog_offset"
        )
        rows = [tuple(r) for r in await cursor.fetchall()]
        await cursor.close()
    return rows


async def owned_rows(db: _pool.AiosqlitePoolAdapter) -> set[tuple[str, int]]:
    async with db.connection() as conn:
        cursor = await conn.execute("SELECT source_topic, source_partition FROM _owned")
        rows = {(r[0], r[1]) for r in await cursor.fetchall()}
        await cursor.close()
    return rows


async def checkpoint_of(
    db: _pool.AiosqlitePoolAdapter, source: TopicPartition
) -> int | None:
    async with db.connection() as conn:
        cursor = await conn.execute(
            "SELECT changelog_offset FROM partition_checkpoint"
            " WHERE source_topic = ? AND source_partition = ?",
            (source.topic, source.partition),
        )
        row = await cursor.fetchone()
        await cursor.close()
    return None if row is None else int(row[0])


async def test_rh01_full_replay(db: _pool.AiosqlitePoolAdapter) -> None:
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 5)
    harness = LifecycleHarness(db, kafka)

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    rows = await all_records(db)
    assert len(rows) == 5
    assert [r[2] for r in rows] == [0, 1, 2, 3, 4]  # changelog offsets
    assert rows[0][:2] == ("messages", 3)  # provenance from assignment context
    assert rows[0][3] == "key-0"  # entity_key reconstructed from the KEY
    assert json.loads(str(rows[0][4])) == {"i": 0}

    assert await checkpoint_of(db, SOURCE) == 4  # N-1
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY
    assert await owned_rows(db) == {("messages", 3)}  # revealed


async def test_rh02_empty_changelog(db: _pool.AiosqlitePoolAdapter) -> None:
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    harness = LifecycleHarness(db, kafka)

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY
    assert await owned_rows(db) == {("messages", 3)}
    assert await checkpoint_of(db, SOURCE) is None  # row ABSENT (pinned)


async def test_rh03_incremental_replay_fetches_only_the_tail(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 6)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert await checkpoint_of(db, SOURCE) == 5

    await seed_changelog(kafka, CHANGELOG, 3, start_index=6)
    consumer = harness.consumers[0]
    consumer.served_offsets.clear()

    await harness.lifecycle.on_partitions_assigned([SOURCE])  # re-assign

    assert consumer.served_offsets == [6, 7, 8]  # ONLY the tail was fetched
    assert await checkpoint_of(db, SOURCE) == 8
    assert len(await all_records(db)) == 9


async def test_rh04_fast_path_zero_fetches(db: _pool.AiosqlitePoolAdapter) -> None:
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 4)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])
    await harness.lifecycle.on_partitions_revoked([SOURCE])

    consumer = harness.consumers[0]
    calls_before = consumer.getmany_calls

    # Warm standby re-assignment: checkpoint+1 == LEO -> reveal directly.
    await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert consumer.getmany_calls == calls_before  # ZERO fetches (pinned)
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY
    assert await owned_rows(db) == {("messages", 3)}


async def test_rh05_foreign_records_skipped_logged_and_checkpointed(
    db: _pool.AiosqlitePoolAdapter, caplog: object
) -> None:
    """A foreign record can't be KSQLite data — skip + log; its offset STILL
    advances the checkpoint, so a foreign tail cannot permanently defeat the
    fast path (spec §7).
    """
    import logging

    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 1)  # good at offset 0
    producer = FakeProducer(kafka)
    await producer.send_and_wait(  # headerless foreign at offset 1
        CHANGELOG, value=b"not-ksqlite", key=b"x", partition=0
    )
    await seed_changelog(kafka, CHANGELOG, 1, start_index=2)  # good at 2
    await producer.send_and_wait(  # trailing null-key foreign at offset 3
        CHANGELOG, value=b"tail", key=None, partition=0
    )

    harness = LifecycleHarness(db, kafka)
    with caplog.at_level(logging.INFO, logger="ksqlite.lifecycle"):
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    rows = await all_records(db)
    assert [r[2] for r in rows] == [0, 2]  # only the KSQLite records
    assert await checkpoint_of(db, SOURCE) == 3  # foreign offsets included

    skips = [
        (getattr(r, "tp", None), getattr(r, "offset", None))
        for r in caplog.records
        if getattr(r, "event", None) == "foreign_record_skipped"
    ]
    assert (SOURCE, 1) in skips
    assert (SOURCE, 3) in skips

    # The trailing foreign did not defeat the fast path.
    consumer = harness.consumers[0]
    await harness.lifecycle.on_partitions_revoked([SOURCE])
    calls_before = consumer.getmany_calls
    await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert consumer.getmany_calls == calls_before


async def test_rh06_unknown_or_missing_version_fails_loud(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """BOTH shapes (parametrized in-body): an unrecognized format_version AND
    a missing one on a record carrying a valid message_id. An inline
    classifier that skips missing-version as foreign must fail this
    (spec §7; the "or" must be an "and").
    """
    import pytest

    from ksqlite.errors import RehydrateError

    mid = "01890a5d-ac96-774b-bcce-b30209999999".encode()
    shapes: list[list[tuple[str, bytes]]] = [
        [("format_version", b"2"), ("message_id", mid)],  # unrecognized
        [("message_id", mid)],  # missing, valid message_id
    ]
    for headers in shapes:
        kafka = InMemoryKafka()
        await seed_changelog(kafka, CHANGELOG, 1)  # a good record first
        producer = FakeProducer(kafka)
        await producer.send_and_wait(
            CHANGELOG, value=b"{}", key=b"k", partition=0, headers=headers
        )
        harness = LifecycleHarness(db, kafka)

        with pytest.raises(RehydrateError):
            await harness.lifecycle.on_partitions_assigned([SOURCE])

        assert await owned_rows(db) == set()  # NOT revealed


async def test_rh09_truncated_log_reset(
    db: _pool.AiosqlitePoolAdapter, caplog: object
) -> None:
    """OOR (raised from the fetch under "none") => WARNING + reset to
    earliest + best-effort replay of the retained log; NO raise (spec §7).
    """
    import logging

    import pytest

    from ksqlite.types import PartitionStatus

    assert isinstance(caplog, pytest.LogCaptureFixture)
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 4)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 3
    await harness.lifecycle.on_partitions_revoked([SOURCE])

    await seed_changelog(kafka, CHANGELOG, 8, start_index=4)  # LEO 12
    kafka.trim(CHANGELOG, 0, before_offset=6)  # log_start 6 > checkpoint+1

    with caplog.at_level(logging.WARNING, logger="ksqlite.lifecycle"):
        await harness.lifecycle.on_partitions_assigned([SOURCE])  # no raise

    resets = [
        (getattr(r, "checkpoint", None), getattr(r, "log_start", None))
        for r in caplog.records
        if getattr(r, "event", None) == "truncation_reset"
    ]
    assert resets == [(3, 6)]  # old checkpoint + new log-start (WARNING)

    rows = await all_records(db)
    # All RETAINED records materialized (count asserted — kills the
    # silent-latest-reset mutant): originals 0..3 plus retained 6..11.
    assert [r[2] for r in rows] == [0, 1, 2, 3, 6, 7, 8, 9, 10, 11]
    assert await checkpoint_of(db, SOURCE) == 11
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY


async def test_rh10_clamp_down_after_log_end_regression(
    db: _pool.AiosqlitePoolAdapter, caplog: object
) -> None:
    """A stored checkpoint above LEO-1 (acks=1 unclean-truncation artifact /
    topic recreate) is clamped DOWN to LEO-1 after the reset+replay —
    otherwise every future rehydrate full-replays forever (spec §7 step 2).
    """
    import logging

    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 8)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 7
    await harness.lifecycle.on_partitions_revoked([SOURCE])

    # The topic shrinks: delete + recreate with only 5 records (LEO 5).
    kafka.delete_topic(CHANGELOG)
    await seed_changelog(kafka, CHANGELOG, 5)

    with caplog.at_level(logging.WARNING, logger="ksqlite.lifecycle"):
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert await checkpoint_of(db, SOURCE) == 4  # clamped DOWN to LEO-1
    assert harness.state.partition_states()[SOURCE].checkpoint_offset == 4
    clamps = [
        r for r in caplog.records if getattr(r, "event", None) == "checkpoint_clamped"
    ]
    assert len(clamps) == 1  # WARNING logged

    # The next rehydrate is incremental: no OOR reset, zero fetches.
    await harness.lifecycle.on_partitions_revoked([SOURCE])
    caplog.clear()
    consumer = harness.consumers[0]
    calls_before = consumer.getmany_calls
    with caplog.at_level(logging.WARNING, logger="ksqlite.lifecycle"):
        await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert consumer.getmany_calls == calls_before  # fast path
    assert not [
        r for r in caplog.records if getattr(r, "event", None) == "truncation_reset"
    ]


async def test_rh10b_clamp_up_after_zero_record_reset(
    db: _pool.AiosqlitePoolAdapter, caplog: object
) -> None:
    """A fully-trimmed tail (log_start == LEO) replays ZERO records after the
    reset; without the clamp UP to log_start-1 every future rehydrate
    re-enters the reset path (spec §7 step 2).
    """
    import logging

    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 6)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 5
    await harness.lifecycle.on_partitions_revoked([SOURCE])

    await seed_changelog(kafka, CHANGELOG, 4, start_index=6)  # LEO 10
    kafka.trim(CHANGELOG, 0, before_offset=10)  # fully-trimmed: log_start == LEO

    with caplog.at_level(logging.WARNING, logger="ksqlite.lifecycle"):
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert await checkpoint_of(db, SOURCE) == 9  # clamped UP to log_start-1
    assert harness.state.partition_states()[SOURCE].checkpoint_offset == 9
    assert [
        r for r in caplog.records if getattr(r, "event", None) == "checkpoint_clamped"
    ]

    # The next rehydrate does NOT re-enter the reset path.
    await harness.lifecycle.on_partitions_revoked([SOURCE])
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="ksqlite.lifecycle"):
        await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert not [
        r for r in caplog.records if getattr(r, "event", None) == "truncation_reset"
    ]


async def test_rh07_re_replay_is_idempotent(db: _pool.AiosqlitePoolAdapter) -> None:
    """I2: replaying any prefix twice => byte-identical `_records`."""
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 5)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])
    snapshot = await all_records(db)

    # Force a FULL re-replay over the existing rows.
    await harness.lifecycle.on_partitions_revoked([SOURCE])
    async with db.connection() as conn:
        await conn.execute("DELETE FROM partition_checkpoint")
    await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert await all_records(db) == snapshot  # identical
    assert await checkpoint_of(db, SOURCE) == 4


async def test_rh08_incremental_equals_full(
    db: _pool.AiosqlitePoolAdapter, tmp_path: object
) -> None:
    """I4: full rebuild == incremental rebuild from any equal checkpoint
    (given gap-free histories). Also covers revoke-then-immediate-reassign.
    """
    from pathlib import Path

    from ksqlite import _schema

    assert isinstance(tmp_path, Path)
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 3)

    # Incremental leg: prefix, revoke, resume incremental.
    incremental = LifecycleHarness(db, kafka)
    await incremental.lifecycle.on_partitions_assigned([SOURCE])
    await incremental.lifecycle.on_partitions_revoked([SOURCE])
    await seed_changelog(kafka, CHANGELOG, 3, start_index=3)
    await incremental.lifecycle.on_partitions_assigned([SOURCE])

    # Full leg: a second store on its own DB replays everything at once.
    full_pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "full.db", pool_size=4, busy_timeout=0.5
    )
    try:
        async with full_pool.connection() as conn:
            await _schema.migrate(conn, generated_columns=(), indexes=())
        full = LifecycleHarness(full_pool, kafka)
        await full.lifecycle.on_partitions_assigned([SOURCE])

        assert await all_records(db) == await all_records(full_pool)
        assert await checkpoint_of(db, SOURCE) == await checkpoint_of(full_pool, SOURCE)
    finally:
        await full_pool.close()


def test_rh11_checkpoint_never_decreases_except_via_clamps(
    tmp_path: object,
) -> None:
    """Hypothesis property (plan RH-11): over random {append, replay, revoke,
    truncate}+ sequences, the persisted checkpoint never decreases except
    across a replay that followed a log-end regression (the explicit clamp).
    """
    import asyncio
    import shutil
    import tempfile
    from pathlib import Path

    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st

    from ksqlite import _schema

    assert isinstance(tmp_path, Path)

    async def scenario(ops: list[str], workdir: Path) -> None:
        pool = _pool.AiosqlitePoolAdapter(
            workdir / "state.db", pool_size=2, busy_timeout=0.5
        )
        try:
            async with pool.connection() as conn:
                await _schema.migrate(conn, generated_columns=(), indexes=())
            kafka = InMemoryKafka()
            await seed_changelog(kafka, CHANGELOG, 1)
            harness = LifecycleHarness(pool, kafka)
            produced = 1
            last_checkpoint: int | None = None
            clamp_possible = False
            for op in ops:
                if op == "append":
                    await seed_changelog(kafka, CHANGELOG, 1, start_index=produced)
                    produced += 1
                elif op == "replay":
                    await harness.lifecycle.on_partitions_assigned([SOURCE])
                    await harness.lifecycle.on_partitions_revoked([SOURCE])
                elif op == "truncate_high":
                    # Log-end regression: recreate shorter than the log.
                    kafka.delete_topic(CHANGELOG)
                    await seed_changelog(kafka, CHANGELOG, 1)
                    produced = 1
                    clamp_possible = True
                current = await checkpoint_of(pool, SOURCE)
                if last_checkpoint is not None and current is not None:
                    if current < last_checkpoint:
                        assert clamp_possible, (
                            f"checkpoint decreased {last_checkpoint}->{current}"
                            f" without a clamp-inducing op (ops={ops})"
                        )
                        clamp_possible = False
                if current is not None:
                    last_checkpoint = current
        finally:
            await pool.close()

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
        derandomize=True,
    )
    @given(
        ops=st.lists(
            st.sampled_from(["append", "replay", "truncate_high"]),
            min_size=1,
            max_size=8,
        )
    )
    def run_property(ops: list[str]) -> None:
        workdir = Path(tempfile.mkdtemp(dir=tmp_path))
        try:
            asyncio.run(scenario(ops, workdir))
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    run_property()


async def test_rh12_batching_boundaries(db: _pool.AiosqlitePoolAdapter) -> None:
    """Batch size B, N = 2.5xB => exactly 3 batch transactions plus the
    reveal transaction (commit events counted through the RecordingPool)."""
    from tests.support.recording_pool import RecordingPool

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 10)
    recording = RecordingPool(db)
    harness = LifecycleHarness(db, kafka, pool=recording, batch_size=4)

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    # 3 batch transactions (4 + 4 + 2 records) + the reveal transaction.
    assert recording.events.count("txn_commit") == 4
    assert len(await all_records(db)) == 10


async def test_rh13_poll_never_inside_a_transaction(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I8: the write lock is never held across a Kafka poll (spec §7 step 3).
    Shared event log: no 'poll' event between txn_begin and its txn_commit.
    """
    from tests.support.recording_pool import RecordingPool

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 10)
    recording = RecordingPool(db)
    harness = LifecycleHarness(
        db, kafka, pool=recording, batch_size=4, event_log=recording.events
    )

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    depth = 0
    polls = 0
    for event in recording.events:
        if event == "txn_begin":
            depth += 1
        elif event in ("txn_commit", "rollback"):
            depth -= 1
        elif event == "poll":
            polls += 1
            assert depth == 0, f"poll inside a transaction: {recording.events}"
    assert polls >= 3  # the interleaving actually happened


async def view_count(db: _pool.AiosqlitePoolAdapter) -> int:
    async with db.connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM records")
        row = await cursor.fetchone()
        await cursor.close()
        assert row is not None
        return int(row[0])


async def test_rh14a_atomic_reveal_event_gated(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I5, deterministic: after batch 1 commits, a concurrent reader sees
    ZERO rows AND a non-READY state (state/view coherence); after the final
    batch + reveal, all rows — never a between state.
    """
    import asyncio

    from ksqlite.types import PartitionStatus
    from tests.support.fakes import TwoSidedGate

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 8)
    gate = TwoSidedGate()
    harness = LifecycleHarness(
        db,
        kafka,
        batch_size=4,
        configure_consumer=lambda c: c.gate_next_getmany(gate, skip=1),
    )

    task = asyncio.ensure_future(harness.lifecycle.on_partitions_assigned([SOURCE]))
    await asyncio.wait_for(gate.arrived.wait(), 5)  # batch 1 IS committed

    assert await view_count(db) == 0  # hidden mid-replay (I1/I5)
    state = harness.state.partition_states()[SOURCE]
    assert state.state is PartitionStatus.RESTORING  # state/view coherence
    assert state.checkpoint_offset == 3  # batch 1 was committed

    gate.proceed.set()
    await asyncio.wait_for(task, 5)

    assert await view_count(db) == 8  # atomically revealed, all rows
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY


async def test_rh14b_final_commit_failure_keeps_partition_hidden(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I5, commit-failure probe: the FINAL batch's commit fails => no reveal,
    RehydrateError raised (kills reveal-before-final-commit and
    swallowed-commit-failure mutants).
    """
    import pytest

    from ksqlite.errors import RehydrateError
    from tests.support.recording_pool import RecordingPool

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 8)
    recording = RecordingPool(db)
    harness = LifecycleHarness(db, kafka, pool=recording, batch_size=4)
    recording.fail_commit_at = 2  # the final batch (8 records / 4 per batch)

    with pytest.raises(RehydrateError):
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert await owned_rows(db) == set()  # NOT revealed
    assert await view_count(db) == 0
    assert await checkpoint_of(db, SOURCE) == 3  # last COMMITTED batch


P1 = TopicPartition("messages", 1)
P2 = TopicPartition("messages", 2)
P3 = TopicPartition("messages", 3)


async def _budget_choreography(
    db: _pool.AiosqlitePoolAdapter,
) -> "tuple[LifecycleHarness, RecordingPool, MutableClock]":
    """RH-15 setup: 3 partitions, scripted clock crossing the deadline
    mid-P2 (each poll advances +6; deadline at 10; batch size 4).
    """
    kafka = InMemoryKafka()
    await seed_changelog(kafka, "messages.p1.changelog", 1)
    await seed_changelog(kafka, "messages.p2.changelog", 8)
    await seed_changelog(kafka, "messages.p3.changelog", 5)

    clock = MutableClock()
    recording = RecordingPool(db)
    harness = LifecycleHarness(
        db,
        kafka,
        pool=recording,
        batch_size=4,
        rehydrate_timeout=10.0,
        clock=clock,
        on_poll=lambda: clock.advance(6.0),
    )
    await harness.lifecycle.on_partitions_assigned([P1, P2, P3])
    return harness, recording, clock


async def test_rh15_assignment_wide_budget_force_stop(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    from ksqlite.types import PartitionStatus

    harness, _recording, _clock = await _budget_choreography(db)
    states = harness.state.partition_states()

    # P1 (first partition): always commits >= 1 batch; completed fully.
    assert states[P1].state is PartitionStatus.READY
    assert states[P1].checkpoint_offset == 0

    # P2: crossed the deadline after its first committed batch.
    assert states[P2].state is PartitionStatus.READY_PARTIAL
    assert states[P2].checkpoint_offset == 3  # committed frontier
    assert states[P2].lag == 4  # 8 - (3+1), exposed

    # P3: the budget never reached it — step 0 STILL ran (budget-exempt),
    # revealed at its kept-checkpoint state.
    assert states[P3].state is PartitionStatus.READY_PARTIAL
    assert states[P3].checkpoint_offset is None
    assert states[P3].lag == 5
    assert harness.admin.describe_calls == 3  # step 0 for ALL three

    # All three are VISIBLE (partial rows included).
    assert await owned_rows(db) == {
        ("messages", 1),
        ("messages", 2),
        ("messages", 3),
    }
    rows = await all_records(db)
    assert len(rows) == 1 + 4 + 0


async def test_rh16_force_stop_is_cooperative_no_rollback(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """The deadline is checked BETWEEN batches, never by cancelling a
    coroutine mid-transaction: no rollback events anywhere (spec §7).
    """
    _harness, recording, _clock = await _budget_choreography(db)
    assert "rollback" not in recording.events


async def test_rh17_budget_convergence_on_reassign(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    from ksqlite.types import PartitionStatus

    harness, _recording, clock = await _budget_choreography(db)

    # Re-assign with ample budget: everything converges to READY.
    harness.lifecycle._rehydrate_timeout = 10_000.0  # noqa: SLF001
    await harness.lifecycle.on_partitions_revoked([P1, P2, P3])
    await harness.lifecycle.on_partitions_assigned([P1, P2, P3])

    states = harness.state.partition_states()
    assert all(states[tp].state is PartitionStatus.READY for tp in (P1, P2, P3))
    assert len(await all_records(db)) == 14  # 1 + 8 + 5, complete
    assert await checkpoint_of(db, P2) == 7
    assert await checkpoint_of(db, P3) == 4


async def test_rh18_revoke_hides_rows_keeps_data(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 4)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])

    await harness.lifecycle.on_partitions_revoked([SOURCE])

    assert await owned_rows(db) == set()  # invisible IMMEDIATELY
    assert await view_count(db) == 0
    assert len(await all_records(db)) == 4  # rows KEPT (warm standby)
    assert await checkpoint_of(db, SOURCE) == 3  # checkpoint KEPT
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.STANDBY


async def test_rh19_revoke_never_owned_and_empty_callbacks(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    kafka = InMemoryKafka()
    harness = LifecycleHarness(db, kafka)

    await harness.lifecycle.on_partitions_revoked([TopicPartition("never", 9)])
    assert harness.state.partition_states() == {}  # true no-op

    # aiokafka emits empty callbacks for both hooks.
    await harness.lifecycle.on_partitions_revoked([])
    await harness.lifecycle.on_partitions_assigned([])


async def test_rh20_replay_error_mid_replay(db: _pool.AiosqlitePoolAdapter) -> None:
    import pytest
    from aiokafka.errors import KafkaError

    from ksqlite.errors import RehydrateError

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 8)
    harness = LifecycleHarness(
        db,
        kafka,
        batch_size=4,
        configure_consumer=lambda c: c.fail_getmany_at_call(
            2, KafkaError("broker connection lost")
        ),
    )

    with pytest.raises(RehydrateError):
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert await owned_rows(db) == set()  # not revealed
    assert await checkpoint_of(db, SOURCE) == 3  # last COMMITTED batch


async def test_rh20b_clamp_not_committed_before_replay_finishes(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """OOR -> reset -> consumer error mid-replay: the checkpoint stays at the
    last committed batch (NOT clamped to log-end); a later ample rehydrate
    converges (plan RH-20b).
    """
    import pytest
    from aiokafka.errors import KafkaError

    from ksqlite.errors import RehydrateError
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 4)
    harness = LifecycleHarness(db, kafka, batch_size=4)
    await harness.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 3
    await harness.lifecycle.on_partitions_revoked([SOURCE])

    await seed_changelog(kafka, CHANGELOG, 8, start_index=4)  # LEO 12
    kafka.trim(CHANGELOG, 0, before_offset=6)  # forces the OOR reset

    consumer = harness.consumers[0]
    # Polls: #N+1 raises OOR naturally, #N+2 serves batch 6..9, #N+3 fails.
    consumer.fail_getmany_at_call(
        consumer.getmany_calls + 3, KafkaError("mid-replay failure")
    )
    with pytest.raises(RehydrateError):
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert await checkpoint_of(db, SOURCE) == 9  # last committed batch only

    await harness.lifecycle.on_partitions_assigned([SOURCE])  # converges
    assert await checkpoint_of(db, SOURCE) == 11
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY


async def test_rh20c_mid_replay_storage_error_wraps_as_rehydrate_error(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    import pytest

    from ksqlite.errors import RehydrateError, StorageError

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 4)
    recording = RecordingPool(db)
    harness = LifecycleHarness(db, kafka, pool=recording)
    recording.fail_on(lambda sql: "INSERT INTO _records" in sql)

    with pytest.raises(RehydrateError) as exc_info:
        await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert isinstance(exc_info.value.__cause__, StorageError)  # cause chained


async def test_rh23_boundary_offset(db: _pool.AiosqlitePoolAdapter) -> None:
    """Checkpoint K, exactly one new record at K+1: the incremental replay
    materializes exactly it, and the FIRST FETCHED OFFSET is K+1 — dedup
    must not silently absorb a seek-one-low mutant (plan RH-23).
    """
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 5)
    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 4

    await seed_changelog(kafka, CHANGELOG, 1, start_index=5)
    consumer = harness.consumers[0]
    consumer.served_offsets.clear()

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert consumer.served_offsets == [5]  # first fetched offset == K+1
    assert len(await all_records(db)) == 6
    assert await checkpoint_of(db, SOURCE) == 5


async def test_rh24_duplicate_id_at_two_offsets_on_replay(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """I2 at the lifecycle level: the same message_id at offsets K and K+1
    (a producer retry) => one row, checkpoint K+1.
    """
    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 3)  # offsets 0..2
    duplicated = _wire.encode(
        entity_key="dup", payload={"v": 1}, message_id=str(uuid.uuid4())
    )
    producer = FakeProducer(kafka)
    for _ in range(2):  # the SAME bytes at offsets 3 and 4
        await producer.send_and_wait(
            CHANGELOG,
            value=duplicated.value,
            key=duplicated.key,
            partition=0,
            headers=duplicated.headers,
        )

    harness = LifecycleHarness(db, kafka)
    await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert len(await all_records(db)) == 4  # 3 + ONE for the duplicate
    assert await checkpoint_of(db, SOURCE) == 4  # K+1


async def test_rh25_cancellation_at_both_cut_points(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Cancel (a) parked in a gated getmany and (b) parked in a gated
    mid-transaction execute: partition stays hidden, checkpoint = last
    committed batch, and the pool hands back a clean connection.
    """
    import asyncio

    import pytest

    from tests.support.fakes import TwoSidedGate

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 8)

    # (a) parked in getmany (poll #2: batch 1 already committed).
    gate_a = TwoSidedGate()
    recording = RecordingPool(db)
    harness = LifecycleHarness(
        db,
        kafka,
        pool=recording,
        batch_size=4,
        configure_consumer=lambda c: c.gate_next_getmany(gate_a, skip=1),
    )
    task = asyncio.ensure_future(harness.lifecycle.on_partitions_assigned([SOURCE]))
    await asyncio.wait_for(gate_a.arrived.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await owned_rows(db) == set()
    assert await checkpoint_of(db, SOURCE) == 3  # last committed batch

    # (b) parked in a mid-transaction execute.
    gate_b = TwoSidedGate()
    recording.gate_on(lambda sql: "INSERT INTO _records" in sql, gate_b)
    task = asyncio.ensure_future(harness.lifecycle.on_partitions_assigned([SOURCE]))
    await asyncio.wait_for(gate_b.arrived.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await owned_rows(db) == set()
    assert await checkpoint_of(db, SOURCE) == 3  # the gated txn rolled back

    # The pool returns clean connections: an ample rehydrate completes.
    await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert await view_count(db) == 8


async def test_rh26_warm_restart_from_persisted_checkpoint(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """The flagship §5.2 scenario: a SECOND service stack (fresh migrate +
    lifecycle) on the same DB replays only the tail from the persisted
    checkpoint; `_owned` starts empty.
    """
    from ksqlite import _schema

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 6)
    first_stack = LifecycleHarness(db, kafka)
    await first_stack.lifecycle.on_partitions_assigned([SOURCE])

    # "Process restart": fresh migrate (clears _owned) + a new stack.
    async with db.connection() as conn:
        await _schema.migrate(conn, generated_columns=(), indexes=())
    assert await owned_rows(db) == set()

    await seed_changelog(kafka, CHANGELOG, 2, start_index=6)
    second_stack = LifecycleHarness(db, kafka)
    await second_stack.lifecycle.on_partitions_assigned([SOURCE])

    consumer = second_stack.consumers[0]
    assert consumer.served_offsets == [6, 7]  # only the tail
    assert len(await all_records(db)) == 8
    assert await owned_rows(db) == {("messages", 3)}


async def test_rh27_spurious_empty_poll_is_not_termination(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Real brokers return {} mid-log on an empty prefetch buffer; treating
    it as done-ness silently reveals partitions missing their tail (spec §7).
    """
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 9)
    harness = LifecycleHarness(
        db,
        kafka,
        batch_size=4,
        configure_consumer=lambda c: c.inject_spurious_empty_poll(),
    )

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    assert len(await all_records(db)) == 9  # ALL records, despite the {}
    assert await checkpoint_of(db, SOURCE) == 8
    assert harness.state.partition_states()[SOURCE].state is PartitionStatus.READY


async def test_rh28_mid_assignment_failure_leaves_the_rest_hidden(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    import pytest
    from aiokafka.errors import KafkaError

    from ksqlite.errors import RehydrateError
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, "messages.p1.changelog", 2)
    await seed_changelog(kafka, "messages.p2.changelog", 2)
    await seed_changelog(kafka, "messages.p3.changelog", 2)
    harness = LifecycleHarness(
        db,
        kafka,
        # Poll #1 serves P1; poll #2 (P2's replay) fails transiently.
        configure_consumer=lambda c: c.fail_getmany_at_call(2, KafkaError("transient")),
    )

    with pytest.raises(RehydrateError):
        await harness.lifecycle.on_partitions_assigned([P1, P2, P3])

    assert await owned_rows(db) == {("messages", 1)}  # P1 revealed
    states = harness.state.partition_states()
    assert states[P1].state is PartitionStatus.READY
    assert P3 not in states  # never entered

    await harness.lifecycle.on_partitions_assigned([P1, P2, P3])  # converges
    assert await owned_rows(db) == {
        ("messages", 1),
        ("messages", 2),
        ("messages", 3),
    }


async def test_t07_fast_path_still_runs_step0(
    db: _pool.AiosqlitePoolAdapter, caplog: object
) -> None:
    """Warm checkpoint == log-end on a store's FIRST assignment + compact
    policy => the best-effort policy check still runs and reports: step 0 is
    unconditional, never skipped by the fast path (spec §7).
    """
    import logging

    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)

    kafka = InMemoryKafka()
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "compact"}
    )
    await seed_changelog(kafka, CHANGELOG, 3)

    warmer = LifecycleHarness(db, kafka, verify=False)  # opt-out warms the DB
    await warmer.lifecycle.on_partitions_assigned([SOURCE])
    await warmer.lifecycle.on_partitions_revoked([SOURCE])

    fresh_store = LifecycleHarness(db, kafka, verify=True)
    with caplog.at_level(logging.ERROR, logger="ksqlite.topics"):
        await fresh_store.lifecycle.on_partitions_assigned([SOURCE])

    # Fast-path eligible, still checked (and reported) — then revealed.
    assert any(
        getattr(r, "event", None) == "compact_policy_detected" for r in caplog.records
    )
    assert fresh_store.admin.describe_calls == 1
    assert await owned_rows(db) == {("messages", 3)}


async def test_w12b_produce_ok_apply_failed_self_heals(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """The §14 promise, exercised: after W-12 (record durably in the
    changelog, checkpoint unchanged), the next rehydrate materializes it.
    """
    import pytest

    from ksqlite import _write
    from ksqlite.errors import StorageError

    kafka = InMemoryKafka()
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    harness = LifecycleHarness(db, kafka)

    recording = RecordingPool(db)
    appender = _write.Appender(
        pool=recording,
        producer=FakeProducer(kafka),
        name_resolver=harness.topics,
        state=harness.state,
        txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
    )
    recording.fail_on(lambda sql: "INSERT INTO _records" in sql)
    with pytest.raises(StorageError):
        await appender.append(source=SOURCE, entity_key="k", payload={"v": 1})
    assert len(await all_records(db)) == 0  # the W-12 state

    await harness.lifecycle.on_partitions_assigned([SOURCE])  # self-heal

    rows = await all_records(db)
    assert len(rows) == 1
    assert rows[0][3] == "k"
    assert await checkpoint_of(db, SOURCE) == 0


async def test_rh22_watermark_gap_semantics(
    db: _pool.AiosqlitePoolAdapter, tmp_path: object
) -> None:
    """I4's one spec-accepted exception (§13/§14): the MAX watermark may skip
    a rare un-materialized offset. An incremental rehydrate does NOT recover
    it; a cold full rebuild DOES. Constructed through public seams: the W-12
    state (durable record, unchanged checkpoint), then a successful append.
    """
    from pathlib import Path

    import pytest

    from ksqlite import _schema, _write
    from ksqlite.errors import StorageError

    assert isinstance(tmp_path, Path)
    kafka = InMemoryKafka()
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    harness = LifecycleHarness(db, kafka)

    def appender_on(pool: object) -> _write.Appender:
        return _write.Appender(
            pool=pool,  # type: ignore[arg-type]
            producer=FakeProducer(kafka),
            name_resolver=harness.topics,
            state=harness.state,
            txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
        )

    # Append #1: produce acked (offset 0, durable) but the local txn fails.
    recording = RecordingPool(db)
    recording.fail_on(lambda sql: "INSERT INTO _records" in sql)
    with pytest.raises(StorageError):
        await appender_on(recording).append(
            source=SOURCE, entity_key="k0", payload={"i": 0}
        )
    # Append #2 succeeds: checkpoint jumps to 1, past the unmaterialized 0.
    await appender_on(db).append(source=SOURCE, entity_key="k1", payload={"i": 1})
    assert await checkpoint_of(db, SOURCE) == 1

    # Incremental rehydrate: fast path (checkpoint+1 == LEO) — the gap at
    # offset 0 is NOT recovered (the accepted best-effort loss).
    await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert [r[2] for r in await all_records(db)] == [1]

    # A cold full rebuild DOES recover it.
    cold_pool = _pool.AiosqlitePoolAdapter(
        tmp_path / "cold.db", pool_size=2, busy_timeout=0.5
    )
    try:
        async with cold_pool.connection() as conn:
            await _schema.migrate(conn, generated_columns=(), indexes=())
        cold = LifecycleHarness(cold_pool, kafka)
        await cold.lifecycle.on_partitions_assigned([SOURCE])
        assert [r[2] for r in await all_records(cold_pool)] == [0, 1]
    finally:
        await cold_pool.close()


# -- post-v1 deep-review regression tests (RH-29..RH-34) ----------------------


async def test_rh29_fast_path_seeds_in_memory_checkpoint(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Warm restart + zero-fetch fast path: ``partition_states()`` must
    report the persisted checkpoint and lag 0 — not ``checkpoint_offset is
    None`` / ``lag == LEO`` (spec §4: state and checkpoint_offset are live).
    """
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 6)
    first_stack = LifecycleHarness(db, kafka)
    await first_stack.lifecycle.on_partitions_assigned([SOURCE])
    assert await checkpoint_of(db, SOURCE) == 5

    # "Process restart": a fresh stack with an EMPTY in-memory state machine
    # on the same DB; checkpoint+1 == LEO -> the zero-fetch fast path.
    second_stack = LifecycleHarness(db, kafka)
    await second_stack.lifecycle.on_partitions_assigned([SOURCE])

    state = second_stack.state.partition_states()[SOURCE]
    assert state.state is PartitionStatus.READY
    assert state.checkpoint_offset == 5  # seeded from the persisted row
    assert state.lag == 0  # LEO(6) - (5+1)


async def test_rh30_failed_consumer_start_is_not_cached(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """A rehydrate-consumer ``start()`` failure must not poison the
    lifecycle: the dead client is stopped best-effort and NOT cached, so the
    next assignment gets a fresh consumer.
    """
    import pytest
    from aiokafka.errors import KafkaConnectionError

    from ksqlite.errors import RehydrateError
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 3)

    class UnstartableConsumer:
        """start() fails; any other use means the dead client was reused."""

        stop_calls = 0

        async def start(self) -> None:
            raise KafkaConnectionError("injected: broker unreachable")

        async def stop(self) -> None:
            self.stop_calls += 1

    made: list[object] = []

    def factory() -> object:
        consumer: object = (
            UnstartableConsumer()
            if not made
            else FakeConsumer(
                kafka,
                group_id=None,
                enable_auto_commit=False,
                auto_offset_reset="none",
            )
        )
        made.append(consumer)
        return consumer

    lifecycle = _lifecycle.PartitionLifecycle(
        pool=db,
        consumer_factory=factory,
        topics=_topics.ChangelogTopics(
            admin=FakeAdmin(kafka),
            template=TEMPLATE,
            create_topics_retention_ms=None,
            verify_changelog_policy=True,
        ),
        state=_state.PartitionStateMachine(),
        txn_runner=_pool.TransactionRunner(busy_timeout=0.5),
        rehydrate_timeout=30.0,
    )

    with pytest.raises(RehydrateError):
        await lifecycle.on_partitions_assigned([SOURCE])

    unstartable = made[0]
    assert isinstance(unstartable, UnstartableConsumer)
    assert unstartable.stop_calls == 1  # best-effort cleanup of the dead client

    await lifecycle.on_partitions_assigned([SOURCE])  # fresh consumer, converges
    assert len(made) == 2
    assert len(await all_records(db)) == 3
    states = lifecycle._state.partition_states()  # noqa: SLF001
    assert states[SOURCE].state is PartitionStatus.READY


async def test_rh31_deadline_enforced_on_empty_polls(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Empty fetches must not extend the replay budget: the deadline is
    checked on EVERY poll iteration, so a degraded broker returning ``{}``
    cannot hold the rebalance callback past ``rehydrate_timeout`` (spec §7).
    """
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 8)
    clock = MutableClock()

    def configure(consumer: FakeConsumer) -> None:
        def on_poll() -> None:
            clock.advance(6.0)
            if consumer.getmany_calls == 2:
                consumer.inject_spurious_empty_poll()

        consumer.on_poll = on_poll

    harness = LifecycleHarness(
        db,
        kafka,
        batch_size=4,
        rehydrate_timeout=10.0,
        clock=clock,
        configure_consumer=configure,
    )

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    # Poll 1 (t=6): records 0-3 committed; t < deadline -> continue.
    # Poll 2 (t=12): an EMPTY fetch past the deadline -> force-stop.
    state = harness.state.partition_states()[SOURCE]
    assert state.state is PartitionStatus.READY_PARTIAL
    assert await checkpoint_of(db, SOURCE) == 3  # the committed frontier
    assert len(await all_records(db)) == 4
    assert await owned_rows(db) == {("messages", 3)}  # revealed partial


async def test_rh32_deadline_enforced_after_truncation_reset(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """The OOR reset path must also observe the deadline: a truncation reset
    landing past the budget force-stops instead of replaying (spec §7). The
    kept checkpoint is unchanged — clamps run only on completed replays.
    """
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 3)
    warm = LifecycleHarness(db, kafka)
    await warm.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 2
    await warm.lifecycle.on_partitions_revoked([SOURCE])

    await seed_changelog(kafka, CHANGELOG, 5, start_index=3)  # LEO 8
    kafka.trim(CHANGELOG, 0, before_offset=5)  # log_start 5 > checkpoint+1

    clock = MutableClock()
    harness = LifecycleHarness(
        db,
        kafka,
        rehydrate_timeout=5.0,
        clock=clock,
        on_poll=lambda: clock.advance(6.0),
    )

    await harness.lifecycle.on_partitions_assigned([SOURCE])

    # Poll 1 (t=6): OOR -> reset to earliest; t >= deadline -> force-stop.
    state = harness.state.partition_states()[SOURCE]
    assert state.state is PartitionStatus.READY_PARTIAL
    assert await checkpoint_of(db, SOURCE) == 2  # unchanged: no clamp ran
    assert len(await all_records(db)) == 3  # nothing replayed post-reset
    assert await owned_rows(db) == {("messages", 3)}


async def test_rh33_revoke_db_failure_maps_to_storage_error(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """A SQLite failure in the revoke hook surfaces as the documented
    ``StorageError``, never a raw sqlite3 error (spec §4 boundary mapping).
    """
    import pytest

    from ksqlite.errors import StorageError

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 2)
    recording = RecordingPool(db)
    harness = LifecycleHarness(db, kafka, pool=recording)
    await harness.lifecycle.on_partitions_assigned([SOURCE])

    recording.fail_on(lambda sql: sql.startswith("DELETE FROM _owned"))
    with pytest.raises(StorageError):
        await harness.lifecycle.on_partitions_revoked([SOURCE])


async def test_rh34_reveal_and_checkpoint_read_failures_map_to_rehydrate_error(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """Mid-replay DB failures on the reveal INSERT and the checkpoint SELECT
    are boundary-mapped like every other replay error: ``RehydrateError``
    with the cause chained, partition NOT revealed (spec §4).
    """
    import pytest

    from ksqlite.errors import RehydrateError

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 2)

    recording = RecordingPool(db)
    harness = LifecycleHarness(db, kafka, pool=recording)
    recording.fail_on(lambda sql: sql.startswith("INSERT OR IGNORE INTO _owned"))
    with pytest.raises(RehydrateError):
        await harness.lifecycle.on_partitions_assigned([SOURCE])
    assert await owned_rows(db) == set()  # NOT revealed

    recording2 = RecordingPool(db)
    harness2 = LifecycleHarness(db, kafka, pool=recording2)
    recording2.fail_on(lambda sql: sql.startswith("SELECT changelog_offset"))
    with pytest.raises(RehydrateError):
        await harness2.lifecycle.on_partitions_assigned([SOURCE])
    assert await owned_rows(db) == set()


async def test_rh35_clamp_db_failure_maps_to_rehydrate_error(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """The post-reset checkpoint clamp is part of replay: its DB failure
    maps to ``RehydrateError`` (partition stays hidden this assignment).
    """
    import pytest

    from ksqlite.errors import RehydrateError

    kafka = InMemoryKafka()
    await seed_changelog(kafka, CHANGELOG, 4)
    recording = RecordingPool(db)
    warm = LifecycleHarness(db, kafka, pool=recording)
    await warm.lifecycle.on_partitions_assigned([SOURCE])  # checkpoint 3
    await warm.lifecycle.on_partitions_revoked([SOURCE])

    # Regress the log end below the checkpoint: recreate the topic empty.
    kafka.delete_topic(CHANGELOG)
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )

    recording.fail_on(
        lambda sql: sql.startswith("INSERT OR REPLACE INTO partition_checkpoint")
    )
    with pytest.raises(RehydrateError):
        await warm.lifecycle.on_partitions_assigned([SOURCE])


async def test_rh36_unreached_partition_with_warm_checkpoint_reports_lag(
    db: _pool.AiosqlitePoolAdapter,
) -> None:
    """A budget-unreached partition that was warmed on a PRIOR run reveals
    READY_PARTIAL at its persisted checkpoint — ``partition_states()`` must
    report that checkpoint and its true lag, not "fully unread" (spec §7
    force-stop; spec §4 live checkpoint_offset).
    """
    from ksqlite.types import PartitionStatus

    kafka = InMemoryKafka()
    await seed_changelog(kafka, "messages.p1.changelog", 1)
    await seed_changelog(kafka, "messages.p3.changelog", 5)

    warm = LifecycleHarness(db, kafka)
    await warm.lifecycle.on_partitions_assigned([P3])  # checkpoint 4
    await warm.lifecycle.on_partitions_revoked([P3])

    # "Process restart": fresh in-memory state. P1 exhausts the budget, so
    # P3 is never reached and is revealed at its kept-checkpoint state.
    clock = MutableClock()
    harness = LifecycleHarness(
        db,
        kafka,
        rehydrate_timeout=5.0,
        clock=clock,
        on_poll=lambda: clock.advance(6.0),
    )
    await harness.lifecycle.on_partitions_assigned([P1, P3])

    states = harness.state.partition_states()
    assert states[P1].state is PartitionStatus.READY
    assert states[P3].state is PartitionStatus.READY_PARTIAL
    assert states[P3].checkpoint_offset == 4  # the persisted checkpoint
    assert states[P3].lag == 0  # 5 - (4+1): caught up, merely unreached
    assert await owned_rows(db) == {("messages", 1), ("messages", 3)}
