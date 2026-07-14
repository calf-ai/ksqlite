"""P10 E2E failure-mode scenarios: E2E-07/08a/08b/11 (plan §5 P10)."""

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any

import pytest
from aiokafka import AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.records_to_delete import RecordsToDelete

from ksqlite import _wire

from tests.e2e.test_e2e_core import changelog_of, make_store, unique

pytestmark = pytest.mark.e2e

PRELOAD = 20_000


async def create_changelog(bootstrap: str, name: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(
                    name,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"cleanup.policy": "delete", "retention.ms": "-1"},
                )
            ]
        )
    finally:
        await admin.close()


async def preload_changelog(
    bootstrap: str, changelog: str, n: int, *, start: int = 0
) -> None:
    """Fast out-of-band preload with VALID KSQLite wire format."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for i in range(start, start + n):
            encoded = _wire.encode(
                entity_key=f"k{i}",
                payload={"i": i},
                message_id=str(uuid.uuid4()),
            )
            await producer.send(
                changelog,
                value=encoded.value,
                key=encoded.key,
                partition=0,
                headers=encoded.headers,
            )
        await producer.flush()
    finally:
        await producer.stop()


async def test_e2e07_budget_force_stop_then_convergence(
    broker: Any, tmp_path: Path
) -> None:
    """rehydrate_timeout << one batch's wall clock => deterministically
    READY_PARTIAL by the pinned deadline placement (the first partition
    always materializes + commits its first batch before the check). A
    second instance with the default budget on the SAME db_path completes
    (doubles as warm-restart + I4 coverage).
    """
    from ksqlite.types import PartitionStatus

    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    changelog = changelog_of(source)
    await create_changelog(bootstrap, changelog)
    await preload_changelog(bootstrap, changelog, PRELOAD)
    db_path = tmp_path / "state.db"

    store = make_store(db_path, bootstrap, rehydrate_timeout=0.001)
    await store.start()
    await store.on_partitions_assigned([source])
    partial = store.partition_states()[source]
    assert partial.state is PartitionStatus.READY_PARTIAL
    assert partial.lag is not None and partial.lag > 0
    partial_rows = await store.query("SELECT COUNT(*) AS n FROM records")
    assert 0 < partial_rows[0]["n"] < PRELOAD  # partial rows ARE visible
    await store.stop()

    # Completion: a second instance, default budget, same db_path (spec §2.8).
    async with make_store(db_path, bootstrap) as second:
        await second.on_partitions_assigned([source])
        assert second.partition_states()[source].state is PartitionStatus.READY
        rows = await second.query("SELECT COUNT(*) AS n FROM records")
        assert rows[0]["n"] == PRELOAD


async def test_e2e08a_truncation_reset(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Choreography pinned so the reset path ACTUALLY runs (plan E2E-08a): a
    naive trim <= checkpoint+1 would pass via the fast path without ever
    resetting.
    """
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    changelog = changelog_of(source)
    caplog.set_level(logging.INFO, logger="ksqlite.lifecycle")

    store = make_store(tmp_path / "s.db", bootstrap)
    await store.start()
    await store.on_partitions_assigned([source])
    for i in range(5):
        await store.append(source=source, entity_key=f"k{i}", payload={"i": i})
    # checkpoint == 4
    await store.on_partitions_revoked([source])

    # An OUT-OF-BAND producer appends a valid tail; then DeleteRecords trims
    # past checkpoint+2 so the stored checkpoint falls below the log start.
    await preload_changelog(bootstrap, changelog, 3, start=5)  # offsets 5..7
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.delete_records({TopicPartition(changelog, 0): RecordsToDelete(6)})
    finally:
        await admin.close()

    caplog.clear()
    await store.on_partitions_assigned([source])  # reset path runs

    resets = [
        r for r in caplog.records if getattr(r, "event", None) == "truncation_reset"
    ]
    assert len(resets) == 1  # the WARNING event asserted (plan E2E-08a)
    rows = await store.query(
        "SELECT changelog_offset FROM records ORDER BY changelog_offset"
    )
    # Originals 0..4 kept locally; retained tail 6..7 materialized.
    assert [r["changelog_offset"] for r in rows] == [0, 1, 2, 3, 4, 6, 7]
    assert store.partition_states()[source].checkpoint_offset == 7  # LEO-1

    # The next rehydrate is incremental: no reset, zero records replayed.
    await store.on_partitions_revoked([source])
    caplog.clear()
    await store.on_partitions_assigned([source])
    assert not [
        r for r in caplog.records if getattr(r, "event", None) == "truncation_reset"
    ]
    ends = [r for r in caplog.records if getattr(r, "event", None) == "rehydrate_end"]
    assert [getattr(r, "records_replayed", None) for r in ends] == [0]
    await store.stop()


async def test_e2e08b_clamp_down_after_topic_recreate(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    changelog = changelog_of(source)
    caplog.set_level(logging.INFO, logger="ksqlite.lifecycle")

    store = make_store(tmp_path / "s.db", bootstrap)
    await store.start()
    await store.on_partitions_assigned([source])
    for i in range(8):
        await store.append(source=source, entity_key=f"k{i}", payload={"i": i})
    assert store.partition_states()[source].checkpoint_offset == 7
    await store.on_partitions_revoked([source])

    # delete_topics + poll-until-absent + recreate + produce M < K.
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.delete_topics([changelog])
        for _ in range(100):
            if changelog not in await admin.list_topics():
                break
            await asyncio.sleep(0.1)
        else:  # pragma: no cover - diagnostics
            pytest.fail("changelog topic never disappeared")
        await create_changelog(bootstrap, changelog)
    finally:
        await admin.close()
    await preload_changelog(bootstrap, changelog, 5)  # M=5 < K=8

    caplog.clear()
    await store.on_partitions_assigned([source])  # OOR -> reset -> clamp DOWN

    clamps = [
        r for r in caplog.records if getattr(r, "event", None) == "checkpoint_clamped"
    ]
    assert len(clamps) == 1
    assert store.partition_states()[source].checkpoint_offset == 4  # DECREASED

    # Subsequent rehydrates are incremental (no reset re-entry).
    await store.on_partitions_revoked([source])
    caplog.clear()
    await store.on_partitions_assigned([source])
    assert not [
        r for r in caplog.records if getattr(r, "event", None) == "truncation_reset"
    ]
    await store.stop()


async def test_e2e11_live_concurrent_reads_during_big_rehydrate(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """I1/I5 live: >=1 hidden-phase sample (0 rows, taken AFTER the
    rehydrate_start event — a pre-assignment sample is vacuous) and >=1
    complete-phase sample (full count). The ~20k preload keeps the hidden
    window wide.
    """
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    changelog = changelog_of(source)
    await create_changelog(bootstrap, changelog)
    await preload_changelog(bootstrap, changelog, PRELOAD)

    caplog.set_level(logging.INFO, logger="ksqlite.lifecycle")
    rehydrate_started = asyncio.Event()

    class StartSignal(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if getattr(record, "event", None) == "rehydrate_start":
                rehydrate_started.set()

    signal_handler = StartSignal()
    logging.getLogger("ksqlite.lifecycle").addHandler(signal_handler)

    store = make_store(tmp_path / "s.db", bootstrap)
    await store.start()
    hidden_samples: list[int] = []
    try:
        assign_task = asyncio.ensure_future(store.on_partitions_assigned([source]))

        async def reader() -> None:
            while not assign_task.done():
                if rehydrate_started.is_set():
                    rows = await store.query("SELECT COUNT(*) AS n FROM records")
                    hidden_samples.append(int(rows[0]["n"]))
                await asyncio.sleep(0.01)

        reader_task = asyncio.ensure_future(reader())
        await assign_task
        await reader_task

        assert hidden_samples, "no sample taken during the rehydrate window"
        assert all(n == 0 for n in hidden_samples)  # excluded, never partial

        final = await store.query("SELECT COUNT(*) AS n FROM records")
        assert final[0]["n"] == PRELOAD  # the complete-phase sample
    finally:
        logging.getLogger("ksqlite.lifecycle").removeHandler(signal_handler)
        await store.stop()
