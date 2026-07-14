"""P10 E2E core scenarios: E2E-01/02/03/05/06/10 (plan §5 P10).
Real broker (pinned Redpanda), real aiokafka clients, real tmp-file SQLite.
"""

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

from ksqlite import KSQLite
from ksqlite.types import GeneratedColumn

pytestmark = pytest.mark.e2e


def unique(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:10]}"


def changelog_of(source: TopicPartition) -> str:
    return f"{source.topic}.p{source.partition}.changelog"


async def raw_read_all(
    bootstrap: str, topic: str, expected: int, *, timeout: float = 20.0
) -> list[Any]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        await consumer.seek_to_beginning(tp)
        records: list[Any] = []
        deadline = time.monotonic() + timeout
        while len(records) < expected and time.monotonic() < deadline:
            batch = await consumer.getmany(tp, timeout_ms=300)
            records.extend(batch.get(tp, []))
        return records
    finally:
        await consumer.stop()


async def topic_config(bootstrap: str, topic: str) -> dict[str, str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        responses = await admin.describe_configs(
            [ConfigResource(ConfigResourceType.TOPIC, topic)]
        )
        found: dict[str, str] = {}
        for response in responses:
            for resource in response.resources:
                for entry in resource[4]:
                    found[entry[0]] = entry[1]
        return found
    finally:
        await admin.close()


def make_store(db_path: Path, bootstrap: str, **overrides: Any) -> KSQLite:
    kwargs: dict[str, Any] = {
        "db_path": db_path,
        "bootstrap_servers": bootstrap,
        "create_topics_retention_ms": -1,
    }
    kwargs.update(overrides)
    return KSQLite(**kwargs)


async def test_e2e01_happy_loop_two_topics(broker: Any, tmp_path: Path) -> None:
    """Two source topics sharing partition numbers (spec §2.6), a unicode
    entity_key, and a ~900 KB payload (inside the client's default 1 MiB
    max_request_size — the defaults posture pinned).
    """
    bootstrap = broker.get_bootstrap_server()
    topic_a, topic_b = unique("srca"), unique("srcb")
    pa, pb = TopicPartition(topic_a, 0), TopicPartition(topic_b, 0)

    async with make_store(
        tmp_path / "s.db",
        bootstrap,
        generated_columns=[GeneratedColumn("thread_id", "$.thread_id")],
    ) as store:
        await store.on_partitions_assigned([pa, pb])

        await store.append(
            source=pa, entity_key="tâsk-🚀", payload={"thread_id": "th-1"}
        )
        await store.append(source=pb, entity_key="key-b", payload={"thread_id": "th-2"})
        big_payload = {"thread_id": "th-big", "data": "x" * 900_000}
        await store.append(source=pa, entity_key="big", payload=big_payload)

        # Correct scoping across topics sharing partition number 0.
        rows_a = await store.query(
            "SELECT entity_key FROM records WHERE source_topic = ?"
            " ORDER BY changelog_offset",
            (topic_a,),
        )
        assert [r["entity_key"] for r in rows_a] == ["tâsk-🚀", "big"]
        rows_b = await store.query(
            "SELECT entity_key FROM records WHERE source_topic = ?", (topic_b,)
        )
        assert [r["entity_key"] for r in rows_b] == ["key-b"]
        by_thread = await store.query(
            "SELECT entity_key FROM records WHERE thread_id = ?", ("th-1",)
        )
        assert [r["entity_key"] for r in by_thread] == ["tâsk-🚀"]

    # Both changelog topics exist: single-partition, delete policy (spec §10).
    for topic in (changelog_of(pa), changelog_of(pb)):
        config = await topic_config(bootstrap, topic)
        assert config["cleanup.policy"] == "delete"

    # Raw wire-format check (spec §10): key = entity_key bytes; headers carry
    # format_version + message_id; the value is the PURE host payload.
    records = await raw_read_all(bootstrap, changelog_of(pa), 2)
    assert len(records) == 2
    first = records[0]
    assert first.key == "tâsk-🚀".encode()
    headers = dict(first.headers)
    assert headers["format_version"] == b"1"
    assert len(headers["message_id"]) == 36
    assert json.loads(first.value.decode()) == {"thread_id": "th-1"}


async def test_e2e02_cold_rebuild_with_foreign_record(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    n = 5

    async with make_store(tmp_path / "one.db", bootstrap) as store:
        await store.on_partitions_assigned([source])
        for i in range(n):
            await store.append(source=source, entity_key=f"k{i}", payload={"i": i})
        original = await store.query(
            "SELECT entity_key, payload FROM records ORDER BY changelog_offset"
        )

    # A foreign producer wrote into the changelog (headerless, null key).
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(changelog_of(source), value=b"not-ksqlite")
    finally:
        await producer.stop()

    # Destroy the local DB: a brand-new instance cold-rebuilds from the log.
    caplog.set_level(logging.INFO, logger="ksqlite.lifecycle")
    async with make_store(tmp_path / "two.db", bootstrap) as rebuilt:
        await rebuilt.on_partitions_assigned([source])
        rebuilt_rows = await rebuilt.query(
            "SELECT entity_key, payload FROM records ORDER BY changelog_offset"
        )
        assert [tuple(r) for r in rebuilt_rows] == [tuple(r) for r in original]

    skips = [
        r
        for r in caplog.records
        if getattr(r, "event", None) == "foreign_record_skipped"
    ]
    assert len(skips) == 1  # skipped + logged (I4 holds regardless)


async def test_e2e03_warm_handoff_incremental(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Deterministic handoff, no consumer group: A owns -> appends -> revoke;
    B rebuilds -> appends -> revoke; A re-assigns INCREMENTALLY. Asserted via
    the structured rehydrate logs (there is no 'consumer position' with
    group_id=None; plan E2E-03).
    """
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    caplog.set_level(logging.INFO, logger="ksqlite.lifecycle")

    store_a = make_store(tmp_path / "a.db", bootstrap)
    await store_a.start()
    await store_a.on_partitions_assigned([source])
    for i in range(3):
        await store_a.append(source=source, entity_key=f"a{i}", payload={"i": i})
    await store_a.on_partitions_revoked([source])

    async with make_store(tmp_path / "b.db", bootstrap) as store_b:
        caplog.clear()
        await store_b.on_partitions_assigned([source])  # full replay: 3
        end_events = [
            r for r in caplog.records if getattr(r, "event", None) == "rehydrate_end"
        ]
        assert [getattr(r, "records_replayed", None) for r in end_events] == [3]
        for i in range(2):
            await store_b.append(source=source, entity_key=f"b{i}", payload={"i": i})
        await store_b.on_partitions_revoked([source])

    caplog.clear()
    await store_a.on_partitions_assigned([source])  # INCREMENTAL from kept cp
    start_events = [
        r for r in caplog.records if getattr(r, "event", None) == "rehydrate_start"
    ]
    assert [getattr(r, "checkpoint", None) for r in start_events] == [2]
    end_events = [
        r for r in caplog.records if getattr(r, "event", None) == "rehydrate_end"
    ]
    assert [getattr(r, "records_replayed", None) for r in end_events] == [2]

    rows = await store_a.query("SELECT COUNT(*) AS n FROM records")
    assert rows[0]["n"] == 5
    await store_a.stop()


async def test_e2e05_compact_policy_live(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The policy check is best-effort (spec §10): a live compact changelog
    is reported loudly (``compact_policy_detected``) and the assignment
    proceeds — verified against a real broker's describe_configs shape.
    """
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    changelog = changelog_of(source)

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        from aiokafka.admin import NewTopic

        await admin.create_topics(
            [
                NewTopic(
                    changelog,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"cleanup.policy": "compact"},
                )
            ]
        )

        async with make_store(tmp_path / "s.db", bootstrap) as store:
            with caplog.at_level(logging.ERROR, logger="ksqlite.topics"):
                await store.on_partitions_assigned([source])  # NO raise
            assert any(
                getattr(r, "event", None) == "compact_policy_detected"
                for r in caplog.records
            )
            rows = await store.query("SELECT COUNT(*) AS n FROM records")
            assert rows[0]["n"] == 0
    finally:
        await admin.close()


async def test_e2e06_auto_create_config(broker: Any, tmp_path: Path) -> None:
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)

    async with make_store(
        tmp_path / "s.db", bootstrap, create_topics_retention_ms=-1
    ) as store:
        await store.on_partitions_assigned([source])

    config = await topic_config(bootstrap, changelog_of(source))
    assert config["cleanup.policy"] == "delete"
    assert config["retention.ms"] == "-1"

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        described = await admin.describe_topics([changelog_of(source)])
        assert len(described) == 1
        assert len(described[0]["partitions"]) == 1  # single-partition (§10)
    finally:
        await admin.close()


async def test_e2e10_stop_drain_against_real_broker(
    broker: Any, tmp_path: Path
) -> None:
    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)
    n = 25

    store = make_store(tmp_path / "s.db", bootstrap)
    await store.start()
    await store.on_partitions_assigned([source])
    append_tasks = [
        asyncio.ensure_future(
            store.append(source=source, entity_key=f"k{i}", payload={"i": i})
        )
        for i in range(n)
    ]
    await asyncio.gather(*append_tasks)
    await store.stop()  # flushes the producer

    records = await raw_read_all(bootstrap, changelog_of(source), n)
    assert len(records) == n  # every acked record is durably in the log
