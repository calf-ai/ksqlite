"""Conformance suite CF-01..CF-08 (plan §4.3): ONE set of test bodies,
executed against the fakes always and the real client under ``-m e2e`` —
fake<->real divergence breaks mechanically, not by review.
"""

import inspect
import time
from typing import Any

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import OffsetOutOfRangeError

from tests.support.kafka_env import FakeKafkaEnv, RealKafkaEnv

KafkaEnv = FakeKafkaEnv | RealKafkaEnv

STRICT = {"group_id": None, "enable_auto_commit": False, "auto_offset_reset": "none"}


async def _produce_n(env: KafkaEnv, topic: str, n: int) -> None:
    producer = env.new_producer()
    await producer.start()
    try:
        for i in range(n):
            await producer.send_and_wait(
                topic,
                value=b'{"i":%d}' % i,
                key=f"k{i}".encode(),
                partition=0,
                headers=[("message_id", f"m{i}".encode())],
            )
    finally:
        await producer.stop()


async def _fetch_until(
    consumer: Any,
    tp: TopicPartition,
    count: int,
    *,
    max_records: int | None = None,
    timeout: float = 15.0,
) -> tuple[list[Any], list[int]]:
    """Bounded-wait polling (no sleeps); returns (records, batch_sizes)."""
    records: list[Any] = []
    batch_sizes: list[int] = []
    deadline = time.monotonic() + timeout
    while len(records) < count and time.monotonic() < deadline:
        batch = await consumer.getmany(tp, timeout_ms=200, max_records=max_records)
        assert isinstance(batch, dict)  # the pinned getmany shape
        taken = batch.get(tp, [])
        if taken:
            records.extend(taken)
            batch_sizes.append(len(taken))
    return records, batch_sizes


async def test_cf01_produce_metadata_shape(kafka_env: KafkaEnv) -> None:
    topic = kafka_env.unique_topic("cf01")
    await kafka_env.create_topic(topic)

    producer = kafka_env.new_producer()
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            topic,
            value=b'{"v":1}',
            key=b"k",
            partition=0,  # explicit partition= accepted (pinned)
            headers=[("format_version", b"1"), ("message_id", b"cf01-mid")],
        )
        assert metadata.offset == 0
        assert metadata.topic == topic
        assert metadata.partition == 0
    finally:
        await producer.stop()


async def test_cf02_seek_shapes_and_getmany(kafka_env: KafkaEnv) -> None:
    topic = kafka_env.unique_topic("cf02")
    await kafka_env.create_topic(topic)
    await _produce_n(kafka_env, topic, 5)

    consumer_type = kafka_env.consumer_type()
    assert not inspect.iscoroutinefunction(consumer_type.seek)  # sync (pinned)
    assert inspect.iscoroutinefunction(consumer_type.seek_to_beginning)

    consumer = kafka_env.new_consumer(**STRICT)
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        consumer.seek(tp, 0)

        records, batch_sizes = await _fetch_until(consumer, tp, 5, max_records=2)
        assert [r.offset for r in records] == [0, 1, 2, 3, 4]
        assert all(size <= 2 for size in batch_sizes)  # max_records honored
    finally:
        await consumer.stop()


async def test_cf03_oor_from_fetch_iff_none(kafka_env: KafkaEnv) -> None:
    from aiokafka.admin.records_to_delete import RecordsToDelete

    topic = kafka_env.unique_topic("cf03")
    await kafka_env.create_topic(topic)
    await _produce_n(kafka_env, topic, 5)
    tp = TopicPartition(topic, 0)

    admin = kafka_env.new_admin()
    await admin.start()
    try:
        await admin.delete_records({tp: RecordsToDelete(before_offset=3)})
    finally:
        await admin.close()

    strict = kafka_env.new_consumer(**STRICT)
    await strict.start()
    try:
        strict.assign([tp])
        strict.seek(tp, 0)  # seek NEVER raises (pinned)
        with pytest.raises(OffsetOutOfRangeError):  # the raise point: the FETCH
            for _ in range(50):
                await strict.getmany(tp, timeout_ms=200)
    finally:
        await strict.stop()

    lenient = kafka_env.new_consumer(
        group_id=None, enable_auto_commit=False, auto_offset_reset="latest"
    )
    await lenient.start()
    try:
        lenient.assign([tp])
        lenient.seek(tp, 0)
        # The silent reset: no error, and the retained records are SKIPPED.
        first = await lenient.getmany(tp, timeout_ms=1000)
        assert first.get(tp, []) == []
        await _produce_n(kafka_env, topic, 1)  # lands at offset 5
        records, _ = await _fetch_until(lenient, tp, 1)
        assert [r.offset for r in records] == [5]  # only the new tail
    finally:
        await lenient.stop()


async def test_cf04_end_offsets_is_leo(kafka_env: KafkaEnv) -> None:
    empty_topic = kafka_env.unique_topic("cf04empty")
    await kafka_env.create_topic(empty_topic)
    topic = kafka_env.unique_topic("cf04")
    await kafka_env.create_topic(topic)
    await _produce_n(kafka_env, topic, 3)

    consumer = kafka_env.new_consumer(**STRICT)
    await consumer.start()
    try:
        empty_tp = TopicPartition(empty_topic, 0)
        tp = TopicPartition(topic, 0)
        assert (await consumer.end_offsets([empty_tp]))[empty_tp] == 0
        assert (await consumer.end_offsets([tp]))[tp] == 3  # last + 1
    finally:
        await consumer.stop()


async def test_cf05_header_types(kafka_env: KafkaEnv) -> None:
    topic = kafka_env.unique_topic("cf05")
    await kafka_env.create_topic(topic)
    await _produce_n(kafka_env, topic, 1)

    consumer = kafka_env.new_consumer(**STRICT)
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        consumer.seek(tp, 0)
        records, _ = await _fetch_until(consumer, tp, 1)
        headers = list(records[0].headers)
        assert headers, "headers survived the round-trip"
        for name, value in headers:
            assert isinstance(name, str)  # keys are str (pinned)
            assert isinstance(value, bytes)  # values are bytes (pinned)
        assert dict(headers)["message_id"] == b"m0"
    finally:
        await consumer.stop()


async def test_cf06_describe_configs_resources_walking(kafka_env: KafkaEnv) -> None:
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

    topic = kafka_env.unique_topic("cf06")
    await kafka_env.create_topic(topic, configs={"cleanup.policy": "delete"})

    admin = kafka_env.new_admin()
    await admin.start()
    try:
        responses = await admin.describe_configs(
            [ConfigResource(ConfigResourceType.TOPIC, topic)]
        )
        found: dict[str, str] = {}
        for response in responses:
            for resource in response.resources:
                _err, _msg, _rtype, resource_name, entries = resource
                assert resource_name == topic
                for entry in entries:
                    found[entry[0]] = entry[1]
        assert found["cleanup.policy"] == "delete"
    finally:
        await admin.close()


async def test_cf07_unknown_ctor_kwarg_is_a_typeerror(kafka_env: KafkaEnv) -> None:
    with pytest.raises(TypeError):
        kafka_env.new_consumer(definitely_not_a_real_kwarg=1)


async def test_cf08_create_topics_reports_per_topic_error_codes(
    kafka_env: KafkaEnv,
) -> None:
    """``create_topics`` NEVER raises per-topic outcomes — they come back as
    ``topic_errors`` entries of (topic, error_code[, error_message]): 0 on
    success, 36 (TopicAlreadyExists) when losing a create race. KSQLite's
    step 0 depends on walking these codes (spec §7 step 0).
    """
    from aiokafka.admin import NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    topic = kafka_env.unique_topic("cf08")
    admin = kafka_env.new_admin()
    await admin.start()
    try:
        first = await admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)]
        )
        codes = {t: c for t, c, *_ in first.topic_errors}
        assert codes[topic] == 0

        duplicate = await admin.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1)]
        )
        codes = {t: c for t, c, *_ in duplicate.topic_errors}
        assert codes[topic] == TopicAlreadyExistsError.errno  # 36, NO raise
    finally:
        await admin.close()
