"""The fakes' own contract tests (plan §4.3). Every behavior here was
verified against aiokafka 0.13/0.14 source or a live broker during the plan
review; the conformance suite (CF-01..07) re-runs shared bodies against the
real client under ``-m e2e``.
"""

import asyncio

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError, OffsetOutOfRangeError

from tests.support.fakes import (
    FakeConsumer,
    FakeProducer,
    InMemoryKafka,
    TwoSidedGate,
)


async def test_fake_producer_produce_metadata_and_offsets() -> None:
    kafka = InMemoryKafka()
    producer = FakeProducer(kafka)
    await producer.start()

    # Offsets are monotonic FROM 0 per topic-partition (pinned).
    m0 = await producer.send_and_wait(
        "t.p0.changelog",
        value=b'{"a":1}',
        key=b"k",
        partition=0,
        headers=[("format_version", b"1"), ("message_id", b"m-0")],
    )
    m1 = await producer.send_and_wait(
        "t.p0.changelog", value=b"{}", key=b"k2", partition=0, headers=[]
    )
    other = await producer.send_and_wait(
        "other.p1.changelog", value=b"{}", key=b"k", partition=0, headers=[]
    )
    assert (m0.offset, m1.offset, other.offset) == (0, 1, 0)
    assert m0.topic == "t.p0.changelog"
    assert m0.partition == 0

    # The shared in-memory log stores real byte/type shapes.
    stored = kafka.records("t.p0.changelog", 0)
    assert [r.offset for r in stored] == [0, 1]
    assert stored[0].key == b"k"
    assert stored[0].value == b'{"a":1}'
    assert stored[0].headers == (("format_version", b"1"), ("message_id", b"m-0"))

    await producer.flush()
    await producer.stop()


def test_fake_producer_rejects_unknown_ctor_kwargs() -> None:
    # Real aiokafka clients accept no **kwargs (pinned; CF-07 mirrors this).
    with pytest.raises(TypeError):
        # The invalid kwarg is the point: pin the runtime rejection.
        FakeProducer(InMemoryKafka(), definitely_not_a_real_kwarg=1)  # type: ignore[call-arg]


async def test_fake_producer_injectable_failure_and_bad_offset() -> None:
    kafka = InMemoryKafka()

    failing = FakeProducer(kafka, fail_with=KafkaError("broker down"))
    await failing.start()
    with pytest.raises(KafkaError):
        await failing.send_and_wait("t", value=b"{}", key=b"k", partition=0)
    assert kafka.records("t", 0) == []  # nothing landed
    await failing.stop()

    bad_offset = FakeProducer(kafka, force_offset=-1)
    await bad_offset.start()
    metadata = await bad_offset.send_and_wait("t", value=b"{}", key=b"k", partition=0)
    assert metadata.offset == -1
    await bad_offset.stop()


async def test_fake_producer_phantom_failure() -> None:
    """A lost ack: the record durably lands but the producer call raises —
    the §14 host-retry-duplicate precondition (consumed by W-14).
    """
    kafka = InMemoryKafka()
    producer = FakeProducer(kafka, phantom_failure=KafkaError("ack lost"))
    await producer.start()

    with pytest.raises(KafkaError):
        await producer.send_and_wait("t", value=b"{}", key=b"k", partition=0)

    assert len(kafka.records("t", 0)) == 1  # landed despite the error


async def test_fake_producer_two_sided_delay() -> None:
    """Two-sided gates everywhere (plan §4.2): the fake parks and signals
    ``arrived``, the test choreographs via ``proceed`` — never a race.
    """
    kafka = InMemoryKafka()
    producer = FakeProducer(kafka)
    gate = TwoSidedGate()
    producer.delay_next(gate)

    task = asyncio.ensure_future(
        producer.send_and_wait("t", value=b"{}", key=b"k", partition=0)
    )
    await asyncio.wait_for(gate.arrived.wait(), 5)
    assert kafka.records("t", 0) == []  # parked: nothing landed yet

    gate.proceed.set()
    metadata = await asyncio.wait_for(task, 5)
    assert metadata.offset == 0
    assert len(kafka.records("t", 0)) == 1


async def _seed_log(kafka: InMemoryKafka, topic: str, n: int) -> None:
    producer = FakeProducer(kafka)
    for i in range(n):
        await producer.send_and_wait(
            topic,
            value=b"{}",
            key=f"k{i}".encode(),
            partition=0,
            headers=[("message_id", f"m{i}".encode())],
        )


async def test_fake_consumer_seek_end_offsets_and_getmany() -> None:
    kafka = InMemoryKafka()
    await _seed_log(kafka, "log", 3)
    tp = TopicPartition("log", 0)

    consumer = FakeConsumer(
        kafka, group_id=None, enable_auto_commit=False, auto_offset_reset="none"
    )
    await consumer.start()
    consumer.assign([tp])

    # end_offsets returns LEO = last offset + 1; empty topic -> 0 (pinned).
    ends = await consumer.end_offsets([tp])
    assert ends[tp] == 3
    # beginning_offsets returns the log start (0 until a trim).
    assert (await consumer.beginning_offsets([tp]))[tp] == 0
    empty_tp = TopicPartition("never-written", 0)
    assert (await consumer.end_offsets([empty_tp]))[empty_tp] == 0

    # seek is SYNC and never raises OOR (pinned; raise point is the fetch);
    # seek_to_beginning is a coroutine (pinned).
    import inspect

    assert not inspect.iscoroutinefunction(FakeConsumer.seek)
    assert inspect.iscoroutinefunction(FakeConsumer.seek_to_beginning)
    consumer.seek(tp, 0)

    # getmany returns dict[TopicPartition, list[record]] honoring max_records.
    batch = await consumer.getmany(timeout_ms=0, max_records=2)
    assert set(batch) == {tp}
    assert [r.offset for r in batch[tp]] == [0, 1]
    record = batch[tp][0]
    assert (record.topic, record.partition) == ("log", 0)
    assert record.key == b"k0" and isinstance(record.value, bytes)
    assert record.headers == (("message_id", b"m0"),)

    assert [r.offset for r in (await consumer.getmany(max_records=2))[tp]] == [2]
    assert await consumer.getmany(max_records=2) == {}  # caught up

    await consumer.stop()


async def test_fake_consumer_oor_from_fetch_iff_none() -> None:
    """Pinned (live-verified): OffsetOutOfRangeError is raised FROM THE
    FETCH, and only under auto_offset_reset="none"; "latest" silently resets
    — an implementation that forgets "none" fails RH-09 instead of passing
    vacuously.
    """
    kafka = InMemoryKafka()
    await _seed_log(kafka, "log", 5)
    kafka.trim("log", 0, before_offset=3)  # log_start becomes 3
    tp = TopicPartition("log", 0)

    strict = FakeConsumer(
        kafka, group_id=None, enable_auto_commit=False, auto_offset_reset="none"
    )
    strict.assign([tp])
    strict.seek(tp, 0)  # below log_start; seek itself NEVER raises
    with pytest.raises(OffsetOutOfRangeError):
        await strict.getmany(max_records=10)

    lenient = FakeConsumer(
        kafka, group_id=None, enable_auto_commit=False, auto_offset_reset="latest"
    )
    lenient.assign([tp])
    lenient.seek(tp, 0)
    assert await lenient.getmany(max_records=10) == {}  # silent reset to LEO
    await _seed_log(kafka, "log", 1)  # one new record at offset 5
    batch = await lenient.getmany(max_records=10)
    assert [r.offset for r in batch[tp]] == [5]  # only the new tail

    # A position ABOVE the log end is also out of range (real Kafka).
    above = FakeConsumer(
        kafka, group_id=None, enable_auto_commit=False, auto_offset_reset="none"
    )
    above.assign([tp])
    above.seek(tp, 999)
    with pytest.raises(OffsetOutOfRangeError):
        await above.getmany(max_records=10)
    # ...but a position EXACTLY at the LEO is caught-up, not out of range.
    above.seek(tp, 6)
    assert await above.getmany(max_records=10) == {}


async def test_fake_consumer_spurious_empty_poll_and_gate() -> None:
    kafka = InMemoryKafka()
    await _seed_log(kafka, "log", 4)
    tp = TopicPartition("log", 0)

    consumer = FakeConsumer(
        kafka, group_id=None, enable_auto_commit=False, auto_offset_reset="none"
    )
    consumer.assign([tp])
    consumer.seek(tp, 0)

    first = await consumer.getmany(max_records=2)
    assert [r.offset for r in first[tp]] == [0, 1]
    consumer.inject_spurious_empty_poll()  # mid-stream
    assert await consumer.getmany(max_records=2) == {}  # the spurious {}
    second = await consumer.getmany(max_records=2)
    assert [r.offset for r in second[tp]] == [2, 3]  # then continues

    # Two-sided gate + poll events into a shared event log.
    events: list[str] = []
    consumer.event_log = events
    gate = TwoSidedGate()
    consumer.gate_next_getmany(gate)
    task = asyncio.ensure_future(consumer.getmany(max_records=2))
    await asyncio.wait_for(gate.arrived.wait(), 5)
    gate.proceed.set()
    await asyncio.wait_for(task, 5)
    assert events == ["poll"]


def test_fake_consumer_rejects_unknown_ctor_kwargs() -> None:
    with pytest.raises(TypeError):
        FakeConsumer(InMemoryKafka(), definitely_bogus=1)  # type: ignore[call-arg]


async def test_fake_admin_describe_configs_resources_shape() -> None:
    """Pinned (V1): describe_configs returns response objects whose
    ``.resources`` entries must be WALKED — resource tuples of (error_code,
    error_message, resource_type, resource_name, config_entries) with
    entries (name, value, ...)."""
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

    from tests.support.fakes import FakeAdmin

    kafka = InMemoryKafka()
    kafka.create_topic(
        "t.p0.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "delete", "retention.ms": "-1"},
    )
    admin = FakeAdmin(kafka)
    await admin.start()

    responses = await admin.describe_configs(
        [ConfigResource(ConfigResourceType.TOPIC, "t.p0.changelog")]
    )
    assert isinstance(responses, list)
    found: dict[str, str] = {}
    for response in responses:
        for resource in response.resources:
            error_code, _msg, _rtype, resource_name, entries = resource
            assert error_code == 0
            assert resource_name == "t.p0.changelog"
            for entry in entries:
                found[entry[0]] = entry[1]
    assert found["cleanup.policy"] == "delete"
    assert found["retention.ms"] == "-1"
    await admin.close()


async def test_fake_admin_create_topics_list_topics_and_already_exists() -> None:
    from aiokafka.admin import NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    from tests.support.fakes import FakeAdmin

    kafka = InMemoryKafka()
    admin = FakeAdmin(kafka)

    assert await admin.list_topics() == []
    response = await admin.create_topics(
        [
            NewTopic(
                "x.changelog",
                1,
                1,
                topic_configs={"cleanup.policy": "delete", "retention.ms": "-1"},
            )
        ]
    )
    # Pinned against the real client (CF-08): per-topic outcomes are error
    # codes ON THE RESPONSE — aiokafka's create_topics never raises them.
    assert [(t, c) for t, c, *_ in response.topic_errors] == [("x.changelog", 0)]
    assert await admin.list_topics() == ["x.changelog"]
    assert kafka.topic_config("x.changelog")["retention.ms"] == "-1"
    assert kafka.topic_partitions("x.changelog") == 1

    duplicate = await admin.create_topics([NewTopic("x.changelog", 1, 1)])
    codes = {t: c for t, c, *_ in duplicate.topic_errors}
    assert codes["x.changelog"] == TopicAlreadyExistsError.errno  # 36, NO raise


async def test_fake_admin_injectable_failures_and_delete_records() -> None:
    from aiokafka.admin import NewTopic
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
    from aiokafka.admin.records_to_delete import RecordsToDelete
    from aiokafka.errors import KafkaConnectionError, TopicAuthorizationFailedError

    from tests.support.fakes import FakeAdmin

    kafka = InMemoryKafka()
    kafka.create_topic("log", num_partitions=1, configs={})
    await _seed_log(kafka, "log", 5)

    # Connection-level failures RAISE — the real client raises those too.
    down = FakeAdmin(kafka, describe_fail_with=KafkaConnectionError("down"))
    with pytest.raises(KafkaConnectionError):
        await down.describe_configs([ConfigResource(ConfigResourceType.TOPIC, "log")])

    # Broker-reported failures (authorization denials included) come back as
    # PER-RESOURCE error codes with empty config entries — never as raised
    # exceptions (pinned against aiokafka's describe_configs).
    denied = FakeAdmin(kafka, describe_error_code=TopicAuthorizationFailedError.errno)
    responses = await denied.describe_configs(
        [ConfigResource(ConfigResourceType.TOPIC, "log")]
    )
    (resource,) = responses[0].resources
    error_code, _msg, _rtype, resource_name, entries = resource
    assert error_code == TopicAuthorizationFailedError.errno
    assert resource_name == "log"
    assert entries == []

    # Per-topic create failures likewise come back as response codes.
    refused = FakeAdmin(kafka, create_error_code=41)  # NOT_CONTROLLER
    response = await refused.create_topics([NewTopic("new-topic", 1, 1)])
    assert [(t, c) for t, c, *_ in response.topic_errors] == [("new-topic", 41)]
    assert not kafka.topic_exists("new-topic")

    admin = FakeAdmin(kafka)
    await admin.delete_records(
        {TopicPartition("log", 0): RecordsToDelete(before_offset=3)}
    )
    assert kafka.log_start("log", 0) == 3
    assert [r.offset for r in kafka.records("log", 0)] == [3, 4]


async def test_fake_producer_duplicate_on_retry() -> None:
    """Matches real retry semantics (pinned): the same bytes land at TWO
    offsets and the returned metadata reports the LAST one.
    """
    kafka = InMemoryKafka()
    producer = FakeProducer(kafka, duplicate_on_retry=True)
    await producer.start()

    metadata = await producer.send_and_wait(
        "t", value=b'{"v":1}', key=b"k", partition=0, headers=[("message_id", b"m")]
    )

    stored = kafka.records("t", 0)
    assert [r.offset for r in stored] == [0, 1]
    assert stored[0].value == stored[1].value == b'{"v":1}'
    assert stored[0].key == stored[1].key == b"k"
    assert stored[0].headers == stored[1].headers == (("message_id", b"m"),)
    assert metadata.offset == 1  # the LAST offset
    await producer.stop()
