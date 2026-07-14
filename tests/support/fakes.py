"""Test-owned Kafka fakes (plan §4.2/§4.3) — the one permitted mock boundary.

They implement the exact aiokafka call surface the production code uses,
with real byte/type shapes (headers ``(str, bytes)``, offsets from 0, LEO =
last offset + 1). Contract-tested in tests/unit/test_fakes.py and shared
with the real client via the conformance suite.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any

from aiokafka import TopicPartition
from aiokafka.errors import OffsetOutOfRangeError, TopicAlreadyExistsError
from aiokafka.structs import RecordMetadata


@dataclass
class TopicMeta:
    num_partitions: int
    configs: dict[str, str]


@dataclass(frozen=True)
class _DescribeConfigsResponse:
    """Mirrors the real response surface: a ``.resources`` attribute whose
    entries are (error_code, error_message, resource_type, resource_name,
    config_entries) tuples (pinned against aiokafka's wire schema).
    """

    resources: list[tuple[int, str | None, int, str, list[tuple[Any, ...]]]]


@dataclass(frozen=True)
class _CreateTopicsResponse:
    """Mirrors the real response surface: per-topic outcomes come back as
    ``topic_errors`` entries of (topic, error_code, error_message) — the
    real client returns them and NEVER raises them (pinned by CF-08 and
    tests/unit/test_fakes.py).
    """

    topic_errors: list[tuple[str, int, str | None]]


@dataclass
class MutableClock:
    """Scripted clock (plan §4.2): the fake consumer advances it per poll via
    ``on_poll``, so deadline tests are exact, never wall-clock.
    """

    value: float = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, delta: float) -> None:
        self.value += delta


@dataclass
class TwoSidedGate:
    """Two-sided event gate (plan §4.2): the fake sets ``arrived`` then
    awaits ``proceed``, so tests choreograph rather than race.
    """

    arrived: asyncio.Event = field(default_factory=asyncio.Event)
    proceed: asyncio.Event = field(default_factory=asyncio.Event)

    async def pass_through(self) -> None:
        self.arrived.set()
        await self.proceed.wait()


@dataclass(frozen=True)
class StoredRecord:
    """ConsumerRecord-shaped: the exact attribute surface replay reads."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes], ...]


@dataclass
class InMemoryKafka:
    """One shared per-topic log store behind FakeProducer/Consumer/Admin."""

    _logs: dict[tuple[str, int], list[StoredRecord]] = field(default_factory=dict)
    _next_offsets: dict[tuple[str, int], int] = field(default_factory=dict)
    _log_starts: dict[tuple[str, int], int] = field(default_factory=dict)
    _topics: dict[str, TopicMeta] = field(default_factory=dict)

    def create_topic(
        self, name: str, *, num_partitions: int, configs: dict[str, str]
    ) -> None:
        self._topics[name] = TopicMeta(num_partitions=num_partitions, configs=configs)

    def topic_exists(self, name: str) -> bool:
        return name in self._topics

    def topic_names(self) -> list[str]:
        return sorted(self._topics)

    def topic_config(self, name: str) -> dict[str, str]:
        return dict(self._topics[name].configs)

    def topic_partitions(self, name: str) -> int:
        return self._topics[name].num_partitions

    def records(self, topic: str, partition: int) -> list[StoredRecord]:
        return list(self._logs.get((topic, partition), []))

    def log_start(self, topic: str, partition: int) -> int:
        return self._log_starts.get((topic, partition), 0)

    def log_end(self, topic: str, partition: int) -> int:
        """LEO = last available offset + 1; 0 for a never-written topic."""
        return self._next_offsets.get((topic, partition), 0)

    def delete_topic(self, name: str) -> None:
        """Drop the topic and its logs entirely (the E2E-08b recreate path)."""
        self._topics.pop(name, None)
        for tp in [tp for tp in self._logs if tp[0] == name]:
            self._logs.pop(tp, None)
            self._next_offsets.pop(tp, None)
            self._log_starts.pop(tp, None)

    def trim(self, topic: str, partition: int, *, before_offset: int) -> None:
        """Delete records below ``before_offset`` (retention / DeleteRecords)."""
        tp = (topic, partition)
        self._log_starts[tp] = max(self.log_start(topic, partition), before_offset)
        self._logs[tp] = [
            r for r in self._logs.get(tp, []) if r.offset >= before_offset
        ]

    def append(
        self,
        topic: str,
        partition: int,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: tuple[tuple[str, bytes], ...],
    ) -> StoredRecord:
        tp = (topic, partition)
        log = self._logs.setdefault(tp, [])
        offset = self._next_offsets.get(tp, 0)
        record = StoredRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            key=key,
            value=value,
            headers=headers,
        )
        log.append(record)
        self._next_offsets[tp] = offset + 1
        return record


class FakeConsumer:
    """In-memory ``AIOKafkaConsumer`` double (plan §4.3). Semantics pinned by
    the contract tests: ``seek`` sync and never raising, OOR raised from the
    FETCH and only under ``auto_offset_reset="none"`` (silent reset
    otherwise), ``end_offsets`` = LEO, ``getmany`` dict honoring
    ``max_records``, injectable spurious empty poll and two-sided gate.
    """

    def __init__(
        self,
        kafka: InMemoryKafka,
        *,
        group_id: str | None = "fake-group",
        enable_auto_commit: bool = True,
        auto_offset_reset: str = "latest",
        bootstrap_servers: str | None = None,
    ) -> None:
        self._kafka = kafka
        self.group_id = group_id
        self.enable_auto_commit = enable_auto_commit
        self.auto_offset_reset = auto_offset_reset
        self.bootstrap_servers = bootstrap_servers
        self._assignment: list[TopicPartition] = []
        self._positions: dict[TopicPartition, int] = {}
        self._spurious_empty = False
        self._gate: TwoSidedGate | None = None
        self._gate_at_call = 0
        self._fail_at_call = 0
        self._fail_with: BaseException | None = None
        self.on_poll: object = None
        self.event_log: list[str] = []
        self.getmany_calls = 0
        self.served_offsets: list[int] = []  # observability for fetch-count asserts
        self.started = False
        self.stopped = False

    def inject_spurious_empty_poll(self) -> None:
        """The next fetch returns ``{}`` once mid-stream, then continues —
        real prefetch behavior (pinned; consumed by RH-27).
        """
        self._spurious_empty = True

    def gate_next_getmany(self, gate: TwoSidedGate, *, skip: int = 0) -> None:
        """Park the (skip+1)-th subsequent getmany call on the gate."""
        self._gate = gate
        self._gate_at_call = self.getmany_calls + 1 + skip

    def fail_getmany_at_call(self, call_number: int, exc: BaseException) -> None:
        """Raise ``exc`` from the fetch when getmany_calls reaches N."""
        self._fail_at_call = call_number
        self._fail_with = exc

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def assign(self, partitions: list[TopicPartition]) -> None:
        self._assignment = list(partitions)

    def seek(self, partition: TopicPartition, offset: int) -> None:
        # Sync; NEVER raises OOR (the raise point is the fetch — pinned).
        self._positions[partition] = offset

    async def seek_to_beginning(self, *partitions: TopicPartition) -> None:
        for tp in partitions:
            self._positions[tp] = self._kafka.log_start(tp.topic, tp.partition)

    async def beginning_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]:
        return {tp: self._kafka.log_start(tp.topic, tp.partition) for tp in partitions}

    async def end_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]:
        return {tp: self._kafka.log_end(tp.topic, tp.partition) for tp in partitions}

    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = 0,
        max_records: int | None = None,
    ) -> dict[TopicPartition, list[StoredRecord]]:
        self.getmany_calls += 1
        self.event_log.append("poll")
        if callable(self.on_poll):
            self.on_poll()
        if self._gate is not None and self.getmany_calls >= self._gate_at_call:
            gate, self._gate = self._gate, None
            await gate.pass_through()
        if self._fail_with is not None and self.getmany_calls == self._fail_at_call:
            exc, self._fail_with = self._fail_with, None
            raise exc
        if self._spurious_empty:
            self._spurious_empty = False
            return {}

        targets = list(partitions) or self._assignment
        remaining = max_records if max_records is not None else float("inf")
        out: dict[TopicPartition, list[StoredRecord]] = {}
        for tp in targets:
            if remaining <= 0:
                break
            position = self._positions.get(tp, 0)
            log_start = self._kafka.log_start(tp.topic, tp.partition)
            log_end = self._kafka.log_end(tp.topic, tp.partition)
            if position < log_start or position > log_end:
                if self.auto_offset_reset == "none":
                    raise OffsetOutOfRangeError({tp: position})
                if self.auto_offset_reset == "earliest":
                    position = log_start
                else:  # "latest": SILENT reset — the RH-09 trap (pinned)
                    position = log_end
                self._positions[tp] = position
            available = [
                r
                for r in self._kafka.records(tp.topic, tp.partition)
                if r.offset >= position
            ]
            taken = available[: int(min(remaining, len(available)))]
            if taken:
                out[tp] = taken
                self._positions[tp] = taken[-1].offset + 1
                remaining -= len(taken)
                self.served_offsets.extend(r.offset for r in taken)
        return out


class FakeAdmin:
    """In-memory ``AIOKafkaAdminClient`` double (plan §4.3)."""

    def __init__(
        self,
        kafka: InMemoryKafka,
        *,
        describe_fail_with: BaseException | None = None,
        describe_error_code: int | None = None,
        create_error_code: int | None = None,
        bootstrap_servers: str | None = None,
    ) -> None:
        self._kafka = kafka
        self._describe_fail_with = describe_fail_with
        self._describe_error_code = describe_error_code
        self._create_error_code = create_error_code
        self.bootstrap_servers = bootstrap_servers
        self.describe_calls = 0
        self.create_calls = 0
        self.started = False
        self.closed = False

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def list_topics(self) -> list[str]:
        return self._kafka.topic_names()

    async def create_topics(self, new_topics: list[Any]) -> _CreateTopicsResponse:
        # Per-topic outcomes are response error codes, never raises — the
        # real client's create_topics does not inspect topic_errors.
        self.create_calls += 1
        topic_errors: list[tuple[str, int, str | None]] = []
        for topic in new_topics:
            if self._create_error_code is not None:
                topic_errors.append((topic.name, self._create_error_code, "injected"))
                continue
            if self._kafka.topic_exists(topic.name):
                topic_errors.append(
                    (
                        topic.name,
                        TopicAlreadyExistsError.errno,
                        f"topic {topic.name!r} exists",
                    )
                )
                continue
            self._kafka.create_topic(
                topic.name,
                num_partitions=topic.num_partitions,
                configs=dict(topic.topic_configs or {}),
            )
            topic_errors.append((topic.name, 0, None))
        return _CreateTopicsResponse(topic_errors=topic_errors)

    async def describe_configs(
        self, config_resources: list[Any]
    ) -> list[_DescribeConfigsResponse]:
        self.describe_calls += 1
        if self._describe_fail_with is not None:
            raise self._describe_fail_with
        if self._describe_error_code is not None:
            # Broker-reported failures (authz denials included) come back as
            # per-resource error codes with EMPTY config entries.
            return [
                _DescribeConfigsResponse(
                    resources=[
                        (self._describe_error_code, "injected error", 2, r.name, [])
                        for r in config_resources
                    ]
                )
            ]
        resources: list[tuple[int, str | None, int, str, list[tuple[Any, ...]]]] = []
        for resource in config_resources:
            name = resource.name
            configs = (
                self._kafka.topic_config(name) if self._kafka.topic_exists(name) else {}
            )
            entries: list[tuple[Any, ...]] = [
                (key, value, False, 5, False, [])
                for key, value in sorted(configs.items())
            ]
            resources.append((0, None, 2, name, entries))
        return [_DescribeConfigsResponse(resources=resources)]

    async def delete_records(
        self, records_to_delete: dict[TopicPartition, Any]
    ) -> dict[TopicPartition, int]:
        out = {}
        for tp, spec in records_to_delete.items():
            self._kafka.trim(tp.topic, tp.partition, before_offset=spec.before_offset)
            out[tp] = self._kafka.log_start(tp.topic, tp.partition)
        return out


def _record_metadata(topic: str, partition: int, offset: int) -> RecordMetadata:
    return RecordMetadata(
        topic=topic,
        partition=partition,
        topic_partition=None,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        log_start_offset=0,
    )


class FakeProducer:
    """In-memory ``AIOKafkaProducer`` double (plan §4.3)."""

    def __init__(
        self,
        kafka: InMemoryKafka,
        *,
        fail_with: BaseException | None = None,
        force_offset: int | None = None,
        duplicate_on_retry: bool = False,
        phantom_failure: BaseException | None = None,
    ) -> None:
        # Real clients accept no unknown kwargs; keyword-only injectables
        # make an unknown kwarg a TypeError exactly like the real ctor.
        self._kafka = kafka
        self._fail_with = fail_with
        self._force_offset = force_offset
        self._duplicate_on_retry = duplicate_on_retry
        self._phantom_failure = phantom_failure
        self._delay_gate: TwoSidedGate | None = None
        self.started = False
        self.stopped = False
        self.flushed = 0

    def delay_next(self, gate: TwoSidedGate) -> None:
        """Park the next ``send_and_wait`` on a two-sided gate (one-shot)."""
        self._delay_gate = gate

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def flush(self) -> None:
        self.flushed += 1

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> RecordMetadata:
        if headers is not None and not isinstance(headers, list):
            # Faithful to aiokafka's (Cython) record-batch builder, which
            # rejects a non-list here with exactly this message.
            raise TypeError(f"Expected list, got {type(headers).__name__}")
        if self._delay_gate is not None:
            gate, self._delay_gate = self._delay_gate, None
            await gate.pass_through()
        if self._fail_with is not None:
            raise self._fail_with
        target_partition = 0 if partition is None else partition
        record = self._kafka.append(
            topic,
            target_partition,
            key=key,
            value=value,
            headers=tuple(headers or ()),
        )
        if self._duplicate_on_retry:
            # Real retry semantics: identical bytes at a second offset, and
            # the producer reports the LAST offset (pinned).
            record = self._kafka.append(
                topic,
                target_partition,
                key=key,
                value=value,
                headers=tuple(headers or ()),
            )
        if self._phantom_failure is not None:
            # A lost ack: the record landed but the caller sees an error.
            raise self._phantom_failure
        offset = record.offset if self._force_offset is None else self._force_offset
        return _record_metadata(topic, target_partition, offset)
