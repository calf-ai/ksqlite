"""P9 `_store` facade tests: F-01..F-13 (plan §5 P9). Fakes + real SQLite."""

from pathlib import Path
from typing import Any

import pytest
from aiokafka import TopicPartition

from ksqlite import KSQLite
from ksqlite.errors import ConfigError
from ksqlite.types import GeneratedColumn, Index
from tests.support import fakes

SOURCE = TopicPartition("messages", 3)
CHANGELOG = "messages.p3.changelog"


class FakeClientFactory:
    """A KafkaClientFactory (spec §4 seam): sync methods, receives the
    fully-merged final kwargs, returns constructed-but-unstarted fakes.
    """

    def __init__(self, kafka: fakes.InMemoryKafka) -> None:
        self.kafka = kafka
        self.producer_kwargs: dict[str, Any] | None = None
        self.consumer_kwargs: dict[str, Any] | None = None
        self.admin_kwargs: dict[str, Any] | None = None
        self.producers: list[Any] = []
        self.consumers: list[fakes.FakeConsumer] = []
        self.admins: list[Any] = []
        self.producer_start_error: BaseException | None = None
        self.admin_start_error: BaseException | None = None

    def producer(self, **kwargs: Any) -> Any:
        self.producer_kwargs = kwargs
        producer: Any
        if self.producer_start_error is not None:
            producer = _FailingStartClient(self.producer_start_error)
        else:
            producer = fakes.FakeProducer(self.kafka)
        self.producers.append(producer)
        return producer

    configure_consumer: Any = None

    def consumer(self, **kwargs: Any) -> fakes.FakeConsumer:
        self.consumer_kwargs = kwargs
        consumer = fakes.FakeConsumer(
            self.kafka,
            group_id=kwargs.get("group_id", "wrong"),
            enable_auto_commit=kwargs.get("enable_auto_commit", True),
            auto_offset_reset=kwargs.get("auto_offset_reset", "latest"),
        )
        if callable(self.configure_consumer):
            self.configure_consumer(consumer)
        self.consumers.append(consumer)
        return consumer

    def admin_client(self, **kwargs: Any) -> Any:
        self.admin_kwargs = kwargs
        admin: Any
        if self.admin_start_error is not None:
            admin = _FailingStartClient(self.admin_start_error)
        else:
            admin = fakes.FakeAdmin(self.kafka)
        self.admins.append(admin)
        return admin

    @property
    def total_calls(self) -> int:
        return len(self.producers) + len(self.consumers) + len(self.admins)


def make_store(tmp_path: Path, factory: FakeClientFactory, **overrides: Any) -> KSQLite:
    kwargs: dict[str, Any] = {
        "db_path": tmp_path / "state.db",
        "bootstrap_servers": "fake:9092",
        "kafka_clients": factory,
        "busy_timeout": 0.5,
    }
    kwargs.update(overrides)
    return KSQLite(**kwargs)


async def test_f01_validation_matrix_from_start_before_any_io(
    tmp_path: Path,
) -> None:
    bad_configs: list[dict[str, Any]] = [
        {"changelog_topic_template": "missing-placeholders"},
        {"changelog_topic_template": "{source_topic}.only"},
        {"pool_size": 0},
        {"busy_timeout": 0},
        {"rehydrate_timeout": -1},
        {"producer_config": {"acks": 0}},
        {"generated_columns": [GeneratedColumn("payload", "$.x")]},  # system
        {"generated_columns": [GeneratedColumn("bad name", "$.x")]},
        {"generated_columns": [GeneratedColumn("c", "not-a-path")]},
        {"generated_columns": [GeneratedColumn("c", "$.c", "TEXT); DROP")]},
        {"indexes": [Index("ix", ["nonexistent_column"])]},
        {"indexes": [Index("bad-name", ["entity_key"])]},
    ]
    for overrides in bad_configs:
        factory = FakeClientFactory(fakes.InMemoryKafka())
        store = make_store(tmp_path, factory, **overrides)
        with pytest.raises(ConfigError):
            await store.start()
        # BEFORE any I/O: no broker client constructed, no DB file created.
        assert factory.total_calls == 0, overrides
        assert not (tmp_path / "state.db").exists(), overrides


class ProbePool:
    """Pool-seam probe: real adapter + an observable closed flag."""

    def __init__(self, inner: Any, order: list[str] | None = None) -> None:
        self.inner = inner
        self.closed = False
        self._order = order

    def connection(self) -> Any:
        return self.inner.connection()

    async def close(self) -> None:
        self.closed = True
        if self._order is not None:
            self._order.append("pool_close")
        await self.inner.close()


class _FailingStartClient:
    """A constructed-but-unstarted client whose start() raises."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.stopped = False

    async def start(self) -> None:
        raise self._exc

    async def stop(self) -> None:
        self.stopped = True

    async def flush(self) -> None:  # pragma: no cover - never reached
        pass

    async def close(self) -> None:
        self.stopped = True


def _seed_stale_owned_row(db_path: Path) -> None:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE _owned (source_topic TEXT NOT NULL,"
            " source_partition INTEGER NOT NULL,"
            " PRIMARY KEY (source_topic, source_partition))"
        )
        conn.execute("INSERT INTO _owned VALUES ('stale', 0)")
        conn.commit()
    finally:
        conn.close()


async def test_f02_start_sequence(tmp_path: Path) -> None:
    factory = FakeClientFactory(fakes.InMemoryKafka())
    _seed_stale_owned_row(tmp_path / "state.db")
    store = make_store(tmp_path, factory)

    await store.start()
    try:
        assert factory.producers[0].started
        assert factory.admins[0].started
        assert factory.consumers == []  # rehydrate consumer NOT started (lazy)
        # Migration ran, including the _owned clear (spec §4/§5.1).
        rows = await store.query("SELECT COUNT(*) AS n FROM _owned")
        assert rows[0]["n"] == 0
        rows = await store.query("SELECT COUNT(*) AS n FROM _records")
        assert rows[0]["n"] == 0
    finally:
        await store.stop()


async def test_f03_rollback_on_partial_failure(tmp_path: Path) -> None:
    from aiokafka.errors import KafkaError

    from ksqlite.errors import KSQLiteError, SchemaMigrationError, StorageError

    # (1) pool open fails -> StorageError; nothing else was touched.
    factory = FakeClientFactory(fakes.InMemoryKafka())
    store = make_store(tmp_path, factory, db_path=tmp_path / "no-such-dir" / "state.db")
    with pytest.raises(StorageError):
        await store.start()
    assert factory.total_calls == 0

    # (2) migrate fails -> SchemaMigrationError; no Kafka client constructed.
    import sqlite3

    conn = sqlite3.connect(tmp_path / "m.db")
    conn.execute("CREATE TABLE ix_collide (x)")  # namespace collision (M-10)
    conn.commit()
    conn.close()
    factory = FakeClientFactory(fakes.InMemoryKafka())
    store = make_store(
        tmp_path,
        factory,
        db_path=tmp_path / "m.db",
        indexes=[Index("ix_collide", ["entity_key"])],
    )
    with pytest.raises(SchemaMigrationError):
        await store.start()
    assert factory.total_calls == 0

    # (3) producer start fails -> base KSQLiteError, cause chained; the pool
    # (via the seam) is closed.
    factory = FakeClientFactory(fakes.InMemoryKafka())
    factory.producer_start_error = KafkaError("no broker")
    pool = ProbePool(
        __import__("ksqlite")._pool.AiosqlitePoolAdapter(
            tmp_path / "p.db", pool_size=2, busy_timeout=0.5
        )
    )
    store = make_store(tmp_path, factory, db_path=tmp_path / "p.db", pool=pool)
    with pytest.raises(KSQLiteError) as exc_info:
        await store.start()
    assert type(exc_info.value) is KSQLiteError  # the base class, no subclass
    assert isinstance(exc_info.value.__cause__, KafkaError)
    assert pool.closed

    # (4) admin start fails -> producer stopped AND pool closed.
    factory = FakeClientFactory(fakes.InMemoryKafka())
    factory.admin_start_error = KafkaError("no broker for admin")
    pool = ProbePool(
        __import__("ksqlite")._pool.AiosqlitePoolAdapter(
            tmp_path / "a.db", pool_size=2, busy_timeout=0.5
        )
    )
    store = make_store(tmp_path, factory, db_path=tmp_path / "a.db", pool=pool)
    with pytest.raises(KSQLiteError):
        await store.start()
    assert factory.producers[0].stopped
    assert pool.closed


async def test_f04_double_start(tmp_path: Path) -> None:
    from ksqlite.errors import LifecycleError

    factory = FakeClientFactory(fakes.InMemoryKafka())
    store = make_store(tmp_path, factory)
    await store.start()
    try:
        with pytest.raises(LifecycleError):
            await store.start()
    finally:
        await store.stop()


async def test_f05_stop_ordering(tmp_path: Path) -> None:
    order: list[str] = []
    kafka = fakes.InMemoryKafka()

    class OrderedProducer(fakes.FakeProducer):
        async def flush(self) -> None:
            order.append("producer_flush")
            await super().flush()

        async def stop(self) -> None:
            order.append("producer_stop")
            await super().stop()

    class OrderedConsumer(fakes.FakeConsumer):
        async def stop(self) -> None:
            order.append("consumer_stop")
            await super().stop()

    class OrderedAdmin(fakes.FakeAdmin):
        async def close(self) -> None:
            order.append("admin_close")
            await super().close()

    class OrderedFactory(FakeClientFactory):
        def producer(self, **kwargs: Any) -> fakes.FakeProducer:
            self.producer_kwargs = kwargs
            producer = OrderedProducer(self.kafka)
            self.producers.append(producer)
            return producer

        def consumer(self, **kwargs: Any) -> fakes.FakeConsumer:
            self.consumer_kwargs = kwargs
            consumer = OrderedConsumer(
                self.kafka,
                group_id=kwargs.get("group_id", "wrong"),
                enable_auto_commit=kwargs.get("enable_auto_commit", True),
                auto_offset_reset=kwargs.get("auto_offset_reset", "latest"),
            )
            self.consumers.append(consumer)
            return consumer

        def admin_client(self, **kwargs: Any) -> fakes.FakeAdmin:
            self.admin_kwargs = kwargs
            admin = OrderedAdmin(self.kafka)
            self.admins.append(admin)
            return admin

    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    factory = OrderedFactory(kafka)
    inner = __import__("ksqlite")._pool.AiosqlitePoolAdapter(
        tmp_path / "state.db", pool_size=2, busy_timeout=0.5
    )
    store = make_store(tmp_path, factory, pool=ProbePool(inner, order))

    await store.start()
    await store.on_partitions_assigned([SOURCE])  # starts the lazy consumer
    await store.stop()

    assert order == [
        "producer_flush",
        "producer_stop",
        "consumer_stop",
        "admin_close",
        "pool_close",
    ]


async def test_f06_pre_start_and_post_stop_calls(tmp_path: Path) -> None:
    from ksqlite.errors import LifecycleError

    factory = FakeClientFactory(fakes.InMemoryKafka())
    store = make_store(tmp_path, factory)

    with pytest.raises(LifecycleError):
        await store.append(source=SOURCE, entity_key="k", payload={})
    with pytest.raises(LifecycleError):
        await store.query("SELECT 1")
    with pytest.raises(LifecycleError):
        await store.on_partitions_assigned([SOURCE])
    with pytest.raises(LifecycleError):
        await store.on_partitions_revoked([SOURCE])
    with pytest.raises(LifecycleError):
        await store.stop()  # stop() before start()

    await store.start()
    await store.stop()

    with pytest.raises(LifecycleError):
        await store.append(source=SOURCE, entity_key="k", payload={})
    with pytest.raises(LifecycleError):
        await store.query("SELECT 1")
    await store.stop()  # a second stop() is a NO-OP (pinned)


async def test_f07_async_context_manager(tmp_path: Path) -> None:
    from ksqlite.errors import LifecycleError

    factory = FakeClientFactory(fakes.InMemoryKafka())
    async with make_store(tmp_path, factory) as store:
        rows = await store.query("SELECT 1 AS one")
        assert rows[0]["one"] == 1

    with pytest.raises(LifecycleError):  # stopped on exit
        await store.query("SELECT 1")


async def _seed_ksqlite_records(
    kafka: fakes.InMemoryKafka, topic: str, n: int, *, start_index: int = 0
) -> None:
    import uuid as uuid_module

    from ksqlite import _wire

    if not kafka.topic_exists(topic):
        kafka.create_topic(
            topic, num_partitions=1, configs={"cleanup.policy": "delete"}
        )
    producer = fakes.FakeProducer(kafka)
    for i in range(start_index, start_index + n):
        encoded = _wire.encode(
            entity_key=f"key-{i}",
            payload={"i": i},
            message_id=str(
                uuid_module.uuid5(uuid_module.NAMESPACE_URL, f"{topic}:{i}")
            ),
        )
        await producer.send_and_wait(
            topic,
            value=encoded.value,
            key=encoded.key,
            partition=0,
            headers=encoded.headers,
        )


async def test_f08_host_listener_integration(tmp_path: Path) -> None:
    """The §4 integration handshake, pinned by test: store-assign completes
    BEFORE host logic reads state; host flush runs BEFORE store-revoke.
    """
    import aiokafka

    from ksqlite.types import PartitionStatus

    kafka = fakes.InMemoryKafka()
    await _seed_ksqlite_records(kafka, CHANGELOG, 3)
    factory = FakeClientFactory(kafka)
    store = make_store(tmp_path, factory)
    await store.start()
    observed: list[Any] = []

    class HostListener(aiokafka.ConsumerRebalanceListener):  # type: ignore[misc]
        async def on_partitions_assigned(self, assigned: Any) -> None:
            await store.on_partitions_assigned(assigned)
            # ...host logic runs AFTER the store rehydrated:
            observed.append(("assigned", store.partition_states()[SOURCE].state))

        async def on_partitions_revoked(self, revoked: Any) -> None:
            observed.append(("host-flush",))
            await store.on_partitions_revoked(revoked)

    listener = HostListener()
    try:
        await listener.on_partitions_assigned([SOURCE])
        assert observed == [("assigned", PartitionStatus.READY)]

        await listener.on_partitions_revoked([SOURCE])
        assert observed[-1] == ("host-flush",)
        assert store.partition_states()[SOURCE].state is PartitionStatus.STANDBY
    finally:
        await store.stop()


async def test_f09_partition_states_end_to_end(tmp_path: Path) -> None:
    """All four statuses across a scripted sequence. The force-stop leg
    drives the lifecycle service directly with the injected clock — the
    facade has no clock seam (LOCKED API; plan F-09).
    """
    import asyncio

    from ksqlite.types import PartitionStatus
    from tests.support.fakes import TwoSidedGate

    kafka = fakes.InMemoryKafka()
    await _seed_ksqlite_records(kafka, CHANGELOG, 4)
    factory = FakeClientFactory(kafka)
    gate = TwoSidedGate()
    factory.configure_consumer = lambda c: c.gate_next_getmany(gate)
    store = make_store(tmp_path, factory)
    await store.start()
    try:
        assert store.partition_states() == {}  # fresh-process scope (ST-05)

        task = asyncio.ensure_future(store.on_partitions_assigned([SOURCE]))
        await asyncio.wait_for(gate.arrived.wait(), 5)
        state = store.partition_states()[SOURCE]
        assert state.state is PartitionStatus.RESTORING  # mid-replay
        assert state.log_end_offset == 4  # cached LEO

        gate.proceed.set()
        await asyncio.wait_for(task, 5)
        state = store.partition_states()[SOURCE]
        assert state.state is PartitionStatus.READY
        assert state.checkpoint_offset == 3
        assert state.lag == 0

        await store.on_partitions_revoked([SOURCE])
        state = store.partition_states()[SOURCE]
        assert state.state is PartitionStatus.STANDBY
        assert state.checkpoint_offset == 3  # kept
    finally:
        await store.stop()

    # READY_PARTIAL: the lifecycle service directly, with the injected clock
    # (the RH-15 machinery; asserted here at the states surface).
    from ksqlite import _lifecycle as lifecycle_mod
    from ksqlite import _pool as pool_mod
    from ksqlite import _state as state_mod
    from ksqlite import _topics as topics_mod
    from tests.support.fakes import MutableClock

    clock = MutableClock()
    pool = pool_mod.AiosqlitePoolAdapter(
        tmp_path / "partial.db", pool_size=2, busy_timeout=0.5
    )
    try:
        async with pool.connection() as conn:
            from ksqlite import _schema

            await _schema.migrate(conn, generated_columns=(), indexes=())
        machine = state_mod.PartitionStateMachine()
        consumer = fakes.FakeConsumer(
            kafka, group_id=None, enable_auto_commit=False, auto_offset_reset="none"
        )
        consumer.on_poll = lambda: clock.advance(100.0)
        service = lifecycle_mod.PartitionLifecycle(
            pool=pool,
            consumer_factory=lambda: consumer,
            topics=topics_mod.ChangelogTopics(
                admin=fakes.FakeAdmin(kafka),
                template="{source_topic}.p{partition}.changelog",
                create_topics_retention_ms=None,
                verify_changelog_policy=False,
            ),
            state=machine,
            txn_runner=pool_mod.TransactionRunner(busy_timeout=0.5),
            rehydrate_timeout=10.0,
            batch_size=2,
            clock=clock,
        )
        await service.on_partitions_assigned([SOURCE])
        partial = machine.partition_states()[SOURCE]
        assert partial.state is PartitionStatus.READY_PARTIAL
        assert partial.lag is not None and partial.lag > 0
    finally:
        await pool.close()


async def test_f10_structured_logs_table_driven(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Every §4 observability item maps 1:1 onto a stable event name;
    asserted by logger + event (+ fields elsewhere), never substrings.
    """
    import logging

    from aiokafka.errors import KafkaError

    from ksqlite.errors import ChangelogProduceError, ConfigError

    caplog.set_level(logging.INFO)

    # migration_ddl + topic_created + topic_verified + rehydrate start/end +
    # foreign skip + non-READY append, in one store run:
    kafka = fakes.InMemoryKafka()
    await _seed_ksqlite_records(kafka, CHANGELOG, 2)
    foreign_producer = fakes.FakeProducer(kafka)
    await foreign_producer.send_and_wait(CHANGELOG, value=b"x", key=None, partition=0)
    factory = FakeClientFactory(kafka)
    store = make_store(
        tmp_path,
        factory,
        generated_columns=[GeneratedColumn("thread_id", "$.thread_id")],
        indexes=[Index("ix_thread", ["thread_id"])],
        create_topics_retention_ms=-1,
    )
    await store.start()
    # Assign FIRST: replays the foreign record (an append would advance the
    # checkpoint past it and the fast path would skip the replay entirely).
    # The second partition's changelog is missing => topic_created.
    await store.on_partitions_assigned([SOURCE, TopicPartition("messages", 4)])
    # A non-READY append: a partition never assigned to this store.
    await store.append(source=TopicPartition("messages", 5), entity_key="k", payload={})
    await store.stop()

    # orphaned column/index: a second boot with them removed from config.
    store2 = make_store(tmp_path, FakeClientFactory(kafka))
    await store2.start()
    await store2.stop()

    # produce_failure:
    failing_factory = FakeClientFactory(fakes.InMemoryKafka())
    store3 = make_store(tmp_path, failing_factory, db_path=tmp_path / "pf.db")
    await store3.start()
    failing_factory.producers[0]._fail_with = KafkaError("down")  # noqa: SLF001
    with pytest.raises(ChangelogProduceError):
        await store3.append(source=SOURCE, entity_key="k", payload={})
    await store3.stop()

    # compact_rejected:
    compact_kafka = fakes.InMemoryKafka()
    compact_kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "compact"}
    )
    store4 = make_store(
        tmp_path, FakeClientFactory(compact_kafka), db_path=tmp_path / "c.db"
    )
    await store4.start()
    with pytest.raises(ConfigError):
        await store4.on_partitions_assigned([SOURCE])
    await store4.stop()

    # truncation_reset + checkpoint_clamped (fully-trimmed tail):
    trim_kafka = fakes.InMemoryKafka()
    await _seed_ksqlite_records(trim_kafka, CHANGELOG, 3)
    store5 = make_store(
        tmp_path, FakeClientFactory(trim_kafka), db_path=tmp_path / "t.db"
    )
    await store5.start()
    await store5.on_partitions_assigned([SOURCE])
    await store5.on_partitions_revoked([SOURCE])
    await _seed_ksqlite_records(trim_kafka, CHANGELOG, 2, start_index=3)
    trim_kafka.trim(CHANGELOG, 0, before_offset=5)
    await store5.on_partitions_assigned([SOURCE])
    await store5.stop()

    events = {(r.name, getattr(r, "event", None)) for r in caplog.records}
    required = {
        ("ksqlite.schema", "migration_ddl"),
        ("ksqlite.schema", "orphaned_column"),
        ("ksqlite.schema", "orphaned_index"),
        ("ksqlite.topics", "topic_created"),
        ("ksqlite.topics", "topic_verified"),
        ("ksqlite.topics", "compact_rejected"),
        ("ksqlite.write", "append_to_non_ready"),
        ("ksqlite.write", "produce_failure"),
        ("ksqlite.lifecycle", "rehydrate_start"),
        ("ksqlite.lifecycle", "rehydrate_end"),
        ("ksqlite.lifecycle", "foreign_record_skipped"),
        ("ksqlite.lifecycle", "truncation_reset"),
        ("ksqlite.lifecycle", "checkpoint_clamped"),
    }
    missing = required - events
    assert not missing, f"missing structured log events: {missing}"


async def test_f11_stop_drains_in_flight_append(tmp_path: Path) -> None:
    import asyncio

    from tests.support.fakes import TwoSidedGate

    kafka = fakes.InMemoryKafka()
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    factory = FakeClientFactory(kafka)
    store = make_store(tmp_path, factory)
    await store.start()

    gate = TwoSidedGate()
    factory.producers[0].delay_next(gate)
    append_task = asyncio.ensure_future(
        store.append(source=SOURCE, entity_key="k", payload={"v": 1})
    )
    await asyncio.wait_for(gate.arrived.wait(), 5)  # parked mid-produce

    stop_task = asyncio.ensure_future(store.stop())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not stop_task.done()  # draining: waits for the in-flight append

    gate.proceed.set()
    await asyncio.wait_for(append_task, 5)  # completes cleanly, no error
    await asyncio.wait_for(stop_task, 5)

    import sqlite3

    conn = sqlite3.connect(tmp_path / "state.db")
    try:
        count = conn.execute("SELECT COUNT(*) FROM _records").fetchone()[0]
        assert count == 1  # no lost row
    finally:
        conn.close()


def test_f12_ctor_defaults_match_the_spec_table() -> None:
    import inspect

    from ksqlite import _store

    defaults = {
        name: p.default
        for name, p in inspect.signature(KSQLite.__init__).parameters.items()
        if p.default is not inspect.Parameter.empty
    }
    assert (
        defaults["changelog_topic_template"] == "{source_topic}.p{partition}.changelog"
    )
    assert defaults["create_topics_retention_ms"] is None
    assert defaults["verify_changelog_policy"] is True
    assert defaults["pool_size"] == 16
    assert defaults["busy_timeout"] == 5.0
    assert defaults["rehydrate_timeout"] == 30.0
    assert defaults["generated_columns"] == ()
    assert defaults["indexes"] == ()
    assert defaults["kafka_clients"] is None
    assert defaults["pool"] is None
    # acks defaults to 1 through the merge (spec §15).
    merged = _store.merged_producer_kwargs(
        bootstrap_servers="b:9092", producer_config={}
    )
    assert merged["acks"] == 1


async def test_f13_config_pass_through_merge(tmp_path: Path) -> None:
    kafka = fakes.InMemoryKafka()
    kafka.create_topic(
        CHANGELOG, num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    factory = FakeClientFactory(kafka)
    store = make_store(
        tmp_path,
        factory,
        producer_config={"acks": "all", "request_timeout_ms": 5000},
        consumer_config={
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "svc",
            "max_poll_records": 5,
            "auto_offset_reset": "latest",  # tries to override a pinned key
            "group_id": "evil",  # tries to override a pinned key
        },
    )
    await store.start()
    try:
        # Producer: defaults survive partial override; overrides reach it.
        assert factory.producer_kwargs is not None
        assert factory.producer_kwargs["acks"] == "all"
        assert factory.producer_kwargs["request_timeout_ms"] == 5000
        assert factory.producer_kwargs["bootstrap_servers"] == "fake:9092"

        # Admin: EXACTLY the security subset (locked #12 introspection).
        assert factory.admin_kwargs == {
            "bootstrap_servers": "fake:9092",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "svc",
        }

        # Consumer (constructed lazily on the first assignment): the three
        # pinned keys are PRESENT WITH their pinned values — not merely
        # overrides-stripped; aiokafka's own defaults are the catastrophic
        # values (plan F-13).
        await store.on_partitions_assigned([SOURCE])
        assert factory.consumer_kwargs is not None
        assert factory.consumer_kwargs["group_id"] is None
        assert factory.consumer_kwargs["enable_auto_commit"] is False
        assert factory.consumer_kwargs["auto_offset_reset"] == "none"
        assert factory.consumer_kwargs["sasl_mechanism"] == "PLAIN"  # pass-through
        assert factory.consumer_kwargs["max_poll_records"] == 5
        assert factory.consumer_kwargs["bootstrap_servers"] == "fake:9092"
    finally:
        await store.stop()
