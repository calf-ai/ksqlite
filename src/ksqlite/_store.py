"""KSQLite facade — pure composition root + delegation (plan §3 ``_store``;
spec §4). No business logic lives here.
"""

import asyncio
import enum
import inspect
import logging
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar, overload

from aiokafka import TopicPartition

from . import _lifecycle, _pool, _read, _schema, _sql, _state, _topics, _write
from .errors import (
    ConfigError,
    KSQLiteError,
    LifecycleError,
    SchemaMigrationError,
    StorageError,
)
from .types import (
    ConnectionPool,
    GeneratedColumn,
    Index,
    KafkaClientFactory,
    PartitionState,
)

logger = logging.getLogger("ksqlite.store")

T = TypeVar("T")

DEFAULT_CHANGELOG_TEMPLATE = "{source_topic}.p{partition}.changelog"


class _RunState(enum.Enum):
    """The facade's lifecycle phase — a single enum, so only the four legal
    states are representable (spec §4): ``stop()`` transitions RUNNING →
    STOPPING synchronously (closing the concurrent-stop window, F-14), then
    STOPPING → CLOSED once the append drain completes.
    """

    NEW = "NEW"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    CLOSED = "CLOSED"


# The three non-overridable rehydrate-consumer keys (spec §7 step 2). The
# first two because a manual-assign consumer must never join or commit into
# a consumer group; auto_offset_reset="none" because without it aiokafka
# never raises OffsetOutOfRangeError — it silently resets to LATEST, turning
# a truncated log into a silently-empty "successful" rehydrate.
_PINNED_CONSUMER_KEYS: dict[str, Any] = {
    "group_id": None,
    "enable_auto_commit": False,
    "auto_offset_reset": "none",
}

_SECURITY_KEY_NAMES = ("security_protocol", "ssl_context")
_SECURITY_KEY_PREFIX = "sasl_"


def merged_consumer_kwargs(
    *, bootstrap_servers: str, consumer_config: Mapping[str, Any]
) -> dict[str, Any]:
    """Merge order (spec §4 "Types"): defaults <- user config <- the pinned
    consumer keys. The merge lives ABOVE the factory seam — the factory
    receives the fully-merged final kwargs, never merges itself.
    """
    merged: dict[str, Any] = {"bootstrap_servers": bootstrap_servers}
    merged.update(consumer_config)
    merged.update(_PINNED_CONSUMER_KEYS)
    return merged


def merged_producer_kwargs(
    *, bootstrap_servers: str, producer_config: Mapping[str, Any]
) -> dict[str, Any]:
    """Defaults (``acks=1``, idempotence off — Tansu-compatible, spec §10)
    merged under the user's pass-through config.
    """
    merged: dict[str, Any] = {"bootstrap_servers": bootstrap_servers, "acks": 1}
    merged.update(producer_config)
    return merged


def admin_security_kwargs(
    *, bootstrap_servers: str, consumer_config: Mapping[str, Any]
) -> dict[str, Any]:
    """The security-relevant subset of ``consumer_config`` — step 0 must
    authenticate on SASL/SSL clusters too. Filtered to the params the admin
    ctor accepts via ``inspect.signature`` introspection, never a hardcoded
    key list (decisions log, implementation-handoff item 12): the aiokafka
    dependency is a floor, not an exact pin.
    """
    from aiokafka.admin import AIOKafkaAdminClient

    accepted = set(inspect.signature(AIOKafkaAdminClient.__init__).parameters)
    subset: dict[str, Any] = {"bootstrap_servers": bootstrap_servers}
    for key, value in consumer_config.items():
        is_security = key in _SECURITY_KEY_NAMES or key.startswith(_SECURITY_KEY_PREFIX)
        if is_security and key in accepted:
            subset[key] = value
    return subset


class _AiokafkaClientFactory:
    """The default KafkaClientFactory: real aiokafka clients (spec §4)."""

    def producer(self, **kwargs: Any) -> Any:
        from aiokafka import AIOKafkaProducer

        return AIOKafkaProducer(**kwargs)

    def consumer(self, **kwargs: Any) -> Any:
        from aiokafka import AIOKafkaConsumer

        return AIOKafkaConsumer(**kwargs)

    def admin_client(self, **kwargs: Any) -> Any:
        from aiokafka.admin import AIOKafkaAdminClient

        return AIOKafkaAdminClient(**kwargs)


class KSQLite:
    """Materialized-view store facade (spec §4 public API)."""

    def __init__(
        self,
        *,
        db_path: str | Path,
        bootstrap_servers: str,
        changelog_topic_template: str = DEFAULT_CHANGELOG_TEMPLATE,
        create_topics_retention_ms: int | None = None,
        verify_changelog_policy: bool = True,
        generated_columns: Sequence[GeneratedColumn] = (),
        indexes: Sequence[Index] = (),
        pool_size: int = 16,
        busy_timeout: float = 5.0,
        rehydrate_timeout: float = 30.0,
        producer_config: Mapping[str, Any] | None = None,
        consumer_config: Mapping[str, Any] | None = None,
        kafka_clients: KafkaClientFactory | None = None,
        pool: ConnectionPool | None = None,
    ) -> None:
        self._db_path = db_path
        self._bootstrap_servers = bootstrap_servers
        self._template = changelog_topic_template
        self._create_topics_retention_ms = create_topics_retention_ms
        self._verify_changelog_policy = verify_changelog_policy
        self._generated_columns = tuple(generated_columns)
        self._indexes = tuple(indexes)
        self._pool_size = pool_size
        self._busy_timeout = busy_timeout
        self._rehydrate_timeout = rehydrate_timeout
        self._producer_config = dict(producer_config or {})
        self._consumer_config = dict(consumer_config or {})
        self._factory: KafkaClientFactory = kafka_clients or _AiokafkaClientFactory()
        self._injected_pool = pool

        self._state = _state.PartitionStateMachine()
        self._run_state = _RunState.NEW
        self._in_flight = 0
        self._drained = asyncio.Event()

        self._pool: Any = None
        self._producer: Any = None
        self._admin: Any = None
        self._appender: _write.Appender | None = None
        self._query_runner: _read.QueryRunner | None = None
        self._lifecycle: _lifecycle.PartitionLifecycle | None = None

    # -- configuration validation (spec §4; fail-fast at start()) ----------

    def _validate_config(self) -> None:
        _sql.validate_changelog_template(self._template)
        if self._pool_size < 1:
            raise ConfigError(f"pool_size must be >= 1, got {self._pool_size}")
        if self._busy_timeout <= 0:
            raise ConfigError(f"busy_timeout must be > 0, got {self._busy_timeout}")
        if self._rehydrate_timeout <= 0:
            raise ConfigError(
                f"rehydrate_timeout must be > 0, got {self._rehydrate_timeout}"
            )
        acks = self._producer_config.get("acks", 1)
        if acks in (0, "0"):
            raise ConfigError(
                "producer_config['acks'] must not be 0: the append path needs"
                " the acked offset"
            )
        declared_names: set[str] = set()
        for column in self._generated_columns:
            _sql.validate_generated_column(column)
            declared_names.add(column.name)
        for index in self._indexes:
            _sql.validate_index(index, declared_names)

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._run_state is not _RunState.NEW:
            raise LifecycleError("start() called twice (or after stop())")
        # Fail-fast BEFORE any I/O: no pool open, no broker connect (F-01).
        self._validate_config()
        _schema.check_sqlite_version()

        pool: Any = None
        producer: Any = None
        admin: Any = None
        try:
            pool = self._injected_pool or _pool.AiosqlitePoolAdapter(
                self._db_path, self._pool_size, self._busy_timeout
            )
            try:
                async with pool.connection() as conn:
                    await _schema.migrate(conn, self._generated_columns, self._indexes)
            except (SchemaMigrationError, ConfigError):
                raise
            except sqlite3.Error as exc:
                raise StorageError(f"cannot open {self._db_path}: {exc}") from exc

            producer = self._factory.producer(
                **merged_producer_kwargs(
                    bootstrap_servers=self._bootstrap_servers,
                    producer_config=self._producer_config,
                )
            )
            try:
                await producer.start()
            except Exception as exc:
                raise KSQLiteError(f"producer failed to start: {exc}") from exc

            admin = self._factory.admin_client(
                **admin_security_kwargs(
                    bootstrap_servers=self._bootstrap_servers,
                    consumer_config=self._consumer_config,
                )
            )
            try:
                await admin.start()
            except Exception as exc:
                raise KSQLiteError(f"admin client failed to start: {exc}") from exc
        except BaseException:
            # Rollback-on-partial-failure (spec §4): close whatever opened,
            # in reverse open order. Cleanup failures are LOGGED and
            # suppressed — they must never mask the original startup error
            # (F-03/F-03b).
            rollback_steps: list[tuple[str, Any]] = []
            if admin is not None:
                rollback_steps.append(("admin.close", admin.close))
            if producer is not None:
                rollback_steps.append(("producer.stop", producer.stop))
            if pool is not None:
                rollback_steps.append(("pool.close", pool.close))
            for step_name, closer in rollback_steps:
                try:
                    await closer()
                except BaseException:
                    # BaseException on purpose (F-03c): a CancelledError
                    # hitting one closer must not skip the rest — a skipped
                    # pool.close() leaks non-daemon SQLite worker threads
                    # (the C-01c hazard). The bare raise below re-raises the
                    # original startup error.
                    logger.warning(
                        "start() rollback: %s failed",
                        step_name,
                        exc_info=True,
                        extra={
                            "event": "start_rollback_close_failed",
                            "step": step_name,
                        },
                    )
            raise

        # Composition root: construct the four services (plan §3 _store).
        self._pool = pool
        self._producer = producer
        self._admin = admin
        self._state = _state.PartitionStateMachine()  # cleared on start()
        txn_runner = _pool.TransactionRunner(busy_timeout=self._busy_timeout)
        topics = _topics.ChangelogTopics(
            admin=admin,
            template=self._template,
            create_topics_retention_ms=self._create_topics_retention_ms,
            verify_changelog_policy=self._verify_changelog_policy,
        )
        self._appender = _write.Appender(
            pool=pool,
            producer=producer,
            name_resolver=topics,
            state=self._state,
            txn_runner=txn_runner,
        )
        self._query_runner = _read.QueryRunner(pool=pool)
        consumer_kwargs = merged_consumer_kwargs(
            bootstrap_servers=self._bootstrap_servers,
            consumer_config=self._consumer_config,
        )
        self._lifecycle = _lifecycle.PartitionLifecycle(
            pool=pool,
            consumer_factory=lambda: self._factory.consumer(**consumer_kwargs),
            topics=topics,
            state=self._state,
            txn_runner=txn_runner,
            rehydrate_timeout=self._rehydrate_timeout,
        )
        self._run_state = _RunState.RUNNING

    async def stop(self) -> None:
        if self._run_state in (_RunState.STOPPING, _RunState.CLOSED):
            # A second stop() — or one racing an in-flight stop() — is a
            # no-op (spec §4, pinned); the winner runs the close sequence.
            return
        if self._run_state is _RunState.NEW:
            raise LifecycleError("stop() before start()")
        # The STOPPING transition is synchronous — no await between the
        # guard above and this write — so a concurrent stop() cannot also
        # pass the guard and double-close the clients (F-14).
        self._run_state = _RunState.STOPPING
        # Happy-path drain, appends only (spec §4, pinned mechanics).
        if self._in_flight > 0:
            await self._drained.wait()
        self._run_state = _RunState.CLOSED

        # Best-effort: every close step is attempted; each failure is LOGGED
        # (a swallowed pool.close() failure hides wedged worker threads); the
        # FIRST exception is re-raised at the end (spec §4).
        first_exc: BaseException | None = None
        assert self._lifecycle is not None
        steps: tuple[tuple[str, Any], ...] = (
            ("producer.flush", self._producer.flush),
            ("producer.stop", self._producer.stop),
            ("consumer.stop", self._lifecycle.aclose),
            ("admin.close", self._admin.close),
            ("pool.close", self._pool.close),
        )
        for step_name, step in steps:
            try:
                await step()
            except BaseException as exc:
                logger.error(
                    "stop(): close step %s failed",
                    step_name,
                    exc_info=exc,
                    extra={"event": "stop_step_failed", "step": step_name},
                )
                if first_exc is None:
                    first_exc = exc
        if first_exc is not None:
            raise first_exc

    async def __aenter__(self) -> "KSQLite":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    def _ensure_running(self) -> None:
        if self._run_state is not _RunState.RUNNING:
            raise LifecycleError(
                "KSQLite is not running: call start() first, and not after stop()"
            )

    # -- delegation (spec §4 public API) -------------------------------------

    async def append(
        self, *, source: TopicPartition, entity_key: str, payload: object
    ) -> None:
        # PINNED drain mechanics (spec §4): the running-state check and the
        # in-flight increment are synchronous — no await between them.
        self._ensure_running()
        self._in_flight += 1
        try:
            assert self._appender is not None
            await self._appender.append(
                source=source, entity_key=entity_key, payload=payload
            )
        finally:
            self._in_flight -= 1
            if self._run_state is not _RunState.RUNNING and self._in_flight == 0:
                self._drained.set()

    @overload
    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        into: None = None,
    ) -> list[sqlite3.Row]: ...

    @overload
    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        into: type[T],
    ) -> list[T]: ...

    async def query(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] = (),
        *,
        into: type[Any] | None = None,
    ) -> list[Any]:
        self._ensure_running()
        assert self._query_runner is not None
        return await self._query_runner.query(sql, params, into=into)

    def partition_states(self) -> dict[TopicPartition, PartitionState]:
        return self._state.partition_states()

    async def on_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        self._ensure_running()
        assert self._lifecycle is not None
        await self._lifecycle.on_partitions_assigned(assigned)

    async def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        self._ensure_running()
        assert self._lifecycle is not None
        await self._lifecycle.on_partitions_revoked(revoked)
