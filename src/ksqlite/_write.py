"""The Appender service (plan §3 ``_write``; spec §6). Depends on the
``ChangelogNameResolver`` protocol, never on ``_topics``.
"""

import logging

import aiosqlite
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError

from . import _ids, _schema, _wire
from ._pool import TransactionRunner
from ._state import PartitionStateMachine
from .errors import ChangelogProduceError
from .types import ChangelogNameResolver, ConnectionPool, PartitionStatus

logger = logging.getLogger("ksqlite.write")


def _validate_entity_key(entity_key: object) -> None:
    """Spec §6: non-empty, UTF-8-encodable ``str`` — it lands in a NOT NULL
    column and on the record key; an invalid key would poison every future
    rehydrate of the partition.
    """
    if not isinstance(entity_key, str) or not entity_key:
        raise ValueError(f"entity_key must be a non-empty str, got {entity_key!r}")
    try:
        entity_key.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"entity_key is not UTF-8-encodable: {entity_key!r}") from exc


class Appender:
    """Implements ``append`` per spec §6."""

    def __init__(
        self,
        *,
        pool: ConnectionPool,
        producer: object,
        name_resolver: ChangelogNameResolver,
        state: PartitionStateMachine,
        txn_runner: TransactionRunner,
    ) -> None:
        self._pool = pool
        self._producer = producer
        self._name_resolver = name_resolver
        self._state = state
        self._txn_runner = txn_runner

    async def append(
        self, *, source: TopicPartition, entity_key: str, payload: object
    ) -> None:
        _validate_entity_key(entity_key)
        if self._state.status_of(source) is not PartitionStatus.READY:
            # Host-contract violation (spec §6): warn, never raise — the
            # write still converges.
            logger.warning(
                "append to non-READY partition %s",
                source,
                extra={"event": "append_to_non_ready", "tp": source},
            )
        message_id = _ids.new_message_id()
        encoded = _wire.encode(
            entity_key=entity_key, payload=payload, message_id=message_id
        )
        topic = self._name_resolver.changelog_name(source)

        # Changelogs are single-partition by construction; producing to an
        # explicit partition 0 keeps write/read symmetry structural
        # (decisions log, implementation-handoff item 7).
        try:
            metadata = await self._producer.send_and_wait(  # type: ignore[attr-defined]
                topic,
                value=encoded.value,
                key=encoded.key,
                partition=0,
                headers=encoded.headers,
            )
        except KafkaError as exc:
            # A produce failure performs no SQLite write (spec §6 step 2).
            logger.warning(
                "changelog produce to %r failed: %s",
                topic,
                exc,
                extra={"event": "produce_failure", "tp": source, "topic": topic},
            )
            raise ChangelogProduceError(
                f"changelog produce to {topic!r} failed: {exc}"
            ) from exc
        offset: int = metadata.offset
        if offset < 0:
            # Defends against the idempotent-producer edge where a retried
            # batch reports offset == -1 (spec §6 step 2).
            logger.warning(
                "produce to %r acked without a usable offset (%s)",
                topic,
                offset,
                extra={"event": "produce_failure", "tp": source, "topic": topic},
            )
            raise ChangelogProduceError(
                f"produce to {topic!r} acked without a usable offset ({offset})"
            )

        async def materialize(conn: aiosqlite.Connection) -> None:
            await _schema.apply_materialization(
                conn,
                message_id=message_id,
                source_topic=source.topic,
                source_partition=source.partition,
                changelog_offset=offset,
                entity_key=entity_key,
                payload=encoded.value.decode("utf-8"),
            )

        async with self._pool.connection() as conn:
            await self._txn_runner.run(conn, materialize)

        # Post-commit in-memory write-back (spec §4): monotonic-MAX.
        self._state.record_applied(source, offset)
