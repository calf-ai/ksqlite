"""Public types and injection-seam protocols (spec §4 "Types"). Leaf module.

Remaining placeholder names from L-01 (export surface) are grown red-first by
L-03/L-04.
"""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, Sequence, runtime_checkable

import aiosqlite
from aiokafka import TopicPartition


@dataclass(frozen=True)
class GeneratedColumn:
    """Host-declared queryable column extracted from the JSON payload.

    Rendered as ``ALTER TABLE _records ADD COLUMN <name> <sql_type>
    GENERATED ALWAYS AS (payload->>'<json_path>') VIRTUAL`` (spec §5.1).
    """

    name: str
    json_path: str
    sql_type: str = "TEXT"


@dataclass(frozen=True)
class Index:
    """Host-declared index over system, base, or generated columns."""

    name: str
    columns: Sequence[str]
    unique: bool = False


class PartitionStatus(Enum):
    """The four-valued partition state (spec §3, §7, §8)."""

    RESTORING = "RESTORING"
    READY = "READY"
    READY_PARTIAL = "READY_PARTIAL"
    STANDBY = "STANDBY"


@dataclass(frozen=True)
class PartitionState:
    """Snapshot of one partition's state (``partition_states()`` value).

    ``state`` and ``checkpoint_offset`` are live; ``log_end_offset`` (= LEO)
    and ``lag`` (= LEO − (checkpoint + 1)) are cached as-of-the-last-rehydrate
    (spec §4).
    """

    state: PartitionStatus
    checkpoint_offset: int | None
    log_end_offset: int | None
    lag: int | None


@runtime_checkable
class ConnectionPool(Protocol):
    """Connection-pool injection seam (spec §4 "Types").

    The default is the ``aiosqlitepool`` adapter in ``ksqlite._pool``; the seam
    exists for testing/injection, not as an advertised extension point.
    """

    def connection(self) -> AbstractAsyncContextManager[aiosqlite.Connection]:
        """Return an async context manager yielding a pooled connection."""
        ...

    async def close(self) -> None:
        """Close the pool and every connection it holds."""
        ...


@runtime_checkable
class KafkaClientFactory(Protocol):
    """Kafka client-factory injection seam (spec §4 "Types").

    Contract: the three methods are synchronous, receive the fully-merged
    final kwargs (defaults ← user config ← the pinned consumer keys — merged
    by KSQLite, never by the factory), and return constructed-but-unstarted
    clients; KSQLite owns ``start()``/``stop()``.
    """

    def producer(self, **kwargs: Any) -> Any:
        """Return an unstarted changelog producer."""
        ...

    def consumer(self, **kwargs: Any) -> Any:
        """Return an unstarted rehydrate consumer."""
        ...

    def admin_client(self, **kwargs: Any) -> Any:
        """Return an unstarted admin client."""
        ...


@runtime_checkable
class ChangelogNameResolver(Protocol):
    """The narrow resolve-name surface ``_write`` needs (plan §3): ``_write``
    depends on this protocol, never on the ``_topics`` module.
    """

    def changelog_name(self, source: TopicPartition) -> str:
        """Resolve the changelog topic name for a source partition."""
        ...
