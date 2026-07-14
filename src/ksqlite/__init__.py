"""KSQLite: a local, SQL-queryable, rebuildable materialized view for
Kafka-consuming applications, backed durably by a per-partition Kafka
changelog.
"""

import logging

from ._store import KSQLite
from .errors import (
    ChangelogProduceError,
    ConfigError,
    HydrationError,
    KSQLiteError,
    LifecycleError,
    RehydrateError,
    SchemaMigrationError,
    SQLiteVersionError,
    StorageError,
    TopicNotFoundError,
)
from .types import (
    ConnectionPool,
    GeneratedColumn,
    Index,
    KafkaClientFactory,
    PartitionState,
    PartitionStatus,
)

__all__ = [
    "ChangelogProduceError",
    "ConfigError",
    "ConnectionPool",
    "GeneratedColumn",
    "HydrationError",
    "Index",
    "KSQLite",
    "KSQLiteError",
    "KafkaClientFactory",
    "LifecycleError",
    "PartitionState",
    "PartitionStatus",
    "RehydrateError",
    "SQLiteVersionError",
    "SchemaMigrationError",
    "StorageError",
    "TopicNotFoundError",
]

logging.getLogger("ksqlite").addHandler(logging.NullHandler())
