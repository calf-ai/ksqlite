"""Error taxonomy (spec §4). Leaf module: imports nothing."""


class KSQLiteError(Exception):
    """Base class for every KSQLite error."""


class ConfigError(KSQLiteError):
    """Bad configuration, DDL drift, or a compact changelog policy."""


class SQLiteVersionError(KSQLiteError):
    """SQLite runtime older than 3.38 detected at ``start()``."""


class SchemaMigrationError(KSQLiteError):
    """An ``ALTER TABLE``/``CREATE INDEX`` execution failure during migration."""


class ChangelogProduceError(KSQLiteError):
    """Changelog produce failure, or the broker reported ``offset < 0``."""


class TopicNotFoundError(KSQLiteError):
    """A changelog topic is missing on its partition's first assignment while
    ``create_topics_retention_ms`` is ``None``.
    """


class RehydrateError(KSQLiteError):
    """Replay/assign/broker/DB error — or an unrecognized ``format_version`` —
    during rehydrate. Propagates through the host's rebalance callback; hosts
    should treat it as a failed assignment.
    """


class HydrationError(KSQLiteError):
    """``into=`` was used but the query result has no ``payload`` column."""


class LifecycleError(KSQLiteError):
    """Lifecycle misuse: ``append``/``query``/hooks before ``start()`` or after
    ``stop()``, double ``start()``, or ``stop()`` before ``start()``.
    """


class StorageError(KSQLiteError):
    """SQLite I/O failure: DB open at ``start()``, ``SQLITE_BUSY`` after
    ``busy_timeout``, ``SQLITE_FULL``, a write attempted under ``query_only``,
    or a pool-layer failure such as an acquisition timeout.
    """
