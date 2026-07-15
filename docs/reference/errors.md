# Errors

```python
from ksqlite import KSQLiteError  # and the subclasses below
```

Every KSQLite error subclasses `KSQLiteError`. The hierarchy is flat: each class
below inherits directly from `KSQLiteError` and from nothing else in the
taxonomy.

```text
KSQLiteError
├── ConfigError
├── SQLiteVersionError
├── SchemaMigrationError
├── ChangelogProduceError
├── TopicNotFoundError
├── RehydrateError
├── HydrationError
├── LifecycleError
└── StorageError
```

`TopicNotFoundError` is **not** a subclass of `RehydrateError`. Catching
`RehydrateError` around an assignment does not catch it.

## The taxonomy

| Class | Meaning | Raised by |
|---|---|---|
| `KSQLiteError` | Base class. Also raised directly when the producer or admin client fails to start. | `start()` |
| `ConfigError` | Invalid configuration, or stored schema drift. | `start()` |
| `SQLiteVersionError` | The SQLite runtime is older than 3.38. | `start()` |
| `SchemaMigrationError` | An `ALTER TABLE` or `CREATE INDEX` failed to execute. | `start()` |
| `ChangelogProduceError` | The changelog produce failed, or the broker acked with `offset < 0`. | `append()` |
| `TopicNotFoundError` | A changelog topic is missing and `create_topics_retention_ms` is `None`. | `on_partitions_assigned()` |
| `RehydrateError` | A replay, broker, or database error during rehydrate, or an unrecognized `format_version`. | `on_partitions_assigned()` |
| `HydrationError` | `into=` was used but the result has no `payload` column. | `query()` |
| `LifecycleError` | Lifecycle misuse. | `start()`, `stop()`, and every operation |
| `StorageError` | A SQLite or connection-pool failure. | `start()`, `append()`, `query()`, both hooks |

## Errors that are not `KSQLiteError`

Two failure modes raise standard Python exceptions rather than members of the
taxonomy.

| Class | Meaning | Raised by |
|---|---|---|
| `ValueError` | `entity_key` is empty, not a `str`, or not UTF-8-encodable; or `payload` is not JSON-serializable, is not valid JSON, or contains `NaN`/`Infinity`/`-Infinity`. | `append()` |
| `TypeError` | `into=` is neither a Pydantic v2 model nor a dataclass. | `query()` |

Both indicate a bug in the calling code rather than a runtime condition, and
both are raised before any produce or write occurs.

## `StorageError` conditions

`StorageError` covers every SQLite and pool failure at the boundary:

- The database file could not be opened at `start()`.
- WAL could not be enabled on the database file.
- `SQLITE_BUSY` persisted past `busy_timeout` and the bounded retry.
- A write was attempted through `query()`, which runs under `PRAGMA query_only=ON`.
- The connection pool was closed, or a connection acquisition timed out.
- Any other `sqlite3.Error` inside a transaction, a `query()`, or a checkpoint read.

## `LifecycleError` conditions

- `append()`, `query()`, `on_partitions_assigned()`, or `on_partitions_revoked()`
  called before `start()` or after `stop()`.
- `start()` called twice, or after `stop()`.
- `stop()` called before `start()`.

Two calls never raise `LifecycleError`: `stop()` called a second time returns
without doing anything, and `partition_states()` has no lifecycle check at all.

## Recovery

| Error | Retryable |
|---|---|
| `ChangelogProduceError` | **No.** Retrying `append()` mints a new `message_id` and can materialize the record twice. See [How to handle failures](../how-to/handle-failures.md). |
| `StorageError` from `append()` | No. The changelog produce already succeeded; the next rehydrate applies the record. |
| `StorageError` from `query()` | Yes. Reads have no side effects. |
| `RehydrateError` | Treat the assignment as failed. Raised through the rebalance callback. |
| `TopicNotFoundError` | Only after the changelog topic exists. |
| `ConfigError`, `SQLiteVersionError` | No. Fix the configuration or the runtime. |

## See also

- [How to handle failures](../how-to/handle-failures.md) — what to do about each
- [About the durability contract](../explanation/durability.md) — why `append()` is not retryable
- [`KSQLite`](ksqlite.md) — per-method error tables
