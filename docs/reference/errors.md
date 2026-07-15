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
| `ConfigError` | Invalid configuration, stored schema drift, or a resolved changelog name that is not a legal Kafka topic name. | `start()`, `append()`, `on_partitions_assigned()` |
| `SQLiteVersionError` | The SQLite runtime is older than 3.38. | `start()` |
| `SchemaMigrationError` | An `ALTER TABLE` or `CREATE INDEX` failed to execute. | `start()` |
| `ChangelogProduceError` | The changelog produce failed, or the broker acked with `offset < 0`. | `append()` |
| `TopicNotFoundError` | A changelog topic is missing and `create_topics_retention_ms` is `None`. | `on_partitions_assigned()` |
| `RehydrateError` | A replay, broker, or database error during rehydrate, or an unrecognized `format_version`. | `on_partitions_assigned()` |
| `HydrationError` | `into=` was used but the result has no `payload` column. | `query()` |
| `LifecycleError` | Lifecycle misuse. | `start()`, `stop()`, and every operation |
| `StorageError` | A SQLite or connection-pool failure. | `start()`, `append()`, `query()`, `on_partitions_revoked()` |

`on_partitions_assigned()` does not raise `StorageError`: a SQLite or pool failure
inside it is converted to `RehydrateError`, with the `StorageError` as
`__cause__`. `on_partitions_revoked()` raises it directly.

`ConfigError` is not only a `start()` error. The resolved changelog topic name is
validated when it is first needed, so a template that `start()` accepts can still
raise `ConfigError` from `append()` or `on_partitions_assigned()` if it resolves
to an illegal name. It is not converted to `RehydrateError`.

## Errors that are not `KSQLiteError`

Two failure modes raise standard Python exceptions rather than members of the
taxonomy.

| Class | Meaning | Raised by |
|---|---|---|
| `ValueError` | `entity_key` is empty, not a `str`, or not UTF-8-encodable; or `payload` is not JSON-serializable, is not valid JSON, or contains `NaN`/`Infinity`/`-Infinity`. | `append()` |
| `TypeError` | `into=` is neither a Pydantic v2 model nor a dataclass; or `into=` is a dataclass whose fields do not match the payload. | `query()` |
| `pydantic.ValidationError` | `into=` is a Pydantic model and a payload fails its validation. | `query()` |

All indicate a mismatch between the calling code and the data rather than a
runtime fault. `ValueError` is raised before anything is produced or written.
The `query()` errors are raised while hydrating rows, so they never fire on an
empty result.

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
| `ChangelogProduceError` | No. A retry mints a new `message_id`, which does not deduplicate against the first attempt. |
| `StorageError` from `append()` | No. The produce already succeeded. |
| `StorageError` from `query()` | Yes. Reads have no side effects. |
| `RehydrateError` | Not in place. The partition can be re-assigned, which replays from its checkpoint. |
| `TopicNotFoundError` | Only once the changelog topic exists. |
| `ConfigError`, `SQLiteVersionError` | No. |

See [How to handle failures](../how-to/handle-failures.md) for what to do about
each.

## See also

- [How to handle failures](../how-to/handle-failures.md) — what to do about each
- [About the durability contract](../explanation/durability.md) — why `append()` is not retryable
- [`KSQLite`](ksqlite.md) — per-method error tables
