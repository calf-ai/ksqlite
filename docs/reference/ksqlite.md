# `KSQLite`

The store facade. One instance owns one SQLite database and the Kafka clients
that back it.

```python
from ksqlite import KSQLite
```

## Constructor

All parameters are keyword-only.

```python
KSQLite(
    *,
    db_path: str | Path,
    bootstrap_servers: str,
    changelog_topic_template: str = "{source_topic}.p{partition}.changelog",
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
)
```

| Name | Type | Default | Description |
|---|---|---|---|
| `db_path` | `str \| Path` | required | Path to the SQLite database file. Must be a regular file on a WAL-capable filesystem. |
| `bootstrap_servers` | `str` | required | Kafka bootstrap servers, passed to the producer, consumer, and admin client. |
| `changelog_topic_template` | `str` | `"{source_topic}.p{partition}.changelog"` | Format template resolving a source partition to its changelog topic name. Must contain both `{source_topic}` and `{partition}`. |
| `create_topics_retention_ms` | `int \| None` | `None` | When set, a missing changelog topic is created with `retention.ms` set to this value and `cleanup.policy=delete`. `-1` means infinite retention. When `None`, a missing changelog topic raises `TopicNotFoundError`. |
| `verify_changelog_policy` | `bool` | `True` | When `True`, the `cleanup.policy` of each changelog topic is checked on the partition's first assignment. |
| `generated_columns` | `Sequence[GeneratedColumn]` | `()` | Queryable columns extracted from the payload. See [`GeneratedColumn`](types.md#generatedcolumn). |
| `indexes` | `Sequence[Index]` | `()` | Indexes over system or generated columns. See [`Index`](types.md#index). |
| `pool_size` | `int` | `16` | Connections in the SQLite pool. Must be >= 1. |
| `busy_timeout` | `float` | `5.0` | Seconds SQLite waits on a locked database. Must be > 0. |
| `rehydrate_timeout` | `float` | `30.0` | Seconds budgeted for one `on_partitions_assigned` call, across all partitions in that call. Must be > 0. |
| `producer_config` | `Mapping[str, Any] \| None` | `None` | Extra kwargs for the changelog producer. Merged over the default `acks=1`. |
| `consumer_config` | `Mapping[str, Any] \| None` | `None` | Extra kwargs for the rehydrate consumer. `group_id`, `enable_auto_commit`, and `auto_offset_reset` are overridden and cannot be set. |
| `kafka_clients` | `KafkaClientFactory \| None` | `None` | Client-factory seam. Defaults to real `aiokafka` clients. |
| `pool` | `ConnectionPool \| None` | `None` | Connection-pool seam. Defaults to an `aiosqlitepool` adapter over `db_path`. |

The constructor performs no I/O and no validation. Configuration is validated in
`start()`.

### Configuration validation

`start()` applies these checks before opening the database or contacting a
broker, and raises `ConfigError` on any failure:

| Rule | |
|---|---|
| `changelog_topic_template` | Contains both `{source_topic}` and `{partition}`, and formats with only those two names. |
| `pool_size` | >= 1 |
| `busy_timeout` | > 0 |
| `rehydrate_timeout` | > 0 |
| `producer_config["acks"]` | Not `0` or `"0"`. The append path requires the acked offset. |
| Each `GeneratedColumn` | See [`GeneratedColumn`](types.md#generatedcolumn) validation rules. |
| Each `Index` | See [`Index`](types.md#index) validation rules. |

`start()` then checks the SQLite runtime version and raises `SQLiteVersionError`
below 3.38.

### Kafka client configuration

Merge order for the changelog producer: `bootstrap_servers` and `acks=1`, then
`producer_config` over them. Any key in `producer_config` other than `acks=0`
wins.

Merge order for the rehydrate consumer: `bootstrap_servers`, then
`consumer_config`, then these three pinned keys, which always win:

| Key | Pinned value |
|---|---|
| `group_id` | `None` |
| `enable_auto_commit` | `False` |
| `auto_offset_reset` | `"none"` |

The admin client receives `bootstrap_servers` plus the security-relevant subset
of `consumer_config`: keys named `security_protocol` or `ssl_context`, or
prefixed `sasl_`, that the `AIOKafkaAdminClient` constructor accepts. No other
`consumer_config` key reaches the admin client.

## Methods

### `async start() -> None`

Validates configuration, checks the SQLite version, opens the connection pool,
migrates the schema, and starts the producer and admin client. The rehydrate
consumer is started lazily on the first assignment.

Clears the `_owned` table, so no partition is visible until a rebalance assigns
it.

Raises `LifecycleError` if called more than once, or after `stop()`. If any step
fails, whatever opened is closed again before the error propagates.

| Raises | When |
|---|---|
| `ConfigError` | Invalid configuration, or stored schema drift. |
| `SQLiteVersionError` | SQLite runtime older than 3.38. |
| `SchemaMigrationError` | An `ALTER TABLE` or `CREATE INDEX` failed. |
| `StorageError` | The database could not be opened, or WAL could not be enabled. |
| `KSQLiteError` | The producer or admin client failed to start. |
| `LifecycleError` | Called twice, or after `stop()`. |

### `async stop() -> None`

Waits for in-flight `append()` calls to finish, then flushes and stops the
producer, stops the rehydrate consumer, closes the admin client, and closes the
pool.

Every close step runs even if an earlier one fails; each failure is logged and
the first exception is re-raised at the end.

A second `stop()`, or one racing an in-flight `stop()`, returns without doing
anything. `stop()` before `start()` raises `LifecycleError`.

### `async __aenter__` / `async __aexit__`

`KSQLite` is an async context manager. `__aenter__` calls `start()` and returns
the store; `__aexit__` calls `stop()`.

```python
async with KSQLite(db_path="state.db", bootstrap_servers="localhost:9092") as store:
    ...
```

### `async append(*, source, entity_key, payload) -> None`

Produces one record to the changelog for `source`, then materializes it locally.

| Parameter | Type | Description |
|---|---|---|
| `source` | `TopicPartition` | The shard this record belongs to. Identifies the changelog topic via `changelog_topic_template`; it is not the changelog partition. The named topic is never read or written by KSQLite and need not exist. |
| `entity_key` | `str` | Non-empty, UTF-8-encodable. Becomes the changelog record key and the `entity_key` column. |
| `payload` | `object` | A JSON-serializable object, or a `str` that parses as JSON. |

`payload` is serialized and validated before anything is produced. A `str`
payload is validated as JSON and then passed through unchanged. `NaN`,
`Infinity`, and `-Infinity` are rejected in both forms.

Appending to a partition that is not `READY` logs a warning and proceeds.

| Raises | When |
|---|---|
| `ValueError` | `entity_key` is empty, not a `str`, or not UTF-8-encodable; or `payload` is not JSON-serializable or not valid JSON. |
| `ChangelogProduceError` | The produce failed, or the broker acked without a usable offset. No local write occurred. |
| `StorageError` | The local write failed after the produce succeeded. |
| `LifecycleError` | Called before `start()` or after `stop()`. |

A failed `append()` must not be retried. See
[How to handle failures](../how-to/handle-failures.md).

### `async query(sql, params=(), *, into=None) -> list[sqlite3.Row] | list[T]`

Runs `sql` against the database with `PRAGMA query_only=ON`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sql` | `str` | required | Any read-only SQL. Usually selects from the `records` view. |
| `params` | `Sequence[Any] \| Mapping[str, Any]` | `()` | Bound parameters. A sequence binds `?` placeholders; a mapping binds named ones. |
| `into` | `type[T] \| None` | `None` | Hydration target. When `None`, rows are returned as `sqlite3.Row`. |

With `into`, each row's `payload` column is hydrated: a Pydantic v2 model via
`model_validate_json`, or a dataclass via `json.loads` into its constructor. The
query must select `payload`.

| Raises | When |
|---|---|
| `StorageError` | The statement failed, including any attempted write. |
| `HydrationError` | `into` was given but the result has no `payload` column. |
| `TypeError` | `into` is neither a Pydantic v2 model nor a dataclass. |
| `LifecycleError` | Called before `start()` or after `stop()`. |

```python
rows = await store.query("SELECT payload FROM records WHERE thread_id = ?", ["th-42"])
```

### `partition_states() -> dict[TopicPartition, PartitionState]`

Returns a snapshot of every partition seen during this process's lifetime, keyed
by source partition. Synchronous.

See [`PartitionState`](types.md#partitionstate) for field semantics.

### `async on_partitions_assigned(assigned) -> None`

Takes ownership of each partition: ensures its changelog topic exists, replays
it, and reveals the partition to the `records` view. Until a partition is
assigned, its rows are invisible to `query()`.

`assigned` is any iterable of `TopicPartition`.

Call it from your consumer's rebalance listener before your own logic reads
state, or call it directly if you are not consuming a source topic. See
[How to use KSQLite as standalone storage](../how-to/use-as-standalone-storage.md).

The whole call shares one `rehydrate_timeout` budget. Partitions the budget does
not cover are revealed as `READY_PARTIAL`.

| Raises | When |
|---|---|
| `TopicNotFoundError` | The changelog topic does not exist and `create_topics_retention_ms` is `None`. |
| `RehydrateError` | A broker or database error during rehydrate, or a changelog record carrying an unrecognized `format_version`. |
| `LifecycleError` | Called before `start()` or after `stop()`. |

On error, partitions not yet revealed in this call stay hidden.

### `async on_partitions_revoked(revoked) -> None`

Releases ownership: hides each revoked partition from the `records` view and
marks it `STANDBY`. Call it from your consumer's rebalance listener after your
own logic has flushed, or directly when releasing a shard.

Rows and checkpoints are kept, so a later reassignment resumes from the
checkpoint instead of replaying the whole changelog. Revoking a partition this
store never owned does nothing.

Raises `LifecycleError` if called before `start()` or after `stop()`.

## See also

- [How to wire KSQLite into your consumer](../how-to/wire-into-your-consumer.md)
- [Types](types.md) — the values this class accepts and returns
- [Errors](errors.md) — the full taxonomy
- [SQL schema](sql-schema.md) — what `query()` reads
