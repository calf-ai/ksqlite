# Types

The values [`KSQLite`](ksqlite.md) accepts and returns.

```python
from ksqlite import (
    ConnectionPool,
    GeneratedColumn,
    Index,
    KafkaClientFactory,
    PartitionState,
    PartitionStatus,
)
```

## `GeneratedColumn`

Frozen dataclass. A queryable column extracted from the JSON payload.

```python
GeneratedColumn(name: str, json_path: str, sql_type: str = "TEXT")
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Column name in the `_records` table and the `records` view. |
| `json_path` | `str` | required | JSON path into the payload. |
| `sql_type` | `str` | `"TEXT"` | SQL type of the column. |

Rendered at `start()` as:

```sql
ALTER TABLE _records ADD COLUMN <name> <sql_type>
    GENERATED ALWAYS AS (payload->>'<json_path>') VIRTUAL
```

The column is `VIRTUAL`: it is computed on read and occupies no row storage.

### Validation

Applied in `start()`; failures raise `ConfigError`.

| Field | Rule |
|---|---|
| `name` | Matches `[A-Za-z_][A-Za-z0-9_]*`. |
| `name` | Not one of the system columns: `message_id`, `source_topic`, `source_partition`, `changelog_offset`, `entity_key`, `payload`. |
| `json_path` | Starts with `$.` and is longer than two characters. |
| `json_path` | Contains no control characters. |
| `sql_type` | Matches `[A-Za-z_][A-Za-z0-9_]*`. |

`sql_type` is a single identifier, so parameterized types such as
`VARCHAR(20)` and multi-word types such as `UNSIGNED BIG INT` are rejected.

### Drift

`start()` raises `ConfigError` when a declared column already exists in the
stored schema under a different clause — that is, when its `sql_type` or
`json_path` no longer matches. A generated column removed from the configuration
is left in the table and logged.

```python
GeneratedColumn("thread_id", "$.thread_id")
GeneratedColumn("priority", "$.meta.priority", "INTEGER")
```

## `Index`

Frozen dataclass. An index over system or generated columns.

```python
Index(name: str, columns: Sequence[str], unique: bool = False)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Index name. |
| `columns` | `Sequence[str]` | required | Indexed columns, in order. Snapshotted to a tuple at construction. |
| `unique` | `bool` | `False` | Whether the index is `UNIQUE`. |

Rendered at `start()` as:

```sql
CREATE [UNIQUE] INDEX IF NOT EXISTS <name> ON _records (<columns>)
```

### Validation

Applied in `start()`; failures raise `ConfigError`.

| Field | Rule |
|---|---|
| `name` | Matches `[A-Za-z_][A-Za-z0-9_]*`. |
| `columns` | Every column is a system column or a declared `GeneratedColumn`. |

### Drift

If a declared index already exists with different key columns or a different
`unique` flag, `start()` raises `ConfigError`. An index removed from the
configuration is left in place and logged.

Two indexes exist regardless of configuration: `ix_records_tp` on
`(source_topic, source_partition)` and `ix_records_key` on `entity_key`.

```python
Index("ix_thread", ["thread_id"])
Index("ix_thread_priority", ["thread_id", "priority"])
```

## `PartitionStatus`

Enum. The state of one partition.

| Member | Value | Visible in `records` | Meaning |
|---|---|---|---|
| `RESTORING` | `"RESTORING"` | No | Replaying its changelog. |
| `READY` | `"READY"` | Yes | Replayed to the log end offset observed at rehydrate. |
| `READY_PARTIAL` | `"READY_PARTIAL"` | Yes | Revealed before replay finished, because `rehydrate_timeout` expired. Rows are visible but incomplete. |
| `STANDBY` | `"STANDBY"` | No | Not owned. Rows and checkpoint are retained on disk. |

## `PartitionState`

Frozen dataclass. One partition's snapshot, as returned by
`KSQLite.partition_states()`.

| Field | Type | Freshness | Description |
|---|---|---|---|
| `state` | `PartitionStatus` | live | The partition's current state. |
| `checkpoint_offset` | `int \| None` | live | The partition's checkpoint: the changelog offset replay would resume after. `None` if nothing has been applied. |
| `log_end_offset` | `int \| None` | as of last rehydrate | Changelog log end offset observed during the last rehydrate. `None` until a rehydrate has run. |
| `lag` | `int \| None` | mixed — see below | `log_end_offset - (checkpoint_offset + 1)`, or `log_end_offset` when `checkpoint_offset` is `None`. `None` until a rehydrate has run. |

`lag` is not stored. It is recomputed on every `partition_states()` call from a
**live** `checkpoint_offset` and a **stale** `log_end_offset`. Since `append()`
advances the checkpoint but never refreshes the log end offset, each append after
a rehydrate lowers `lag` by one, and `lag` goes **negative** as soon as the
checkpoint reaches the log end offset observed at that rehydrate (`lag < 0` when
`checkpoint_offset >= log_end_offset`):

```text
after rehydrate:  checkpoint_offset=99   log_end_offset=100   lag=0
after 5 appends:  checkpoint_offset=104  log_end_offset=100   lag=-5
```

`checkpoint_offset` is normally the highest changelog offset applied locally, but
a truncation reset can clamp it to an offset that was never applied.

## `ConnectionPool`

Runtime-checkable protocol. The connection-pool seam, used by the `pool`
constructor parameter.

```python
class ConnectionPool(Protocol):
    def connection(self) -> AbstractAsyncContextManager[aiosqlite.Connection]: ...
    async def close(self) -> None: ...
```

The default implementation wraps `aiosqlitepool`, opening each connection with
`isolation_level=None`, `PRAGMA busy_timeout`, and WAL.

## `KafkaClientFactory`

Runtime-checkable protocol. The Kafka client seam, used by the `kafka_clients`
constructor parameter.

```python
class KafkaClientFactory(Protocol):
    def producer(self, **kwargs: Any) -> Any: ...
    def consumer(self, **kwargs: Any) -> Any: ...
    def admin_client(self, **kwargs: Any) -> Any: ...
```

All three methods are synchronous and return constructed-but-unstarted clients.
They receive the fully merged kwargs; the factory never merges configuration
itself. `KSQLite` owns each client's lifecycle.

## See also

- [`KSQLite`](ksqlite.md) — the class these types configure
- [SQL schema](sql-schema.md) — where generated columns and indexes land
- [How to declare queryable columns](../how-to/declare-queryable-columns.md)
