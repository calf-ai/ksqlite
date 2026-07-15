# SQL schema

The schema KSQLite creates and migrates in the database at `db_path`. It is
created idempotently by `start()`.

## `records` (view)

The only object host SQL should read. It exposes rows of `_records` scoped to the
partitions this process currently owns.

```sql
CREATE VIEW IF NOT EXISTS records AS
    SELECT r.* FROM _records r
    JOIN _owned o ON r.source_topic = o.source_topic
                 AND r.source_partition = o.source_partition
```

Because the view selects `r.*`, SQLite re-expands the column list when a
statement is prepared. Generated columns added after the view was created are
therefore visible through it.

### Columns

| Column | Type | Description |
|---|---|---|
| `message_id` | `TEXT` | UUIDv7 assigned at `append()`. Unique across the table. |
| `source_topic` | `TEXT` | `source.topic` of the `append()` that produced the row. |
| `source_partition` | `INTEGER` | `source.partition` of that `append()`. |
| `changelog_offset` | `INTEGER` | Offset of the record in its changelog topic. |
| `entity_key` | `TEXT` | The `entity_key` passed to `append()`. |
| `payload` | `TEXT` | The payload as JSON text, normalized by SQLite's `json()`. |

Declared `GeneratedColumn`s appear as additional columns. See
[`GeneratedColumn`](types.md#generatedcolumn).

### Visibility

A partition's rows appear in `records` only while a row for
`(source_topic, source_partition)` exists in `_owned`.

| Partition state | In `records` |
|---|---|
| `RESTORING` | No |
| `READY` | Yes |
| `READY_PARTIAL` | Yes |
| `STANDBY` | No |
| Never assigned | No |

Rows are revealed atomically per partition: a partition's rows are written to
`_records` before its `_owned` row is inserted, so a query either sees the
partition at its revealed state or does not see it at all.

`READY_PARTIAL` rows are visible but incomplete. See
[How to check partition readiness](../how-to/check-partition-readiness.md).

## `_records` (table)

The materialized rows for every partition this database has ever held, including
partitions not currently owned.

```sql
CREATE TABLE IF NOT EXISTS _records (
    message_id       TEXT    NOT NULL,
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    changelog_offset INTEGER NOT NULL,
    entity_key       TEXT    NOT NULL,
    payload          TEXT    NOT NULL,
    UNIQUE (message_id)
)
```

The `UNIQUE (message_id)` constraint is what makes replay idempotent: both the
append path and replay insert with `ON CONFLICT (message_id) DO NOTHING`.

Do not read `_records` directly from host SQL. It is not scoped to owned
partitions.

## `partition_checkpoint` (table)

The highest changelog offset applied locally per partition.

```sql
CREATE TABLE IF NOT EXISTS partition_checkpoint (
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    changelog_offset INTEGER NOT NULL,
    PRIMARY KEY (source_topic, source_partition)
)
```

Normally advanced with `MAX(changelog_offset, excluded.changelog_offset)`, so it
only moves forward. It moves backward only when a truncated changelog forces a
reset. See [About the partition lifecycle](../explanation/partition-lifecycle.md).

## `_owned` (table)

Presence-only visibility rows backing the `records` view.

```sql
CREATE TABLE IF NOT EXISTS _owned (
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    PRIMARY KEY (source_topic, source_partition)
)
```

Emptied by `start()`, so a prior run's ownership never leaks into a new process.
Populated by `on_partitions_assigned()` and cleared per partition by
`on_partitions_revoked()`.

## Indexes

Created unconditionally on `_records`:

| Name | Columns |
|---|---|
| `ix_records_tp` | `(source_topic, source_partition)` |
| `ix_records_key` | `entity_key` |

Declared `Index`es are created in addition. See [`Index`](types.md#index).

## Connection settings

Every pooled connection is opened with:

| Setting | Value |
|---|---|
| `isolation_level` | `None` (autocommit; all transactions are explicit) |
| `journal_mode` | `WAL` |
| `busy_timeout` | `busy_timeout` seconds, in milliseconds |

If WAL cannot be enabled, `start()` raises `StorageError`. `query()` additionally
runs under `PRAGMA query_only=ON`.

## See also

- [How to declare queryable columns](../how-to/declare-queryable-columns.md)
- [Types](types.md) — `GeneratedColumn` and `Index`
- [About the partition lifecycle](../explanation/partition-lifecycle.md) — why visibility is scoped
