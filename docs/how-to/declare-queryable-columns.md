# How to declare queryable columns

This guide shows you how to make fields inside your JSON payloads queryable as
ordinary SQL columns, and how to index them.

By default the `records` view exposes `payload` as JSON text. Filtering on a
field inside it means extracting it in every query and scanning every row. A
`GeneratedColumn` gives that field a real column name; an `Index` makes looking
it up fast.

## Step 1 — Declare the columns

Each `GeneratedColumn` names a column, a JSON path into the payload, and a SQL
type:

```python
from ksqlite import GeneratedColumn, KSQLite

store = KSQLite(
    db_path="state.db",
    bootstrap_servers="broker:9092",
    generated_columns=[
        GeneratedColumn("thread_id", "$.thread_id"),                # TEXT
        GeneratedColumn("priority", "$.meta.priority", "INTEGER"),  # nested
        GeneratedColumn("score", "$.score", "REAL"),
    ],
)
```

The type defaults to `TEXT`. Paths may be nested. A path that a payload does not
contain yields `NULL` for that row, so a field that only some payloads carry is
fine.

`sql_type` must be a single identifier. `INTEGER`, `REAL`, and `TEXT` work;
`VARCHAR(20)` and `UNSIGNED BIG INT` are rejected with `ConfigError`. SQLite's
type affinity means the plain types are almost always what you want anyway.

The columns are created by `start()` and can be queried by name:

```python
rows = await store.query(
    "SELECT payload FROM records WHERE thread_id = ? AND priority > ?",
    ["th-42", 3],
)
```

## Step 2 — Index what you filter on

A generated column alone does not make lookups fast — without an index, SQLite
still scans. Declare an index for the columns you filter or sort on:

```python
from ksqlite import Index

store = KSQLite(
    ...,
    generated_columns=[
        GeneratedColumn("thread_id", "$.thread_id"),
        GeneratedColumn("priority", "$.meta.priority", "INTEGER"),
    ],
    indexes=[
        Index("ix_thread", ["thread_id"]),
        Index("ix_thread_priority", ["thread_id", "priority"]),
    ],
)
```

Index columns may be generated columns or system columns (`message_id`,
`source_topic`, `source_partition`, `changelog_offset`, `entity_key`, `payload`).
Referencing anything else raises `ConfigError` at `start()`.

Two indexes always exist and need not be declared: `ix_records_tp` on
`(source_topic, source_partition)` and `ix_records_key` on `entity_key`.

## Adding a column to a store that already has data

Just declare it and restart. `start()` runs the `ALTER TABLE`, and because
generated columns are `VIRTUAL` — computed on read, not stored — the new column
is immediately populated for rows that were written before it was declared:

```python
# Rows written earlier, with no `priority` column declared at the time:
GeneratedColumn("priority", "$.meta.priority", "INTEGER")
# After restart, those rows report their priority; rows whose payload has no
# `$.meta.priority` report NULL.
```

There is no backfill to run and no rehydrate needed.

## Changing or removing a column

**Changing** a declared column's `json_path` or `sql_type` while keeping its name
raises `ConfigError` at `start()`:

```text
ConfigError: generated column 'priority' drifted from the stored schema: ...
```

The same applies to an `Index` whose columns or `unique` flag changed. To make
such a change, use a new name, or drop the old object yourself with SQL before
restarting.

**Removing** a column or index from the configuration is not an error. KSQLite
leaves it in the database and logs it (`orphaned_column` / `orphaned_index`). It
keeps working; it is simply no longer declared. Drop it with SQL if you want it
gone.

## See also

- [`GeneratedColumn` and `Index`](../reference/types.md) — full validation rules
- [SQL schema](../reference/sql-schema.md) — the columns you can index
- [How to query the latest value per entity](query-latest-per-entity.md)
