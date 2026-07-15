# How to query the latest value per entity

This guide shows you how to get the most recent record for each `entity_key`,
which is what you want whenever you are using KSQLite to hold current state
rather than a history.

KSQLite is append-only. `append()` inserts a row every time, so an entity written
five times has five rows and `entity_key` is not unique:

```python
await store.append(source=SHARD, entity_key="user-1", payload={"v": 1})
await store.append(source=SHARD, entity_key="user-1", payload={"v": 2})
# SELECT * FROM records WHERE entity_key = 'user-1'  ->  two rows
```

"Latest" means the row with the highest `changelog_offset`, because a changelog's
offsets increase with every append.

## Get the latest row per entity

Use a window function, partitioning by the shard **and** the entity key:

```python
LATEST_SQL = """
    SELECT source_topic, source_partition, entity_key, payload
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY source_topic, source_partition, entity_key
            ORDER BY changelog_offset DESC
        ) AS rn
        FROM records
    )
    WHERE rn = 1
"""

rows = await store.query(LATEST_SQL)
```

To narrow it to one entity, filter inside the subquery so the window only ranks
that entity's rows:

```sql
SELECT payload FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY source_topic, source_partition, entity_key
        ORDER BY changelog_offset DESC
    ) AS rn
    FROM records
    WHERE entity_key = ?
)
WHERE rn = 1
```

## Partition by the shard, not just the entity key

`changelog_offset` is only meaningful **within one shard's changelog**. Every
changelog starts at offset 0, so offsets from different shards are not
comparable, and comparing them silently drops rows.

This query looks reasonable and is wrong:

```sql
-- WRONG: compares offsets across shards
SELECT entity_key, payload FROM records r
WHERE changelog_offset = (
    SELECT MAX(changelog_offset) FROM records WHERE entity_key = r.entity_key
)
```

With `user-1` present on two shards — at offset 2 on one and offset 0 on the
other — this returns **one** row, not two. The subquery's maximum is 2, which the
second shard's row can never match, so that entity disappears from the result
entirely.

Always include `source_topic` and `source_partition` in the partitioning, as the
working query above does. If your process only ever owns one shard the bug is
invisible; it appears the day it owns two.

## Local growth is unbounded

Nothing prunes superseded rows, so a store holding many revisions per entity
grows without limit. Two facts constrain what you can do about it:

- `query()` is read-only. A `DELETE` through it raises `StorageError`.
- Deleting rows by other means only changes this local copy. The changelog still
  holds every record, so the next rehydrate restores them.

The levers that remain are appending less, or bounding the changelog's retention
— which permanently discards records you may be treating as a source of truth.
See [How to provision changelog topics](provision-changelog-topics.md).

## See also

- [SQL schema](../reference/sql-schema.md) — `changelog_offset` and the system columns
- [How to declare queryable columns](declare-queryable-columns.md)
- [About the KSQLite architecture](../explanation/architecture.md) — why it is append-only
