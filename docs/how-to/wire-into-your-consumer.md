# How to wire KSQLite into your consumer

This guide shows you how to hook KSQLite into an application that already
consumes a Kafka topic, so each instance keeps a queryable local copy of the
partitions it owns and rebuilds that copy when partitions move.

KSQLite does not consume your source topic. You keep your own consumer, your own
group, and your own offset handling; KSQLite only needs to be told when
partitions arrive and leave.

It assumes you have a working `aiokafka` consumer and KSQLite installed.

If you are not consuming a topic at all, see
[How to use KSQLite as standalone storage](use-as-standalone-storage.md) instead.

## Step 1 — Create the store

One store per process, holding one SQLite database:

```python
from ksqlite import GeneratedColumn, Index, KSQLite

store = KSQLite(
    db_path="state.db",
    bootstrap_servers="broker:9092",
    generated_columns=[GeneratedColumn("thread_id", "$.thread_id")],
    indexes=[Index("ix_thread", ["thread_id"])],
)
await store.start()
```

Leave `create_topics_retention_ms` unset in production so a missing changelog
fails loudly rather than being created with whatever retention happens to be
convenient. See [How to provision changelog topics](provision-changelog-topics.md).

## Step 2 — Call the hooks from your rebalance listener

The ordering is what matters, and it differs between the two hooks:

- **On assign, call KSQLite first.** It replays the changelog and reveals the
  partition. Until it returns, `query()` cannot see that partition's rows, so
  any of your logic that reads state must run after it.
- **On revoke, call KSQLite last.** Your code may still need to read or flush
  state; once KSQLite revokes, those rows are hidden.

```python
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener


class StoreListener(ConsumerRebalanceListener):
    async def on_partitions_assigned(self, assigned):
        await store.on_partitions_assigned(assigned)   # first: rehydrate
        ...  # your logic may now read state

    async def on_partitions_revoked(self, revoked):
        ...  # your logic flushes first
        await store.on_partitions_revoked(revoked)     # last: hide the rows
```

Both hooks run inside the rebalance callback, so the whole group waits on them.
`on_partitions_assigned` is bounded by `rehydrate_timeout` (30 seconds by
default) across the entire assignment, not per partition. Partitions the budget
does not reach are still revealed, marked `READY_PARTIAL`. See
[How to check partition readiness](check-partition-readiness.md).

## Step 3 — Subscribe with the listener

```python
consumer = AIOKafkaConsumer(
    bootstrap_servers="broker:9092",
    group_id="my-app",
    enable_auto_commit=True,
    auto_offset_reset="earliest",
)
await consumer.start()
consumer.subscribe(["messages"], listener=StoreListener())
```

This is your consumer, configured however your application needs. KSQLite's own
rehydrate consumer is separate and configured independently through
`consumer_config`.

## Step 4 — Append as you consume

Pass the record's own partition as `source`. That is what ties a record to the
changelog that will restore it.

```python
from aiokafka import TopicPartition

async for record in consumer:
    await store.append(
        source=TopicPartition(record.topic, record.partition),
        entity_key=record.key.decode(),
        payload=record.value.decode(),
    )
```

A `str` payload must parse as JSON; anything else must be JSON-serializable.
Await each `append()` for a partition before the next.

If a record's key can be `null`, handle it before calling `append()` — an empty
or non-string `entity_key` raises `ValueError`.

Do not retry a failed `append()`; it is not idempotent. See
[How to handle failures](handle-failures.md).

## Step 5 — Shut down

```python
await consumer.stop()
await store.stop()
```

Stop the consumer first so no new appends start, then stop the store, which
drains in-flight appends and flushes the changelog producer.

## Variations

- **Manual offset commits.** KSQLite does not read or commit your source offsets,
  so commit however you like. Note that committing before `append()` returns
  widens the window in which a record is consumed but never materialized.
- **Several source topics.** One store handles them all; `source` distinguishes
  them, and each `(topic, partition)` gets its own changelog.
- **Your own consumer already has a listener.** Call the store's hooks from
  inside it, in the order shown in step 2, rather than adding a second listener.

## See also

- [How to check partition readiness](check-partition-readiness.md)
- [How to handle failures](handle-failures.md)
- [About the partition lifecycle](../explanation/partition-lifecycle.md)
