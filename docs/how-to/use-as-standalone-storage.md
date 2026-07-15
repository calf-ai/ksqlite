# How to use KSQLite as standalone storage

This guide shows you how to use KSQLite as durable local SQL storage without
consuming a Kafka topic — for example on edge nodes that need fast local reads
and a copy of their data somewhere safe.

In this mode Kafka is not an input. It is the durability and distribution layer:
each shard's changelog is the source of truth, and any instance that claims the
shard rebuilds its local SQLite from it.

It assumes you have a broker reachable and KSQLite installed. If you have not
used KSQLite before, work through
[Build a local store that rebuilds itself from Kafka](../tutorial/build-a-local-store.md)
first.

## Constraints to design around

- **One writer per shard, always.** Two instances appending to one shard diverge
  permanently and silently — rehydrating does not reconcile them.
- **No tailing.** An instance sees its own appends plus whatever it replayed when
  it claimed the shard. Nothing arrives after that without another rehydrate.
- **Append-only.** `append()` inserts; it never updates or deletes. See
  [How to query the latest value per entity](query-latest-per-entity.md).
- **The changelog is the only durable copy.** Anything it no longer holds is gone.

If you need many instances reading the same continuously-updated data, KSQLite is
the wrong tool. For why these hold, see
[About the KSQLite architecture](../explanation/architecture.md).

## Step 1 — Choose shard identities

A shard is a `TopicPartition`. Despite the type, it is only a name: KSQLite uses
it to derive the changelog topic and never reads or writes the topic it names.
That topic does not need to exist.

```python
from aiokafka import TopicPartition

SHARD = TopicPartition("device-state", 0)  # -> changelog: device-state.p0.changelog
```

The name is free, but the changelog name it resolves to through
`changelog_topic_template` must be a legal Kafka topic name — at most 249
characters, and only `[A-Za-z0-9._-]`. A shard named `"device state"` resolves to
`device state.p0.changelog` and raises `ConfigError`, not at `start()` but on the
first `append()` or assignment that needs it.

Choose the number of shards up front. Shards are how you divide data across
instances, and re-dividing later means rebuilding state, so pick a count you can
live with. If you only ever run one instance, one shard is fine.

## Step 2 — Create the store with infinite retention

The changelog is your only durable copy, so it must not expire.

```python
from ksqlite import GeneratedColumn, Index, KSQLite

store = KSQLite(
    db_path="/var/lib/myapp/state.db",
    bootstrap_servers="broker:9092",
    generated_columns=[GeneratedColumn("device_id", "$.device_id")],
    indexes=[Index("ix_device", ["device_id"])],
    create_topics_retention_ms=-1,  # -1 = infinite
)
await store.start()
```

`create_topics_retention_ms=-1` lets KSQLite create missing changelogs with
infinite retention, which is convenient but means your application needs topic
creation rights. In production, prefer pre-provisioning the topics and leaving
this `None` so a missing changelog fails loudly. See
[How to provision changelog topics](provision-changelog-topics.md).

## Step 3 — Claim the shards you own

Nothing is visible to `query()` until it is assigned. Without a consumer there is
no rebalance listener, so call the hook yourself at startup:

```python
await store.on_partitions_assigned([SHARD])
```

This creates the changelog if needed, replays it into local SQLite, and reveals
the shard's rows. It is the same call a rebalance listener would make.

If you run several instances, give each a disjoint set of shards. Deciding which
instance owns what is your job — a static config map, an ordinal from your
scheduler, or a lock service. For example, with an instance ordinal:

```python
SHARD_COUNT = 8
ordinal = int(os.environ["INSTANCE_ORDINAL"])
instances = int(os.environ["INSTANCE_COUNT"])

mine = [
    TopicPartition("device-state", p)
    for p in range(SHARD_COUNT)
    if p % instances == ordinal
]
await store.on_partitions_assigned(mine)
```

## Step 4 — Append and query

```python
await store.append(
    source=SHARD,
    entity_key="device-7",
    payload={"device_id": "device-7", "battery": 82},
)

rows = await store.query(
    "SELECT payload FROM records WHERE device_id = ?", ["device-7"]
)
```

`append()` produces to the changelog and waits for the ack before writing
locally, so it is not a local-only write — it costs a broker round trip. Await
each `append()` for a shard before starting the next.

Do not retry a failed `append()`. See [How to handle failures](handle-failures.md).

## Step 5 — Hand a shard over

To move a shard to another instance, release it on the old one first:

```python
await store.on_partitions_revoked([SHARD])
```

The rows and the checkpoint stay on disk, so if this instance claims the shard
again it resumes from the checkpoint instead of replaying everything. The new
owner calls `on_partitions_assigned` and replays the changelog.

Release before the new owner claims. Two owners writing at once will not corrupt
the changelog, but each ends up permanently missing the other's records — a gap
no rehydrate repairs.

At shutdown, `await store.stop()` is enough on its own — `start()` clears
ownership, so a restarted process never serves rows it has not re-claimed.

## Variations

- **Read-only replicas.** An instance that only reads can claim a shard, replay,
  and query it — as long as you accept that its data is frozen as of the claim.
  To refresh, revoke and re-assign, which replays from the checkpoint forward.
- **A single instance, no sharding.** Use one shard and claim it at startup. You
  get a local SQL database whose durable copy lives in Kafka and which any
  replacement instance can rebuild.

## See also

- [How to provision changelog topics](provision-changelog-topics.md)
- [How to query the latest value per entity](query-latest-per-entity.md)
- [About the KSQLite architecture](../explanation/architecture.md)
