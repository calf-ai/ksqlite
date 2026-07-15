# How to provision changelog topics

This guide shows you how to create and configure changelog topics for a
production deployment.

A changelog is the durable copy of a shard's data. If it is compacted, trimmed,
or under-replicated, you lose data that no local SQLite file can give back. It
deserves the same care as any other system of record.

## Step 1 — Work out the topic names

One changelog per source `(topic, partition)`, named by
`changelog_topic_template`:

```text
{source_topic}.p{partition}.changelog
```

So a source topic `messages` with 6 partitions needs six changelogs:

```text
messages.p0.changelog
messages.p1.changelog
...
messages.p5.changelog
```

If you set a custom template, it must contain both `{source_topic}` and
`{partition}`, and every name it resolves to must be a legal Kafka topic name —
at most 249 characters, matching `[A-Za-z0-9._-]+`. KSQLite raises `ConfigError`
otherwise.

For standalone use, the shard names you choose determine the changelog names in
exactly the same way. See
[How to use KSQLite as standalone storage](use-as-standalone-storage.md).

## Step 2 — Create them with the right settings

| Setting | Value | Why |
|---|---|---|
| `partitions` | `1` | KSQLite always produces to partition 0 and replays partition 0. |
| `cleanup.policy` | `delete` | **Never `compact`.** |
| `retention.ms` | Long, or `-1` | Bounds how much history a rebuild can recover. |
| `replication.factor` | 3, or your cluster's standard | This is a system of record. |

```sh
rpk topic create messages.p0.changelog \
  --partitions 1 \
  --replicas 3 \
  --topic-config cleanup.policy=delete \
  --topic-config retention.ms=-1
```

### Never compact a changelog

Compaction keeps only the last record per key. KSQLite's changelog holds *every*
append, and `entity_key` is the record key — so compaction throws away all but
the most recent record for each entity. A rebuild from a compacted changelog
silently produces a store missing history it should have.

KSQLite checks this on a partition's first assignment and logs an error:

```text
ERROR ksqlite.topics changelog topic 'messages.p0.changelog' has
cleanup.policy='compact'; compaction collapses an entity to its last message —
rebuilds from this changelog can lose history
```

**The assignment still proceeds.** Topic configuration is yours to own, so
KSQLite reports the problem rather than refusing to run. Alert on the
`compact_policy_detected` event. To fix it, recreate the topic with
`cleanup.policy=delete` — changing the policy on a topic that has already been
compacted does not bring back what compaction removed.

### Choose retention deliberately

Retention decides what a rebuild can recover. A changelog you treat as a source
of truth wants infinite retention (`-1`).

If retention does trim records that a local checkpoint depended on, the next
rehydrate detects it, logs `truncation_reset` and `checkpoint_clamped` at
`WARNING`, and replays what is left. The store keeps working; the trimmed records
are gone. Alert on both events.

## Step 3 — Let a missing topic fail loudly

Leave `create_topics_retention_ms` as `None` in production:

```python
store = KSQLite(
    db_path="state.db",
    bootstrap_servers="broker:9092",
    # create_topics_retention_ms stays None
)
```

Then a changelog that has not been provisioned raises rather than being created
with settings nobody chose:

```text
TopicNotFoundError: changelog topic 'messages.p0.changelog' does not exist and
create_topics_retention_ms is None
```

Setting `create_topics_retention_ms` is a development convenience. Topics KSQLite
creates get `replication_factor=1` and a single partition — fine on a laptop,
not on a cluster, because a replication factor of 1 means one broker failure
loses the changelog permanently. Provision the topics yourself and leave this
`None`.

## Step 4 — Grant the right permissions

| Client | Needs |
|---|---|
| Producer | `WRITE` on each changelog topic |
| Rehydrate consumer | `READ` on each changelog topic |
| Admin | `DESCRIBE` to list topics; `DESCRIBE_CONFIGS` to verify `cleanup.policy` |

If `DESCRIBE_CONFIGS` is denied, KSQLite cannot verify the policy. It warns and
proceeds rather than claiming the topic is safe:

```text
WARNING ksqlite.topics cannot verify cleanup.policy of 'messages.p0.changelog'
(error code 29: ...); proceeding unverified
```

Alert on `policy_verify_unauthorized` and `policy_verify_failed` — an unverified
changelog might be compacted without you hearing about it.

You can turn the check off with `verify_changelog_policy=False`, which skips it
entirely. Only do that when something else guarantees the policy, since you are
removing the only automatic warning you get.

## Repartitioning

Do not change a source topic's partition count once you are running. Local state
is scoped per partition, so repartitioning moves entities to partitions whose
changelogs never held them. KSQLite assumes a fixed partition count and cannot
detect or repair this.

Add capacity by adding instances, not partitions. If you must repartition, treat
it as a migration: stop, rebuild the changelogs, rebuild local state.

## See also

- [Changelog format](../reference/changelog-format.md) — what is in these topics
- [Logging](../reference/logging.md) — the events named above
- [About the durability contract](../explanation/durability.md)
