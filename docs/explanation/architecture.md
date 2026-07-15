# About the KSQLite architecture

KSQLite exists to answer a question that comes up whenever an application needs
fast local state: *where does the data live when the box it lives on goes away?*

The usual answers are unsatisfying. Keep state in memory and it dies with the
process. Keep it in a local database and it dies with the disk, or strands itself
on one machine. Put it in a remote database and every read becomes a network
round trip — the thing you were trying to avoid.

KSQLite's answer is to split the two jobs that a database normally does. SQLite
handles fast local reads. Kafka handles durability and movement. Neither is asked
to do the other's job.

## Two halves

**SQLite is the read side.** It sits on local disk, answers ordinary SQL in
microseconds, and is treated as disposable. Nothing important is lost if the file
is deleted, because nothing lives only there.

**Kafka is the durable side.** Every write goes to a per-shard *changelog* topic
before it touches SQLite. The changelog is the source of truth. It lives on the
network, it is replicated, and any instance that can reach the broker can rebuild
a shard's SQLite by replaying it.

This is why the tutorial's payoff — deleting the database and querying again —
works. There was never anything in that file that the changelog did not already
have.

The trade is explicit: writes cost a broker round trip, reads cost nothing. That
is a good trade for read-heavy state and a poor one for write-heavy queues. If
your workload is mostly writes you are paying Kafka's latency on every one of
them and getting little back.

## Why a changelog per shard

One changelog per `(topic, partition)` — never one big topic for everything —
because the changelog's boundaries are what make a rebuild bounded. When an
instance claims shard 3, it replays shard 3's changelog and nothing else. A
shared changelog would force every instance to read every other instance's writes
to find its own.

Single-partition changelogs, too. Ordering within a shard is the whole point:
records replay in the order they were appended, so replaying produces the same
state the writer had. Kafka only orders within a partition, so a multi-partition
changelog would have no meaningful replay order.

This is also why repartitioning a source topic is so damaging. Local state is
scoped per partition. Change the partition count and entities move to partitions
whose changelogs never held them — the data still exists, but no rebuild will
find it where it now belongs.

## Why append-only

`append()` inserts. It never updates or deletes, and `entity_key` is not unique:
write an entity five times and you get five rows.

This follows from the changelog rather than being a separate decision. A
changelog is an ordered log of what happened; replaying it must reproduce the
state it described. If `append()` overwrote by key, replay would have to
reproduce the overwriting too — and the log would have to record deletions,
orderings, and conflicts to get there. Keeping every record makes replay a
straight re-application of the log, in order, with nothing to reconcile.

The cost lands on the reader: current state per entity is a query, not a lookup
(see [How to query the latest value per entity](../how-to/query-latest-per-entity.md)),
and nothing prunes old revisions. That is a real cost, and it is the one place
where the append-only model asks something of you.

It also explains why compaction is so dangerous here. Compaction keeps the last
record per key, which is exactly the information a keyed store wants and exactly
the information a log must not lose.

## Ownership, not replication

The most common wrong assumption about KSQLite is that instances share a view of
the data. They do not.

An instance sees its own appends, plus whatever it replayed when it claimed the
shard. Nothing tails the changelog afterwards.

Two instances writing to the same shard therefore each hold a local view missing
the other's writes — and, worse than being merely stale, they cannot recover.
Rehydrating does not fix it. Each instance's own appends have already advanced
its checkpoint past the other's interleaved offsets, and replay resumes from the
checkpoint, so every co-writer record at or below that checkpoint is skipped
permanently. (Records *above* it replay normally, which is what makes the damage
so easy to miss: some of the other writer's data does arrive.) Both instances
settle at `READY` with a lag of 0, each missing records the changelog plainly
holds, neither showing any sign of it. Only rebuilding from an empty database
recovers the full log.

This is why "one writer per shard" is a hard rule rather than a performance
guideline. Violating it does not cause conflicts you can detect and reconcile —
it causes silent, permanent divergence.

So the model is *partition ownership*: one owner per shard at a time, holding a
private local copy, with the changelog as the handoff mechanism between owners.
When ownership moves, the new owner replays and the data follows.

This is a deliberate narrowing. Continuous tailing would mean every instance
paying to read every write, plus a real story for conflicting concurrent writes
to the same entity — the hard part of distributed databases. KSQLite declines
that problem by declaring one writer per shard. It is a smaller promise, honestly
kept, and it is why the library is small enough to read in an afternoon.

The practical consequence: if you need many instances reading the same live data,
KSQLite is the wrong tool. If you need each instance to own a slice of data, read
it fast, and not lose it when the box dies, it fits.

## The library is not a service

KSQLite does not own your consumer. It does not subscribe to your topics, join
your group, or commit your offsets. It exposes two hooks and expects you to call
them when partitions arrive and leave.

That is why it works equally well with no consumer at all. `on_partitions_assigned`
does not care whether a rebalance called it or your `main()` did — either way it
means "I own this shard now; make it real." The `TopicPartition` you pass is a
name for a shard, not a subscription. It does not even need to be a topic that
exists.

Two shapes fall out of the same mechanism:

- **Attached to a consumer.** A rebalance listener calls the hooks, so local state
  follows partition ownership around the fleet automatically.
- **Standalone.** You call the hooks yourself and get a local SQL store whose
  durable copy lives in Kafka. Kafka is deployed to hold the data, not to feed it.

The second is not a workaround. It is the same machinery with the rebalance
listener left out.

## See also

- [About the partition lifecycle](partition-lifecycle.md) — how ownership changes hands
- [About the durability contract](durability.md) — what "durable" does and does not promise
- [How to use KSQLite as standalone storage](../how-to/use-as-standalone-storage.md)
