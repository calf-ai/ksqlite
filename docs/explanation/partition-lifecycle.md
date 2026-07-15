# About the partition lifecycle

A partition's life in KSQLite is short to describe: it arrives, it is replayed,
it becomes visible, and eventually it leaves. The interesting part is what
happens when that story goes wrong — when replay takes too long, or the changelog
has lost the records the local checkpoint was counting on.

## Four states

| State | Visible | Meaning |
|---|---|---|
| `RESTORING` | No | Replaying its changelog. |
| `READY` | Yes | Replayed to the log end offset seen at rehydrate. |
| `READY_PARTIAL` | Yes | Revealed before replay finished. |
| `STANDBY` | No | Not owned. |

Visibility is not a property of the partition so much as a property of a small
table. The `records` view joins against `_owned`, and a partition's rows are
visible exactly when it has a row there. Rehydrate writes the rows first and
inserts into `_owned` last, so a partition appears all at once, fully replayed —
never half-restored.

That ordering is the whole reveal mechanism, and it is why a failed assignment
cannot expose partial data: if replay raises, the `_owned` row was never written,
so nothing became visible.

`start()` empties `_owned` entirely. A restarted process inherits a database full
of rows from partitions it may no longer own, and serving those would be a
correctness bug — the rows are stale, and their partitions belong to someone else
now. So ownership is always re-established by an assignment, never inherited from
disk.

## Why rehydrate blocks the rebalance

`on_partitions_assigned` replays synchronously inside your rebalance callback.
The whole consumer group waits on it. This looks like a bad idea until you
consider the alternative.

If replay happened in the background, the callback would return immediately and
your application would start processing records for a partition whose state was
still half-loaded. Every read would be a race. The only way to make that safe is
for the application to check readiness before every read — which is to say, to do
the blocking itself, less reliably.

Blocking makes the contract simple: when the callback returns, the partitions it
was given are queryable. The cost is rebalance latency, and that cost is why the
budget exists.

## Why one budget for the whole assignment

`rehydrate_timeout` covers the entire `on_partitions_assigned` call, not each
partition in it.

A per-partition budget would multiply by the assignment size. An instance handed
40 partitions with a 30-second-per-partition budget could hold the group's
rebalance open for twenty minutes — and a rebalance that slow triggers session
timeouts, which trigger another rebalance, which hands out partitions again. The
budget exists to prevent rebalance storms, so a budget that scales with the
assignment defeats its own purpose.

One absolute deadline for the callback keeps the worst case bounded no matter how
many partitions arrive.

There is one exception: the changelog topic check is exempt from the budget.
Correctness checks are not something to skip because time ran short — a missing
changelog is a real problem whether or not the clock has run out.

## Why an expired budget reveals instead of failing

When the deadline passes mid-replay, KSQLite does not raise. It marks the
partition `READY_PARTIAL` and reveals it anyway, at whatever it managed to
replay.

Failing the assignment would be the more obviously "correct" choice, and it is
the wrong one. A failed assignment means the group rebalances again, hands the
same partitions to someone else, and they hit the same timeout. Data that is
slow to restore does not get faster by being passed around, and the group never
converges.

Revealing partial data is a deliberate trade: the group makes progress and keeps
serving, and you are told clearly that it happened — through the state, and
through a `rehydrate_force_stop` log event. The judgement about whether stale data
is acceptable belongs to your application, not to the library, so KSQLite
surfaces the condition rather than deciding for you. If it is not acceptable, gate
on it (see
[How to check partition readiness](../how-to/check-partition-readiness.md)).

The same applies to a partition the budget never reached. It is revealed at its
stored checkpoint rather than left invisible, for the same reason: an invisible
partition and a stale one are both wrong, but only one of them lets you notice.

## Checkpoints and warm standby

Each partition records the highest changelog offset it has applied. Rehydrate
starts from that offset plus one, so a partition that was owned before, lost, and
regained replays only what it missed.

This is why revoke keeps the data. It would be simpler to delete a revoked
partition's rows — the view hides them anyway — but then every reassignment would
be a full replay from the start of the changelog. Keeping them makes a returning
partition cheap, and the cost is disk holding rows nobody can currently see.

A rebalance that shuffles partitions around a fleet therefore tends to be fast:
most instances are getting back partitions they held recently, and most of the
changelog has already been applied.

## When the changelog has moved on

The checkpoint is a promise about the changelog: *I have applied everything up to
here.* Retention can break that promise, and the two ways it breaks are opposite.

**The changelog trimmed past the checkpoint.** Records the local state depended on
are gone. There is no way to recover them — they do not exist anywhere. KSQLite
resets to the earliest surviving offset and replays from there, leaving a
permanent hole in the local state. This is data loss, and it is logged loudly
(`truncation_reset`) because no library-level cleverness can undo it. The only
real fix is retention that does not trim a source of truth.

**The checkpoint sits above the log end.** The local state claims to have applied
records the changelog does not have. Left alone, this would make every future
rehydrate try to read past the end and reset again, forever.

Both are repaired by moving the checkpoint — the only writes that move it
backwards, and the reason the checkpoint is otherwise strictly monotonic. Without
the clamp, a partition in either state would re-enter the reset path on every
single assignment, replaying the entire changelog each time and never
converging. The clamp turns a permanent malfunction into a single logged
incident.

Both clamps log at `WARNING` rather than `INFO` because they are only ever
symptoms. Nothing that happens in normal operation moves a checkpoint backwards.

## See also

- [About the durability contract](durability.md) — what survives a crash
- [How to check partition readiness](../how-to/check-partition-readiness.md)
- [`PartitionState`](../reference/types.md) — the values you can inspect
