# How to check partition readiness

This guide shows you how to tell whether the data you are querying is complete,
using `partition_states()`.

The `records` view hides partitions you do not own — but it does **not** hide
partitions that were revealed before their replay finished. Those are
`READY_PARTIAL`: their rows are visible and incomplete. If serving stale data
matters to you, check for them.

## Read the states

`partition_states()` is synchronous and returns a snapshot of every partition
this process has seen:

```python
from ksqlite import PartitionStatus

states = store.partition_states()
for tp, state in states.items():
    print(tp, state.state, state.checkpoint_offset, state.log_end_offset, state.lag)
```

| State | In `records` | Meaning |
|---|---|---|
| `RESTORING` | No | Replaying now. |
| `READY` | Yes | Replayed fully, as of its rehydrate. |
| `READY_PARTIAL` | Yes | Revealed before replay finished. **Incomplete.** |
| `STANDBY` | No | Not owned. Data kept on disk. |

## Detect incomplete partitions

```python
partial = [
    tp for tp, s in store.partition_states().items()
    if s.state is PartitionStatus.READY_PARTIAL
]
if partial:
    logger.warning("serving incomplete data for %s", partial)
```

A partition becomes `READY_PARTIAL` when `rehydrate_timeout` expires during
`on_partitions_assigned` — either mid-replay, or before its replay ever started.
The budget covers the whole assignment, not each partition, so a large assignment
is the usual cause.

KSQLite reveals these partitions deliberately rather than failing the
assignment: holding the rebalance callback open longer would stall the whole
consumer group. See
[About the partition lifecycle](../explanation/partition-lifecycle.md).

## Fail a health check on partial data

If your service should not serve incomplete results, gate readiness on it:

```python
def is_ready() -> bool:
    return all(
        s.state is not PartitionStatus.READY_PARTIAL
        for s in store.partition_states().values()
    )
```

To let a `READY_PARTIAL` partition finish, trigger another rehydrate: revoke and
re-assign it, which resumes from its checkpoint rather than replaying from the
start.

```python
await store.on_partitions_revoked([tp])
await store.on_partitions_assigned([tp])
```

If partitions are routinely partial, raise `rehydrate_timeout` — but remember the
group waits on that callback, so raising it trades rebalance latency for
completeness.

## Do not use `lag` as a live metric

`lag` and `log_end_offset` are captured at rehydrate and **not refreshed
afterwards**. They describe the gap measured when the partition was last
assigned, not the gap now:

```python
state = store.partition_states()[tp]
state.lag  # as of the last rehydrate; appends do not update it
```

Both are `None` until a rehydrate has run. `checkpoint_offset` and `state` are
live.

So `lag` answers "how far behind was this partition when I claimed it", which is
useful for spotting a slow or partial restore. It does not answer "how far behind
am I now" — nothing in KSQLite tracks that, because it never tails the changelog
after rehydrate.

## See also

- [`PartitionState` and `PartitionStatus`](../reference/types.md)
- [Logging](../reference/logging.md) — the `rehydrate_force_stop` event
- [About the partition lifecycle](../explanation/partition-lifecycle.md)
