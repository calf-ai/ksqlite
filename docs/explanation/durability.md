# About the durability contract

KSQLite calls the changelog its source of truth, and that phrase does real work.
It is worth being precise about what it promises, because the gap between "the
data is in Kafka" and "no record is ever lost or duplicated" is where the
interesting failure modes live.

The honest summary: **KSQLite is best-effort, not end-to-end crash-durable.**
Committed SQLite state survives restarts, and the changelog survives the machine
entirely. But there are windows where a record can be lost before it ever reaches
the changelog, and windows where one can arrive twice.

## Produce first, write second

Every `append()` does two things in a fixed order: produce to the changelog and
wait for the ack, then write to local SQLite.

The order is the contract. The durable copy lands first, so the local copy is
always derived from something that already exists elsewhere. Reverse it and a
crash between the two would leave a row in SQLite that no changelog knows about —
a fact invented locally, which a rebuild would silently erase.

That order gives each failure a clear meaning:

**Produce fails.** Nothing is written anywhere. The record does not exist as far
as KSQLite is concerned. This is clean: no local state to reconcile, nothing to
undo.

**Produce succeeds, local write fails.** The record is in the changelog but not in
SQLite. The changelog is the truth and the local copy is behind, so a rehydrate
of that partition replays the record and applies it.

That recovery is real but **conditional**, and the condition is easy to miss:

> The record is recovered only if the partition rehydrates before any *later*
> append to it commits locally.

The reason is that `append()` advances the partition's checkpoint to its own
offset. A later successful append therefore moves the checkpoint *past* the
missing record, and rehydrate resumes from the checkpoint — so the gap is never
replayed. The partition then reports `READY` with a lag of 0 while permanently
missing a record the changelog still holds. Nothing detects this.

Concretely, with offsets 0, 1, 2 where 1's local write failed and 2 succeeded:
the changelog holds `[0, 1, 2]`, SQLite holds `[0, 2]`, the checkpoint is `2`,
and every future rehydrate starts at `3`. Record 1 is gone locally, forever.

Rehydrating immediately — before appending to that partition again — does
recover it. See [How to handle failures](../how-to/handle-failures.md).

So the asymmetry is narrower than it first looks. Failures that leave the durable
copy intact are *recoverable*, but not *automatically recovered*: the changelog
still has the record, and only a rehydrate that has not been overtaken will bring
it back.

## Why `append()` cannot be retried

This is the sharpest edge in the library, and it is worth understanding rather
than just obeying.

Deduplication works on `message_id`: a UUID minted per `append()` call, carried in
the record's headers, and enforced by a unique constraint. Replaying a changelog
any number of times converges, because the same record carries the same
`message_id` every time and the second insert does nothing.

A retry breaks that, because a retry is a *new call*, and a new call mints a *new*
`message_id`. The two attempts are indistinguishable in content and unrelated in
identity, so nothing deduplicates them.

That only matters when the first attempt actually succeeded — which is exactly the
case you cannot detect. If a produce reaches the broker and the acknowledgement is
lost on the way back, `append()` raises `ChangelogProduceError` having genuinely
written the record. A retry then writes it a second time, and both copies are now
permanently in the source of truth. Every future rebuild faithfully reproduces
the duplicate, because from the changelog's point of view it is not a duplicate at
all: two records, two identities, both real.

So the rule is not "retries are discouraged." It is that a retry converts an
*uncertain* failure into a *certain* corruption of the source of truth, and it
does so in the case where the operation had already worked.

The library could have hidden this. Reusing the `message_id` across retries would
make `append()` idempotent — but only for retries the process itself performs. A
process that crashes and re-drives the record from its consumer has no memory of
the old `message_id`, and the duplicate returns. The honest half-measure was
judged worse than the clear rule, because it would make the hazard rare rather
than absent, and rare hazards are the ones that reach production undetected.

The practical consequence: treat a failed `append()` as a failed unit of work and
let your consumer's redelivery re-drive it. See
[How to handle failures](../how-to/handle-failures.md).

## Where records can be lost

KSQLite's own loss window is narrow, because a record it never produced is a
record it never accepted. The wider window is at the boundary it does not
control: between your consumer taking a record and `append()` returning.

If your consumer commits offsets before `append()` succeeds — which is what
`enable_auto_commit=True` does, on a timer — a crash in that gap loses the record.
Kafka believes it was processed; no changelog holds it. Nothing downstream can
detect this, because the evidence is gone from both sides.

This is not a defect KSQLite can fix. It sits in your consumer's offset handling,
which KSQLite deliberately does not own. What you can do is choose where the risk
lands: commit after `append()` returns and you trade the loss window for a
duplicate window, since a crash after `append()` and before the commit means the
record is re-consumed and appended again under a new `message_id`.

There is no configuration that removes both. Exactly-once between two systems
requires a transaction spanning them, and there isn't one here. So the choice is
which way to fail, and the library's job is to make the choice visible rather
than to pretend it away.

## Where duplicates come from

Three sources, all outside the changelog's own guarantees:

- **A retried `append()`** — the case above. Avoidable, entirely, by not retrying.
- **A phantom produce.** The broker wrote the record; the ack was lost. The
  caller sees a failure that was really a success. This is indistinguishable from
  a genuine failure at the client, which is precisely why the retry rule is
  absolute.
- **Re-consumption after a crash.** The source record is delivered again and
  appended again, with a new `message_id`.

Note what is *not* on this list: replay. Replaying a changelog, in whole or in
part, any number of times, cannot duplicate anything. That is the one guarantee
the design makes unconditionally, and it is what makes rehydration safe to do
whenever ownership moves.

The reason is structural rather than careful: the append path and the replay path
execute the *same* insert statement, with the same conflict clause, on the same
identity column. They are not two implementations kept in agreement by discipline
— they are one statement used twice.

## What retention decides

Retention is the one setting that can quietly destroy data that KSQLite otherwise
guarantees.

Local SQLite is disposable because the changelog can rebuild it. That is true only
while the changelog still holds the records. Trim it and the rebuild is
incomplete — not wrong, not detectably broken, just missing history that nothing
can recover.

A changelog that is genuinely a source of truth wants infinite retention. If that
is not acceptable, then what you have is a store with a horizon, and you should
know where that horizon is. KSQLite will tell you when it crosses one
(`truncation_reset`), but by then the records are already gone.

Compaction is the same hazard wearing a friendlier face. It looks like retention
tuning and behaves like deletion: it keeps the last record per key and discards
the rest. For an append-only log where every record matters, that is silent,
permanent, and shaped exactly like normal operation. See
[How to provision changelog topics](../how-to/provision-changelog-topics.md).

## See also

- [How to handle failures](../how-to/handle-failures.md) — what to do about each case
- [Changelog format](../reference/changelog-format.md) — `message_id` and dedup
- [About the KSQLite architecture](architecture.md) — why the changelog is the source of truth
