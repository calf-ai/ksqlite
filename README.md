# KSQLite

A library that gives a Kafka-consuming application a **local, SQL-queryable,
rebuildable materialized view**, backed durably by a per-partition Kafka
**changelog**. Local state lives in SQLite; when the host consumer rebalances,
the state for a reassigned partition is rehydrated by replaying its changelog.
It is a hand-rolled Kafka Streams state-store + changelog pattern, with SQLite
(for real SQL querying) in place of RocksDB.

KSQLite is an embeddable library, not a service. It does not own source-topic
consumption — your application keeps its own consumer and hooks KSQLite into
its rebalance listener. For the full design, guarantees, and rationale, read
[`docs/DESIGN.md`](docs/DESIGN.md).

## Requirements

- Python >= 3.10
- SQLite >= 3.38 linked into your interpreter (`start()` fails fast with
  `SQLiteVersionError` otherwise — see [Older SQLite runtimes](#older-sqlite-runtimes))
- A Kafka-compatible broker (tested against Redpanda; `acks=1` and
  idempotence-off defaults are Tansu-compatible)

## Quickstart: materialize a topic and query it

1. **Create the store.** One SQLite DB per process. Declare the payload fields
   you want to query as generated columns:

   ```python
   from ksqlite import GeneratedColumn, Index, KSQLite

   store = KSQLite(
       db_path="state.db",
       bootstrap_servers="localhost:9092",
       generated_columns=[GeneratedColumn("thread_id", "$.thread_id")],
       indexes=[Index("ix_thread", ["thread_id"])],
       create_topics_retention_ms=-1,  # dev convenience; ops pre-creates in prod
   )
   await store.start()  # or: async with KSQLite(...) as store:
   ```

2. **Wire the rebalance hooks into your own listener.** Call the store's
   assign hook *before* your own logic reads state, and the revoke hook
   *after* your own flush:

   ```python
   from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition

   class HostListener(ConsumerRebalanceListener):
       async def on_partitions_assigned(self, assigned):
           await store.on_partitions_assigned(assigned)  # rehydrates first
           ...  # your logic may now read state

       async def on_partitions_revoked(self, revoked):
           ...  # flush your own state first
           await store.on_partitions_revoked(revoked)

   consumer = AIOKafkaConsumer(
       bootstrap_servers="localhost:9092",
       group_id="my-app",
       enable_auto_commit=True,   # at-most-once source consumption (spec §2)
       auto_offset_reset="earliest",
   )
   await consumer.start()
   consumer.subscribe(["messages"], listener=HostListener())
   ```

3. **Append as you consume.** The payload is any JSON-serializable object, or
   a string that parses as JSON. Await each append before the next one for a
   given partition:

   ```python
   async for record in consumer:
       await store.append(
           source=TopicPartition(record.topic, record.partition),
           entity_key=record.key.decode(),
           payload=record.value.decode(),
       )
   ```

4. **Query with plain SQL** against the auto-scoped `records` view (it shows
   only partitions this process currently owns and has finished revealing):

   ```python
   rows = await store.query(
       "SELECT payload FROM records WHERE thread_id = ?", ["th-42"]
   )
   ```

   Add `into=` to hydrate the `payload` column into a Pydantic v2 model or a
   dataclass (the query must select `payload`):

   ```python
   from dataclasses import dataclass

   @dataclass
   class Message:
       thread_id: str
       text: str

   messages = await store.query(
       "SELECT payload FROM records WHERE thread_id = ?", ["th-42"], into=Message
   )
   ```

5. **Shut down** with `await store.stop()` — it drains in-flight appends and
   flushes the changelog producer.

## Observing partition state

`store.partition_states()` (synchronous) returns a
`dict[TopicPartition, PartitionState]` with each partition's live status
(`RESTORING`, `READY`, `READY_PARTIAL`, `STANDBY`), its live checkpoint
offset, and the log-end/lag values cached at the last rehydrate. A
`READY_PARTIAL` partition was force-stopped at the replay budget and serves
partial state — see `docs/DESIGN.md` §7.

KSQLite logs structured events on the `ksqlite.*` loggers (a `NullHandler` is
installed; enable INFO to see them). Events carry an `event` field —
`rehydrate_start`, `rehydrate_end`, `rehydrate_force_stop`,
`truncation_reset`, `checkpoint_clamped`, `foreign_record_skipped`,
`produce_failure`, `append_to_non_ready`, `compact_policy_detected`, and
more. The truncation reset, checkpoint clamps, and compact-policy detection
are WARNING/ERROR-level and operator-actionable.

## Operational contract (the short version)

Full details: `docs/DESIGN.md` §10 and §13–§16.

- **Changelog topics** are one per source `(topic, partition)`,
  single-partition, `cleanup.policy=delete` — **never compact** (compaction
  would collapse an entity to its last message). The policy check is
  best-effort: KSQLite reports a compact changelog loudly
  (`compact_policy_detected`, ERROR) at the partition's first assignment and
  proceeds — topic config is ops' domain; remediation is documented in §10.
  Retention bounds rehydrate completeness — a source-of-truth log wants long
  or infinite retention.
- **Best-effort, not crash-durable.** Two accepted loss windows and the
  duplicate taxonomy are documented in §14. In particular: **do not retry a
  failed `append()`** — the retry mints a new `message_id`, and if the first
  produce was a phantom the record materializes twice. A produce-acked /
  local-write-failed append self-heals on the next rehydrate.
- **Repartitioning** a source topic breaks partition-scoped local state; the
  partition count is assumed fixed (§2).

## Older SQLite runtimes

Generated columns use the `->>` operator, which needs SQLite >= 3.38. If your
interpreter links an older SQLite, place this at your application entrypoint
**before any `ksqlite`/`aiosqlite` import**:

```python
import sys

import pysqlite3  # package: pysqlite3-binary

sys.modules["sqlite3"] = pysqlite3
```

Caveat: `pysqlite3-binary` publishes wheels for **linux/x86_64 only**; on
other platforms run an interpreter linking a newer SQLite. The `start()`
version check catches every miss loudly.

## Documentation map

- [`docs/DESIGN.md`](docs/DESIGN.md) — the v1 specification: architecture,
  data model, write/rehydrate/read paths, guarantees and non-guarantees.
- [`docs/IMPLEMENTATION-PLAN.md`](docs/IMPLEMENTATION-PLAN.md) — the build
  plan and the authoritative test matrix.
- [`docs/DESIGN-REVIEW-DECISIONS.md`](docs/DESIGN-REVIEW-DECISIONS.md) — the
  decision audit trail.

## Development

```sh
uv sync
uv run pytest                      # unit + integration (fakes, real SQLite)
uv run pytest -m e2e               # real-broker suite (needs Docker)
uv run ruff check && uv run ruff format --check && uv run mypy
```

The end-to-end suite runs against a pinned Redpanda container
(`docker.redpanda.com/redpandadata/redpanda:v24.2.20`) and auto-skips when
Docker is unavailable.
