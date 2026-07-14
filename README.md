# KSQLite

*Turn SQLite into a distributed, durable store for your Kafka consumers:
millisecond-local SQL on every instance, with Kafka distributing your data
across the fleet and rehydrating it on demand.*

KSQLite gives each instance of your Kafka-consuming app a fast, local SQLite
database of the partitions it owns — durable on disk and queryable with plain
SQL. Kafka is the distribution and recovery layer: a per-partition
**changelog** carries every write and lives on the network, so a new or
rebalanced instance rehydrates its local SQLite by replaying that changelog.
Your data isn't pinned to one box — it follows partition ownership across the
fleet.

KSQLite is an embeddable library, not a service. It does not own source-topic
consumption — your application keeps its own consumer and hooks KSQLite into
its rebalance listener.

## Requirements

- Python >= 3.10
- SQLite >= 3.38 linked into your interpreter (`start()` fails fast with
  `SQLiteVersionError` otherwise — see [Older SQLite runtimes](#older-sqlite-runtimes))
- A Kafka-compatible broker (tested against Redpanda; the defaults — `acks=1`,
  idempotence off — also work with lightweight brokers like Tansu)

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
       enable_auto_commit=True,   # at-most-once source consumption
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
   only partitions this process currently owns and has finished loading —
   rehydrate completed and the partition was atomically revealed to the view):

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

## Operational contract (the short version)

- **Changelog topics** are one per source `(topic, partition)`,
  single-partition, `cleanup.policy=delete` — **never compact** (compaction
  would collapse an entity to its last message). KSQLite checks the policy on
  a partition's first assignment and logs a loud error if the changelog is
  compacted, but the assignment still proceeds (topic config is ops' domain);
  recreate the topic with `cleanup.policy=delete` to remediate.
- **Retention bounds rehydrate completeness** — a source-of-truth log wants
  long or infinite retention.
- **Best-effort, not end-to-end crash-durable.** Committed SQLite state
  persists across restarts, but there are two accepted loss windows (at the
  consume/produce boundary) and a duplicate taxonomy. In particular: **do not
  retry a failed `append()`** — the retry mints a new `message_id`, and if the
  first produce was a phantom the record materializes twice. A produce-acked /
  local-write-failed append self-heals on the next rehydrate.
- **Repartitioning** a source topic breaks partition-scoped local state; the
  partition count is assumed fixed.

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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE)
file for details.
