# KSQLite

*Turn SQLite into a distributed, durable store: millisecond-local SQL on every
instance, with Kafka holding the data and rehydrating it on demand.*

KSQLite gives each instance of your app a fast, local SQLite database of the
shards it owns — durable on disk and queryable with plain SQL. Kafka is the
distribution and recovery layer: a per-shard **changelog** carries every write
and lives on the network, so a new or replacement instance rehydrates its local
SQLite by replaying that changelog. Your data isn't pinned to one box — it
follows ownership across the fleet.

KSQLite is an embeddable library, not a service. Use it two ways:

- **Attached to a Kafka consumer.** You keep your own consumer and hook KSQLite
  into its rebalance listener; local state follows partition ownership
  automatically.
- **Standalone.** No consumer at all. You claim shards yourself and get durable
  local SQL storage that any instance can rebuild — Kafka is deployed to hold
  the data, not to feed it.

## Requirements

- Python >= 3.10
- SQLite >= 3.38 linked into your interpreter (`start()` fails fast with
  `SQLiteVersionError` otherwise — see
  [How to run on an older SQLite](docs/how-to/run-on-older-sqlite.md))
- A Kafka-compatible broker (tested against Redpanda; the defaults — `acks=1`,
  idempotence off — also work with lightweight brokers like Tansu)

## Install

```sh
uv add ksqlite-py
```

> KSQLite has not had its first release yet. Until it does, install from the
> repository: `uv add git+ssh://git@github.com/calf-ai/ksqlite`

The distribution is `ksqlite-py`; the import package is `ksqlite`.

## Quickstart

```python
import asyncio

from aiokafka import TopicPartition

from ksqlite import GeneratedColumn, Index, KSQLite

SHARD = TopicPartition("messages", 0)


async def main() -> None:
    async with KSQLite(
        db_path="state.db",
        bootstrap_servers="localhost:9092",
        generated_columns=[GeneratedColumn("thread_id", "$.thread_id")],
        indexes=[Index("ix_thread", ["thread_id"])],
        create_topics_retention_ms=-1,  # dev convenience; ops pre-creates in prod
    ) as store:
        # Claim the shard: creates the changelog if needed, replays it, and
        # makes its rows visible. A consumer app calls this from its rebalance
        # listener instead.
        await store.on_partitions_assigned([SHARD])

        await store.append(
            source=SHARD,
            entity_key="th-42",
            payload={"thread_id": "th-42", "text": "hello"},
        )

        rows = await store.query(
            "SELECT payload FROM records WHERE thread_id = ?", ["th-42"]
        )
        print([row["payload"] for row in rows])


asyncio.run(main())
```

Delete `state.db` and run it again — the row from the previous run is still
there, replayed from the changelog before the new append. (You get one more row
each run: `append()` is append-only, so it inserts rather than overwriting.)

For the same thing at a walking pace, see the
[tutorial](docs/tutorial/build-a-local-store.md).

## Documentation

Full documentation lives in [docs/](docs/README.md).

- **[Tutorial](docs/tutorial/build-a-local-store.md)** — build a store that
  rebuilds itself from Kafka
- **[How-to guides](docs/README.md#how-to-guides)** — wire into a consumer, use
  it standalone, declare queryable columns, provision topics, handle failures
- **[Reference](docs/README.md#reference)** — API, types, errors, SQL schema,
  changelog format, logging
- **[Explanation](docs/README.md#explanation)** — architecture, partition
  lifecycle, the durability contract

## Operational contract (the short version)

- **Changelog topics** are one per shard, single-partition,
  `cleanup.policy=delete` — **never compact** (compaction would collapse an
  entity to its last message). KSQLite checks the policy on a shard's first
  assignment and logs a loud error if the changelog is compacted, but the
  assignment still proceeds (topic config is ops' domain).
- **Retention bounds rehydrate completeness** — a source-of-truth log wants long
  or infinite retention.
- **Append-only.** `append()` inserts; it never updates or deletes. Two appends
  with the same `entity_key` produce two rows.
- **One writer per shard.** KSQLite does not arbitrate ownership and does not
  tail the changelog — an instance sees its own appends plus whatever it replayed
  when it claimed the shard.
- **Best-effort, not end-to-end crash-durable.** Committed SQLite state persists
  across restarts, but records can be lost at the consume/produce boundary. In
  particular: **do not retry a failed `append()`** — the retry mints a new
  `message_id`, and if the first produce was a phantom the record materializes
  twice. A produce-acked / local-write-failed append self-heals on the next
  rehydrate.
- **Repartitioning** a source topic breaks partition-scoped local state; the
  partition count is assumed fixed.

See [About the durability contract](docs/explanation/durability.md) for why.

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
