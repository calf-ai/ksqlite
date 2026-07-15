# Build a local store that rebuilds itself from Kafka

In this tutorial we will build a small store of weather readings. It lives in a
local SQLite file and answers ordinary SQL queries — and when we delete that
file, it rebuilds itself from Kafka. Along the way we'll meet changelog topics,
generated columns, and rehydration.

We won't consume a Kafka topic at all. KSQLite can do that (see
[How to wire KSQLite into your consumer](../how-to/wire-into-your-consumer.md)),
but it isn't needed here, and leaving it out keeps the path short.

You'll need:

- Python 3.10 or newer
- [uv](https://docs.astral.sh/uv/)
- Docker, to run a broker
- SSH access to the KSQLite repository — it is private and not yet released, so
  step 2 installs it from git

## Step 1 — Start a broker

KSQLite keeps its durable copy in Kafka, so we need a broker. We'll use
Redpanda, which is Kafka-compatible and starts in one command.

```sh
docker run -d --name ksqlite-tutorial -p 19092:19092 \
  docker.redpanda.com/redpandadata/redpanda:v24.2.20 \
  redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
  --node-id 0 --check=false \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 \
  --advertise-kafka-addr PLAINTEXT://localhost:19092
```

Docker prints a long container id. After a few seconds, check the broker is up:

```sh
docker exec ksqlite-tutorial rpk cluster info --brokers localhost:19092
```

You should see a `BROKERS` heading with one broker listed. If the command errors,
wait a couple of seconds and run it again — the broker is still starting.

## Step 2 — Set up the project

```sh
uv init ksqlite-tutorial
cd ksqlite-tutorial
uv add git+ssh://git@github.com/calf-ai/ksqlite
```

The last command prints the list of packages it installed. Check that KSQLite
imports:

```sh
uv run python -c "import ksqlite; print(ksqlite.KSQLite.__name__)"
```

You should see:

```text
KSQLite
```

Once KSQLite has its first release, `uv add ksqlite-py` will replace the git
command above.

## Step 3 — Write some readings

Create `write_readings.py`:

```python
import asyncio
import logging

from aiokafka import TopicPartition

from ksqlite import GeneratedColumn, KSQLite

logging.basicConfig(level=logging.WARNING, format="%(name)s %(message)s")
logging.getLogger("ksqlite").setLevel(logging.INFO)

STATION = TopicPartition("readings", 0)

COLUMNS = [
    GeneratedColumn("station", "$.station"),
    GeneratedColumn("celsius", "$.celsius", "REAL"),
]


async def main() -> None:
    async with KSQLite(
        db_path="readings.db",
        bootstrap_servers="localhost:19092",
        generated_columns=COLUMNS,
        create_topics_retention_ms=-1,
    ) as store:
        await store.on_partitions_assigned([STATION])

        await store.append(
            source=STATION,
            entity_key="oslo",
            payload={"station": "oslo", "celsius": 1.5},
        )
        await store.append(
            source=STATION,
            entity_key="lisbon",
            payload={"station": "lisbon", "celsius": 18.2},
        )
        print("appended 2 readings")


asyncio.run(main())
```

Run it:

```sh
uv run python write_readings.py
```

You should see:

```text
ksqlite.schema executed migration DDL
ksqlite.schema executed migration DDL
ksqlite.topics created changelog topic 'readings.p0.changelog'
ksqlite.topics verified cleanup.policy of 'readings.p0.changelog'
ksqlite.lifecycle rehydrate start
ksqlite.lifecycle rehydrate end
appended 2 readings
```

Notice that KSQLite created a topic called `readings.p0.changelog`. That is the
**changelog**: the durable copy of everything we append. `STATION` names the
shard the readings belong to, and the changelog topic name is derived from it.

Notice too that it rehydrated before we appended anything — there was simply
nothing to replay yet.

There is now a `readings.db` file in the directory. Let's query it.

## Step 4 — Query the store

Create `query_readings.py`:

```python
import asyncio
import logging

from aiokafka import TopicPartition

from ksqlite import GeneratedColumn, KSQLite

logging.basicConfig(level=logging.WARNING, format="%(name)s %(message)s")
logging.getLogger("ksqlite").setLevel(logging.INFO)

STATION = TopicPartition("readings", 0)

COLUMNS = [
    GeneratedColumn("station", "$.station"),
    GeneratedColumn("celsius", "$.celsius", "REAL"),
]


async def main() -> None:
    async with KSQLite(
        db_path="readings.db",
        bootstrap_servers="localhost:19092",
        generated_columns=COLUMNS,
        create_topics_retention_ms=-1,
    ) as store:
        await store.on_partitions_assigned([STATION])

        rows = await store.query("SELECT station, celsius FROM records ORDER BY station")
        for row in rows:
            print(f"{row['station']:8} {row['celsius']}")


asyncio.run(main())
```

Run it:

```sh
uv run python query_readings.py
```

You should see:

```text
ksqlite.topics verified cleanup.policy of 'readings.p0.changelog'
ksqlite.lifecycle rehydrate start
ksqlite.lifecycle rehydrate end
lisbon   18.2
oslo     1.5
```

We queried a table called `records` and selected `station` and `celsius` — but we
never created those columns. They are the `GeneratedColumn`s we declared: KSQLite
extracts them from the JSON payload, so plain SQL can filter and sort on them.

Notice there are no `executed migration DDL` lines this time. The columns already
existed, so there was nothing to migrate.

## Step 5 — Delete the database

Now the interesting part. Delete the SQLite file entirely:

```sh
rm readings.db
```

The local data is gone. Run the same query again:

```sh
uv run python query_readings.py
```

```text
ksqlite.schema executed migration DDL
ksqlite.schema executed migration DDL
ksqlite.topics verified cleanup.policy of 'readings.p0.changelog'
ksqlite.lifecycle rehydrate start
ksqlite.lifecycle rehydrate end
lisbon   18.2
oslo     1.5
```

The readings are back. Nothing read `readings.db`, because there wasn't one —
KSQLite rebuilt the database by replaying `readings.p0.changelog` from the
broker. That replay is what `rehydrate start` and `rehydrate end` mark, and the
migration lines are back because the schema had to be recreated from scratch.

Run it once more, as many times as you like. The result is the same every time.

## What we built

We built a store that keeps its data in a local SQLite file, answers ordinary
SQL, and treats that file as disposable — because the real copy lives in a Kafka
changelog.

Along the way we met:

- **Changelog topics** — one per shard, holding every record we appended
- **Generated columns** — queryable SQL columns extracted from JSON payloads
- **Rehydration** — replaying a changelog to rebuild local state
- **`on_partitions_assigned`** — how a process claims a shard and makes its rows
  visible

To clean up:

```sh
docker rm -f ksqlite-tutorial
```

Where to go next:

- [How to use KSQLite as standalone storage](../how-to/use-as-standalone-storage.md)
  — the pattern from this tutorial, taken to production
- [How to wire KSQLite into your consumer](../how-to/wire-into-your-consumer.md)
  — the other way to use KSQLite, alongside a Kafka consumer
- [About the KSQLite architecture](../explanation/architecture.md) — why it is
  built this way
