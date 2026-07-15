# KSQLite documentation

KSQLite gives an application a fast local SQLite database whose durable copy
lives in Kafka. Each instance owns a set of shards, queries them with plain SQL
at local-disk speed, and rebuilds them by replaying a per-shard changelog
whenever ownership moves or a machine is replaced.

It is a library, not a service. It works attached to a Kafka consumer, where
local state follows partition ownership around a fleet — and equally well with no
consumer at all, as durable local storage that any instance can rebuild.

> **Shard** is the word these docs use for one unit of ownership: a single
> `(topic, partition)` pair, backed by one changelog topic. The API spells it
> `TopicPartition` and calls it a partition — when you are consuming a topic, a
> shard *is* one of its partitions. When you are not, it is just a name you chose.

## Tutorial

Start here if you are new. A single guided path, from an empty directory to a
store that survives having its database deleted.

- [Build a local store that rebuilds itself from Kafka](tutorial/build-a-local-store.md)

## How-to guides

Directions for specific goals, assuming you already know the basics.

**Integrating**

- [How to use KSQLite as standalone storage](how-to/use-as-standalone-storage.md)
  — durable local SQL, no consumer involved
- [How to wire KSQLite into your consumer](how-to/wire-into-your-consumer.md)
  — rebalance hooks, ordering, appends

**Querying**

- [How to declare queryable columns](how-to/declare-queryable-columns.md)
  — turn payload fields into indexed SQL columns
- [How to query the latest value per entity](how-to/query-latest-per-entity.md)
  — current state from an append-only store
- [How to load results into models](how-to/load-results-into-models.md)
  — Pydantic models and dataclasses via `into=`

**Operating**

- [How to provision changelog topics](how-to/provision-changelog-topics.md)
  — retention, replication, and why never to compact
- [How to check partition readiness](how-to/check-partition-readiness.md)
  — detect partitions serving incomplete data
- [How to handle failures](how-to/handle-failures.md)
  — what each error means, and why never to retry `append()`

**Environment**

- [How to connect to a secured cluster](how-to/connect-to-a-secured-cluster.md)
  — SASL and TLS across all three clients
- [How to run on an older SQLite](how-to/run-on-older-sqlite.md)
  — when your interpreter links SQLite < 3.38

## Reference

Technical description of the machinery. Consult while working.

- [`KSQLite`](reference/ksqlite.md) — constructor parameters, methods, errors
- [Types](reference/types.md) — `GeneratedColumn`, `Index`, `PartitionState`, the protocols
- [Errors](reference/errors.md) — the full taxonomy and what raises each
- [SQL schema](reference/sql-schema.md) — the `records` view and the tables behind it
- [Changelog format](reference/changelog-format.md) — the wire format and replay rules
- [Logging](reference/logging.md) — logger names and structured events

## Explanation

Background and design reasoning. Read away from the keyboard.

- [About the KSQLite architecture](explanation/architecture.md)
  — why Kafka holds the data and SQLite only serves it
- [About the partition lifecycle](explanation/partition-lifecycle.md)
  — ownership, rehydration, and what happens when replay runs out of time
- [About the durability contract](explanation/durability.md)
  — what "source of truth" promises, and where records can still be lost

## Requirements

- Python 3.10 or newer
- SQLite 3.38 or newer linked into your interpreter
  ([what if it isn't?](how-to/run-on-older-sqlite.md))
- A Kafka-compatible broker. Tested against Redpanda; the defaults (`acks=1`,
  idempotence off) also suit lightweight brokers such as Tansu.
