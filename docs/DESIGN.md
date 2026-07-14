# KSQLite — Design & Specification

A library that gives a Kafka-consuming application a **local, SQL-queryable, rebuildable
materialized view** backed durably by a per-partition Kafka **changelog**. Local state lives in
**SQLite**; on the host consumer's **rebalance** the state for a reassigned partition is
**rehydrated** by replaying its changelog. It is a hand-rolled **Kafka Streams state-store +
changelog** pattern, with SQLite (for real SQL querying) in place of RocksDB.

> **Status:** v1 spec, round-5 (round-4 + the implementation-plan-review amendment rounds). Decision
> audit trail: `docs/DESIGN-REVIEW-DECISIONS.md`. All prior ⚑ judgement calls are resolved.

---

## 1. Purpose & non-goals

**Purpose.** An embeddable **library** (not a service) that a Kafka consumer app uses to persist
derived state. Durable persistence = the **changelog topic** (Kafka). Local **SQLite** = a
disposable projection that serves fast, arbitrary **SQL** reads and is rebuilt from the changelog on
rebalance. Payload-agnostic: the host writes whatever JSON payload it wants; KSQLite never interprets
its schema.

**Non-goals (v1).** Not crash-durable (best-effort). Not exactly-once. Does not own source-topic
consumption. No query builder — raw SQL + typed hydration. One SQLite DB per process. No local
growth-bounding / snapshotting (v2).

---

## 2. Operating assumptions (LOCKED)

| # | Assumption |
|---|---|
| 2.1 | **Best-effort**; data loss on crash is acceptable. |
| 2.2 | Source topic consumed **at-most-once** by the host (auto-commit on reception). |
| 2.3 | **No crash handling** — targets steady state + rebalance only; steady state assumed safe. |
| 2.4 | Records are **append-only, immutable**. |
| 2.5 | A process may own **multiple** `(topic, partition)` assignments. |
| 2.6 | Multiple source topics may materialize into one DB (all **same entity type**). |
| 2.7 | **Payload is JSON** (stored as JSON text). Schema-agnostic, not format-agnostic. |
| 2.8 | **One SQLite DB per process.** |
| 2.9 | Source-topic **partition count is fixed** (stateful topics must not be repartitioned). |
| 2.10 | Changelog topics are **`cleanup.policy=delete`, never compact**. |
| 2.11 | **Appends to a single `(source_topic, source_partition)` are issued in offset order** (the host awaits each `append` before the next for a given partition — the natural way to consume a Kafka partition). |

> **Domain note (example, not an assumption):** the initial host models `messages → threads → tasks`
> with agent-grain = thread; it keys its **source topic by `task_id`** so a whole task is local to one
> partition. KSQLite itself is entity-agnostic — `entity_key` is just the routing/grouping key, and
> `task_id`/`thread_id`/`user_id` are host-declared generated columns.

---

## 3. Architecture

**Components.**
- **Connection pool** — a single **uniform** `aiosqlitepool` (every connection reads *and* writes),
  WAL + `busy_timeout`, behind a `ConnectionPool` seam. (Design decision & benchmark: §11.)
- **Changelog producer** — one `AIOKafkaProducer`.
- **Rehydrate consumer** — one assign-based `AIOKafkaConsumer` (replay only; started lazily).
- **Admin client** — one `AIOKafkaAdminClient` for topic creation + the compact-policy check;
  constructed with `bootstrap_servers` plus the **security-relevant subset of `consumer_config`**
  (`security_protocol`/`sasl_*`/`ssl_context`, filtered to the params the admin ctor accepts via
  `inspect.signature` introspection — never a hardcoded key list, since the aiokafka dependency is
  a floor, not an exact pin; step 0 must authenticate on SASL/SSL clusters too; the admin ctor
  accepts no `**kwargs`).
- **Checkpoint store** (`partition_checkpoint`) + a per-`(topic,partition)` **state machine**
  (`RESTORING → READY` / `READY_PARTIAL`, and `STANDBY` after revoke, §8). The authoritative 4-valued
  state lives **in-memory** (cleared on `start()`, rebuilt from rebalances); the **`_owned` table** is
  only its **presence-only visibility projection**, driving the host-facing **`records` view** (§5.3).

**Three Kafka clients, and why.** Kafka separates producers from consumers, so writing the changelog
(producer) and replaying it (consumer) are two clients; the admin client (topic creation + policy
check) makes three. The host owns a **fourth** — its source consumer — which is not ours.

**Data flow.** Host consumes a source record → `store.append(...)` produces to the changelog
(**key=`entity_key`**, value=payload, headers=`{format_version, message_id}`) and upserts SQLite in one
transaction. On rebalance, the host's listener calls `store.on_partitions_{assigned,revoked}`;
assignment replays the changelog into `_records` and then `INSERT`s the partition's `_owned` row so
the `records` view reveals it atomically (§5.3, §9).

---

## 4. Public API (LOCKED shape)

```python
from ksqlite import KSQLite, GeneratedColumn, Index, KSQLiteError
from aiokafka import TopicPartition

store = KSQLite(
    db_path="state.db",
    bootstrap_servers="localhost:9092",
    changelog_topic_template="{source_topic}.p{partition}.changelog",
    create_topics_retention_ms=None,         # None = don't create; int retention (-1 = infinite) auto-creates missing changelog topics (dev)
    verify_changelog_policy=True,            # fail-fast (on a partition's first assignment) if its changelog is cleanup.policy=compact
    generated_columns=[GeneratedColumn("task_id", "$.task_id"),
                       GeneratedColumn("thread_id", "$.thread_id")],
    indexes=[Index("ix_thread", ["thread_id"])],
    pool_size=16,
    busy_timeout=5.0,                        # SQLite busy handler wait (seconds)
    rehydrate_timeout=30.0,                  # total replay budget per rebalance callback (< max.poll.interval.ms)
    producer_config={"acks": 1},             # pass-through, merged over defaults
    consumer_config={},                      # pass-through (needed for SASL/SSL clusters)
    kafka_clients=None,                      # KafkaClientFactory seam; None = real aiokafka clients
)
await store.start()                          # ...or:  async with KSQLite(...) as store:

# WRITE — keyword-only, TopicPartition-grouped:
await store.append(source=TopicPartition("messages", 3), entity_key=task_id, payload={...})

# READ — raw SQL + optional typed hydration (auto-scoped; enforced read-only):
rows   = await store.query("SELECT payload FROM records WHERE thread_id=?", [tid])
models = await store.query("SELECT payload FROM records WHERE thread_id=?", [tid], into=Message)  # list[Message]

# INTROSPECTION:
states = store.partition_states()            # {TopicPartition: PartitionState}

# REBALANCE — host wires these into ITS OWN ConsumerRebalanceListener:
await store.on_partitions_assigned(assigned)
await store.on_partitions_revoked(revoked)

await store.stop()
```

**Integration handshake (LOCKED).** The host owns its consumer and listener; KSQLite hooks into it:
```python
class HostListener(aiokafka.ConsumerRebalanceListener):
    async def on_partitions_assigned(self, assigned):
        await store.on_partitions_assigned(assigned)   # rehydrate (RESTORING→READY) BEFORE reads
        ...host logic...
    async def on_partitions_revoked(self, revoked):
        ...host flush...
        await store.on_partitions_revoked(revoked)     # standby bookkeeping AFTER host flush
consumer.subscribe(["messages"], listener=HostListener())
```
Documented ordering: store-assign before host reads state; host-flush before store-revoke.

**`query(sql, params=(), *, into=None)`.** Runs against the auto-scoped **`records` view** (owned +
visible partitions only) on a connection held **read-only for the duration** (`PRAGMA query_only`, §9);
`into` hydrates the `payload` column (§9). Returns `list[Row]` or `list[into]`.

**`partition_states() -> dict[TopicPartition, PartitionState]`** (synchronous) where
`PartitionState = {state: RESTORING|READY|READY_PARTIAL|STANDBY, checkpoint_offset: int|None,
log_end_offset: int|None, lag: int|None}`. It reads the **in-memory** state machine — `state` and
`checkpoint_offset` are live; `STANDBY` = "seen this process lifetime ∧ not currently owned"
(checkpoint may be `None` — e.g. revoked off an empty changelog; an off-contract append to a
never-assigned partition also reports STANDBY). `log_end_offset` (= LEO) and `lag`
(= `LEO − (checkpoint + 1)`) are **cached as-of-the-last-rehydrate** (a live value would need an
async broker round-trip) — meaningful for `RESTORING`/`READY_PARTIAL`, stale for a long-`READY`
partition. The map reflects **only partitions encountered during this process's lifetime**
(assignments/revocations/appends since `start()`); a fresh process reports `{}` until then — it does
not read persisted checkpoints from SQLite (the method is synchronous). The write paths keep it live:
`append` and each rehydrate batch push the committed offset into the in-memory machine after their
transaction commits via a **monotonic-MAX** entry point (mirroring the SQL upsert); the two §7 clamps
push via a separate, sole non-monotonic entry point — so the in-memory checkpoint can never silently
disagree with the persisted one.

**Types.** `GeneratedColumn(name, json_path, sql_type="TEXT")`; `Index(name, columns, unique=False)`;
a `ConnectionPool` seam (optional `pool=` injection; defaults to the `aiosqlitepool` adapter); a
`KafkaClientFactory` seam (optional `kafka_clients=` injection — `producer()` / `consumer()` /
`admin_client()`; defaults to real aiokafka clients). Factory contract: the three methods are
**synchronous**, receive the **fully-merged final kwargs** (defaults ← user config ← the pinned
consumer keys — merged by KSQLite, never by the factory), and return constructed-but-**unstarted**
clients; KSQLite owns `start()`/`stop()`. Both seams exist for testing/injection — the honest
alternative to monkeypatching — not as advertised extension points. `PartitionState` is a frozen
dataclass; its `state` field is an exported `PartitionStatus` enum.

**Error taxonomy.** `KSQLiteError` (base) → `ConfigError` (bad config / DDL drift / compact changelog
policy), `SQLiteVersionError` (SQLite < 3.38 at start), `SchemaMigrationError` (an `ALTER
TABLE`/`CREATE INDEX` execution failure during migration), `ChangelogProduceError` (produce failure or
`offset < 0`), `TopicNotFoundError` (a changelog topic is missing on its partition's first assignment
and `create_topics_retention_ms` is `None` — raised from `on_partitions_assigned`, §7 step 0),
`RehydrateError` (replay/assign/broker/DB error — or an unrecognized `format_version` — during
rehydrate; propagates through the host's rebalance callback; host should treat it as a failed
assignment), `HydrationError` (`into=` used but the result has no `payload` column), `LifecycleError`
(misuse of the lifecycle: `append`/`query`/hooks before `start()` or after `stop()`, double
`start()`, `stop()` before `start()`), `StorageError` (SQLite I/O failure — DB open at `start()`,
`SQLITE_BUSY` after `busy_timeout` on `append`/`query`, `SQLITE_FULL`, a write attempted under
`query_only`, etc.). aiokafka/aiosqlite exceptions are wrapped into these at the boundary. Boundary
mappings pinned: pool-layer failures (e.g. acquisition timeout) → `StorageError`; a mid-replay
`StorageError` is re-raised by the lifecycle as `RehydrateError` (cause chained); step-0 non-authz
broker failures → `RehydrateError`; a Kafka-client start failure during `start()` wraps as base
`KSQLiteError` (cause chained; no dedicated subclass in v1). Error-family rule: **per-call argument
errors raise builtins** (`ValueError`/`TypeError` — `entity_key`, payload, `into=` target; a Pydantic
validation failure inside `into=` propagates raw); **construction/configuration errors raise
`ConfigError`**.

**Config validation (fail-fast at `start()`, `ConfigError`).** `changelog_topic_template` must contain
both `{source_topic}` and `{partition}` (resolved names are validated as legal Kafka topic names at
first resolution — partitions are unknown at `start()`); `pool_size ≥ 1`; `busy_timeout` and
`rehydrate_timeout` `> 0`;
`producer_config['acks']` **must not be `0`** (need the offset). `GeneratedColumn.name` (identifier
charset; **must not collide with a system column** — `message_id`, `source_topic`,
`source_partition`, `changelog_offset`, `entity_key`, `payload`), `json_path` (safe JSON-path /
escaped string literal), and `sql_type` (same identifier charset — DDL-safety validation,
deliberately not a type-affinity allowlist) validated. `Index.name` is validated by the same
identifier rule. Every `Index` column must reference a system column or a declared generated column
(base columns are legal index targets).

**Lifecycle.** `start()` = open pool → migrate schema (§5.1 — includes **clearing `_owned`**) → start
producer → **start admin client**, with **rollback-on-partial-failure** (close whatever was opened);
second `start()` raises. The **rehydrate consumer** and **per-changelog-topic ensure/verify** are both
lazy: the consumer starts on the first `on_partitions_assigned`, and each changelog topic is
created/verified on the **first assignment of its `(topic, partition)`** (§7 step 0) — topic names are
unknowable at `start()`, since the ctor learns partitions only from assignments. `stop()` = happy-path
drain in-flight appends → flush producer → close producer, rehydrate consumer, admin, pool.
Post-`stop()` calls raise. Async CM supported. Drain mechanics (pinned): the closed-flag check and
in-flight counter increment are synchronous (no `await` between them); decrement in `finally`;
completion signaled by an `asyncio.Event` (no polling). The drain covers **appends only** — a
`query()` concurrent with `stop()` may surface `StorageError`. `stop()` is **best-effort**: every
close step is attempted and the first exception re-raised at the end; a second `stop()` is a no-op.

**Observability.** Structured logs on: rehydrate start / end / force-stop, topic ensure/verify, produce
failure, schema migration (`ALTER`/`CREATE INDEX`), compact-policy rejection, appends to a
non-READY partition (§6), foreign-record skips (§7), orphaned column/index on config removal (§5.1),
and — WARNING level, data-loss-adjacent and operator-actionable — the **truncation reset**
(offset-out-of-range recovery) and **checkpoint clamps** (§7).
`partition_states()` exposes per-partition state (live) + checkpoint + cached lag (see above).

---

## 5. Data model (LOCKED)

### 5.1 `_records` (base table — internal; writes go here)
```sql
CREATE TABLE _records (
    message_id       TEXT    NOT NULL,          -- uuidv7, KSQLite-generated; unique dedup key
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    changelog_offset INTEGER NOT NULL,          -- observed offset; host/ops debugging provenance only (not canonical; §13/§14)
    entity_key       TEXT    NOT NULL,          -- routing/grouping key (host's task_id, etc.)
    payload          TEXT    NOT NULL,          -- JSON text (validated via json(?))
    UNIQUE (message_id)                         -- dedup target for ON CONFLICT (message_id)
);  -- normal rowid table: secondary indexes reference the compact rowid, not the 36-char message_id

CREATE INDEX ix_records_tp  ON _records(source_topic, source_partition);   -- eviction / owned-scope
CREATE INDEX ix_records_key ON _records(entity_key);                       -- entity reads
```
Hosts never query `_records` directly — they query the auto-scoped **`records` view** (§5.3).
- **`message_id` (uuidv7) is the dedup key.** Idempotent replay/retry via `ON CONFLICT (message_id)
  DO NOTHING` (dedups aiokafka producer retries **and** rehydrate re-reads; **not** a host that retries
  a failed `append` — that mints a new id, §14).
- **Schema migration (idempotent) at `start()`:** first `CREATE TABLE IF NOT EXISTS` for `_records`,
  `partition_checkpoint`, `_owned` + the base indexes + the `records` view; then **`DELETE FROM
  _owned`** (ownership is per-process, re-established by rebalances — `CREATE IF NOT EXISTS` alone
  would leave a prior run's stale rows serving un-owned partitions at boot); then reconcile host-declared
  generated columns — compute existing columns via **`PRAGMA table_xinfo(_records)`** (generated columns
  are **invisible to `PRAGMA table_info`** — using it re-adds them on every boot and crashes with
  `duplicate column name`), and `ALTER TABLE _records ADD COLUMN <name> ... GENERATED ALWAYS AS
  (payload->>'<path>') VIRTUAL` only for
  missing ones (VIRTUAL is alterable; STORED can't be added to a populated table), then
  `CREATE INDEX IF NOT EXISTS`. An execution failure of any such `ALTER`/`CREATE INDEX` raises
  `SchemaMigrationError`. (Creating the `records` view *before* the generated columns exist is safe
  and load-bearing: SQLite re-expands `SELECT r.*` at statement-prepare time, so later-added columns
  surface through the view — empirically verified.)
- **Drift** (same column name, changed `json_path`/`sql_type`): the extraction expression lives **only
  in `sqlite_schema.sql`** (not exposed by either pragma). KSQLite emits that DDL itself, so drift is
  detected by **re-rendering the declared config through the same DDL emitter** and comparing the
  normalized column clause against the stored schema text — no free-form SQL parsing (parsing SQLite's
  stored DDL is brittle across quoting/whitespace variants) → fail-fast `ConfigError`.
  Index drift (same name, changed columns/`unique`, via `PRAGMA index_xinfo`) → `ConfigError`.
  **Removal** from config is **not** automatic (orphan column/index left in place and logged).
- DDL identifiers/paths are **validated/escaped** (identifier charset for `name`; string-literal
  escaping for `json_path`).

### 5.2 `partition_checkpoint`
```sql
CREATE TABLE partition_checkpoint (
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    changelog_offset INTEGER NOT NULL,          -- MAX offset materialized (monotonic high-water mark)
    PRIMARY KEY (source_topic, source_partition)
);
```
Updated **in the same transaction** as the record insert. It is a **`MAX(offset)` high-water mark**
(§6) storing the highest *processed* changelog offset (materialized rows and classified-foreign
skips both advance it, §7), not a contiguous frontier — see §13 for the rationale. The only writes outside the MAX-upsert are the two explicit post-reset clamps in §7 step 2
(clamp-down decreases it; clamp-up raises it past a fully-trimmed gap).

### 5.3 `_owned` + the `records` view (partition scoping)
```sql
CREATE TABLE _owned (                             -- KSQLite-maintained; the process's visible ownership
    source_topic     TEXT    NOT NULL,
    source_partition INTEGER NOT NULL,
    PRIMARY KEY (source_topic, source_partition)  -- presence-only: a row exists ⇔ owned AND visible
);
CREATE VIEW records AS                            -- the host's read surface (§9)
    SELECT r.* FROM _records r
    JOIN _owned o ON r.source_topic = o.source_topic AND r.source_partition = o.source_partition;
```
`_owned` is **presence-only** — no `ready` column: a hidden (RESTORING/STANDBY) partition is simply
**absent**, which is behaviorally identical to a `ready=0` row and one less state to keep in sync. It
is **cleared on `start()`** (§5.1 — ownership is per-process, established by rebalances) and updated
transactionally: `assign` → **nothing** (row absent ⇒ rows hidden, including a warm-standby
partition's kept rows); `rehydrate complete` (or force-stop) → **`INSERT OR IGNORE`** (atomic reveal;
idempotent across re-reveal paths); `revoke` →
**`DELETE`** (rows vanish). The plain presence join makes the `records` view reflect exactly the owned
+ visible partitions, so all reads are auto-scoped — no SQL parsing, no host hints (§9). SQLite inlines
the view, so index usage is preserved (verified: `SEARCH … USING INDEX ix_thread`). The view itself is
not writable, and `query()` additionally enforces read-only via `PRAGMA query_only` (§9). `_owned` is
tiny (this process's partitions), indexed by its PK; the 4-valued partition state (incl. `READY` vs
`READY_PARTIAL`) lives in-memory (§3) — `_owned` carries only visibility.

---

## 6. Write path — `append(source, entity_key, payload)` (LOCKED)

1. Generate a **uuidv7 `message_id`** (stdlib `uuid.uuid7()` on Python ≥ 3.14; on older runtimes a
   pinned third-party generator, e.g. `uuid6` — only *uniqueness* is load-bearing; v7 is for index
   locality). **Validate `entity_key`** (non-empty, UTF-8-encodable `str`; else `ValueError`) — it
   lands in a `NOT NULL` column and on the record key, and an invalid key would otherwise poison every
   future rehydrate of the partition (§7). **Serialize + validate the payload before any produce**
   (accepted: a JSON-serializable object, or a `str` that must parse as JSON) — `ValueError` on
   failure. Serialization uses **`json.dumps(..., allow_nan=False)`** and str-parsing rejects
   `NaN`/`Infinity` via a raising `parse_constant` — Python's defaults would emit/accept non-standard
   `NaN` tokens that SQLite < 3.42 rejects on every replay (poisoned partition) and SQLite ≥ 3.42
   **silently coerces to `null`** (payload corruption diverging from the changelog bytes). Ordering matters: validated only at insert time (`json(?)`), an unmaterializable payload
   would land durably in the changelog *with valid KSQLite headers* and fail every future replay —
   the same poisoning class as a bad key. The SQL-level `json(?)` remains as defense-in-depth only.
2. **Produce** to the `(topic, partition)` changelog: **key = `entity_key.encode()`**, value = the
   payload as UTF-8 JSON-text bytes, headers = `{format_version, message_id}` (header values `bytes` —
   aiokafka carries only bytes on the wire). **Await the ack**; take the offset from
   `RecordMetadata.offset`. (`acks=0` is rejected at `start()` (§4), so `send_and_wait` always returns
   a `RecordMetadata`; the `offset ≥ 0` guard defends against the idempotent-producer edge case where a
   retried batch can report `offset == -1` — on violation raise `ChangelogProduceError`.) A produce
   failure raises `ChangelogProduceError` and performs no SQLite write.
3. In **one `BEGIN IMMEDIATE` transaction** (write lock acquired up front — avoids the
   `SQLITE_BUSY_SNAPSHOT` lock-upgrade path that `busy_timeout` does *not* retry):
   ```sql
   INSERT INTO _records(message_id, source_topic, source_partition, changelog_offset, entity_key, payload)
     VALUES (:mid, :t, :p, :off, :key, json(:payload))
     ON CONFLICT (message_id) DO NOTHING;
   INSERT INTO partition_checkpoint(source_topic, source_partition, changelog_offset)
     VALUES (:t, :p, :off)
     ON CONFLICT(source_topic, source_partition)
     DO UPDATE SET changelog_offset = MAX(changelog_offset, excluded.changelog_offset);
   ```
   Neither statement performs an application-level read-then-write, and `BEGIN IMMEDIATE` already
   holds the write lock — so the internal read inside the `MAX()` upsert cannot trigger a snapshot
   (reader→writer) upgrade.
- Uniform pool → `append` grabs a pooled connection; SQLite serializes concurrent writers, and
  `busy_timeout` + a bounded retry (§11) handle contention. No app-level writer lock.
- `append` to a partition that is **not READY** in the state machine is a **host-contract violation**
  (the §4 handshake runs assign-rehydrate before the host processes messages). KSQLite **logs a
  warning** rather than raising — the write itself still converges (new offsets extend the log;
  `message_id` dedup keeps an in-flight replay idempotent) — but hosts must not rely on it.

---

## 7. Rehydrate — `on_partitions_assigned(assigned)` (LOCKED)

Runs **inside the host's rebalance callback (blocking)**, under a **single wall-clock replay budget**
(`rehydrate_timeout`) shared by *all* partitions in the assignment (see Force-stop). Per assigned
partition, in order:
0. **Ensure the changelog topic — unconditional on the partition's first assignment, and exempt from
   the replay budget** (correctness checks are never force-stopped away; only replay is). Resolve the
   name from the template + assignment context (validated as a legal Kafka topic name at resolution).
   If the topic is missing: create it when `create_topics_retention_ms` is set (single-partition,
   `cleanup.policy=delete`, that retention; a concurrent-create `TopicAlreadyExists` counts as
   success), else raise **`TopicNotFoundError`**. If `verify_changelog_policy`, run `describe_configs`
   and raise `ConfigError` when `cleanup.policy` contains `compact` (§10); on a missing
   `DESCRIBE_CONFIGS` ACL, log a warning and proceed. Caching: **success and the warn+proceed outcome
   are cached per TP for the store's lifetime** (warn once; a fresh store re-checks); a **failed**
   ensure/verify is NOT cached and re-runs on the partition's next assignment. Any other broker
   failure here (describe/create timeout, connection loss) raises **`RehydrateError`**.
   **Fast path (after step 0):** if the local checkpoint is already at the changelog log-end
   (`checkpoint + 1 == LEO`; conventions in step 2), mark **READY** (in-memory) and `INSERT` its
   `_owned` row — revealed without replay, zero fetches (correct under eager *and* cooperative
   assignors: after an eager revoke-all the partition is STANDBY with a current checkpoint, so no
   replay and no hidden window). Otherwise continue:
1. Mark **RESTORING** (in-memory). Its `_owned` row is **absent**, so the `records` view hides its
   rows for the whole replay — including a warm-standby partition's kept rows. (Exception: a
   partition re-assigned while already revealed **`READY_PARTIAL`**, with no intervening revoke,
   keeps its row — its partial rows remain visible during catch-up; the step-4 reveal `INSERT` is
   `OR IGNORE` for exactly this path.)
2. Read its checkpoint. Assign the changelog partition on the (lazily-started) rehydrate consumer —
   which runs with **`group_id=None`, `enable_auto_commit=False`, and `auto_offset_reset="none"`**.
   The first two because a manual-`assign` consumer must never join or commit into a consumer group;
   the third because **without `"none"`, aiokafka never raises `OffsetOutOfRangeError` — it silently
   resets to *latest*, turning a truncated log into a silently-empty "successful" rehydrate**
   (empirically verified against a live broker). `consumer_config` pass-through cannot override these
   three keys. `seek(checkpoint+1)` if a checkpoint exists, else **`await seek_to_beginning(tp)`**.
   `OffsetOutOfRangeError` is raised **from the fetch (`getmany`)**, never from `seek()` — wrap the
   poll: on it, log a WARNING (old checkpoint + new log-start; a data-loss-adjacent event), reset to
   earliest, and replay (best-effort on a retention-trimmed log; **no raise**). **Offset
   conventions:** the checkpoint stores the last *materialized* record offset; Kafka's `end_offsets`
   returns **LEO = last available offset + 1**; the fast path is `checkpoint + 1 == LEO`. After a
   reset-and-replay: a stored checkpoint above `LEO − 1` (an `acks=1` unclean-truncation artifact) is
   **clamped down to `LEO − 1`** — otherwise every future rehydrate full-replays forever; a reset that
   replayed **zero records** (fully-trimmed tail, `log_start == LEO`) **clamps *up* to
   `log_start − 1`** — otherwise every future rehydrate re-enters the reset path. Both clamps log a
   WARNING.
3. Replay to log-end in **batches**: **poll/accumulate each batch *outside* any transaction, then**
   one `BEGIN IMMEDIATE` txn for its local inserts — the write lock is never held across a Kafka
   `poll()` (that would block every concurrent `append` for the fetch's network latency). Per record,
   reconstruct the row: `entity_key = record.key.decode()`, `message_id`/`format_version` from
   **headers** (bytes-decoded), `changelog_offset = record.offset`, and
   `source_topic`/`source_partition` from the **assignment context** (never by parsing the topic
   name). `INSERT INTO _records ... ON CONFLICT(message_id) DO NOTHING` + the `MAX` checkpoint upsert
   (§6). Headers are looked up **by name** (order-insensitive; unknown extra headers — e.g. tracing —
   are ignored). A record with a **missing/malformed `message_id` header, or a null/undecodable key or
   value** is **skipped + logged** (it can't be a KSQLite record — a foreign producer; delete-policy
   changelogs have no tombstones). A record carrying a valid `message_id` but a **missing or
   unrecognized `format_version`** is a KSQLite record this reader can't understand (rolling
   upgrade/downgrade) and **fails loud** with `RehydrateError` — never silently dropped.
   **Skipped-foreign offsets still advance the checkpoint** (they are by definition not KSQLite data),
   so a foreign tail cannot permanently defeat the fast path. **Replay terminates when the consumer
   position reaches the LEO captured in step 2** — an empty `getmany` return is **not** a termination
   signal (a real broker's empty prefetch buffer routinely yields `{}` mid-log; treating it as
   done-ness silently reveals partitions missing their tail). Any replay / assign / broker / DB error
   raises **`RehydrateError`** (a mid-replay `StorageError` is re-raised as `RehydrateError` with the
   cause chained; propagates through the host callback). On such an error, partitions not yet
   revealed in this assignment stay hidden (no `_owned` row) until a later rehydrate.
4. Mark **READY** (in-memory) and **`INSERT` its `_owned` row**, atomically revealing the partition's
   rows in the `records` view.
- **Force-stop (cooperative, assignment-wide):** `rehydrate_timeout` is a **single wall-clock deadline
  for the entire `on_partitions_assigned` callback**, shared across all its partitions — a
  *per-partition* budget would multiply by the assignment size (e.g. 8 partitions × 30 s ≫
  `max.poll.interval.ms`) and trigger the very rebalance storm it exists to prevent. The deadline is
  checked **between batches**, never by cancelling a coroutine mid-transaction (an async cancel inside
  `BEGIN IMMEDIATE` could return a connection to the pool still holding the write lock — see the
  connection-hygiene rules in §11). On expiry: stop, keep the checkpoint at the last materialized
  offset, **log**, mark **`READY_PARTIAL`** (in-memory) and `INSERT` the `_owned` row (partial rows
  become visible); partitions the budget never reached are revealed the same way at their
  kept-checkpoint state. There is **no forward-progress guarantee within a single assignment** — the
  remaining tail advances only on a *later* rehydrate; `partition_states()` exposes `READY_PARTIAL` +
  lag so the host can detect and, if needed, act. (Background catch-up = v2.)
- No generation-fencing/cancellation needed: blocking + Kafka's one-consumer-per-partition guarantee
  mean no concurrent rehydrate and no stale-completion race.

## 8. Revoke — `on_partitions_revoked(revoked)` (LOCKED)
Mark **STANDBY** (in-memory) — **`DELETE` the partition's `_owned` row** (its rows vanish from the
`records` view at once, so they can never be served stale), but **keep** the rows in `_records` + the
checkpoint (warm standby; no delete in v1). A later re-assign replays incrementally from the kept
checkpoint with the `_owned` row **absent** (rows hidden) until completion re-`INSERT`s it.

## 9. Read path — `query(sql, params, *, into=None)` (LOCKED)
Queries run against the host-facing **`records` view** (§5.3), which joins `_records` to `_owned` and
therefore **auto-scopes every read to the process's owned + visible partitions** — no SQL parsing, no
partition hints. This is *structural*: a RESTORING partition is **excluded** (never a partial read)
until rehydrate atomically `INSERT`s its `_owned` row; a revoked (STANDBY) partition is **excluded**
the instant its row is deleted (never stale).
- **Enforced read-only.** `query()` sets **`PRAGMA query_only=ON`** on the checked-out connection and
  resets it **`OFF` in a `finally`**; checkout hygiene also resets a stale flag (§11). Any write
  through `query()` — including against `_records`/`_owned` directly — fails with *attempt to write a
  readonly database* (surfaced as `StorageError`). The view alone cannot provide this (it protects only
  the name `records`; the pooled connection is otherwise read/write). Benchmarked cost of the two extra
  PRAGMA round-trips: **≈ +55–60 µs per read** sequentially (mean 117 → 177 µs on a 50k-row view read),
  ≈ +20–28 % on mean latency under concurrency, **tail latencies unchanged** — accepted. Note
  `query_only` cannot block *reads* of `_records`/`_owned`: naming an `_`-prefixed internal bypasses
  the ownership scoping (documented footgun — reads must target `records`).
- Runs on a pooled connection; `row_factory=Row`. Arbitrary SQL over `records` keeps full index usage
  (SQLite inlines the view — verified: `SEARCH … USING INDEX ix_thread`).
- **`into=Model`** hydrates the row's **`payload`** column (JSON text). Targets are **exactly**:
  Pydantic-v2 models (`model_validate_json` present) or `dataclasses.is_dataclass` classes
  (`Model(**json.loads(...))`); anything else raises `TypeError` — no generic-kwargs fallback. The
  query **must select a `payload` column**; otherwise raise `HydrationError`.
- **Semantics = exclude-not-wait (non-blocking).** A read issued while a partition is RESTORING simply
  omits that partition's data (returns the ready subset) rather than blocking. Because rehydrate blocks
  in the rebalance callback, all owned partitions are visible once a rebalance completes; the exclusion
  window only matters for a read racing an in-progress rehydrate on another task — poll
  `partition_states()` to tell "no data" from "not-ready-yet". (A `wait_for` blocking gate was
  **dropped to v2**: it served only that narrow race, cost a config knob + two exception types, and
  silently unblocked on `READY_PARTIAL` — §18.)
- Cross-partition / cross-process assembly (an entity split across partitions or owners) is the host's
  job — the local store is a shard (Kafka-Streams interactive-queries limitation).

---

## 10. Kafka & changelog conventions (LOCKED)

- **Changelog topic:** one per source `(topic,partition)`, **single-partition**, named
  `"{source_topic}.p{partition}.changelog"`. (Co-partitioned single topic considered and rejected;
  reversibility caveat accepted.)
- **Wire format:** **key = `entity_key`** (UTF-8 bytes); **value = pure host payload** (UTF-8 JSON-text
  bytes); **headers = `{format_version, message_id}`** (bytes values — aiokafka carries only bytes;
  decoded on replay). `message_id` is the **36-char UUID string, UTF-8-encoded** (16-byte binary ids
  are §17 v2 — the dedup equality between write path and replay depends on one canonical encoding).
  `format_version` is **KSQLite-owned**; **v1 emits `format_version="1"`** — a missing or
  unrecognized value on a record carrying a valid `message_id` fails loud (§7). All `records`
  provenance is recoverable on replay (§7).
- **`cleanup.policy=delete`, NEVER `compact`** — compaction would collapse an entity to its last
  message. On the **first assignment of each `(topic,partition)`** (when `verify_changelog_policy`;
  §7 step 0 — changelog names are unknowable at `start()`), read the topic's policy via the admin
  client's `describe_configs` — walk the response's `.resources` config entries and substring-match
  `compact` (the value may be `compact,delete`) — and **raise `ConfigError` on `compact`**. If the describe call fails on
  **authorization** (missing `DESCRIBE_CONFIGS` ACL), **log a warning and proceed** ("could not verify"
  ≠ "policy is wrong") — full enforcement requires the ACL. **Remediation is documented:** the policy is
  a dynamic config —
  `kafka-configs.sh --alter --entity-type topics --entity-name <t> --add-config cleanup.policy=delete`
  (aiokafka: `AIOKafkaAdminClient.alter_configs`, which replaces the topic's full config set; the Java
  method is `incrementalAlterConfigs`). *Caveat:* compact→delete stops future compaction but does **not**
  restore already-collapsed records (rebuild from source or accept loss).
- **Retention:** ops-owned; rehydrate completeness is bounded by retained log (default 7-day would
  truncate; a source-of-truth log wants ~infinite). Documented, not enforced.
- **Producer/consumer config:** `producer_config` / `consumer_config` pass-through, merged over moderate
  defaults (`acks=1`, idempotence off — Tansu-compatible). **`acks` must be ≥ 1** (need the offset;
  `acks=0` is rejected at start, §4). Content-duplicates from producer retries are deduped by
  `message_id`, so `acks=1`/idempotence-off is safe for *duplicates*; see §13 for the `acks=1` durability
  caveat vs the "authority" claim.
- **Topic creation:** ops in prod; `create_topics_retention_ms=…` creates a missing changelog topic
  (single-partition, delete-policy, that retention) via the admin client on the partition's first
  assignment (dev/test convenience).

## 11. Connection layer & concurrency (LOCKED — Design B)

A **single uniform `aiosqlitepool`**: every connection reads and writes. The connection factory opens
in **autocommit** (`isolation_level=None` — no implicit stdlib transactions; all transactions are
explicit, which also prevents a failed write attempt under `query_only` from stranding an implicit
open transaction) and sets `PRAGMA journal_mode=WAL` + `PRAGMA busy_timeout=<busy_timeout>`. **No
dedicated writer, no writer-lock**; read-only is enforced **per-`query()`** via a `PRAGMA query_only`
toggle (§9), not by a pool split. Writers serialize at the SQLite level; write transactions use
**`BEGIN IMMEDIATE`** so the write lock is taken up front (never a mid-transaction upgrade →
`SQLITE_BUSY_SNAPSHOT`, which `busy_timeout` does not retry). On a `busy_timeout`-exceeded
`SQLITE_BUSY`/`database is locked`, `append` applies a **bounded retry** (fixed in v1: 3 attempts with
short backoff, total deadline ≈ 2× `busy_timeout`) and then raises **`StorageError`**. **Connection
hygiene:** every write transaction rolls back on any exception before its connection returns to the
pool, and checkout resets a connection found with an open transaction or a stale `query_only` flag.
The pool is injectable via an optional `pool=` (a `ConnectionPool` seam); the default is the concrete
`aiosqlitepool` adapter (a zero-dep built-in adapter is deferred to v2 — the seam exists mainly for
testing/injection, not as an advertised extension point).

*Rationale (benchmarked, 5 trials, 1 KB payloads, 4/16/48 writers):* a uniform pool beat a
single-writer+reader-pool design on **mean/median/throughput** and is **simpler**; the single-writer
design's only edge was **bounded tail latency under high concurrent-write load**, which KSQLite's
sequential-append pattern (§2.11) does not produce. Simplicity + typical-case speed won.

## 12. SQLite version (LOCKED — simplified by JSON-text)
Generated columns use `->>` (SQLite **≥ 3.38**) over JSON text — **no JSONB, no 3.45 requirement**.
(A floor is irreducible regardless of storage format: VIRTUAL generated columns themselves require
SQLite ≥ 3.31.) `start()` fails fast: raises `SQLiteVersionError` **unless**
`sqlite3.sqlite_version_info >= (3, 38)` — and that check is the **entire in-code version story**.
KSQLite ships **no swap helper and no `sqlite_compat` module**: for the rare runtime whose bundled
SQLite predates 3.38, the README documents the standard two-line `sys.modules` swap
(`pysqlite3-binary`) placed at the host's entrypoint before any `aiosqlite` import. Documented caveat:
`pysqlite3-binary` publishes wheels for **linux/x86_64 only** — other platforms must run an
interpreter linking a newer SQLite; the `start()` check catches every miss loudly. (No `json_extract`
fallback path — the generated-column DDL emits `->>` and the floor is 3.38.)

---

## 13. Correctness & guarantees
- **Idempotent** materialization by `message_id` (`ON CONFLICT DO NOTHING`) — dedups producer retries
  and rehydrate re-reads.
- **Checkpoint = `MAX(offset)` watermark**, not a contiguous frontier. Rationale: a contiguous
  checkpoint is only correct if the produced-offset stream is gap-free and in order, which requires
  `enable_idempotence=True`+`acks=all` — incompatible with the `acks=1`/idempotence-off (Tansu) default.
  Under those defaults a lost ack lands a physical duplicate at a *new* offset, which would **pin a
  contiguous guard forever** (turning a one-record gap into a full-log re-read). The `MAX` watermark
  never stalls; its only failure is skipping the rare un-materialized offset — exactly the "narrow
  best-effort loss" §14 already accepts (and often a deduped duplicate, not a real loss).
- **Changelog = authority under `acks=all`.** At the default `acks=1`, a leader failure after ack but
  before replication can drop a changelog record the projection already has — the authority then
  inherits leader-failure loss. This is within the best-effort contract; hosts needing a strictly
  authoritative log should set `acks=all` (throughput trade-off) — but see the Tansu constraint.
- Ordering is the **host's responsibility** (a sortable field in the payload); `changelog_offset` is *an*
  observed offset, not a canonical/first-occurrence one.

## 14. Non-guarantees & failure modes
- **No crash durability.** Two loss windows, both accepted (best-effort): (1) **pre-changelog** — the
  host commits the source offset (at-most-once) then crashes before `append` → permanently lost; (2) the
  local SQLite tail not yet produced — recovered on rehydrate.
- **Host-retry duplicate:** retrying `append()` after a `ChangelogProduceError` mints a **new
  `message_id`** → if the first produce was a phantom, the record is materialized twice. Hosts should
  **not** retry a failed `append` (or should dedup logically); a host-supplied idempotency key is a v2
  option.
- **Watermark gap:** the `MAX` checkpoint may skip a rare un-materialized offset (best-effort loss).
- **Rehydrate completeness** bounded by changelog retention.
- **Produce-ok / local-apply-failed self-heals:** if the produce acked but the SQLite txn then failed
  (`StorageError`), the record is durably in the changelog and the checkpoint did not advance — the
  next rehydrate materializes it. **Do not retry the `append`** (same trap as above: a retry mints a
  new `message_id` → genuine duplicate).
- **`READY_PARTIAL`:** a force-stopped rehydrate serves partial state with **no in-assignment
  forward-progress guarantee**; converges only across rebalances. Surfaced via `partition_states()`.
- **STANDBY rows** (revoked partitions) are stale-by-design and **excluded from the `records` view**;
  only a direct read of `_records` (bypassing the view — documented footgun, §9) can observe them.
- **Standby state grows unbounded** in v1 (no eviction).
- **Repartitioning** the source breaks partition-scoped local state (fixed count assumed).

## 15. Configuration reference
| arg | default | meaning |
|---|---|---|
| `db_path` | — | SQLite file (one per process). |
| `bootstrap_servers` | — | Kafka bootstrap. |
| `changelog_topic_template` | `{source_topic}.p{partition}.changelog` | must contain `{source_topic}` **and** `{partition}`. |
| `create_topics_retention_ms` | `None` | `None` = don't create; an int retention (`-1` = infinite) auto-creates a missing single-partition delete-policy changelog topic on its partition's first assignment. |
| `verify_changelog_policy` | `True` | fail-fast on a partition's first assignment if its changelog is `cleanup.policy=compact` (warn+proceed on missing ACL). |
| `generated_columns` / `indexes` | `()` | host-declared queryable columns (VIRTUAL, `->>`) + indexes. |
| `pool_size` | `16` | uniform pool size (`≥ 1`). |
| `busy_timeout` | `5.0` | SQLite busy-handler wait, seconds (`> 0`). |
| `rehydrate_timeout` | `30.0` | total replay budget per rebalance callback (cooperative, checked between batches); keep `< max.poll.interval.ms` (`> 0`). |
| `producer_config` / `consumer_config` | `{"acks":1}` / `{}` | pass-through over defaults; `acks` must be `≥ 1`. |

## 16. Deployment & testing
- One SQLite DB per process; WAL sidecar files (`-wal`/`-shm`) next to it.
- Ops pre-creates changelog topics (single-partition, `cleanup.policy=delete`, long/∞ retention).
- **Assignor honesty:** aiokafka (through 0.14) ships **no cooperative/incremental assignor** — only
  eager `range`/`roundrobin`/`sticky` (default: RoundRobin), so for aiokafka hosts every rebalance is
  an eager revoke-all: all `_owned` rows are deleted and re-revealed in the assign callback — an
  availability dip for partitions that never moved (excluded, not stale; the checkpoint-at-log-end
  fast path in §7 keeps that window small — no replay, immediate reveal). Hosts on clients that do
  support cooperative rebalancing (e.g. Java) should prefer it; aiokafka hosts wanting stickiness must
  pass `StickyPartitionAssignor` explicitly.
- **Tests:** write-path + `MAX`-checkpoint atomicity under `BEGIN IMMEDIATE`; `SQLITE_BUSY` bounded
  retry → `StorageError`; `entity_key` validation (+ bad-key/foreign-record skip on replay);
  `message_id` idempotency (retry + re-read); generated-column migration idempotency (**`table_xinfo`**
  diff) + drift fail-fast (canonical re-rendered DDL) + index drift; `_owned` cleared on `start()`;
  lazy topic ensure/verify on first assignment (`TopicNotFoundError`, compact `ConfigError`,
  ACL warn+proceed); rehydrate (full, incremental, keep-on-revoke, assignment-wide budget
  force-stop→`READY_PARTIAL`, truncated-log reset + checkpoint clamp, `entity_key`-from-key
  reconstruction, unknown-`format_version` fail-loud, **no write lock held across a `poll()`**, no
  mid-transaction cancellation + pool resets an open-txn / stale-`query_only` connection); **view
  scoping** (RESTORING/STANDBY excluded via `_owned` presence, atomic reveal on `INSERT`, index usage
  through the view); `query()` read-only enforcement (`query_only` blocks writes incl. direct
  `_records` DML, resets for `append`); rehydrate-consumer isolation (`group_id=None`, no auto-commit,
  **`auto_offset_reset="none"`**); config validation; version fail-fast; `into=Model` hydration
  (+ `HydrationError`); broker-compat run with idempotence/acks off. (The full, authoritative test
  matrix lives in `docs/IMPLEMENTATION-PLAN.md`.)

## 17. Future work (v2+)
Background catch-up after force-stop; host-supplied idempotency key; standby growth-bounding (LRU +
checkpoint reset); snapshotting for bounded rehydrate; optional exactly-once (`enable_idempotence`+`acks=all`
+ transactions/fencing) as a stronger-authority mode; convenience read helpers (`get(entity_key, into=)`)
and a `wait_for` block-until-ready read gate; zero-dep pool adapter; co-partitioned changelog option;
binary/16-byte `message_id` if index size matters.

## 18. Considered & rejected (rationale trail)
Transactional outbox · SQLAlchemy plugin/ORM events · sqlite-utils/dataset · Kafka Streams/Faust/Quix
(want SQLite as queryable store) · exactly-once/transactions (v1) · "absolute-values-only" ·
**contiguity checkpoint (→ `MAX(offset)` watermark: contiguity pins forever under `acks=1`/retries)** ·
per-partition/per-topic tables (→ one table + discriminators) · per-thread counter for ordering
(→ host payload field) · single-writer + reader pool / read-only enforcement / reader-writer protocol
split (→ uniform pool, Design B, benchmarked; read-only later reinstated **per-query** via a `PRAGMA
query_only` toggle, §9 — benchmarked at ≈ +55–60 µs/read, tails unchanged) · JSONB storage (→ JSON text: simpler, drops pysqlite3,
perf gap negligible) · mandatory `pysqlite3` swap (→ optional fallback) · `use_pysqlite3()` helper +
`sqlite_compat` module (→ **dropped, owner-confirmed**: the package `__init__` import chain would
always trip the helper's own too-late guard unless the whole package went lazy-import — real
machinery for what a 2-line documented entrypoint snippet covers; `pysqlite3-binary` is
linux/x86_64-only anyway; the `start()` fail-fast is the real guard) · `WITHOUT ROWID` + 36-char TEXT
PK (→ rowid table + `UNIQUE(message_id)`: avoids duplicating the PK into every secondary index) ·
store-owned rebalance listener (→ host-owned) · `entity_key` in headers (→ record key: natural home,
value stays pure payload) · coarse SQL read-gate / `wait_for`-primary (→ `_owned`-driven `records`
view: structural auto-scoping, non-blocking) · `wait_for` + `read_wait_timeout` +
`PartitionNotReadyError`/`PartitionNotOwnedError` (→ dropped to v2: the view is non-blocking, the
residual cross-task race is covered by `partition_states()`, and the gate silently unblocked on
`READY_PARTIAL`) · `_owned.ready` column (→ presence-only rows: a `ready=0` row is behaviorally
identical to absence) · `TopicCreation` type (→ plain `create_topics_retention_ms`: partitions and
policy are fixed by design, retention was its only field) · `start()`-time topic verify/create
(→ lazy per-assignment, §7 step 0: changelog names are unknowable at `start()`) · compacted changelog
(would destroy append-only data).
