# KSQLite — v1 Implementation Plan (round-3)

> **Status: shipped as v1 (0.1.0, PR #1).** The phase-gate build instructions below are
> historical; the plan is retained because its test matrix is live — the `W-*`/`RH-*`/`S-*`/
> `M-*`/`C-*`/`F-*`/`E2E-*`/`CF-*`/`ST-*`/`WF-*`/`T-*` IDs are referenced by docstrings across
> `tests/` and `src/`.

Implements `docs/DESIGN.md` (v1 spec, round-5). The spec is the **source of
truth**; where this plan and the spec disagree, the spec wins and this plan must be corrected — or
the spec is amended via `docs/DESIGN-REVIEW-DECISIONS.md`, never silently. This revision folds in the
five-agent round-1 plan review (spec-fidelity, test-adequacy, API-feasibility, eng-quality,
E2E-realism) and the four-agent round-2 amendment set (fold-audit, test-adequacy, eng-quality,
feasibility+E2E); all resolutions are logged in the decisions file.

> **Before Phase 0 begins:** confirm worktree-vs-local (per project CLAUDE.md: new feature work
> defaults to a worktree branched from `main` unless the owner says otherwise).

---

## 1. Ground rules

- **TDD is mandatory and literal** (`/test-driven-development`): every unit of production code is
  preceded by a failing test that was *observed to fail for the right reason* (feature missing, not a
  typo), then minimal code to green, then refactor. No test → no code. Code written before its test
  gets deleted and re-implemented from the test. Tests are implemented **one at a time** (red → green
  → refactor per test ID), never as a pre-written batch. Fakes carry the same discipline: each fake
  behavior is grown red-first against its own contract test (`test_fakes.py` / conformance body), one
  behavior at a time — a wrong fake is this plan's single biggest hazard.
- **No test hooks in production code.** Nothing under `src/ksqlite/` may exist only for tests — no
  failure-injection flags, no instrumentation counters, no `_test_*` attributes. All injection and
  observation goes through the two sanctioned seams (`pool=`, `kafka_clients=`) and test-owned
  wrappers (§4.2 `RecordingPool`, event-gated fakes, injectable clock/backoff parameters on internal
  service constructors — real operating parameters with defaults, not hooks).
- **uv** for everything: `uv add` for deps, `uv run pytest` / `uv run ruff` / `uv run mypy`.
- **Phase gates:** a phase is done only when its listed test IDs pass, the full suite is green, and
  `ruff check` + `ruff format --check` + `mypy` are clean. **From P8 onward the CI gate also runs the
  E2E job (`-m e2e`)** — the conformance suite (§4.3) only forces fake↔real convergence if the real
  half actually executes; a fakes-only gate would let a wrong fake certify a wrong implementation.
- **Real-vs-fake boundary (fixed):** SQLite is **always real** (tmp-file DBs via `tmp_path`; never
  mocked, never `:memory:` — WAL and file-lock semantics don't hold there). Kafka is **faked at the
  client seam in unit tests** (the network is the one unavoidable mock boundary) and **real
  (containerized Redpanda) in E2E**. Fakes implement the exact aiokafka call surface the code uses,
  are contract-tested in `tests/unit/test_fakes.py`, and share test bodies with the real client via
  the **conformance suite** (§4.3).
- **Coverage:** `pytest-cov`; gate ≥ 90 % line coverage on `src/ksqlite/` from the unit+integration
  suite. Coverage flags live in the CI unit-job invocation (NOT in `addopts` — otherwise
  `pytest -m e2e` would enforce the unit-coverage gate and fail by construction).

## 2. Dependencies (verification items V1–V5: RESOLVED, empirically)

Runtime (`uv add`):
| dep | why | note |
|---|---|---|
| `aiosqlite` | async SQLite driver | exposes `isolation_level=None` passthrough + `in_transaction` (verified) |
| `aiokafka>=0.11` | producer / rehydrate consumer / admin | 0.11 is the exact feature floor (`delete_records` added there); resolver takes 0.14.x |
| `aiosqlitepool` | uniform connection pool (§11) | upstream v1; owner intends a later fork swap (decisions log, Batch 3 A note) |
| `uuid6; python_version < "3.14"` | uuidv7 `message_id` (§6) | `uuid6.uuid7()` verified; stdlib `uuid.uuid7` on ≥ 3.14 |

No pysqlite3 packaging (owner-confirmed): the §12 old-SQLite fallback is **documentation-only** — a
README-documented 2-line entrypoint swap; `pysqlite3-binary` publishes linux/x86_64-only wheels, so
no extra and no helper module ship. The `start()` ≥3.38 fail-fast is the in-code guard.

Dev (`uv add --dev`): `pytest`, `pytest-asyncio` (auto mode; set
`asyncio_default_fixture_loop_scope`), `pytest-cov`, `pytest-timeout`, `hypothesis` (RH-11),
`ruff`, `mypy`, `pydantic>=2` (test-only, `into=` tests), `testcontainers` (E2E broker).

Python floor: `requires-python = ">=3.10"` (all deps verified resolvable on CPython 3.10; several dev
deps require exactly `>=3.10` — zero slack, acceptable).

**V1–V5 resolutions (all verified against installed packages / a live broker):**
- **V1 YES** — `AIOKafkaAdminClient.describe_configs/alter_configs/create_topics` exist (coroutines);
  `NewTopic(..., topic_configs={...})` carries `cleanup.policy`/`retention.ms`; `describe_configs`
  returns response objects whose **`.resources`** entries must be walked (not dicts).
- **V2 YES** — `delete_records({tp: RecordsToDelete(before_offset=N)})` exists since 0.11.0 and works
  against Redpanda ≥ 23.2 (live-verified). No `rpk` fallback needed.
- **V3 NO** — aiokafka (≤ 0.14) has **no cooperative assignor**; only eager `range`/`roundrobin`/
  `sticky`, default **RoundRobin**. Spec §16 note updated accordingly; E2E-04 runs eager and passes
  `StickyPartitionAssignor` explicitly if stickiness is wanted.
- **V4 MATCHES** — `SQLiteConnectionPool(connection_factory, pool_size=5, acquisition_timeout=30,
  idle_timeout=86400, operation_timeout=10)`; `pool.connection()` is a sync method returning an async
  CM yielding the raw factory connection; timeouts are `int()`-coerced (never pass `None`/floats
  expecting precision). **The pool already rolls back on release** and health-checks with `SELECT 1`
  on acquire; a stale `query_only=ON` **passes** that health check — so the `query_only` checkout
  reset genuinely must live in KSQLite's adapter (C-08).
- **V5 RESOLVED, corrected** — the class is **`testcontainers.kafka.RedpandaContainer`** (no
  `testcontainers.redpanda` module). Its default image (v23.1.13) predates DeleteRecords: the E2E
  broker fixture **pins ≥ v24.x** (e.g. `docker.redpanda.com/redpandadata/redpanda:v24.2.20`) with
  `start(timeout=60)`.

## 3. Package layout and module dependency order

```
src/ksqlite/
  __init__.py       # explicit __all__: KSQLite, GeneratedColumn, Index, PartitionState,
                    # PartitionStatus, ConnectionPool, KafkaClientFactory + every §4 error class;
                    # ships py.typed
  errors.py         # §4 taxonomy (incl. LifecycleError) — leaf, no imports
  types.py          # GeneratedColumn, Index, PartitionState (frozen dataclass; `state` field is the
                    # exported PartitionStatus enum), ConnectionPool protocol, KafkaClientFactory
                    # protocol (producer()/consumer()/admin_client() — SYNCHRONOUS, receive the
                    # fully-merged final kwargs [merging incl. the pinned consumer keys is KSQLite's
                    # job, never the factory's], return constructed-but-UNSTARTED clients; KSQLite
                    # owns start()/stop()), ChangelogNameResolver protocol (the narrow resolve-name
                    # surface `_write` needs, so `_write` never imports `_topics`) — leaf
  _ids.py           # uuidv7 source: stdlib uuid.uuid7 (≥3.14) else uuid6 (§6)
  _sql.py           # identifier/json_path/system-column validation + escaping; DDL emitter;
                    # canonical column-clause rendering (used by BOTH migration and drift, §5.1);
                    # pure changelog-template + Kafka-topic-name validation (S-07 — composed by
                    # _topics at resolution time; template checked at start())
  _pool.py          # connection factory (autocommit isolation_level=None, WAL, busy_timeout);
                    # aiosqlitepool adapter behind the ConnectionPool seam; checkout hygiene
                    # (rollback open txn — defense-in-depth over the pool's own release-rollback —
                    # and reset stale query_only); BEGIN IMMEDIATE txn helper with bounded retry →
                    # StorageError. Named constants BUSY_RETRY_MAX_ATTEMPTS=3,
                    # BUSY_RETRY_DEADLINE_FACTOR=2.0 — semantics: attempts, capped by whichever
                    # bound trips first; per-attempt wait bounded by remaining deadline. Backoff
                    # sleep is an injectable ctor param of the txn helper (deterministic C-04/05).
  _schema.py        # migration (§5.1): CREATE IF NOT EXISTS (tables/indexes/view), DELETE FROM
                    # _owned, table_xinfo diff, drift via canonical re-render vs sqlite_schema.sql,
                    # index drift, version fail-fast (§12). Also the SINGLE home of shared DML:
                    # the materialization pair (INSERT ... ON CONFLICT(message_id) DO NOTHING +
                    # MAX-checkpoint upsert — byte-identical for _write and _lifecycle, I2/I4
                    # depend on it) and the three _owned statements (INSERT / DELETE / DELETE-ALL).
  _state.py         # in-memory 4-state machine (pure). Write-back API keeps it live:
                    # record_applied(tp, offset) is MONOTONIC-MAX (mirrors the SQL upsert) — called
                    # by _write post-commit and _lifecycle per batch commit; record_clamped(tp,
                    # offset) is the SOLE non-monotonic entry, called only from the two §7 clamp
                    # sites; record_log_end(tp, leo) at rehydrate. Conventions: log_end_offset = LEO,
                    # lag = LEO − (checkpoint+1). STANDBY = seen-this-lifetime ∧ not-owned (checkpoint
                    # may be None). partition_states() snapshots it (shallow copy; single loop).
  _wire.py          # encode/decode: key/value/header bytes; header lookup BY NAME (order-
                    # insensitive, extras ignored); payload serialize+validate PRE-produce
                    # (ValueError); classification sum type: KSQLITE | FOREIGN(skip) |
                    # UNKNOWN_VERSION(fail) per §7 rules (consumed only by _lifecycle)
  _topics.py        # ChangelogTopics service INSTANCE: takes admin client + config as ctor deps;
                    # holds the verified-TP set as instance state (dies with the store); caches
                    # success AND the ACL warn+proceed outcome (warn once per TP per store lifetime);
                    # a FAILED ensure/verify is NOT cached (re-attempted on the next assignment);
                    # implements ChangelogNameResolver; non-authz broker failures → RehydrateError;
                    # step-0 ensure/verify: create / TopicNotFoundError / compact ConfigError /
                    # ACL warn+proceed (§7 step 0, §10)
  _write.py         # Appender service (pool, producer, name_resolver: ChangelogNameResolver, state)
                    # — depends on the PROTOCOL, not the _topics module (P6 builds before P8; tests
                    # inject a two-line stub): append per §6 —
                    # validate entity_key + payload, produce+ack, offset guard, one BEGIN
                    # IMMEDIATE txn (shared materialization DML), state write-back, non-READY
                    # warning
  _read.py          # QueryRunner service (pool): query() — query_only ON/finally-OFF wrap,
                    # row_factory, into= hydration (Pydantic-v2 | dataclass only), HydrationError
  _lifecycle.py     # PartitionLifecycle service (pool, consumer-factory, topics, state, clock=
                    # time.monotonic, batch_size=DEFAULT_BATCH): owns BOTH hooks — assign
                    # (steps 0–4, poll-outside-txn batching, assignment-wide ABSOLUTE monotonic
                    # deadline computed once per callback, offset conventions + both clamps,
                    # cooperative force-stop → READY_PARTIAL) and revoke (STANDBY bookkeeping).
                    # Deadline-check placement (PINNED): a fetched batch is always materialized +
                    # committed before the check; checks run after each committed batch and before
                    # entering each partition AFTER the first — the assignment's first partition
                    # always commits at least one batch (makes E2E-07 deterministic; RH-15 exact).
                    # Replay terminates at the LEO captured in step 2 — an empty getmany is NOT
                    # termination (real brokers return {} mid-log on empty prefetch; RH-27).
                    # Mid-replay StorageError re-raised as RehydrateError (cause chained; RH-20c);
                    # on error, unrevealed partitions stay hidden (RH-28). Owns its consumer's close
                    # (aclose()); _store delegates. batch_size/clock are real operating parameters
                    # with defaults, not test hooks.
  _store.py         # KSQLite facade = pure COMPOSITION ROOT + delegation: ctor validation,
                    # lifecycle (start/stop/async CM, rollback-on-partial-failure, in-flight
                    # append counter + closed flag for stop()-drain — PINNED: flag-check + increment
                    # synchronous, decrement in finally, completion via asyncio.Event; drain covers
                    # appends only; stop() best-effort with first exception re-raised at the end;
                    # second stop() = no-op), constructs the four services in start(), owns client
                    # lifecycles (producer/admin at start; rehydrate consumer lazy, closed via
                    # _lifecycle.aclose()). Admin client gets bootstrap_servers + the security subset
                    # of consumer_config (security_protocol/sasl_*/ssl_context ∩ the admin ctor's
                    # signature via inspect.signature — no **kwargs, no hardcoded list).
                    # partition_states() delegation. No business logic.
```

Build order = import order (each module only imports what's above it):
`errors/types/_ids` → `_sql` → `_pool` → `_schema` → `_state`/`_wire` → `_topics` →
`_write`/`_read` → `_lifecycle` → `_store`.

**Logging convention:** child loggers under the `ksqlite` namespace (`ksqlite.lifecycle`,
`ksqlite.write`, …); stable event names + context via `extra=` key-values (e.g.
`event="rehydrate_start", tp=..., seek_offset=...`). Tests assert logger name + event + fields —
never message substrings. The §4 observability list (incl. truncation-reset and clamp WARNINGs) maps
1:1 onto event names.

## 4. Test strategy

### 4.1 Layout & markers
```
tests/
  unit/          # pure modules; no I/O (incl. test_fakes.py — the fakes' own contract tests)
  sqlite/        # real tmp-file SQLite via aiosqlite/pool
  kafka_fakes/   # write/lifecycle/topics/store against fakes + real SQLite
  conformance/   # SAME test bodies, parametrized: fake client (always) + real client (-m e2e)
  e2e/           # real broker; pytest -m e2e; auto-skip without Docker
```
`pytest-asyncio` auto mode. `timeout = 60` (pytest-timeout) lives in ini for unit/local runs; the
**e2e CI job overrides with `--timeout=180` and pre-pulls the pinned broker image** before pytest —
pytest-timeout covers fixture setup, and a cold image pull would trip the ini value. P0 gate:
`uv run pytest` with one placeholder test (plain `pytest` exits 5 on zero collected tests).

### 4.2 Fixtures & test utilities (all test-owned; zero production hooks)
- `db(tmp_path)` → migrated pool on a fresh file; `two_conns` → two independent raw connections to
  the same file (lock-contention tests).
- `fake_kafka` → `FakeProducer`/`FakeConsumer`/`FakeAdmin` over one shared in-memory log store;
  `store(...)` → `KSQLite` wired via the **`kafka_clients=` seam** (and `pool=` where needed).
- **`RecordingPool`** — wraps the real pool adapter (via the `pool=` seam), proxying real aiosqlite
  connections; can (a) fail the Nth `execute` / an execute matching a predicate (W-10, W-12, M-10),
  (b) append `txn_begin`/`txn_commit`/`rollback` events to a shared event log (RH-12/13/16),
  (c) verify no query ran (ST-06), and (d) **block an execute matching a predicate on a two-sided
  gate** (RH-25(b), R-11).
- **Event gates are two-sided everywhere** (the fake/wrapper sets an `arrived` event, then awaits
  `proceed`) so tests choreograph rather than race: `FakeConsumer.getmany` takes an optional per-call
  gate and appends `poll` events to the shared event log (RH-13/14, RH-25(a)); `FakeProducer.delay`
  is two-sided too (F-11).
- **Injectable clock:** `_lifecycle`'s `clock` param; the fake `getmany` advances a scripted fake
  clock per batch (RH-15/16/17 exact, not wall-clock).
- `broker` (E2E) → session-scoped `testcontainers.kafka.RedpandaContainer` pinned ≥ v24.x,
  `start(timeout=60)`; per-test unique topic prefixes; **all pause/stop mutations undone in
  `finally`** (a poisoned session container fails everything downstream).

### 4.3 Fakes (the one permitted mock boundary) — corrected to real-client semantics
All fidelity claims below were verified against aiokafka 0.13/0.14 source or a live broker:
- `FakeProducer`: `start/stop/flush`, `send_and_wait(topic, value=, key=, partition=, headers=) ->
  RecordMetadata` appending to the in-memory per-topic log (offsets monotonic **from 0** — pinned in
  test_fakes). key/value `bytes`; headers `list[(str, bytes)]` — **header keys are `str`**, not
  bytes. Unknown ctor kwargs raise `TypeError` (real clients accept no `**kwargs`). Injectable:
  raise `KafkaError`, return `offset=-1`, two-sided delay (stop-drain), duplicate-on-retry (same
  bytes at two offsets; **returned metadata = the LAST offset**, matching real retry semantics —
  consumed by W-15/RH-24).
- `FakeConsumer`: `start/stop`, `assign`, `seek` (**sync; never raises OOR**), `seek_to_beginning`
  (async), `end_offsets` (**returns LEO = last offset + 1; empty topic → 0**), `getmany/getone`
  (**`getmany` returns `dict[TopicPartition, list[record]]` and honors `max_records`** — RH-12/13/
  14/15 all presuppose per-poll batch boundaries) serving the shared log. **`OffsetOutOfRangeError`
  is raised from the fetch, and only when constructed with `auto_offset_reset="none"`**; with
  `"latest"`/`"earliest"` the fake silently resets — so an implementation that forgets `"none"`
  *fails* RH-09 (zero rows materialized) instead of passing vacuously. Injectable **spurious empty
  poll** (returns `{}` once mid-stream, then continues — real prefetch behavior; RH-27). Records
  carry `key/value/headers/offset/topic/partition` with real byte/type shapes.
- `FakeAdmin`: `describe_configs` returns **`.resources`-shaped response objects** (injectable:
  `compact`, `compact,delete`, authorization error), `create_topics`, `delete_records`; records
  created-topic configs for assertion.
- `tests/unit/test_fakes.py` pins the contract: offset base 0 + monotonicity, LEO convention (incl.
  empty-topic 0), header types `(str, bytes)`, `getmany` dict shape + `max_records`, OOR raise point
  (fetch) + condition (`"none"`), `seek` never raising, duplicate-on-retry metadata = last offset.
- **Conformance suite (`tests/conformance/`, IDs CF-01…CF-07)**: one parametrized set of test bodies
  — CF-01 produce → metadata shape (incl. `partition=` accepted); CF-02 `seek` (sync) /
  `seek_to_beginning` (coroutine) / `getmany` dict + `max_records`; CF-03 OOR from fetch under
  `"none"` vs silent reset under `"latest"`; CF-04 `end_offsets` = LEO incl. empty-topic 0; CF-05
  header types `(str, bytes)`; CF-06 `describe_configs` `.resources` walking; CF-07 unknown ctor
  kwarg → `TypeError` — executed against the fake **always** and the real client under `-m e2e`.
  Producer bodies land with P6, consumer/admin bodies with P8; **the `broker` fixture is built at
  P8** (that is what makes the P8 `-m e2e` gate non-vacuous). Fake↔real divergence breaks
  mechanically, not by review.

### 4.4 Correctness invariants (asserted repeatedly, not once)
| ID | Invariant | Spec | Enforced by tests |
|---|---|---|---|
| I1 | The `records` view never returns a row whose `(topic,partition)` has no `_owned` row — including across **topics sharing a partition number** | §5.3/§9 | R-01 (matrix), RH-*, E2E-11 |
| I2 | Replaying any changelog prefix twice ⇒ byte-identical `_records` (dedup by `message_id`, incl. the same id at **two different offsets**) | §13 | W-09/W-15, RH-07/RH-24, E2E-03 |
| I3 | Checkpoint = last materialized record offset (MAX), monotonic except the two explicit §7 clamps | §5.2/§6/§7 | W-07/08, RH-10/10b/11/23 |
| I4 | Final SQLite state is a deterministic function of (changelog contents, owned set) **given gap-free histories** — full rebuild ≡ incremental rebuild from any equal checkpoint. The one spec-accepted exception is the §13/§14 watermark gap, pinned by RH-22 (incremental skips it; cold rebuild recovers it) | §1/§13/§14 | RH-08, RH-22, E2E-02/03 |
| I5 | Atomic reveal: a concurrent reader sees zero rows for a partition mid-replay, then all-materialized-so-far after the `_owned` INSERT — never a between state | §5.3/§7 | RH-14 (event-gated + commit-failure probe), E2E-11 |
| I6 | `query()` cannot mutate any DB state (incl. `_records`/`_owned` directly), even under cancellation | §9 | R-05/06/07/11 |
| I7 | Record insert + checkpoint upsert are atomic (same txn: both or neither) | §5.2/§6 | W-10 |
| I8 | The write lock is never held across a Kafka `poll()` | §7 | RH-13 |

---

## 5. Phases

### P0 — Project scaffold
Deliverables: `uv init` package (`pyproject.toml`, `src/` layout, `requires-python >= 3.10`), deps
(§2), `ruff`/`mypy`/`pytest` config (`asyncio_mode = "auto"`, `asyncio_default_fixture_loop_scope`,
`-m "not e2e"` in `addopts`, coverage flags **only** in the CI unit job), markers (`e2e`), `py.typed`,
a `NullHandler` on the `ksqlite` root logger, one placeholder test, E2E broker image pin recorded.
Gate: `uv run pytest` (1 passing placeholder), lint/type clean.

### P1 — Leaf modules: `errors`, `types`, `_ids`
| ID | Test | Expected |
|---|---|---|
| L-01 | taxonomy + exports | every §4 error (incl. `LifecycleError`) subclasses `KSQLiteError`; no extras/omissions; `ksqlite.__all__` matches the spec surface exactly |
| L-02 | `GeneratedColumn`/`Index` are frozen dataclasses w/ documented defaults | per §4 |
| L-03 | `PartitionState` shape | frozen dataclass; fields exactly `state/checkpoint_offset/log_end_offset/lag` (§4); `state` is the exported `PartitionStatus` enum |
| L-04 | protocol surfaces | `ConnectionPool` (`connection()`/`close()`); `KafkaClientFactory` (three **sync**, kwargs-taking methods); `ChangelogNameResolver` |
| L-05 | `_ids.new_message_id()` | valid RFC-9562 v7 UUID **string** (36-char); unique across 10⁵ calls |
| L-06 | `_ids` source selection | stdlib on ≥3.14, `uuid6` otherwise (monkeypatched version check) |

(The original L-03/L-04 — `sqlite_compat` import purity + too-late guard — were retired with the
owner-confirmed module drop; the IDs are reused above for the type/protocol surface.)

### P2 — `_sql`: validation, escaping, DDL emission, canonical rendering
| ID | Test | Expected |
|---|---|---|
| S-01 | identifier charset | `task_id` ok; `task-id`, `task id`, `täsk`, `"x";DROP`, empty, leading digit → `ConfigError` |
| S-02 | system-column collision | host column named `payload`/`message_id`/`entity_key`/`source_topic`/`source_partition`/`changelog_offset` → `ConfigError` |
| S-03 | json_path escaping | `$.a'b` renders a correctly-escaped SQL string literal; executes on real SQLite without error |
| S-04 | json_path validation | must start `$.`; empty / bare `$` / control chars → `ConfigError` |
| S-05 | emitter output | `ALTER TABLE _records ADD COLUMN x TEXT GENERATED ALWAYS AS (payload->>'$.x') VIRTUAL` — exact, executable |
| S-06 | canonical render round-trip | emit DDL → create column on real SQLite → re-render from config → equals the column clause found in `sqlite_schema.sql` (this equality IS the drift mechanism, §5.1) |
| S-07 | template + topic-name validation (pure fns; composed by `_topics`) | missing `{partition}` or `{source_topic}` → `ConfigError`; resolved-name charset + 249-char limit checks |
| S-08 | index-column cross-validation | `Index` referencing an undeclared, non-system, non-base column → `ConfigError`; base columns (e.g. `entity_key`) legal |
| S-09 | `sql_type` validation | same identifier-charset rule as S-01 (DDL safety, not a type allowlist): `TEXT`, `INTEGER`, `BOOLEAN` ok; `TEXT, x INTEGER`, `TEXT); DROP`, empty, leading digit → `ConfigError` |
| S-10 | `Index.name` validation | S-01's identifier rule applied to the index name: `ix_thread` ok; `ix-thread`, `"x";DROP`, empty → `ConfigError` |

### P3 — `_pool`: connection layer (real SQLite, two-connection contention)
| ID | Test | Expected |
|---|---|---|
| C-01 | factory pragmas | new conn: `journal_mode=WAL`, `busy_timeout` set, autocommit (`isolation_level=None`) |
| C-02 | txn helper commits | `BEGIN IMMEDIATE` … commit; row visible from second connection |
| C-03 | txn helper rolls back on any exception | raise inside txn → no partial writes, connection returned clean |
| C-04 | `SQLITE_BUSY` bounded retry | conn B holds write lock beyond retries → `StorageError`; observable behavior asserted (raise + loose total-duration bounds under a small fixture `busy_timeout`), not internal attempt counts; deterministic via the injectable backoff sleep |
| C-05 | retry succeeds when lock frees | B's release driven deterministically (injected backoff callback releases B) → helper succeeds |
| C-06 | no `SQLITE_BUSY_SNAPSHOT` path | read-then-write inside `BEGIN IMMEDIATE` under concurrent writer never raises SNAPSHOT (upfront lock) |
| C-07 | checkout hygiene: open txn | tested against the **adapter seam** (aiosqlitepool's own release already rolls back — V4; the adapter's defense-in-depth is what's under test): a connection abandoned mid-txn → next checkout gets a rolled-back, working connection |
| C-08 | checkout hygiene: stale `query_only` | flag left ON (passes the pool's `SELECT 1` health check — V4) → next checkout can write; the reset provably lives in KSQLite's adapter |
| C-09 | `query_only` scope helper | ON blocks INSERT/UPDATE/DELETE/DROP on any table; OFF restores; reset in `finally` on exception |
| C-10 | pool seam | injected fake `ConnectionPool` used verbatim (protocol honored) |

### P4 — `_schema`: migration + drift (real SQLite)
| ID | Test | Expected |
|---|---|---|
| M-01 | fresh migrate | `_records`, `partition_checkpoint`, `_owned`, both base indexes, **the `UNIQUE(message_id)` index**, and the `records` view all exist; checkpoint PK columns NOT NULL |
| M-02 | idempotent second migrate | zero DDL errors (the `table_xinfo` fix; would crash with `table_info`) |
| M-03 | `_owned` cleared on migrate | pre-seeded `_owned` row gone after migration |
| M-04 | generated column added + indexed | declared column queryable via `->>`; `EXPLAIN QUERY PLAN` uses its index; json_path targeting a missing payload field → NULL, no error |
| M-05 | add-column-later | v2 config adds `user_id` on a **populated** table → works (VIRTUAL); values extracted for existing rows |
| M-06 | drift: changed json_path | same name, `$.task_id`→`$.meta.task_id` → `ConfigError` naming column + both paths |
| M-07 | drift: changed sql_type | TEXT→INTEGER → `ConfigError` |
| M-08 | index drift | same index name, different columns/unique → `ConfigError` |
| M-09 | removal not automatic | column/index removed from config → still present, **logged** (caplog: logger+event), no error |
| M-10 | migration DDL failure | via `RecordingPool` predicate-failure (or the hook-free route: pre-create a *table* named like the index — shared namespace makes `CREATE INDEX` genuinely fail) → `SchemaMigrationError` |
| M-11 | version fail-fast | monkeypatched `sqlite_version_info=(3,37,0)` → `SQLiteVersionError`; `(3,38,0)` passes |
| M-12 | view exposes later-added generated columns | `SELECT r.*` re-expansion (spec-verified behavior pinned) |

### P5 — `_state` + `_wire` (pure)
| ID | Test | Expected |
|---|---|---|
| ST-01 | transitions | UNKNOWN→RESTORING→READY; force-stop→READY_PARTIAL; revoke→STANDBY (checkpoint kept); re-assign STANDBY→RESTORING; fast path = STANDBY/UNKNOWN→READY directly (no observable RESTORING) |
| ST-02 | STANDBY derivation | seen-this-lifetime ∧ not owned → STANDBY (checkpoint may be `None` — revoked off an empty changelog; an appended-never-assigned TP also reports STANDBY) |
| ST-03 | cached lag semantics | `log_end_offset` = LEO, `lag = LEO − (checkpoint+1)` (values pinned); captured at last `record_log_end`; not refreshed by reads |
| ST-04 | `partition_states()` snapshot | plain dict copy; mutating it doesn't affect internal state |
| ST-05 | fresh-process scope | new machine → `{}`; only partitions seen this lifetime appear (no SQLite read) |
| ST-06 | write-back keeps checkpoint live | after `record_applied(tp, off)`, `partition_states()[tp].checkpoint_offset == off` — **with zero DB reads** (RecordingPool verifies) |
| ST-07 | `record_applied` is monotonic-MAX | applied 5 then 3 → stays 5 (mirrors the SQL upsert) |
| ST-08 | clamps only via `record_clamped` | `record_clamped` regresses the value; `record_applied` cannot — sole non-monotonic entry |
| WF-01 | encode | key=`entity_key.encode()`; value = UTF-8 JSON bytes; headers `[("format_version", b"1"), ("message_id", <36-char uuid str, UTF-8>)]` — **header keys `str`, values `bytes`** |
| WF-02 | decode round-trip | encode→decode identical entity_key/message_id/payload |
| WF-03 | classification: foreign | missing/malformed `message_id` header, null/undecodable key, null/undecodable **value** → FOREIGN (skip) |
| WF-04 | classification: unknown version | `format_version=b"2"` → UNKNOWN_VERSION (→ `RehydrateError` in P8) |
| WF-05 | payload pre-produce validation | non-JSON-serializable object or invalid JSON text → **`ValueError` at encode, before any produce**; `{}` / nested-null payloads OK |
| WF-05b | NaN/Infinity rejection | `{"x": float("nan")}`, `float("inf")`, and the JSON-text `'{"x": NaN}'` → `ValueError`, fake log unchanged (`allow_nan=False` + raising `parse_constant` — SQLite ≥ 3.42 silently coerces `NaN`→`null`; < 3.42 poisons every replay) |
| WF-06 | header robustness | decode succeeds with headers **shuffled** and with **extra unknown headers** present (lookup by name) |
| WF-07 | missing `format_version`, valid `message_id` | UNKNOWN_VERSION (fail loud — it carries the KSQLite marker) |

### P6 — `_write` (opens by building + contract-testing `FakeProducer`)
| ID | Test | Expected |
|---|---|---|
| W-01 | happy path | row in `_records` with produced offset; checkpoint = offset; payload stored as JSON text (`json(?)`) |
| W-02 | entity_key validation | `None`, `""`, non-str, non-UTF-8-encodable → `ValueError`; **nothing produced** |
| W-03 | payload validation ordering | invalid payload → `ValueError`, **fake log unchanged** (pre-produce, per §6) |
| W-04 | produce failure | injected `KafkaError` → `ChangelogProduceError`, **no SQLite write**, checkpoint unchanged |
| W-05 | offset guard | injected `offset=-1` → `ChangelogProduceError`, no SQLite write |
| W-07 | checkpoint MAX monotonic | appends at offsets 5 then 3 (injected) → checkpoint stays 5 |
| W-08 | checkpoint per-partition | interleaved appends on (t,0) and (t,1) → independent checkpoints |
| W-09 | duplicate `message_id`, same offset | replayed twice → one row, checkpoint advances |
| W-10 | atomicity (I7) | `RecordingPool` fails the checkpoint-upsert execute → **neither** insert nor checkpoint persisted (single txn) |
| W-11 | non-READY append | append to partition not READY → **warning logged** (caplog: logger+event+tp), write proceeds |
| W-12 | produce-ok/apply-failed | SQLite txn fails after ack → `StorageError`; record IS in fake log; checkpoint unchanged |
| W-12b | …self-heal completes | after W-12, a rehydrate materializes that record (the §14 promise, exercised) — implemented at P8 alongside the RH suite (needs `PartitionLifecycle`) |
| W-13 | concurrent appends | 2 tasks × 100 appends via pool → all rows present, no `StorageError`, correct checkpoints (labeled sanity; locking weight is carried by C-04..C-06) |
| W-14 | host-retry duplicate (documented §14 loss) | host re-calls `append` after `ChangelogProduceError` where the first produce was a phantom → **two rows** (new `message_id`) — pins the documented behavior |
| W-15 | producer-retry duplicate (I2) | FakeProducer duplicate-on-retry (metadata = last offset): drive the **shared materialization DML** with both copies (the §3 `_schema` pair — shared with replay, so the kill transfers) → **one row**, checkpoint = K+1; RH-24 re-kills at the lifecycle level |

(W-06 `acks=0` moved to P9 F-01 — ctor/`start()` validation is `_store`'s surface.)

### P7 — `_read`
| ID | Test | Expected |
|---|---|---|
| R-01 | scoping matrix (I1) | rows for `(tA,0), (tA,1), (tB,0)`; `_owned` = `{(tA,0)}` → view returns exactly the `(tA,0)` rows (kills both dropped-JOIN-clause mutants) |
| R-02 | `into=` Pydantic v2 | `list[Message]` via `model_validate_json` |
| R-03 | `into=` dataclass; everything else `TypeError` | `dataclasses.is_dataclass` path; a plain class with matching kwargs → `TypeError` (no kwargs fallback — pinned) |
| R-04 | `HydrationError` | `into=` with a query not selecting `payload` |
| R-05 | write via view | `INSERT INTO records …` through `query()` → `StorageError` |
| R-06 | write via internals (I6) | `DELETE FROM _records` / `UPDATE _owned` / `DROP VIEW records` through `query()` → `StorageError` (query_only) |
| R-07 | query_only reset | **`pool_size=1`**: after R-06 failures, `append` on the same pool (same connection) works |
| R-08 | index through view | `EXPLAIN QUERY PLAN` on `WHERE thread_id=?` → `USING INDEX` |
| R-09 | rows are `sqlite3.Row` | mapping access by column name |
| R-10 | read of `_records` directly | allowed (documented footgun): returns unscoped rows — documents, not forbids |
| R-11 | cancellation safety (I6) | **`pool_size=1`**, event-gated query blocked mid-read, task cancelled → subsequent `append` on the same connection succeeds (finally-reset or checkout hygiene — the spec's two-layer contract) |

### P8 — `_topics` + `_lifecycle` (opens by building + contract-testing `FakeConsumer`/`FakeAdmin`; E2E CI job becomes a required gate from here)
| ID | Test | Expected |
|---|---|---|
| T-01 | step 0 create | topic missing + `create_topics_retention_ms=-1` → created (1 partition, delete policy, retention −1) |
| T-02 | step 0 missing | topic missing + `None` → `TopicNotFoundError` |
| T-03 | step 0 compact | policy `compact` and `compact,delete` (via `.resources`-shaped response) → `ConfigError`; `delete` passes |
| T-04 | step 0 ACL degrade | injected authz error on describe → **warning logged** + proceed |
| T-04b | ACL degrade cached-as-attempted | second assignment after warn+proceed: no second describe call, no second warning (a fresh store re-checks) |
| T-05 | success cached per-TP | second assignment of the same TP does not re-verify |
| T-05b | cache is per-TP, not global | after `(t,0)` verified, first assignment of `(t,1)` with compact policy → `ConfigError` |
| T-06 | `verify_changelog_policy=False` | no `describe_configs` call; assignment on a compact topic proceeds (LOCKED opt-out exercised) |
| T-07 | fast path still runs step 0 | warm checkpoint == log-end on **first** assignment + compact policy → `ConfigError` (spec: step 0 unconditional) |
| T-08 | failure NOT cached | after T-03's `ConfigError`, a later assignment of the same TP re-verifies (fixed policy → succeeds) |
| T-09 | resolution-time name validation | template resolving to an illegal/>249-char changelog name → `ConfigError` on first assignment (proves `_topics` invokes the S-07 validators) |
| RH-01 | full replay | empty DB, fake log (offsets from 0) with N records → all N materialized, checkpoint = N−1, READY, `_owned` INSERTed |
| RH-02 | empty changelog | LEO 0 → immediate READY, no error; checkpoint row absent (pinned) |
| RH-03 | incremental replay | checkpoint K → `seek(K+1)`; only the tail fetched (assert fetch count) |
| RH-04 | fast path | checkpoint+1 == LEO → reveal directly; **zero `getmany`/`getone` calls** (`end_offsets` and step-0 calls ARE expected) |
| RH-05 | foreign records skipped **and checkpointed** | FOREIGN-class records (incl. a trailing-foreign tail) → skipped + **logged** (caplog: logger+event+tp+offset); replay completes; **checkpoint advances past them** (fast path preserved next time) |
| RH-06 | unknown/missing version fails loud | **parametrized over BOTH shapes** — `format_version=b"2"` AND missing-version-with-valid-`message_id` — each raises `RehydrateError`; partition NOT revealed (the "or" must be an "and": an inline classifier that skips missing-version as foreign must fail this) |
| RH-07 | re-replay idempotent (I2) | full replay over existing rows → identical table bytes |
| RH-08 | incremental ≡ full (I4) | same log: (full) vs (prefix, revoke, resume incremental) → identical `_records` |
| RH-09 | truncated log reset | consumer runs `auto_offset_reset="none"`; OOR raised **from the fake's fetch** → reset-to-beginning, **all retained records materialized** (count asserted — kills the silent-latest-reset mutant), **WARNING logged** (old checkpoint + new log-start), no raise |
| RH-10 | clamp-down conventions | checkpoint 600, LEO 550 (post topic-shrink) → after reset+replay checkpoint == **549 (LEO−1)**; next rehydrate is incremental (no OOR) |
| RH-10b | clamp-up after zero-record reset | fully-trimmed tail (log_start == LEO) → checkpoint == log_start−1; next rehydrate does NOT re-enter the reset path; WARNING logged |
| RH-11 | checkpoint property test | `hypothesis` stateful test over {append, replay, revoke, reset+clamp}: checkpoint never decreases except via the two explicit clamps (logged seed / reproducible) |
| RH-12 | batching boundaries | batch size B (service ctor param), N=2.5×B → 3 txns (commit events counted via `RecordingPool`) |
| RH-13 | poll outside txn (I8) | shared event log: no `poll` event between a `txn_begin` and its `txn_commit` |
| RH-14 | atomic reveal (I5), deterministic | (a) event-gated batches: after batch 1 commits, reader sees **0** rows AND `partition_states() != READY` (state/view coherence); after final batch + reveal, all rows. (b) commit-failure probe: `RecordingPool` fails the **final** batch's commit → `_owned` row absent, partition not revealed, **`RehydrateError` raised** (kills reveal-before-final-commit and swallowed-commit-failure) |
| RH-15 | assignment-wide budget (fake clock) | 3 partitions, scripted clock crossing the deadline mid-P2 → P1 READY, P2 READY_PARTIAL at its committed frontier (batch always materialized+committed before the check — pinned placement, §3), P3 READY_PARTIAL at kept checkpoint; all three visible; lags exposed; a first-assignment partition beyond the deadline **still runs step 0** (ensure/verify is never budget-skipped) |
| RH-16 | cooperative force-stop | deadline checked between batches: no rollback event after the deadline crossing; checkpoint == last committed batch boundary |
| RH-17 | budget→convergence | after RH-15, re-assign with ample fake clock → all READY, complete |
| RH-18 | revoke | `_owned` row DELETEd (rows invisible immediately), `_records` + checkpoint kept, state STANDBY |
| RH-19 | revoke never-owned TP | no-op, no error (also: the empty `[]` callbacks aiokafka emits — both revoke and assign) |
| RH-20 | replay error | injected consumer error mid-replay → `RehydrateError`, partition not revealed, checkpoint = last committed batch |
| RH-20b | clamp not committed before replay | OOR → reset → consumer error mid-replay → checkpoint == last committed batch (NOT log-end); later ample rehydrate converges |
| RH-20c | error-type precedence | `RecordingPool` fails a mid-replay batch execute → **`RehydrateError`** (not `StorageError`), cause chained |
| RH-21 | consumer isolation (all three keys) | rehydrate consumer constructed with `group_id=None`, `enable_auto_commit=False`, **`auto_offset_reset="none"`** even when `consumer_config` tries to override each |
| RH-22 | watermark gap semantics (I4 exception; §13/§14 pinned) | fake log has offset K un-materialized, checkpoint > K → incremental rehydrate does **not** recover K (accepted loss); a cold full rebuild **does**. Constructed through public seams: append #1 with `RecordingPool` failing its txn (record durably at K, checkpoint unchanged — the W-12 state), then append #2 succeeds (checkpoint = K+1) |
| RH-23 | boundary offset | checkpoint K, exactly one new record at K+1 → incremental replay materializes exactly it; **also asserts the first fetched offset == K+1** (dedup must not silently absorb a seek-one-low mutant) |
| RH-24 | duplicate id at two offsets on replay (I2) | same `message_id` at offsets K, K+1 in the log → one row, checkpoint K+1 |
| RH-25 | cancellation (two cut points) | (a) cancel while parked in a two-sided-gated `getmany`; (b) cancel while parked in a `RecordingPool`-gated mid-txn execute → in both: partition hidden, checkpoint = last committed batch, pool returns a clean connection (checkout hygiene exercised at lifecycle level) |
| RH-26 | warm restart (persisted checkpoint) | **second service stack (fresh migrate + `PartitionLifecycle`) on the same tmp DB** (the facade is P9): `_owned` empty at start, assignment replays only the tail from the persisted checkpoint (the flagship §5.2 scenario, end to end on fakes) |
| RH-27 | spurious-empty poll is not termination | N records, injected `{}` return after batch 1 → replay continues to the captured LEO: all N materialized, checkpoint = N−1, READY (kills loop-until-`getmany`-empty — real brokers return `{}` mid-log routinely) |
| RH-28 | mid-assignment failure leaves the rest hidden | 3 partitions, error during P2's replay → P1 READY, P2 and P3 have **no** `_owned` row (hidden), `RehydrateError` propagates; a later assignment converges |

### P9 — `_store`: facade, ctor validation, lifecycle
| ID | Test | Expected |
|---|---|---|
| F-01 | validation matrix — **raises `ConfigError` from `start()`, before any I/O** (pool open, broker connect) | bad template / `pool_size<1` / non-positive timeouts / **`acks=0`** / bad `GeneratedColumn` (incl. system-column collision, bad `sql_type`) / bad `Index` (column ref or name) → `ConfigError` |
| F-02 | start sequence | pool → migrate (incl. `_owned` clear) → producer → admin; rehydrate consumer NOT started; services constructed (composition root) |
| F-03 | rollback-on-partial-failure, **parameterized over every start step** | producer-start fail → pool closed; admin-start fail → producer + pool closed; migrate fail → pool closed; no leaked tasks/connections; each failing step's **surfaced exception type asserted** (pool-open → `StorageError`, migrate → `SchemaMigrationError`, client start → base `KSQLiteError`, cause chained) |
| F-04 | double start | `LifecycleError` |
| F-05 | stop ordering | drain in-flight appends (counter+flag mechanism) → producer flush+stop → consumer stop (if started) → admin close → pool close; call order recorded via the seams |
| F-06 | pre-start / post-stop calls | `append`/`query`/hooks before `start()` or after `stop()`, and `stop()` before `start()` → `LifecycleError`; a second `stop()` after `stop()` = **no-op** (pinned) |
| F-07 | async CM | `async with KSQLite(...)` == start/stop |
| F-08 | listener integration | host-style `ConsumerRebalanceListener` calling the hooks; ordering doc pinned by test |
| F-09 | `partition_states()` end-to-end | RESTORING/READY/READY_PARTIAL/STANDBY across a scripted assign/force-stop/revoke sequence; the force-stop leg drives the **lifecycle service directly with the injected clock** (the facade has no clock seam — LOCKED API) |
| F-10 | structured logs, **table-driven per event** | rehydrate start/end/force-stop, topic ensure/verify, produce failure, migration, compact rejection, non-READY append, foreign-record skip, orphaned column/index on removal, **truncation reset, clamps** — each asserted by logger name + event + fields (never substrings) |
| F-11 | stop during in-flight append | `stop()` awaits the append's txn (drain), then closes — no `StorageError`, no lost row |
| F-12 | §15 defaults snapshot | every ctor default equals the spec table (template, pool_size 16, busy_timeout 5.0, rehydrate_timeout 30.0, verify True, create None, acks 1) |
| F-13 | config pass-through merge | arbitrary **valid aiokafka** kwargs (`security_protocol`, `sasl_*`, `acks="all"`) reach the factory calls as fully-merged kwargs (the merge lives ABOVE the seam — production code under test, not the fake); defaults survive partial override; the three pinned consumer keys are **present with their pinned values** in the received kwargs (not merely overrides-stripped — aiokafka's own defaults are the catastrophic values); the admin factory receives the security subset |

### P10 — E2E: real broker (pinned Redpanda ≥ v24.x via `testcontainers.kafka`; `pytest -m e2e`)
| ID | Scenario | Must hold |
|---|---|---|
| E2E-01 | happy loop, **two source topics** (§2.6) + unicode `entity_key` + one large (~900 KB) payload (the client's default `max_request_size` is 1 MiB — a serialized 1 MB JSON exceeds it; staying under pins the defaults posture) | rows queryable per topic with correct scoping (same partition numbers across topics); two changelog topics exist (1 partition, delete policy); raw-consumer check of key/headers wire format per §10 |
| E2E-02 | cold rebuild + **pre-existing foreign record** | append N, pre-produce a headerless/null-key record into the changelog, destroy local DB, new instance, assign → full replay reproduces N rows (foreign skipped+logged), identical query results (I4) |
| E2E-03 | warm handoff (deterministic, no group) | two stores, separate DBs, **direct hook calls**: A owns→appends→revoke; B assigns (full replay)→appends→revoke; A re-assigns → **incremental**: asserted via the structured rehydrate-start log (`seek_offset == checkpoint+1`, `records_replayed == tail_count`) — not "consumer position" (none exists with `group_id=None`) |
| E2E-04 | real consumer group, two consumers | membership change + **bounded-wait polling** (no sleeps); **quiesce before rebalancing** (produce N, wait until both stores hold N, then change membership — at-most-once means in-flight records are legitimately lost otherwise); assert disjoint-union assignment + row conservation, never specific placements; sticky assignor passed explicitly if used; the eager no-op re-reveal exercises the fast path (assert zero-replay via logs) |
| E2E-05 | compact-policy live check | pre-created `cleanup.policy=compact` → first assignment `ConfigError`; `alter_configs`→`delete` (full-replace semantics — fine on a fresh test topic); **a fresh store instance** then assigns successfully (no retry-after-failed-assignment semantics needed) |
| E2E-06 | auto-create config | `create_topics_retention_ms=-1` → topic created; assert partitions=1, `cleanup.policy=delete`, `retention.ms=-1` via describe |
| E2E-07 | budget force-stop, deterministic | preload ~20k records; `rehydrate_timeout` ≪ one batch's wall-clock (e.g. 0.001 s) → deterministic by the pinned placement (§3: first partition always materializes+commits its first batch before the check): READY_PARTIAL, partial rows visible, lag > 0. Completion: instance 1 is **stopped**, then **a second instance with default budget on the same `db_path`** (§2.8) → READY, full count, incremental from the kept checkpoint (doubles as warm-restart + I4 coverage) |
| E2E-08a | truncation (trim) | choreography pinned so the reset path actually runs (a naive trim ≤ checkpoint+1 passes via the fast path without ever resetting): store appends K; an **out-of-band** producer (valid KSQLite wire) appends a tail; `delete_records(before_offset ≥ checkpoint+2)`; re-assign → **truncation-reset WARNING event asserted**, retained records materialized, checkpoint == LEO−1, next rehydrate incremental (zero fetches) |
| E2E-08b | clamp-down (log-end regression) | append K records → `delete_topics` + poll-until-absent + recreate + produce M<K → re-assign → OOR reset → checkpoint **decreased** to M−1; subsequent rehydrates incremental |
| E2E-09 | broker outage | `producer_config={"request_timeout_ms": 2000}`; `docker pause` (via wrapped container, **unpause in `finally`**) → `append` raises `ChangelogProduceError` in ~2–4 s, no SQLite row; unpause → appends resume. Must NOT assert the failed record is absent from the changelog (the phantom may land — that's §14) nor exact post-unpause offsets. `pytest-timeout` 30 s |
| E2E-10 | stop-drain against real broker | `stop()` flushes producer; raw consumer sees every acked record |
| E2E-11 | live concurrent reads during a big rehydrate (I1/I5) | reader task records **≥1 hidden-phase sample (0 rows) and ≥1 complete-phase sample (full count)** — a hidden-phase sample counts only if taken **after the `rehydrate_start` log event** (a pre-assignment 0-rows sample is vacuous); the ≥20k preload is the mechanism keeping the window wide |
| E2E-13 | broker restart persistence | **dedicated container with a fixed host port** (bind a free port, `with_bind_ports(9092, port)`) — the session container **cannot** be restarted: Docker remaps the ephemeral port AND Redpanda re-advertises the stale one from its persisted start script (verified live). append N → `docker stop`/`start` (readiness = second `Started Kafka API server` log line) → same store's appends resume; a fresh instance cold-rebuilds N rows. Session broker untouched → no ordering constraint |

**Broker-compat profile (formerly E2E-12):** the whole suite runs `acks=1`/idempotence-off (the
defaults) — that *is* the compat posture; optional manual CI jobs: Tansu broker, `acks=all`
profile, SASL/SCRAM-enabled Redpanda (config-passthrough smoke; the merge logic itself is F-13).

**Accepted-untestable failure modes (declared, not covered):** `acks=1` unclean-leader /
leader-failover loss (needs multi-node + mid-produce leader kill; not deterministically triggerable);
the MAX-watermark lost-ack gap (needs a deterministic phantom-produce; its downstream semantics ARE
pinned on fakes by RH-22); pre-changelog crash loss (§14(1) — host-process contract, no observable
KSQLite behavior); real retention-expiry deletion (proxied by `delete_records`, E2E-08a). These are
contract documentation, not test gaps — see spec §13/§14.

Infra: session-scoped pinned container (`start(timeout=60)`); per-test unique topic prefixes; every
container mutation undone in `finally`; expected suite wall-clock ~60–90 s post-pull.

### P11 — Polish & release readiness
`ruff` + `mypy` clean; coverage gate met; `py.typed` shipped; README (Diátaxis: quickstart how-to +
§-linked reference + the documented pysqlite3 entrypoint snippet with its linux/x86_64-only caveat);
docstrings + examples exercise the **public** surface only (no `_`-module imports outside tests);
`docs/DESIGN.md` cross-checked — implementation-discovered drift goes through the decisions log;
CHANGELOG; version `0.1.0`.

---

## 6. Cross-cutting edge-case catalog (each pinned by at least one test above)

Unicode entity_key (emoji, CJK) round-trips (W/RH, E2E-01); ~900 KB payload vs the client's
`max_request_size` (E2E-01); missing payload field → NULL generated column (M-04); quote in
json_path (S-03); zero generated columns declared (M-01 bare); `Index` on base vs undeclared columns
(S-08); template resolving to an illegal/over-long topic name (S-07); append during another
partition's replay window (W-11 × RH-14); `partition_states()` on a never-assigned store → `{}` and
fresh-process-with-warm-DB → `{}` until assignment (ST-05); revoke-then-immediate-reassign (RH-08);
two stores on separate DBs sharing a broker (E2E-03/04); `append`/`query`/`stop` before `start()` →
`LifecycleError` (F-06); header bytes that decode but aren't a UUID → FOREIGN (WF-03); shuffled/extra
headers (WF-06); trailing-foreign tail vs fast path (RH-05); same-id-two-offsets (W-15/RH-24).

## 7. Correctness argument (what the suite proves, per spec section)

- **§5/§6 write path:** I3 + I7 (W-07/08/10) ⇒ the checkpoint never lies within its MAX-watermark
  contract; W-02..W-05/W-12/W-14/W-15 pin the full failure/duplicate taxonomy including both §14
  documented losses.
- **§7 rehydrate:** I2 + I4 (RH-07/08/22/24) ⇒ replay converges under interruption and duplication,
  with the watermark gap explicitly pinned as the one accepted exception; RH-09/10/10b/11/23 pin
  truncation/clamp/offset conventions; RH-13..17/20b/25 pin liveness (budget, lock discipline,
  cancellation).
- **§9 read path:** I1 + I5 + I6 (R-01/05/06/11, RH-14) ⇒ visibility and immutability are enforced
  invariants, not conventions.
- **§13/§14 accepted losses** are *documented by tests* (E2E-09, W-12/W-12b/W-14, RH-22) so any
  silent behavior change trips them; the modes no test can produce are **declared** in P10's
  accepted-untestable block.

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Fake drift from real client semantics | conformance suite (§4.3) + required `-m e2e` CI gate from P8 |
| aiokafka has no cooperative assignor (V3) | host-side; spec §16 corrected; E2E-04 runs eager |
| Redpanda image capabilities (V5) | pinned ≥ v24.x; `delete_records` live-verified |
| aiosqlitepool API drift (V4) | pinned; adapter isolates behind the seam; C-07/C-08/C-10 |
| E2E flakiness | determinism from mechanisms, not timing: budget≪batch, event gates, bounded-wait polling, tuned timeouts, `finally` cleanup, `pytest-timeout` |
| Timing-based unit tests | injectable clock + injectable backoff; wall-clock asserted only loosely and only in E2E |
| uuid7 dep confusion | single `_ids` chokepoint + L-06 |
| Windows/file-locking differences | CI matrix linux+macos; WAL tests marked platform-sensitive |
| Implied-but-untested failure modes | declared accepted-untestable block (P10) |
