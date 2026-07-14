# KSQLite — Review Resolutions Log

Append-only log of decisions from the design-review process. Fold-in and convergence are
**complete** (see "Fold verified: PASS" and the convergence declaration below); the file now
stands as the historical audit trail behind `DESIGN.md`. Entries are never rewritten — a
superseded decision is annotated by a later entry, so early entries may name symbols the final
API dropped. Status tags: **[LOCKED]** confirmed by owner; **[REC]** my recommendation pending
confirm; **[OPEN]** not yet discussed.

## Terminology
- **[LOCKED]** `conversation_id` → **`task_id`**. Model: messages → threads → **tasks**.
  `task_id` is the container / source-partition key; `thread_id` is a generated column.

## Critical / correctness
- **[LOCKED] C1 (checkpoint gap):** advance the checkpoint only to a **contiguous** offset
  (never past an unmaterialized offset). Cheap insurance vs the narrow "phantom write" gap.
  **If it proves non-trivial to implement, DROP it** and accept the narrow gap as best-effort loss.
- **[LOCKED] C2 (cross-partition tasks):** key the **source topic** by `task_id` so a whole task
  (all its threads) is local to one partition. `key` = task_id; `thread_id` = generated column.
  Do NOT re-partition inside KSQLite. Watch partition skew.
- **[LOCKED] C3 (migration idempotency):** generated-column migration diffs
  `PRAGMA table_info(records)` and only `ADD COLUMN` the missing ones (VIRTUAL; columns before
  their indexes). Verified on 3.45.3: naive re-`ALTER` → `duplicate column name`; no
  `ADD COLUMN IF NOT EXISTS`. *(Superseded by round 2: the diff must use **`PRAGMA table_xinfo`** —
  generated columns are invisible to `table_info`.)*
- **[LOCKED] Idempotency / dedup:** KSQLite **generates a uuidv7 `message_id` per `append()`**,
  carried in **Kafka record headers** (alongside a `format_version`), used as the dedup key via
  **`ON CONFLICT (message_id) DO NOTHING`**. Dedups producer retries + rehydrate re-reads
  (NOT host double-appends — host's job if needed). Enforce **unique `message_id`**. Lets us keep
  `acks=1` / idempotence-off (Tansu-compatible). Replaces blunt `INSERT OR IGNORE`.
- **[LOCKED] Seek:** `seek_to_beginning(tp)` for full replay; incremental `seek(checkpoint+1)`
  wrapped in `OffsetOutOfRangeError` → reset to earliest (best-effort; no raise on truncated log).

## Batch 1 — operational contracts
1. **[LOCKED] Error taxonomy:** `KSQLiteError` base → `ConfigError`, `SQLiteVersionError`,
   `SchemaMigrationError`, `ChangelogProduceError`, `TopicNotFoundError`, `RehydrateError`,
   `PartitionNotReadyError`. Wrap aiokafka/aiosqlite at the boundary.
2. **[LOCKED] Lifecycle:** `start()` ordered with rollback-on-partial-failure; second `start()`
   raises. `stop()` = happy-path: drain in-flight appends → flush producer → close producer,
   rehydrate consumer, writer, reader pool. **Async CM supported.** Post-`stop()` calls raise.
   *(Superseded by Batch 3: uniform pool — no writer/reader split to close.)*
3. **[LOCKED] Configs:** `producer_config` AND `consumer_config`, both **pass-through** dicts
   merged over sane defaults (arbitrary aiokafka kwargs).
4. **[LOCKED] Config validation (fail-fast at `start()`):** require `{source_topic}` and
   `{partition}` in template; validate topic names; bound `pool_size≥1`, `read_wait_timeout>0`;
   reject `acks=0`. (Precondition assertion, not ops-policing.)
5. **[REC] query() / DX:** store payload as **JSON text** (via `json(?)`, validated) for effortless
   hydration; `query(sql, params) -> list[Row]` (Row=dict-like) + typed
   `query(sql, params, into=Model) -> list[Model]` (duck-typed: Pydantic `model_validate_json`,
   else dataclass `**json.loads`). No Pydantic dep in KSQLite. *(Reverses the JSONB storage choice —
   confirm.)*
6. **[LOCKED] Observability:** `partition_states()` (per-TP: state, checkpoint offset, lag) +
   structured logs (rehydrate start/end/force-stop, produce failure, migration). Metrics = v2.
7. **[LOCKED] Wire format:** Kafka **headers** carry `{format_version, message_id}`; value = **pure
   host payload** (no JSON envelope wrapping the payload).

## Earlier-resolved (this review)
- **[LOCKED]** Rehydrate **blocks** in `on_partitions_assigned`, bounded by a **force-stop timeout**
  (< `max.poll.interval.ms`) → serve partial state, keep checkpoint at contiguous frontier, log.
- **[LOCKED]** Read gate is **per-partition** (each TP owns RESTORING/READY); **skip already-ready**
  partitions on assign (works under eager and cooperative assignors).
- **[LOCKED]** No generation-fencing/cancellation needed (blocking + one-owner-per-partition;
  Kafka guarantees one consumer per partition).
- **[SUPERSEDED by Batch 3]** ~~Writer = single connection + asyncio writer-lock~~ → replaced by a
  single **uniform read/write pool** (Design B). Batch rehydrate inserts for write latency still applies.

## Batch 2 — wrong-default majors + hardening
1. **[LOCKED] Changelog topology:** KEEP one single-partition changelog topic per source partition
   (co-partitioning shelved; reversibility caveat accepted).
2. **[LOCKED] Compact precondition check:** at `start()`, read the changelog topic's
   `cleanup.policy` via admin `describe_configs`; **raise `ConfigError` if it contains `compact`**.
   *(Revised in round 3: runs on each partition's **first assignment**, not at `start()` — changelog
   names are unknowable at `start()`.)*
   **Opt-out-able** via ctor flag. Docs must include remediation — verified (Kafka/Confluent docs):
   `cleanup.policy` is a dynamically-alterable topic config
   (`kafka-configs.sh --alter --add-config cleanup.policy=delete`, or AdminClient
   `incrementalAlterConfigs`). **Caveat:** compact→delete stops future compaction but does NOT
   restore already-collapsed records — rebuild from source or accept loss.
   (Doc-verified, not live-broker-tested; live proof available on request.)
3. **[LOCKED] `use_pysqlite3()`:** KEEP, but in a **zero-import leaf module**, and it **raises if
   aiosqlite is already imported** (called too late). It performs the swap; the *guarantee* is the
   separate `start()` version check. *(Revised in plan-review round, **owner-confirmed**: helper +
   `sqlite_compat` module **DROPPED** — the package `__init__` import chain always trips the
   too-late guard unless the whole package goes lazy-import (PEP 562 + subprocess tests: real
   machinery for a 2-line documented snippet), and `pysqlite3-binary` ships linux/x86_64-only
   wheels. v1 keeps only the `start()` ≥3.38 fail-fast; the entrypoint swap is README-documented.)*
4. **[LOCKED] DDL escaping:** validate/escape `GeneratedColumn.name` (identifier charset) and
   `json_path` (string-literal escaping); fail-fast `ConfigError` on unsafe input.

## Batch 3 — API refinements + concurrency model
- **[LOCKED] Concurrency model = Design B (uniform pool):** one `aiosqlitepool` where every
  connection reads *and* writes; WAL + `busy_timeout`; **no dedicated writer, no writer-lock, no
  `query_only`.** Chosen after benchmarking (5 trials, 1KB payloads, 4/16/48 writers): B wins
  mean/median/throughput and is simpler; A's only edge is bounded tail latency under *high concurrent
  write* load, which KSQLite's sequential-append pattern won't hit. **Supersedes** the earlier
  single-writer / reader-split / E / D items.
- **[LOCKED] A:** ship ONE pool adapter (aiosqlitepool) + the `ConnectionPool` Protocol; defer the
  zero-dep built-in adapter. *(Owner note, recorded from the design discussion: v1 depends on
  upstream `aiosqlitepool`; the owner intends to later publish a maintained fork and switch the
  dependency to it.)*
- **[LOCKED] B (append):** keyword-only, group the TopicPartition —
  `append(source=TopicPartition, *, entity_key, payload)`.
- **[LOCKED] C (rebalance):** primary = host calls `store.on_partitions_assigned/revoked` from its
  OWN `ConsumerRebalanceListener` (store hooks into host's seam); document ordering. (Store-owned
  composable listener rejected — inverts ownership.)
- **[LOCKED] D:** dropped (single `Connection` protocol; no reader/writer split).
- **[LOCKED] E:** dropped (uniform pool → no read-only reader enforcement needed). *(Revised in
  round 3: reinstated **per-query** via `PRAGMA query_only` — the view alone did not enforce
  read-only; benchmarked ≈ +55–60 µs/read, tails unchanged.)*
- **[LOCKED] F:** `entity_key` (renamed from `key`).
- **Nits:** partition_checkpoint kept (contiguous resume frontier); pragmas = one set (WAL +
  busy_timeout, drop `foreign_keys`); domain model (messages→threads→tasks) → *example*, spec stays
  entity-agnostic; §3 acknowledge admin client (4th Kafka client); §15 add pre-changelog (at-most-once)
  loss window; checkpoint PK `NOT NULL`; `send_and_wait` → `.offset`; version check via
  `sqlite_version_info` tuple; `GeneratedColumn`/`Index` DSL kept-not-grown; `create_topics` knobs =
  documented dev-convenience exception.

## Resolved-final
- **[LOCKED] #5 payload format = JSON text** (stored via `json(?)`, validated). Chosen for
  simplicity + correctness + decent perf. Benchmark (50k×, 2.5KB nested payload): JSONB is
  faster/smaller on writes (~20–31%), storage (~13%), field-extraction (~1.6–1.8×) and scans (~5×);
  JSON text is faster only on full-payload hydration (~1.6×). All gaps are microseconds — non-issue
  at KSQLite scale. Deciding factor: JSON text **removes the `pysqlite3`/version machinery**
  (generated columns via `->>` need only SQLite **3.38**; `json_extract` needs 3.9). Hydration cost
  of either is ~1 line (`json()` wrap for JSONB), so DX was not the differentiator.
  **Consequence:** drop hard `pysqlite3-binary` dep; light `sqlite_version_info >= (3,38)` fail-fast;
  `pysqlite3` = optional documented fallback only.
- **[LOCKED] C1 contiguity fix:** implement as cheap insurance; drop if non-trivial.

**Round-1 items resolved.** Comprehensive `DESIGN.md` revision done; 2nd review round run — see below.

---

## Round 2 review — resolutions applied (DESIGN.md round-3)
Second deep review (5 opus agents) against the revised spec. Fixes applied + folded into DESIGN.md:
- **[CRITICAL] `entity_key` on the wire:** changelog record **key = `entity_key`**; recovered as
  `record.key` on rehydrate. (§6/§7/§10)
- **[CRITICAL] migration idempotency:** use **`PRAGMA table_xinfo`** (generated columns are invisible
  to `table_info`; empirically proven idempotent). (§5.1)
- **[CRITICAL / decision-flip] C1 dropped → `MAX(offset)` watermark.** Contiguity pins forever under
  `acks=1`/retries (returned-offset stream has gaps/reorders); watermark never stalls, its gap = the
  accepted best-effort loss. (§5.2/§6/§13/§18)
- **[HIGH] read gate:** `query(..., wait_for=TopicPartition|None)` — precise gate when scoped, coarse
  (block if any owned partition RESTORING) when not; `PartitionNotOwnedError` for standby. (§4/§9)
- **[HIGH] uniform-pool writes:** `BEGIN IMMEDIATE` (avoids `SQLITE_BUSY_SNAPSHOT`); write-only
  checkpoint upsert; bounded `SQLITE_BUSY` retry → `StorageError`; `busy_timeout` config. (§6/§11)
- **[MAJOR] drift detection:** via `sqlite_schema.sql` (expression not in pragmas); index drift via
  `index_xinfo`; removal not automatic. (§5.1)
- **Restored dropped-LOCKED:** `partition_states()` observability + structured logs; full config
  validation (template placeholders, `pool_size`, timeouts, reject `acks=0`). (§4)
- **Error taxonomy:** added `StorageError`, `PartitionNotOwnedError`; specified raise sites. (§4)
- **State model:** 3-state `{RESTORING, READY/READY_PARTIAL, STANDBY}`; force-stop → `READY_PARTIAL` +
  lag, no in-assignment forward-progress guarantee. (§3/§7/§8/§14)
- **Lifecycle:** start admin before verify; rehydrate consumer lazy. (§4)
- **Version story:** dropped dead `json_extract` fallback; `pysqlite3` = sole optional fallback;
  `use_pysqlite3()` leaf-module helper. (§12)
- **Schema:** `records` → rowid table + `UNIQUE(message_id)` (avoids 36-char PK in every index). (§5.1)
- **Kafka:** remediation names aiokafka `alter_configs` (not Java `incrementalAlterConfigs`);
  `verify_changelog_policy` warns+proceeds on missing `DESCRIBE_CONFIGS` ACL. (§10)
- **§2.11 added:** appends per source-partition are in offset order.
- Nits: `entity_key` = routing/grouping key (not dedup); `create_topics` folds retention; `into=`
  requires a `payload` column (clear error); host-retry duplicate + non-canonical `changelog_offset`
  documented (§14); `format_version`/missing-header handling (§7).

Verified: grep pass confirms round-2 fixes present and no LOCKED decision dropped. Empirical proofs
this round: `table_xinfo` migration idempotency, `cleanup.policy` dynamic alterability (doc-verified),
uniform-pool and json-format benchmarks.

---

## Implementation-plan review — resolutions applied (spec round-5 amendments + plan round-2)
Five agents (spec-fidelity, test-adequacy, API-feasibility, eng-quality, E2E-realism); heavy empirical
verification (aiokafka 0.13/0.14 source + live Redpanda v24.2 broker probes). Owner decisions:
**`kafka_clients=` injection seam adopted** (cleanest fix vs monkeypatching/test-only hooks; mirrors
`pool=`); **clamp-up after zero-record reset adopted**; **`use_pysqlite3()` + `sqlite_compat`
DROPPED** (see Batch 2 #3 annotation). Spec amendments (all folded into DESIGN.md):
- **[CRITICAL] `auto_offset_reset="none"`** = third non-overridable rehydrate-consumer key — without
  it aiokafka *silently resets to latest* on truncation (never raises OOR; raises from fetch, not
  seek; live-verified) → §7's truncation handling would be dead code and truncation = silent total
  loss. (§7 step 2, §16)
- **[HIGH] Step 0 unconditional** on first assignment, before the fast path (warm restart must not
  skip the compact/exists checks). (§7)
- **[HIGH] Offset conventions pinned:** checkpoint = last materialized offset; `end_offsets` = LEO =
  last+1; fast path `checkpoint+1 == LEO`; clamp-down to `LEO−1`; **clamp-up to `log_start−1`** after
  a zero-record reset. (§5.2/§7)
- **[HIGH] Payload serialized+validated pre-produce** (`ValueError`; `json(?)` = defense-in-depth) —
  else an unmaterializable payload with valid headers poisons every future replay. (§6)
- **[HIGH] Truncation-reset + clamps added to observability** (WARNING, operator-actionable). (§4)
- **[HIGH] `kafka_clients=` seam** added to §4 (LOCKED-API additive, owner-approved).
- Foreign-record rules: null/undecodable **value** = foreign-skip; headers looked up by name
  (order-insensitive, extras ignored); **missing** `format_version` w/ valid `message_id` = fail-loud;
  **foreign skips advance the checkpoint** (fast path preserved). (§7)
- `LifecycleError` added; `StorageError` covers `start()` DB-open; system-column collision +
  `Index`-column cross-validation in config validation; `into=` targets pinned (Pydantic-v2 |
  dataclass, no kwargs fallback); `partition_states()` scope = this-process-lifetime, `{}` on fresh
  process, write-paths push offsets in-memory; `message_id` wire encoding = 36-char UTF-8;
  `sqlite_schema` view-`r.*`-re-expansion noted as load-bearing; §16 assignor note corrected —
  aiokafka has **no cooperative assignor** (recommendation was unimplementable as phrased).
- Plan round-2 (IMPLEMENTATION-PLAN.md rewritten): fake-fidelity corrections (headers `(str,bytes)`,
  OOR-from-fetch-iff-`none`, `end_offsets`=LEO, `getmany` dict, `.resources`-shaped admin responses)
  + fake↔real **conformance suite** + required `-m e2e` CI gate; `RecordingPool`/event-gate/injectable-
  clock seams (no test hooks in src/); service-class decomposition + composition-root `_store`;
  `partition_states` write-back API; ~20 new mutant-killer tests (cross-topic scoping, dup-id-two-
  offsets, boundary offsets, clamp-not-before-replay, per-TP verify, verify=False, RH-22 watermark-gap
  semantics, warm-restart-intact-DB, defaults snapshot, config passthrough, host-retry dup); E2E
  redesigns (08a/08b split — clamp unreachable via trim, needs topic recreate; 03 via structured-log
  assertions; 07 budget≪batch determinism; 09 tuned timeouts; 12→profile; +13 broker restart;
  accepted-untestable block; Redpanda image pin ≥ v24.x via `testcontainers.kafka`). V1–V5 all
  resolved empirically (aiokafka 0.14: full admin surface incl. `delete_records`; no cooperative
  assignor; aiosqlitepool 1.0.0 verified incl. rollback-on-release; `RedpandaContainer` in
  `testcontainers.kafka`).

### Round-2 plan review — final amendment set (spec round-5 final + plan round-3)
Four agents (fold-audit, test-adequacy, eng-quality, feasibility+E2E; live-broker probes). Fold-audit:
**converged, zero regressions** (offset arithmetic coherent across §5.2/§7/RH tests; historic
dropped-content class did not recur). Remaining findings — zero criticals — folded:
- **[HIGH, live-verified] Replay termination = position reaches captured LEO**; empty `getmany` is
  NOT termination (real brokers return `{}` mid-log) — spec §7 + FakeConsumer spurious-empty
  injectable + RH-27.
- **[HIGH, live-verified] NaN/Infinity payloads**: `json.dumps(allow_nan=False)` + raising
  `parse_constant` (SQLite ≥3.42 silently coerces `NaN`→`null`; <3.42 poisons replay) — §6 + WF-05b.
- **[HIGH] `KafkaClientFactory` contract pinned**: sync methods, fully-merged final kwargs (merge
  lives above the seam), constructed-but-unstarted clients; F-13 asserts pinned keys present WITH
  values (aiokafka defaults are the catastrophic values).
- **[HIGH, live-verified] E2E-13**: dedicated fixed-port container (`with_bind_ports`) — session
  container cannot restart (Docker remaps port + Redpanda re-advertises the stale one).
- **[HIGH] `record_clamped()`** added as the sole non-monotonic state write-back (`record_applied` =
  monotonic-MAX) — in-memory checkpoint can never silently diverge post-clamp; ST-07/08.
- §7 restructured: step 0 hoisted above the fast path (numbered list no longer nests it under
  "Otherwise"); step 0 budget-exempt; ACL warn+proceed cached-as-attempted (warn once per TP per
  store lifetime), failures never cached; step-0 broker failures → `RehydrateError`;
  `TopicAlreadyExists` on create = success; mid-assignment error leaves remaining partitions hidden.
- Taxonomy boundary mappings (pool → `StorageError`; mid-replay wrap → `RehydrateError` chained;
  client-start failure → base `KSQLiteError`); builtin-vs-`ConfigError` family rule; stop()-drain
  atomicity pinned (sync check+increment, finally-decrement, Event; appends-only; best-effort close;
  double-stop no-op); admin client gets the security subset of `consumer_config`; `_write` depends on
  a `ChangelogNameResolver` protocol (P6 precedes P8); deadline-check placement pinned (batch always
  committed before check; first partition always commits ≥1 batch); `PartitionState` frozen dataclass
  + exported `PartitionStatus`; STANDBY = seen-this-lifetime ∧ not-owned; lag conventions; `_owned`
  reveal = INSERT OR IGNORE; conformance suite got IDs CF-01…07 + phase homes (fixes its ungated
  status); fakes: `max_records` honored, dup-metadata = last offset, `partition=` accepted, strict
  kwargs; two-sided gates everywhere; RecordingPool capability (d) gated-execute; e2e job
  `--timeout=180` + image pre-pull; E2E-08a out-of-band-tail choreography + WARNING assertion;
  E2E-01 payload ~900 KB (1 MiB `max_request_size`); L-03/L-04 reused for type/protocol surface
  (trail added); new tests: ST-07/08, WF-05b, T-04b, T-09, RH-20c, RH-27, RH-28 + extensions
  (RH-06 both-shapes, RH-14 state-coherence + error assert, RH-15 step-0-exempt, RH-23 fetch-offset,
  RH-25 two cut points, F-03 exception types, F-06 double-stop, F-10 two more events).

**Fold verified: PASS** (focused verification agent — all amendments landed as coherent normative
text, no LOCKED damage, cross-document contracts agree; its 5 cosmetic fixes applied: round labels,
em-dash spacing, §6-catalog payload ref, `PartitionStatus` in `__all__`, §7-step-1 READY_PARTIAL
re-assign qualification, plus the §5.2 "processed offset" wording precision).
**CONVERGENCE DECLARED:** design round 3 clean → plan round 1 (2 crit) → round 2 (0 crit) → amendment
fold verified. Spec round-5 + plan round-3 are implementation-ready. Next: Phase 0 (owner to choose
worktree vs local, per CLAUDE.md).

### Post-round-2 refinement — view-based read scoping (owner-proposed)
Replaced the round-2 read-gate approach (`wait_for` primary + coarse fallback) with **view-based
auto-scoping**: an `_owned` table (KSQLite-maintained — assign→`ready=0`, rehydrate-done/force-stop→
`ready=1`, revoke→`DELETE`) + a **`records` view** = `_records JOIN _owned WHERE ready=1`. Hosts run
arbitrary SQL on `records`; the view **structurally** excludes RESTORING (hidden until an atomic
`ready=1`) and STANDBY (hidden on revoke) partitions — no SQL parsing, no host hints, non-blocking, and
**read-only by construction**. `wait_for` demoted to an optional *block-until-ready* hint. Empirically
verified: index usage preserved through the view (`SEARCH … USING INDEX ix_thread`), correct
exclude/reveal across assign/rehydrate/revoke. Base table renamed `records → _records`; host reads the
`records` view. Folded into §3/§5/§7/§8/§9/§16/§18. *(Refined in round 3: `_owned` collapsed to
**presence-only** — no `ready` column; `wait_for` dropped to v2.)*

---

## Round 3 review — resolutions applied (DESIGN.md round-4)
Third deep review (5 opus agents, full-scope). No criticals; 3 HIGHs + mediums/minors, all folded.
Owner decisions this round: **enforce read-only via `query_only`** (benchmark-gated, accepted),
**drop `wait_for` machinery to v2**, **collapse `_owned` to presence-only**, **`partition_states()`
stays sync with documented-cached lag**.
- **[HIGH] Lazy topic ensure/verify:** verify/create moved from `start()` (impossible — ctor never
  learns `(topic,partition)` pairs) to **first assignment per partition** (§7 step 0); `start()` only
  starts the admin client; gives `TopicNotFoundError` its raise site. (§4/§7/§10/§15)
- **[HIGH] Read-only enforced, not claimed:** `query()` wraps execution in `PRAGMA query_only=ON/OFF`
  (empirically: blocks all writes incl. direct `_records` DML, toggles back cleanly). **Benchmarked**
  (aiosqlite pool, WAL, 50k rows): ≈ +55–60 µs/read sequential (117→177 µs mean), +20–28 % mean under
  concurrency, **tails unchanged** → accepted. Reads of `_`-internals remain a documented footgun.
  Bonus find: a failed write under `query_only` strands an implicit stdlib txn → connections open
  **autocommit (`isolation_level=None`)**, explicit `BEGIN IMMEDIATE` only. (§9/§11/§18)
- **[HIGH] Assignment-wide replay budget:** `rehydrate_timeout` = single wall-clock deadline for the
  entire `on_partitions_assigned` callback (per-partition budgets multiply past
  `max.poll.interval.ms` → rebalance storm), checked cooperatively **between batches** (never
  mid-txn cancellation); unreached partitions revealed `READY_PARTIAL` at kept-checkpoint. (§4/§7/§15)
- **[MED] `format_version` split:** missing/malformed `message_id` or bad key → skip+log (foreign
  record); **unrecognized `format_version` → `RehydrateError` (fail loud)** — never silent data loss
  on rolling upgrade/downgrade. v1 emits `format_version="1"`, KSQLite-owned. (§7/§10)
- **[MED] State model:** authoritative 4-valued state is **in-memory**; `_owned` = presence-only
  visibility projection; `partition_states()` reads the in-memory map; STANDBY = checkpoint ∧ not
  owned; `log_end_offset`/`lag` documented **cached as-of-last-rehydrate**. (§3/§4/§5.3)
- **[MED] `_owned` cleared on `start()`** added to the §5.1 migration + §4 lifecycle (stale rows from
  a prior run must not serve un-owned partitions at boot).
- **[MED] Rehydrate hardening:** poll/accumulate **outside** the txn (write lock never spans a Kafka
  `poll()`); rehydrate consumer isolation (`group_id=None`, `enable_auto_commit=False`, not
  overridable); checkpoint **clamped to log-end** after truncated-log reset; `entity_key` validated at
  `append` (bad key would poison partition rehydrates). (§6/§7)
- **[MED] §3 client count fixed:** three KSQLite clients + the host's fourth.
- **Simplifications:** `wait_for`/`read_wait_timeout`/`PartitionNotReadyError`/`PartitionNotOwnedError`
  dropped to v2; `_owned.ready` column dropped (presence-only); `TopicCreation` type →
  `create_topics_retention_ms: int | None`; `HydrationError` replaces bare `KSQLiteError` for `into=`.
- **Minors:** §12 version-check phrasing un-inverted; `acks=0` returns `None` not `-1` (inert —
  rejected at start; guard covers the idempotent-producer `-1` edge); wire bytes (de)serialization
  specified; drift check compares **canonically re-rendered DDL** (no free-form parsing); uuidv7
  source named (stdlib ≥ 3.14, else `uuid6`); `describe_configs` response-walking + `compact,delete`
  substring match; "write-only statements" precision; `StorageError`-after-produce-ack documented
  self-healing ("do not retry"); `_records.changelog_offset` kept as named debug/ops provenance;
  bounded `SQLITE_BUSY` retry pinned (3 attempts, ≈ 2× `busy_timeout`); cooperative-sticky assignor
  recommended; append-to-non-READY = logged contract violation.

---

## Implementation-handoff clarifications (pre-Phase-0, 2026-07-13)
Raised by the implementing engineer after a full review of spec round-5 + plan round-3. Items with
exactly one defensible answer were folded on owner instruction; the two genuinely contestable ones
went to the owner. **Owner resolution (2026-07-13): items 1–11 confirmed as folded (no vetoes);
12 = signature introspection (option B); 13 = new worktree off the latest `origin/main`.** All
items below are now **[LOCKED]**. Patches applied on lock: DESIGN.md §3 (admin-subset mechanism)
and §4 (`sql_type`/`Index.name` validation); plan P2 (+S-09/S-10), F-01, and the `_store.py` note.

1. **[LOCKED] CI = GitHub Actions, authored as plan deliverables.** Unit job at P0 (linux+macos
   matrix per the plan's risk table; coverage flags live in its invocation — the plan forbids
   `addopts`), e2e job added at P8 (pinned-image pre-pull, `--timeout=180`, required gate from P8
   per §1). The plan's gates have no home without CI config, and the remote is GitHub.
2. **[LOCKED] Redpanda e2e image pinned exactly
   `docker.redpanda.com/redpandadata/redpanda:v24.2.20`** — the version the plan review's live
   probes verified; any other pin abandons the verified baseline. Recorded at P0 as a named
   constant in the e2e conftest (its sole consumer).
3. **[LOCKED] `GeneratedColumn.sql_type` is validated** (new test **S-09**): same identifier-charset
   rule as `name`; `ConfigError` on failure. Closes the one DDL-interpolated input the spec left
   unvalidated. Scoped to safety, consistent with the spec's validation philosophy (`name` charset,
   `json_path` escaping) — no type-vocabulary allowlist, so single-token affinity names like
   `BOOLEAN` stay legal. M-07 drift semantics unchanged.
4. **[LOCKED] `Index.name` validated by the same identifier rule** (new test **S-10**; `ConfigError`)
   — it is interpolated into `CREATE INDEX` exactly as column names are into `ALTER TABLE`.
5. **[LOCKED] `append` is fully keyword-only** — `async def append(*, source, entity_key, payload)`.
   Spec §4 is normative ("keyword-only") and its example passes `source=`; the Batch-3 notation was
   informal shorthand. Spec-wins rule.
6. **[LOCKED] `query()` `params` passes through to sqlite3 unmodified** — `Sequence` (`?`
   placeholders) or `Mapping` (`:name`) both legal; rejecting mappings would take deliberate extra
   code for no benefit. Typed `Sequence[Any] | Mapping[str, Any]`, default `()`.
7. **[LOCKED] `append` produces with explicit `partition=0`** — changelogs are single-partition by
   construction and rehydrate assigns partition 0; explicit targeting makes write/read symmetry
   structural (a mis-provisioned multi-partition changelog cannot scatter records beyond replay's
   reach) and matches the pinned fake/CF-01 surface (`partition=` accepted).
8. **[LOCKED] `str` payloads are produced as-is** (validated by parse, then the original string
   UTF-8-encoded) — §10 pins "value = **pure host payload**"; re-serializing would rewrite host
   bytes for zero correctness gain (`_records` convergence comes from `json(?)` normalization
   regardless). Object payloads are, as already specified, the `json.dumps(..., allow_nan=False)`
   output.
9. **[LOCKED] Structured-log event names are chosen during implementation** under the pinned §3
   convention (stable names, 1:1 with the spec-§4 observability list), reviewed through the
   table-driven F-10 — no upfront registry; v1 has no external log consumers to coordinate with.
10. **[LOCKED] mypy runs `strict = true`** — greenfield library shipping `py.typed`; the plan's "mypy
    clean" gate under default laxity would skip untyped internals. Ruff gates exactly as
    plan-written (`ruff check` + `ruff format --check`, default rules); rule-set extensions remain
    owner preference, addable anytime, non-blocking.
11. **[LOCKED] pyproject metadata from repo facts** — name `ksqlite`, version `0.1.0`, MIT (LICENSE
    file), author `calf ai` (LICENSE copyright), `requires-python = ">=3.10"` (plan §2),
    description = the spec's opening line. Publishing/PyPI out of v1 scope.
12. **[LOCKED, owner-picked] Admin-client security subset via signature introspection** (option B):
    pass the `consumer_config` keys matching `security_protocol`/`sasl_*`/`ssl_context` ∩ the
    parameter names of `inspect.signature(AIOKafkaAdminClient.__init__)`. Implements §3's "filtered
    to its accepted params" literally and cannot drift when the host's resolved aiokafka version
    moves (the dependency is a floor, `>=0.11`, not an exact pin). Rejected alternative: a hardcoded
    allowlist — a frozen copy of another library's signature, the drift class this design rejects
    elsewhere. F-13 asserts the received keys.
13. **[LOCKED, owner-picked] Implementation runs in a new worktree** branched off the latest
    `origin/main`. Note: `origin/main` contains only `LICENSE`; the design docs + project config
    (`docs/`, `CLAUDE.md`, `.agents/`, `.claude/skills`, `skills-lock.json`) were until now
    untracked in the primary checkout, so they land as the feature branch's bootstrap commit
    (`.claude/settings.local.json` stays untracked/ignored — local settings).

---

## Implementation-discovered findings (P3/P8, 2026-07-14)
Recorded per the plan's drift protocol ("never silently"). Neither changes a LOCKED decision;
both are implementation-level facts the spec text already accommodates.

1. **Retried `BEGIN IMMEDIATE` can spuriously succeed without the write lock** (observed live:
   macOS, SQLite 3.45.3, WAL, two same-process connections; a retry issued microseconds after a
   busy failure "succeeds" and the transaction's first write then raises busy — adding
   microseconds of delay masks it). Resolution: the bounded SQLITE_BUSY retry in
   `_pool.TransactionRunner` covers the WHOLE attempt (BEGIN + work + commit), not just BEGIN —
   `work` only ever re-runs after a full rollback, so nothing partial persists. Spec §11's
   wording ("on a busy_timeout-exceeded SQLITE_BUSY ... append applies a bounded retry") already
   describes the retry at attempt granularity; no spec change. Pinned by C-04/C-05.
2. **Replay-loop termination refined to `position != LEO`** (not `position < LEO`): a stale-HIGH
   checkpoint after a log-end regression must still fetch so the broker answers with
   OffsetOutOfRange, which is what routes it into the §7 reset + clamp-down path (real Kafka
   raises OOR for fetch offsets above the log end as well as below the log start — the fake
   mirrors this, contract-tested). Consistent with §7's "replay terminates when the consumer
   position reaches the LEO"; pinned by RH-10 and E2E-08b.

## 2026-07-14 — Post-v1 deep-review fixes (owner-approved batch)

An 8-agent deep review of the merged v1 surfaced 2 critical and ~9 important findings; the owner
approved this fix batch explicitly (per-finding dispositions below). All fixes landed TDD-first on
branch `worktree-deep-review-fixes`.

1. **[LOCKED] `create_topics` response codes are walked** (spec §7 step 0 amended): aiokafka
   returns per-topic outcomes as `topic_errors` codes and never raises them (verified against the
   installed 0.14 source; its `create_partitions` does the equivalent walk). Code 36
   (TopicAlreadyExists) = concurrent-create success; any other non-zero code fails step 0 via
   `for_code(code)` → `RehydrateError` wrap, un-cached. The fake was corrected to return codes
   (not raise), and CF-08 pins the shape against fake AND real clients.
2. **[LOCKED — owner] Compact-policy check is best-effort**: a `cleanup.policy` containing
   `compact` is reported loudly (ERROR, event `compact_policy_detected`) and the assignment
   PROCEEDS — no more `ConfigError` (spec §10/§7 step 0, README, `ConfigError` docstring all
   amended; T-03/T-05b/T-07/T-08/F-10/E2E-05 re-pinned). Rationale (owner): topic config is
   ops-owned; the check is advisory.
3. **[LOCKED] `describe_configs` per-resource error codes are checked first**: aiokafka reports
   failures (authz denials included) as per-resource codes with empty entries — the old
   `except TopicAuthorizationFailedError` was dead against the real client and a describe error
   was logged as "verified". Non-zero code → warn + proceed (`policy_verify_unauthorized` for
   codes 29/31, else `policy_verify_failed`), never `topic_verified`. Fake corrected to inject
   codes; the raise-hook remains for connection-level errors only.
4. **[LOCKED] Replay deadline is checked on every poll iteration** (spec §7 force-stop amended):
   the empty-fetch and truncation-reset paths previously skipped the check, so a degraded broker
   returning `{}` forever could hold the rebalance callback unboundedly — the exact storm the
   budget exists to prevent. A fetched batch is still always materialized + committed before the
   check (RH-31/RH-32).
5. **[LOCKED] Fast-path rehydrate seeds the in-memory checkpoint** from the persisted row
   (RH-29): a warm restart previously reported `checkpoint_offset=None` / `lag=LEO` for a fully
   caught-up partition, indefinitely.
6. **[LOCKED] A failed rehydrate-consumer `start()` is not cached** (RH-30): the dead client is
   best-effort-stopped and the next assignment builds a fresh one (previously one transient broker
   blip wedged every later rehydrate).
7. **[LOCKED] Lifecycle single-statement writes route through `TransactionRunner`** (revoke
   DELETE, reveal INSERT, checkpoint clamp — RH-33/34/35): uniform busy retry + `StorageError`
   mapping; the checkpoint SELECT maps `sqlite3.Error → StorageError` directly (no write lock for
   reads). The clamp SQL moved next to `CHECKPOINT_UPSERT_SQL` in `_schema` as
   `CHECKPOINT_CLAMP_SQL`. The assignment hook's boundary try now covers step 0 and the
   unreached-partition path too. RH-12's commit count is 3 batch + 1 reveal.
8. **[LOCKED] Pool-layer failures map to `StorageError` in the adapter** (C-11):
   `PoolConnectionAcquireTimeoutError`/`PoolClosedError` are plain Exceptions with EMPTY `str()`
   (text lives in `.message`) raised on checkout `__aenter__` — previously they bypassed the
   documented taxonomy entirely.
9. **[LOCKED] `open_connection` verifies WAL actually engaged** (C-01d): `PRAGMA
   journal_mode=WAL` returns the RESULTING mode and never raises (empirically: file → `wal`,
   `:memory:` → `memory`); a silent rollback-journal fallback breaks the uniform-pool concurrency
   model with zero signal → now `StorageError`.
10. **[LOCKED] `stop()` is race-free and loud** (F-14/F-15): the facade's boolean lifecycle flags
    were replaced by a private `_RunState` enum (NEW/RUNNING/STOPPING/CLOSED); STOPPING is set
    synchronously before the drain await (no double-close), and every failing close step is
    logged (`stop_step_failed`) with the first exception still re-raised.
11. **[LOCKED] `start()` rollback closes the admin client too and logs suppressed cleanup
    failures** (`start_rollback_close_failed`; F-03 case 4 + F-03b).
12. **[LOCKED — owner] Zombie appends stay warn-not-raise**: spec §6 wording tightened instead —
    convergence is guaranteed only for the sole current owner; a post-revoke append can strand a
    record the new owner never re-reads. Hosts must stop appending once the revoke hook has run.
13. **[LOCKED] Type/API tightening**: `kafka_clients`/`pool` ctor params typed with the exported
    Protocols; `query()` gains `@overload`s (`into=None → list[sqlite3.Row]`, `into=T → list[T]`);
    `Index.columns`/`EncodedRecord.headers` snapshot to tuples in `__post_init__` (frozen means
    frozen; L-02b/WF-01); `query()` resets `row_factory` on release (R-12);
    `changelog_topic_template` rejects unknown placeholders/malformed braces at `start()` (S-07).
14. **[LOCKED] Statement helpers** `_pool.execute/fetch_one/fetch_all` centralize the
    close-the-cursor-immediately rule (~23 sites); `apply_materialization` feeds one params dict
    to both statements (sqlite3 ignores unreferenced named parameters — verified empirically).
    NOTE: aiosqlite's native `async with conn.execute(...)` idiom was deliberately NOT adopted —
    the test suite's `RecordingConnection.execute` is a plain coroutine, so the CM idiom would
    break every RecordingPool-proxied path.
15. **[LOCKED — owner, same day] Replay's per-record checkpoint upserts collapsed to one per
    batch** (was [REC]: N+1 → 1 MAX upserts per batch; end state provably identical). Owner
    approved the spec amendment: §7 step 3 now reads "the MAX checkpoint upsert runs once per
    committed batch, inside the same transaction as the batch's inserts". The dedup-equivalence
    framing (I2/I4) rests on the byte-identical INSERT, which write and replay still share; the
    batch-level upsert already existed and remains what advances the checkpoint past
    skipped-foreign offsets; the atomicity invariant (I7) holds — inserts + upsert commit in one
    transaction. Write path unchanged. Pinned by RH-37 (statement count + end-state equivalence);
    RH-07/RH-08/W-15 continue to pin the round-trip equivalence.
16. **Skipped with rationale — busy-detection via `sqlite_errorcode`**: the attribute and the
    `sqlite3.SQLITE_*` result-code constants do not exist on Python 3.10 (project floor), and
    `sqlite_errorcode` returns EXTENDED codes (517 for BUSY_SNAPSHOT), so a naive `== 5` check
    would regress; the current message-substring check is 3.10-safe and handles extended codes.
17. **Skipped with rationale — `PartitionStateMachine` rename/transition-guard**: the spec's
    vocabulary is "state machine" throughout; renaming would create doc-code drift, and edge
    enforcement risks mis-modeled transitions. A docstring note now states that edges are
    enforced by `_lifecycle`'s choreography.
18. **[LOCKED — owner] CI matrix is floor + latest**: py3.10 + py3.14 (3.13 dropped; e2e job on
    3.14). 3.14 exercises the stdlib-uuid7 branch; `test_ids.py` was restructured so the uuid6
    backport half skips where the backport isn't installed.
