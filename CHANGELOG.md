# Changelog

## Unreleased

Deep-review fix batch (see `docs/DESIGN-REVIEW-DECISIONS.md`, 2026-07-14
section, for per-finding rationale).

Fixed:

- `create_topics` per-topic error codes are now inspected (aiokafka returns
  them on the response and never raises them): a failed changelog creation
  fails the assignment loudly instead of being silently cached as success;
  losing a concurrent-create race (code 36) still counts as success.
- `describe_configs` per-resource error codes are now checked: an
  unverifiable topic (authorization denial included) warns and proceeds
  instead of being falsely logged as verified.
- The replay budget (`rehydrate_timeout`) is enforced on every poll
  iteration — empty fetches and truncation resets can no longer hold the
  rebalance callback past the deadline.
- The zero-fetch fast path seeds the in-memory checkpoint, so a warm restart
  reports the true `checkpoint_offset`/`lag` instead of `None`/`LEO`.
- A failed rehydrate-consumer `start()` no longer poisons every later
  rehydrate; the dead client is stopped and rebuilt on the next assignment.
- Revoke/reveal/clamp writes run through the `BEGIN IMMEDIATE` transaction
  runner (bounded busy retry + `StorageError` mapping); the checkpoint read
  maps `sqlite3.Error` to `StorageError` — raw sqlite3 errors no longer
  escape the rebalance hooks.
- Pool-layer failures (acquisition timeout, closed pool) surface as the
  documented `StorageError`.
- `open_connection` verifies WAL actually engaged and fails loud on a silent
  journal-mode fallback.
- Concurrent `stop()` calls can no longer double-close the clients; failing
  close steps are logged (`stop_step_failed`) and the first failure is still
  re-raised; the `start()` rollback also closes the admin client and logs
  suppressed cleanup failures.
- `changelog_topic_template` rejects unknown placeholders and malformed
  braces at `start()`.
- `query()` resets `row_factory` on connection release;
  `Index.columns`/`EncodedRecord.headers` snapshot to tuples at construction.

Changed:

- **Compact-policy handling is now best-effort**: a compact changelog is
  reported loudly (`compact_policy_detected`, ERROR) and the assignment
  proceeds; `ConfigError` is no longer raised for it.
- Non-`READY` appends: spec §6 now states convergence holds only for the
  sole current owner (no code change; hosts must stop appending after
  revoke).
- Public ctor seams are Protocol-typed (`KafkaClientFactory`,
  `ConnectionPool`); `query()` gains typed `@overload`s (`into=` returns
  `list[T]`).
- CI matrix is Python 3.10 + 3.14 (floor + latest; the 3.14 leg exercises
  the stdlib-`uuid7` branch).
- Replay batches the checkpoint upsert: one `MAX()` upsert per committed
  batch instead of one per record plus one per batch (identical end state;
  the write path and the shared dedup `INSERT` are unchanged).

## 0.1.0 — 2026-07-14

Initial release, implementing `docs/DESIGN.md` (v1 spec, round-5) in full.

- `KSQLite` store facade: `start`/`stop` (drain + best-effort close), async
  context manager, keyword-only `append`, auto-scoped `query` with optional
  Pydantic-v2/dataclass hydration, synchronous `partition_states()`, and the
  `on_partitions_assigned`/`on_partitions_revoked` rebalance hooks.
- Per-partition changelog produce/replay with `message_id` (uuidv7) dedup,
  `MAX(offset)` watermark checkpoints, foreign-record skip, fail-loud
  `format_version` handling, truncation reset with the two checkpoint clamps,
  assignment-wide cooperative replay budget (`READY_PARTIAL`), and warm
  standby on revoke.
- Uniform aiosqlitepool connection layer: WAL, `BEGIN IMMEDIATE` transactions
  with bounded `SQLITE_BUSY` retry, per-query `PRAGMA query_only` read-only
  enforcement, checkout hygiene.
- Idempotent schema migration with generated-column/index drift detection;
  SQLite >= 3.38 fail-fast.
- Step-0 changelog ensure/verify per partition (auto-create for dev, compact
  policy rejection, ACL warn-and-proceed), lazily on first assignment.
- Structured logging under the `ksqlite.*` namespace with stable event names.
- Injection seams for testing: `pool=` (ConnectionPool) and `kafka_clients=`
  (KafkaClientFactory).
