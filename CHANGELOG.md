# Changelog

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
