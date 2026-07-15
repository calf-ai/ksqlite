# Logging

KSQLite logs through the standard `logging` module. The root logger `ksqlite` has
a `NullHandler` attached, so nothing is emitted until the host configures
logging.

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("ksqlite").setLevel(logging.INFO)
```

## Loggers

| Logger | Covers |
|---|---|
| `ksqlite` | Root. Attach handlers and levels here. |
| `ksqlite.store` | Lifecycle: `start()` rollback, `stop()` close steps. |
| `ksqlite.schema` | Schema migration. |
| `ksqlite.write` | The append path. |
| `ksqlite.lifecycle` | Rehydrate and the rebalance hooks. |
| `ksqlite.topics` | Changelog topic creation and policy verification. |

## Structured events

Every KSQLite log record carries an `event` field in its `extra` mapping, giving
each record a stable machine-readable name independent of its message text.

The `extra` fields land as attributes on the `LogRecord`, so a structured
formatter can read `record.event`, `record.tp`, and so on.

### `ksqlite.store`

| Event | Level | Meaning |
|---|---|---|
| `start_rollback_close_failed` | `WARNING` | A cleanup step failed while rolling back a failed `start()`. The original error still propagates. |
| `stop_step_failed` | `ERROR` | A close step failed during `stop()`. Remaining steps still run. |

### `ksqlite.schema`

| Event | Level | Meaning |
|---|---|---|
| `migration_ddl` | `INFO` | An `ALTER TABLE` or `CREATE INDEX` was executed. |
| `orphaned_column` | `INFO` | A generated column in the database is no longer declared. Left in place. |
| `orphaned_index` | `INFO` | An index in the database is no longer declared. Left in place. |

### `ksqlite.write`

| Event | Level | Meaning |
|---|---|---|
| `append_to_non_ready` | `WARNING` | `append()` targeted a partition that is not `READY`. |
| `produce_failure` | `WARNING` | The changelog produce failed, or acked without a usable offset. Accompanies `ChangelogProduceError`. |

### `ksqlite.lifecycle`

| Event | Level | Meaning |
|---|---|---|
| `rehydrate_start` | `INFO` | Replay began. Carries `checkpoint` and `log_end_offset`. |
| `rehydrate_end` | `INFO` | Replay finished. Carries `records_replayed`. |
| `rehydrate_force_stop` | `INFO` | `rehydrate_timeout` expired. The partition was revealed `READY_PARTIAL`. |
| `foreign_record_skipped` | `INFO` | A non-KSQLite record was skipped. Carries `offset` and `reason`. |
| `truncation_reset` | `WARNING` | The checkpoint fell below the changelog log start; replay reset to earliest. |
| `checkpoint_clamped` | `WARNING` | The checkpoint was moved after a truncation reset. Carries `old_checkpoint` and `new_checkpoint`. |

### `ksqlite.topics`

| Event | Level | Meaning |
|---|---|---|
| `topic_created` | `INFO` | A changelog topic was created. |
| `topic_verified` | `INFO` | A changelog topic's `cleanup.policy` was verified as non-compacting. |
| `compact_policy_detected` | `ERROR` | A changelog topic is compacted. The assignment still proceeds. |
| `policy_verify_failed` | `WARNING` | `cleanup.policy` could not be read. Verification was skipped. |
| `policy_verify_unauthorized` | `WARNING` | `cleanup.policy` could not be read because of an authorization denial. |

## Events worth alerting on

| Event | Why |
|---|---|
| `compact_policy_detected` | A compacted changelog silently loses history on rebuild. |
| `truncation_reset`, `checkpoint_clamped` | Retention removed changelog data the local state depended on. |
| `stop_step_failed` | Shutdown did not complete cleanly. |
| `rehydrate_force_stop` | Partitions are serving incomplete data. |

## See also

- [How to provision changelog topics](../how-to/provision-changelog-topics.md)
- [How to check partition readiness](../how-to/check-partition-readiness.md)
- [Errors](errors.md)
