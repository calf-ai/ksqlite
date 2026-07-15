# How to handle failures

This guide shows you what to do when `append()`, `query()`, or a rebalance hook
fails.

The single most important rule: **never retry a failed `append()`**. The rest of
this guide explains what to do instead.

## Never retry `append()`

`append()` is not idempotent. Each call mints a fresh `message_id`, and
`message_id` is the key KSQLite deduplicates on. A retry is a *different* record,
so if the first attempt actually reached the broker and only its acknowledgement
was lost, you now have the same data twice — permanently, in the source of truth.

```python
# WRONG
for attempt in range(3):
    try:
        await store.append(source=tp, entity_key=key, payload=value)
        break
    except ChangelogProduceError:
        continue   # may duplicate the record in the changelog forever
```

Let it fail instead, and let your consumer's own redelivery re-drive the work:

```python
try:
    await store.append(source=tp, entity_key=key, payload=value)
except ChangelogProduceError:
    logger.exception("append failed for %s", key)
    raise   # do not retry in place
```

If duplicates are tolerable for your data, a retry is your call to make
deliberately — not a default. Where they are not, treat a failed `append()` as a
failed unit of work.

## Know which failures wrote what

Each `append()` error tells you a different thing about what happened:

| Error | Changelog | Local SQLite | What to do |
|---|---|---|---|
| `ValueError` | Nothing produced | Nothing written | Fix the call. Bad `entity_key` or payload. |
| `ChangelogProduceError` | Possibly written | Nothing written | Do not retry. The record may or may not be in the changelog. |
| `StorageError` | Written | Failed | Do nothing. The next rehydrate applies it. |

`StorageError` from `append()` is the benign one: the durable copy landed, only
the local copy failed. The record materializes on the next rehydrate of that
partition, so the store self-heals.

`ChangelogProduceError` is raised both when the produce fails outright and when
the broker acks without a usable offset. In both cases no local write happened.

## Handle bad input before it reaches `append()`

`ValueError` means the call was wrong, not the system:

```python
key = record.key.decode() if record.key else None
if not key:
    logger.warning("skipping record with no key at offset %s", record.offset)
    continue

await store.append(source=tp, entity_key=key, payload=record.value.decode())
```

`entity_key` must be a non-empty, UTF-8-encodable `str`. A payload must be
JSON-serializable, or a `str` that parses as JSON; `NaN` and `Infinity` are
rejected in both forms.

## Handle rehydrate failures

`on_partitions_assigned` raises through your rebalance callback. Treat it as a
failed assignment — do not swallow it:

```python
class StoreListener(ConsumerRebalanceListener):
    async def on_partitions_assigned(self, assigned):
        try:
            await store.on_partitions_assigned(assigned)
        except TopicNotFoundError:
            logger.exception("changelog missing; provision it")
            raise
        except RehydrateError:
            logger.exception("rehydrate failed for %s", assigned)
            raise
```

Catching `RehydrateError` does **not** catch `TopicNotFoundError` — they are
siblings, not parent and child. Catch `KSQLiteError` if you want both.

Partitions not yet revealed when the error hits stay hidden, so a failed
assignment never exposes half-restored data.

A `RehydrateError` naming a `format_version` means the changelog holds records
written by a newer KSQLite than this process understands. Do not delete the
topic; roll the reader forward.

## Handle query failures

`StorageError` from `query()` is safe to retry — reads have no side effects. It
usually means contention (`SQLITE_BUSY` past `busy_timeout`), pool exhaustion, or
an accidental write:

```python
await store.query("DELETE FROM _records")
# StorageError: query failed: attempt to write a readonly database
```

`query()` runs read-only. There is no supported way to write through it.

If you see busy timeouts under load, raise `busy_timeout` or `pool_size`.

## Handle startup failures

`start()` cleans up after itself — if a step fails, everything already opened is
closed before the error propagates, so a failed `start()` leaks nothing and you
can fix the cause and construct a new store.

| Error | Cause |
|---|---|
| `ConfigError` | Bad configuration, or the stored schema drifted from what you declared. |
| `SQLiteVersionError` | SQLite older than 3.38. See [How to run on an older SQLite](run-on-older-sqlite.md). |
| `StorageError` | The database could not be opened, or WAL could not be enabled. |
| `KSQLiteError` | The producer or admin client could not start — usually the broker is unreachable. |

Note that a store is not reusable: `start()` after a `stop()` raises
`LifecycleError`. Construct a new `KSQLite`.

## See also

- [Errors](../reference/errors.md) — the full taxonomy
- [About the durability contract](../explanation/durability.md) — why `append()` cannot be idempotent
- [Logging](../reference/logging.md) — events to alert on
