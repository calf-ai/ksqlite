# Changelog format

The wire format KSQLite writes to changelog topics and reads back during
rehydrate. This page describes format version `1`.

## Topics

One changelog topic per shard, named by `changelog_topic_template`:

```text
{source_topic}.p{partition}.changelog
```

For `TopicPartition("messages", 3)` the default template resolves to
`messages.p3.changelog`.

| Property | Value |
|---|---|
| Partitions | 1. Records are always produced to partition `0`. |
| `cleanup.policy` | `delete`. Never `compact`. |
| `retention.ms` | Set from `create_topics_retention_ms` when KSQLite creates the topic. |
| Replication factor | 1 when KSQLite creates the topic. |

A resolved topic name must be a legal Kafka topic name: at most 249 characters,
matching `[A-Za-z0-9._-]+`, and not `.` or `..`. Otherwise `ConfigError` is
raised.

Compaction collapses an entity to its last message, which discards history the
changelog exists to hold. See
[How to provision changelog topics](../how-to/provision-changelog-topics.md).

## Record

| Part | Content |
|---|---|
| Key | `entity_key`, UTF-8 encoded. |
| Value | The payload as UTF-8 JSON text, byte-for-byte as serialized. |
| Headers | `format_version`, then `message_id`. |

| Header | Value |
|---|---|
| `format_version` | `"1"`, UTF-8 encoded. |
| `message_id` | A UUIDv7 in canonical 36-character form, UTF-8 encoded. |

The value is the host payload and nothing else: no envelope, no wrapper fields.
A `str` payload is validated as JSON and produced unchanged; any other object is
serialized with `json.dumps(payload, allow_nan=False)`.

Headers are read back by name, so their order does not matter and unrecognized
extra headers are ignored.

### Example

`append(source=TopicPartition("messages", 3), entity_key="th-42", payload={"text": "hi"})`
produces to `messages.p3.changelog`, partition `0`:

```text
key:     b"th-42"
value:   b'{"text": "hi"}'
headers: [("format_version", b"1"),
          ("message_id", b"0199f3a2-...-....-............")]
```

## Replay classification

Every record read during rehydrate is classified by its headers and body.

| Condition | Outcome |
|---|---|
| No `message_id` header | Skipped as foreign; logged. |
| `message_id` is not UTF-8 or not a UUID | Skipped as foreign; logged. |
| No `format_version` header | `RehydrateError`. |
| `format_version` is not `"1"` | `RehydrateError`. |
| Key is null, or not UTF-8 | Skipped as foreign; logged. |
| Value is null, or not UTF-8 | Skipped as foreign; logged. |
| Otherwise | Applied to `_records`. |

The order matters: a record carrying a valid `message_id` is a KSQLite record, so
a version this reader does not understand fails loudly rather than being dropped.
A record without a valid `message_id` was written by something else and is
skipped.

A skipped foreign record still advances the checkpoint, so foreign records in a
changelog do not force repeated replay.

## Idempotence

`message_id` is the deduplication key. Both `append()` and replay insert with
`ON CONFLICT (message_id) DO NOTHING` using the same statement, so replaying a
changelog any number of times converges to the same rows.

This is why a failed `append()` must not be retried: a retry mints a **new**
`message_id`, which does not deduplicate against the first attempt. See
[About the durability contract](../explanation/durability.md).

## See also

- [How to provision changelog topics](../how-to/provision-changelog-topics.md)
- [SQL schema](sql-schema.md) — where replayed records land
- [About the durability contract](../explanation/durability.md)
