# How to load results into models

This guide shows you how to get typed objects out of `query()` instead of raw
rows, using `into=`.

By default `query()` returns `sqlite3.Row` objects and `payload` is JSON text you
have to parse yourself. `into=` parses it for you into a Pydantic v2 model or a
dataclass.

## Hydrate into a dataclass

```python
from dataclasses import dataclass

@dataclass
class Message:
    thread_id: str
    text: str

messages = await store.query(
    "SELECT payload FROM records WHERE thread_id = ?", ["th-42"], into=Message
)
# [Message(thread_id='th-42', text='hi')]
```

## Hydrate into a Pydantic v2 model

```python
from pydantic import BaseModel

class Message(BaseModel):
    thread_id: str
    text: str

messages = await store.query(
    "SELECT payload FROM records WHERE thread_id = ?", ["th-42"], into=Message
)
```

Pydantic models are validated with `model_validate_json`, so you get Pydantic's
coercion and validation errors.

## Select `payload`

`into=` reads the `payload` column, so the query must select it. This raises
`HydrationError`:

```python
await store.query("SELECT entity_key FROM records", into=Message)
# HydrationError: into= requires the query to select a `payload` column
```

Selecting other columns alongside `payload` is fine — they are ignored by
hydration. Only `payload` is used.

## Choose the target to match your payloads

The two targets behave differently when a payload does not exactly match the
model, and this is the thing most likely to catch you out:

| Payload has | Dataclass | Pydantic v2 |
|---|---|---|
| A field the model lacks | `TypeError` | Ignored |
| A field missing from the payload | `TypeError` | `ValidationError`, unless the field has a default |

A dataclass is hydrated with `into(**json.loads(payload))`, so an extra key in the
payload is an unexpected keyword argument:

```python
@dataclass
class Partial:
    thread_id: str

await store.query("SELECT payload FROM records", into=Partial)
# TypeError: Partial.__init__() got an unexpected keyword argument 'text'
```

The same query with a Pydantic model returns `Partial(thread_id='th-42')`.

So: if your payloads carry fields your model does not need, or your payload shape
evolves over time, use Pydantic. Use a dataclass when the payload and the model
match exactly and you want no extra dependency.

Anything that is neither raises `TypeError`:

```python
await store.query("SELECT payload FROM records", into=dict)
# TypeError: into= must be a Pydantic v2 model or a dataclass
```

## Work with raw rows instead

Without `into=`, rows are `sqlite3.Row`, indexable by column name:

```python
rows = await store.query("SELECT entity_key, payload FROM records")
for row in rows:
    print(row["entity_key"], row["payload"])   # payload is JSON text
```

This is what you want when you are aggregating, selecting generated columns, or
otherwise not reconstructing whole payloads.

## See also

- [`KSQLite`](../reference/ksqlite.md) — the `query()` signature and its errors
- [How to declare queryable columns](declare-queryable-columns.md)
- [Errors](../reference/errors.md)
