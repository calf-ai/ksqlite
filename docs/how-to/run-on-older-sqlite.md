# How to run on an older SQLite

This guide shows you what to do when your interpreter links a SQLite older than
3.38 and `start()` refuses to run:

```text
SQLiteVersionError: KSQLite requires SQLite >= 3.38; found (3, 34, 1)
```

KSQLite needs 3.38 because generated columns are built with the `->>` operator,
which does not exist before it.

## Check what you have

The version that matters is the one linked into your interpreter, not the `sqlite3`
binary on your `PATH`:

```sh
python -c "import sqlite3; print(sqlite3.sqlite_version)"
```

If that prints 3.38 or newer, this guide is not your problem.

## Prefer a newer interpreter

The cleanest fix is an interpreter linking a newer SQLite. Recent official Python
Docker images and `uv python install` builds ship one:

```sh
uv python install 3.12
uv run --python 3.12 python -c "import sqlite3; print(sqlite3.sqlite_version)"
```

This is worth trying before the swap below, which adds a dependency and a
constraint on where you can deploy.

## Swap in `pysqlite3` at the entrypoint

If you cannot change the interpreter, replace the `sqlite3` module with
`pysqlite3`, which bundles its own newer SQLite:

```sh
uv add pysqlite3-binary
```

Then, at your application entrypoint, **before anything imports `ksqlite` or
`aiosqlite`**:

```python
import sys

import pysqlite3  # package: pysqlite3-binary

sys.modules["sqlite3"] = pysqlite3
```

Placement is the whole trick. `aiosqlite` imports `sqlite3` at import time, so if
anything has already imported it, the swap comes too late and has no effect. Put
it at the very top of the module that runs first, above your other imports.

```python
# main.py
import sys

import pysqlite3

sys.modules["sqlite3"] = pysqlite3

# only now
import asyncio

from ksqlite import KSQLite
```

Verify it took effect:

```python
import sqlite3

print(sqlite3.sqlite_version)  # should be >= 3.38
```

If you get the swap wrong, `start()` raises `SQLiteVersionError` exactly as
before — the check runs at startup, so a missed swap fails loudly rather than
surfacing later as a broken query.

### Know the platform limit

`pysqlite3-binary` publishes wheels for **linux/x86_64 only**. On macOS, Windows,
or ARM Linux there is no wheel, so this approach does not apply and you need an
interpreter that links a newer SQLite.

## See also

- [Errors](../reference/errors.md) — `SQLiteVersionError`
- [SQL schema](../reference/sql-schema.md) — what `->>` is used for
