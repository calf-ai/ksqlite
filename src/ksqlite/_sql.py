"""Identifier/json-path validation, escaping, and DDL emission (plan §3
``_sql``; spec §4 config validation, §5.1 DDL).

Pure module: no I/O.
"""

import re
from collections.abc import Collection

from .errors import ConfigError
from .types import GeneratedColumn, Index

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

SYSTEM_COLUMNS = frozenset(
    {
        "message_id",
        "source_topic",
        "source_partition",
        "changelog_offset",
        "entity_key",
        "payload",
    }
)


def validate_identifier(name: str) -> None:
    """Reject anything but a plain SQL identifier (spec §4, §5.1)."""
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ConfigError(f"invalid SQL identifier: {name!r}")


def escape_string_literal(value: str) -> str:
    """Render ``value`` as a SQL single-quoted string literal (spec §5.1)."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def validate_json_path(path: str) -> None:
    """Validate a host-declared JSON path (spec §4: safe JSON-path)."""
    if not path.startswith("$.") or len(path) <= 2:
        raise ConfigError(f"invalid json_path (must start with '$.'): {path!r}")
    if any(ord(ch) < 0x20 or ch == "\x7f" for ch in path):
        raise ConfigError(f"json_path contains control characters: {path!r}")


_TOPIC_NAME_RE = re.compile(r"[A-Za-z0-9._-]+")
_KAFKA_TOPIC_NAME_MAX = 249


def validate_changelog_template(template: str) -> None:
    """The template must carry both placeholders (spec §4) — and format
    cleanly with exactly those two: an unknown placeholder or malformed
    brace must fail HERE as ConfigError, not as a raw KeyError/ValueError
    out of the first rebalance callback (F-01 fail-fast).
    """
    if "{source_topic}" not in template or "{partition}" not in template:
        raise ConfigError(
            "changelog_topic_template must contain both {source_topic} and "
            f"{{partition}}: {template!r}"
        )
    try:
        template.format(source_topic="probe", partition=0)
    except (KeyError, IndexError, ValueError) as exc:
        raise ConfigError(
            f"changelog_topic_template is not a usable format template:"
            f" {template!r} ({exc})"
        ) from exc


def validate_topic_name(name: str) -> None:
    """Reject an illegal Kafka topic name (charset, 249-char limit; S-07)."""
    if (
        name in (".", "..")
        or len(name) > _KAFKA_TOPIC_NAME_MAX
        or not _TOPIC_NAME_RE.fullmatch(name)
    ):
        raise ConfigError(f"illegal Kafka topic name: {name!r}")


def render_column_clause(column: GeneratedColumn) -> str:
    """Render the canonical column clause for a generated column (spec §5.1).

    This single rendering is used by BOTH migration (via
    :func:`emit_add_column`) and drift detection: SQLite stores an
    ``ALTER TABLE ... ADD COLUMN`` clause verbatim in ``sqlite_schema.sql``,
    so re-rendering the declared config and substring-matching against the
    stored schema text detects drift without parsing SQL.
    """
    path_literal = escape_string_literal(column.json_path)
    return (
        f"{column.name} {column.sql_type} "
        f"GENERATED ALWAYS AS (payload->>{path_literal}) VIRTUAL"
    )


def emit_add_column(column: GeneratedColumn) -> str:
    """Emit the ``ALTER TABLE`` DDL adding a generated column (spec §5.1).

    Assumes ``column`` already passed :func:`validate_generated_column`.
    """
    return f"ALTER TABLE _records ADD COLUMN {render_column_clause(column)}"


def emit_create_index(index: Index) -> str:
    """Emit the ``CREATE INDEX IF NOT EXISTS`` DDL for a host-declared index
    (spec §5.1). Assumes ``index`` already passed :func:`validate_index`.
    """
    unique = "UNIQUE " if index.unique else ""
    columns = ", ".join(index.columns)
    return f"CREATE {unique}INDEX IF NOT EXISTS {index.name} ON _records ({columns})"


def validate_generated_column(column: GeneratedColumn) -> None:
    """Validate a host-declared generated column (spec §4 config validation)."""
    validate_identifier(column.name)
    if column.name in SYSTEM_COLUMNS:
        raise ConfigError(
            f"generated column {column.name!r} collides with a system column"
        )
    validate_json_path(column.json_path)
    if not _IDENTIFIER_RE.fullmatch(column.sql_type):
        raise ConfigError(
            f"invalid sql_type for generated column {column.name!r}: "
            f"{column.sql_type!r}"
        )


def validate_index(index: Index, generated_column_names: Collection[str]) -> None:
    """Every ``Index`` column must reference a system column or a declared
    generated column (spec §4; base columns are legal index targets). The
    index name obeys the same identifier rule as column names (decisions log,
    implementation-handoff item 4).
    """
    validate_identifier(index.name)
    for column in index.columns:
        if column not in SYSTEM_COLUMNS and column not in generated_column_names:
            raise ConfigError(
                f"index {index.name!r} references unknown column {column!r}"
            )
