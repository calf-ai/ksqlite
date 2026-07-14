"""P2 `_sql` tests: S-01..S-10 (plan §5 P2)."""

import pytest

from ksqlite import _sql
from ksqlite.errors import ConfigError
from ksqlite.types import GeneratedColumn, Index

SYSTEM_COLUMNS = (
    "message_id",
    "source_topic",
    "source_partition",
    "changelog_offset",
    "entity_key",
    "payload",
)


def test_s01_identifier_charset() -> None:
    _sql.validate_identifier("task_id")
    _sql.validate_identifier("_private")
    _sql.validate_identifier("x9")

    for bad in ("task-id", "task id", "täsk", '"x";DROP', "", "9lives"):
        with pytest.raises(ConfigError):
            _sql.validate_identifier(bad)


def test_s02_system_column_collision() -> None:
    for sys_col in SYSTEM_COLUMNS:
        with pytest.raises(ConfigError):
            _sql.validate_generated_column(GeneratedColumn(sys_col, "$.x"))

    _sql.validate_generated_column(GeneratedColumn("thread_id", "$.thread_id"))


def test_s04_json_path_validation() -> None:
    _sql.validate_json_path("$.task_id")
    _sql.validate_json_path("$.meta.task_id")
    _sql.validate_json_path("$.a'b")

    for bad in ("", "$", "$.", "x.y", "task_id", "$.a\nb", "$.a\x00b"):
        with pytest.raises(ConfigError):
            _sql.validate_json_path(bad)

    # Wired into generated-column validation.
    with pytest.raises(ConfigError):
        _sql.validate_generated_column(GeneratedColumn("ok_name", "$"))


def test_s07_template_and_topic_name_validation() -> None:
    _sql.validate_changelog_template("{source_topic}.p{partition}.changelog")

    for bad_template in (
        "{source_topic}.changelog",  # missing {partition}
        "p{partition}.changelog",  # missing {source_topic}
        "static-name",
    ):
        with pytest.raises(ConfigError):
            _sql.validate_changelog_template(bad_template)

    _sql.validate_topic_name("messages.p3.changelog")
    _sql.validate_topic_name("a-b_c.9")
    _sql.validate_topic_name("x" * 249)  # boundary: exactly the Kafka limit

    # A template that cannot format with exactly {source_topic}/{partition}
    # must fail HERE as ConfigError — not as a raw KeyError/ValueError from
    # the first rebalance callback (F-01 fail-fast).
    for unformattable in (
        "{source_topic}.p{partition}.{env}",  # unknown placeholder
        "{source_topic}.p{partition}{",  # malformed braces
        "{source_topic}.p{partition}.{}",  # positional field
    ):
        with pytest.raises(ConfigError):
            _sql.validate_changelog_template(unformattable)

    for bad_name in ("", "bad topic!", "café", ".", "..", "x" * 250):
        with pytest.raises(ConfigError):
            _sql.validate_topic_name(bad_name)


def test_s08_index_column_cross_validation() -> None:
    declared = frozenset({"thread_id"})

    _sql.validate_index(Index("ix_thread", ["thread_id"]), declared)
    _sql.validate_index(Index("ix_key", ["entity_key"]), declared)  # base column
    _sql.validate_index(Index("ix_both", ["entity_key", "thread_id"]), declared)

    with pytest.raises(ConfigError):
        _sql.validate_index(Index("ix_bad", ["nonexistent"]), declared)
    with pytest.raises(ConfigError):
        _sql.validate_index(Index("ix_bad2", ["thread_id", "nope"]), declared)


def test_s09_sql_type_validation() -> None:
    # Identifier-charset rule: DDL safety, deliberately not a type allowlist
    # (decisions log, implementation-handoff item 3).
    for good in ("TEXT", "INTEGER", "BOOLEAN"):
        _sql.validate_generated_column(GeneratedColumn("c", "$.c", good))

    for bad in ("TEXT, x INTEGER", "TEXT); DROP", "", "9X"):
        with pytest.raises(ConfigError):
            _sql.validate_generated_column(GeneratedColumn("c", "$.c", bad))


def test_s10_index_name_validation() -> None:
    _sql.validate_index(Index("ix_thread", ["entity_key"]), frozenset())

    for bad in ("ix-thread", '"x";DROP', "", "9ix"):
        with pytest.raises(ConfigError):
            _sql.validate_index(Index(bad, ["entity_key"]), frozenset())
