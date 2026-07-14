"""P1 public-surface tests: L-01..L-04 (plan §5 P1)."""

import dataclasses
import enum
import inspect
from typing import Any

import pytest
from aiokafka import TopicPartition

import ksqlite

SPEC_ERRORS = {
    "KSQLiteError",
    "ConfigError",
    "SQLiteVersionError",
    "SchemaMigrationError",
    "ChangelogProduceError",
    "TopicNotFoundError",
    "RehydrateError",
    "HydrationError",
    "LifecycleError",
    "StorageError",
}

SPEC_SURFACE = SPEC_ERRORS | {
    "KSQLite",
    "GeneratedColumn",
    "Index",
    "PartitionState",
    "PartitionStatus",
    "ConnectionPool",
    "KafkaClientFactory",
}


def test_l01_all_matches_spec_surface_exactly() -> None:
    assert hasattr(ksqlite, "__all__")
    assert set(ksqlite.__all__) == SPEC_SURFACE
    for name in ksqlite.__all__:
        assert getattr(ksqlite, name) is not None


def test_l02_generated_column_and_index_are_frozen_dataclasses() -> None:
    gc = ksqlite.GeneratedColumn("task_id", "$.task_id")
    assert dataclasses.is_dataclass(gc)
    assert (gc.name, gc.json_path, gc.sql_type) == ("task_id", "$.task_id", "TEXT")

    ix = ksqlite.Index("ix_thread", ["thread_id"])
    assert dataclasses.is_dataclass(ix)
    assert ix.name == "ix_thread"
    assert list(ix.columns) == ["thread_id"]
    assert ix.unique is False

    with pytest.raises(dataclasses.FrozenInstanceError):
        gc.sql_type = "INTEGER"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        ix.unique = True  # type: ignore[misc]


def test_l03_partition_state_shape() -> None:
    assert issubclass(ksqlite.PartitionStatus, enum.Enum)
    assert {m.name for m in ksqlite.PartitionStatus} == {
        "RESTORING",
        "READY",
        "READY_PARTIAL",
        "STANDBY",
    }

    assert dataclasses.is_dataclass(ksqlite.PartitionState)
    field_names = [f.name for f in dataclasses.fields(ksqlite.PartitionState)]
    assert field_names == ["state", "checkpoint_offset", "log_end_offset", "lag"]

    ps = ksqlite.PartitionState(
        state=ksqlite.PartitionStatus.READY,
        checkpoint_offset=41,
        log_end_offset=42,
        lag=0,
    )
    assert ps.state is ksqlite.PartitionStatus.READY
    assert (ps.checkpoint_offset, ps.log_end_offset, ps.lag) == (41, 42, 0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        ps.lag = 1  # type: ignore[misc]


def test_l04_protocol_surfaces() -> None:
    import ksqlite.types as ktypes

    class GoodPool:
        def connection(self) -> Any: ...

        async def close(self) -> None: ...

    class BadPool:  # missing close()
        def connection(self) -> Any: ...

    assert isinstance(GoodPool(), ksqlite.ConnectionPool)
    assert not isinstance(BadPool(), ksqlite.ConnectionPool)

    class GoodFactory:
        def producer(self, **kwargs: Any) -> Any: ...

        def consumer(self, **kwargs: Any) -> Any: ...

        def admin_client(self, **kwargs: Any) -> Any: ...

    class BadFactory:  # missing consumer()/admin_client()
        def producer(self, **kwargs: Any) -> Any: ...

    assert isinstance(GoodFactory(), ksqlite.KafkaClientFactory)
    assert not isinstance(BadFactory(), ksqlite.KafkaClientFactory)
    # Spec §4: the three factory methods are SYNCHRONOUS.
    for name in ("producer", "consumer", "admin_client"):
        assert not inspect.iscoroutinefunction(
            getattr(ksqlite.KafkaClientFactory, name)
        )

    resolver_proto = getattr(ktypes, "ChangelogNameResolver", None)
    assert resolver_proto is not None, "ChangelogNameResolver protocol missing"

    class GoodResolver:
        def changelog_name(self, source: TopicPartition) -> str:
            return "t.p0.changelog"

    assert isinstance(GoodResolver(), resolver_proto)
    assert not isinstance(object(), resolver_proto)


def test_l01_every_error_subclasses_ksqlite_error() -> None:
    base = getattr(ksqlite, "KSQLiteError", None)
    assert base is not None
    assert issubclass(base, Exception)
    for name in sorted(SPEC_ERRORS - {"KSQLiteError"}):
        cls = getattr(ksqlite, name, None)
        assert cls is not None, f"missing error class: {name}"
        assert issubclass(cls, base), name
