"""P5 `_state` tests: ST-01..ST-08 (plan §5 P5). Pure; no I/O."""

from aiokafka import TopicPartition

from ksqlite import _state
from ksqlite.types import PartitionStatus

TP = TopicPartition("messages", 3)


def test_st01_transitions() -> None:
    machine = _state.PartitionStateMachine()

    # UNKNOWN -> RESTORING -> READY
    machine.mark_restoring(TP)
    assert machine.partition_states()[TP].state is PartitionStatus.RESTORING
    machine.mark_ready(TP)
    assert machine.partition_states()[TP].state is PartitionStatus.READY

    # force-stop -> READY_PARTIAL
    machine.mark_restoring(TP)
    machine.mark_ready_partial(TP)
    assert machine.partition_states()[TP].state is PartitionStatus.READY_PARTIAL

    # revoke -> STANDBY, checkpoint kept
    machine.record_applied(TP, 41)
    machine.mark_revoked(TP)
    state = machine.partition_states()[TP]
    assert state.state is PartitionStatus.STANDBY
    assert state.checkpoint_offset == 41

    # re-assign: STANDBY -> RESTORING
    machine.mark_restoring(TP)
    assert machine.partition_states()[TP].state is PartitionStatus.RESTORING

    # fast path: STANDBY/UNKNOWN -> READY directly (no observable RESTORING)
    machine.mark_revoked(TP)
    machine.mark_ready(TP)
    assert machine.partition_states()[TP].state is PartitionStatus.READY

    other = TopicPartition("messages", 9)
    machine.mark_ready(other)  # UNKNOWN -> READY (fast path, first assignment)
    assert machine.partition_states()[other].state is PartitionStatus.READY


def test_st02_standby_derivation() -> None:
    machine = _state.PartitionStateMachine()

    # Revoked off an empty changelog: STANDBY with checkpoint None.
    empty = TopicPartition("t", 0)
    machine.mark_restoring(empty)
    machine.mark_ready(empty)
    machine.mark_revoked(empty)
    state = machine.partition_states()[empty]
    assert state.state is PartitionStatus.STANDBY
    assert state.checkpoint_offset is None

    # An off-contract append to a never-assigned TP also reports STANDBY
    # (seen this lifetime, not owned; spec §4).
    appended = TopicPartition("t", 1)
    machine.record_applied(appended, 7)
    state = machine.partition_states()[appended]
    assert state.state is PartitionStatus.STANDBY
    assert state.checkpoint_offset == 7


def test_st03_cached_lag_semantics() -> None:
    machine = _state.PartitionStateMachine()
    machine.mark_restoring(TP)
    machine.record_log_end(TP, 100)  # LEO = last available offset + 1
    machine.record_applied(TP, 89)

    state = machine.partition_states()[TP]
    assert state.log_end_offset == 100
    assert state.lag == 10  # LEO - (checkpoint+1) = 100 - 90

    # The live checkpoint moves; the cached LEO does not refresh on reads.
    machine.record_applied(TP, 99)
    state = machine.partition_states()[TP]
    assert state.log_end_offset == 100
    assert state.lag == 0

    # LEO cached with no checkpoint yet: the whole log is the lag.
    unread = TopicPartition("t", 2)
    machine.mark_restoring(unread)
    machine.record_log_end(unread, 5)
    state = machine.partition_states()[unread]
    assert state.log_end_offset == 5
    assert state.lag == 5

    # No LEO recorded (never rehydrated): both cached fields are None.
    fresh = TopicPartition("t", 5)
    machine.record_applied(fresh, 3)
    state = machine.partition_states()[fresh]
    assert state.log_end_offset is None
    assert state.lag is None


def test_st04_partition_states_is_a_snapshot() -> None:
    machine = _state.PartitionStateMachine()
    machine.mark_ready(TP)

    snapshot = machine.partition_states()
    snapshot.clear()  # mutating the snapshot must not affect internal state

    assert TP in machine.partition_states()


def test_st05_fresh_process_scope_is_empty() -> None:
    # Only partitions seen this lifetime appear; no SQLite read (spec §4).
    assert _state.PartitionStateMachine().partition_states() == {}


def test_st06_write_back_keeps_checkpoint_live() -> None:
    """`record_applied` is the write-path's post-commit push; the snapshot
    reflects it immediately. Pure in-memory — the zero-DB-reads property is
    structural here and re-asserted at the store level (F-09).
    """
    machine = _state.PartitionStateMachine()
    machine.mark_ready(TP)
    machine.record_applied(TP, 17)
    assert machine.partition_states()[TP].checkpoint_offset == 17


def test_st07_record_applied_is_monotonic_max() -> None:
    machine = _state.PartitionStateMachine()
    machine.record_applied(TP, 5)
    machine.record_applied(TP, 3)  # a late/duplicate lower offset
    assert machine.partition_states()[TP].checkpoint_offset == 5


def test_st08_clamps_only_via_record_clamped() -> None:
    """`record_clamped` is the SOLE non-monotonic entry point (the two §7
    clamp sites); `record_applied` can never regress the value.
    """
    machine = _state.PartitionStateMachine()
    machine.record_applied(TP, 600)

    machine.record_clamped(TP, 549)  # clamp-down after a truncated-log reset
    assert machine.partition_states()[TP].checkpoint_offset == 549

    machine.record_applied(TP, 100)  # still cannot regress
    assert machine.partition_states()[TP].checkpoint_offset == 549

    machine.record_clamped(TP, 999)  # clamp-up past a fully-trimmed gap
    assert machine.partition_states()[TP].checkpoint_offset == 999
