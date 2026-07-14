"""In-memory 4-state partition machine (plan §3 ``_state``; spec §3, §4).

Pure module: the authoritative state lives here (cleared with the store's
lifetime); ``_owned`` in SQLite is only its visibility projection.
"""

from dataclasses import dataclass

from aiokafka import TopicPartition

from .types import PartitionState, PartitionStatus


@dataclass
class _Entry:
    status: PartitionStatus
    checkpoint_offset: int | None = None
    log_end_offset: int | None = None


class PartitionStateMachine:
    """Tracks every partition encountered during this process lifetime.

    The ``mark_*`` methods store states; the legal transition EDGES (spec
    §3/§7/§8) are enforced by the callers' choreography in ``_lifecycle``,
    not by this class — it stays an allocation-cheap store by design.
    """

    def __init__(self) -> None:
        self._entries: dict[TopicPartition, _Entry] = {}

    def _entry(self, tp: TopicPartition) -> _Entry:
        entry = self._entries.get(tp)
        if entry is None:
            entry = _Entry(status=PartitionStatus.STANDBY)
            self._entries[tp] = entry
        return entry

    def mark_restoring(self, tp: TopicPartition) -> None:
        self._entry(tp).status = PartitionStatus.RESTORING

    def mark_ready(self, tp: TopicPartition) -> None:
        self._entry(tp).status = PartitionStatus.READY

    def mark_ready_partial(self, tp: TopicPartition) -> None:
        self._entry(tp).status = PartitionStatus.READY_PARTIAL

    def mark_revoked(self, tp: TopicPartition) -> None:
        self._entry(tp).status = PartitionStatus.STANDBY

    def record_applied(self, tp: TopicPartition, offset: int) -> None:
        """Monotonic-MAX checkpoint write-back (mirrors the SQL upsert)."""
        entry = self._entry(tp)
        if entry.checkpoint_offset is None or offset > entry.checkpoint_offset:
            entry.checkpoint_offset = offset

    def status_of(self, tp: TopicPartition) -> PartitionStatus | None:
        """The live status, or ``None`` for a never-seen partition."""
        entry = self._entries.get(tp)
        return entry.status if entry is not None else None

    def record_clamped(self, tp: TopicPartition, offset: int) -> None:
        """The SOLE non-monotonic write-back — called only from the two §7
        clamp sites (clamp-down to LEO−1; clamp-up to log_start−1), so the
        in-memory checkpoint can never silently disagree with the persisted
        one after a clamp.
        """
        self._entry(tp).checkpoint_offset = offset

    def record_log_end(self, tp: TopicPartition, log_end_offset: int) -> None:
        """Cache the LEO observed at rehydrate (spec §4: as-of-last-rehydrate)."""
        self._entry(tp).log_end_offset = log_end_offset

    def partition_states(self) -> dict[TopicPartition, PartitionState]:
        return {
            tp: PartitionState(
                state=entry.status,
                checkpoint_offset=entry.checkpoint_offset,
                log_end_offset=entry.log_end_offset,
                lag=_lag(entry),
            )
            for tp, entry in self._entries.items()
        }


def _lag(entry: _Entry) -> int | None:
    """``lag = LEO − (checkpoint + 1)`` (spec §4); None until a LEO is cached."""
    if entry.log_end_offset is None:
        return None
    next_offset = 0 if entry.checkpoint_offset is None else entry.checkpoint_offset + 1
    return entry.log_end_offset - next_offset
