"""The PartitionLifecycle service (plan §3 ``_lifecycle``; spec §7/§8).

Owns BOTH rebalance hooks. ``batch_size`` and ``clock`` are real operating
parameters with defaults, not test hooks.
"""

import contextlib
import logging
import sqlite3
import time
from collections.abc import Callable, Iterable
from typing import Any

import aiosqlite
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError, OffsetOutOfRangeError

from . import _pool, _schema, _wire
from ._pool import TransactionRunner
from ._state import PartitionStateMachine
from ._topics import ChangelogTopics
from .errors import RehydrateError, StorageError
from .types import ConnectionPool

logger = logging.getLogger("ksqlite.lifecycle")

DEFAULT_BATCH_SIZE = 500


class PartitionLifecycle:
    """Blocking rehydrate inside the host's rebalance callback (spec §7)."""

    def __init__(
        self,
        *,
        pool: ConnectionPool,
        consumer_factory: Callable[[], Any],
        topics: ChangelogTopics,
        state: PartitionStateMachine,
        txn_runner: TransactionRunner,
        rehydrate_timeout: float,
        batch_size: int = DEFAULT_BATCH_SIZE,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._pool = pool
        self._consumer_factory = consumer_factory
        self._topics = topics
        self._state = state
        self._txn_runner = txn_runner
        self._rehydrate_timeout = rehydrate_timeout
        self._batch_size = batch_size
        self._clock = clock
        self._consumer: Any = None

    async def _get_consumer(self) -> Any:
        # Lazily started on the first assignment (spec §4 lifecycle). The
        # field is assigned only AFTER a successful start(): caching a client
        # whose start() failed would wedge every later rehydrate (and
        # aclose() would stop() a never-started consumer).
        if self._consumer is None:
            consumer = self._consumer_factory()
            try:
                await consumer.start()
            except BaseException:
                with contextlib.suppress(Exception):
                    await consumer.stop()
                raise
            self._consumer = consumer
        return self._consumer

    async def aclose(self) -> None:
        """Close the lazily-started rehydrate consumer (if it ever started)."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def on_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        # One ABSOLUTE wall-clock deadline for the entire callback (spec §7):
        # a per-partition budget would multiply by the assignment size and
        # trigger the very rebalance storm it exists to prevent.
        deadline = self._clock() + self._rehydrate_timeout
        first = True
        for tp in assigned:
            try:
                # Step 0 — unconditional on first assignment; budget-EXEMPT
                # (correctness checks are never force-stopped away).
                changelog_name = await self._topics.ensure_and_verify(tp)
                # The deadline is checked before entering each partition
                # AFTER the first — the first partition always gets a full
                # chance to replay.
                if not first and self._clock() >= deadline:
                    await self._reveal_partial_unreached(tp, changelog_name)
                    continue
                first = False
                await self._rehydrate_one(tp, changelog_name, deadline)
            except (StorageError, KafkaError) as exc:
                # Boundary mapping (spec §4): any broker or DB error inside
                # the assignment hook is re-raised as RehydrateError with the
                # cause chained. On any error, partitions not yet revealed in
                # this assignment stay hidden (RH-28).
                raise RehydrateError(f"rehydrate of {tp} failed: {exc}") from exc

    async def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        """STANDBY bookkeeping (spec §8): hide the rows at once, keep the
        data + checkpoint (warm standby).
        """
        for tp in revoked:
            if self._state.status_of(tp) is None:
                continue  # revoke of a never-owned TP is a no-op (RH-19)
            await self._execute_in_txn(
                _schema.OWNED_DELETE_SQL, (tp.topic, tp.partition)
            )
            self._state.mark_revoked(tp)

    async def _rehydrate_one(
        self, tp: TopicPartition, changelog_name: str, deadline: float
    ) -> None:
        checkpoint = await self._read_checkpoint(tp)
        if checkpoint is not None:
            # Seed the in-memory checkpoint (spec §4: checkpoint_offset is
            # live): the zero-fetch fast path never reaches record_applied,
            # so a warm restart would otherwise report checkpoint None /
            # lag == LEO for a fully caught-up partition (RH-29).
            self._state.record_applied(tp, checkpoint)
        consumer = await self._get_consumer()
        changelog_tp = TopicPartition(changelog_name, 0)
        leo: int = (await consumer.end_offsets([changelog_tp]))[changelog_tp]
        self._state.record_log_end(tp, leo)

        logger.info(
            "rehydrate start",
            extra={
                "event": "rehydrate_start",
                "tp": tp,
                "checkpoint": checkpoint,
                "log_end_offset": leo,
            },
        )

        self._state.mark_restoring(tp)
        consumer.assign([changelog_tp])
        if checkpoint is None:
            await consumer.seek_to_beginning(changelog_tp)
            position = 0
        else:
            consumer.seek(changelog_tp, checkpoint + 1)
            position = checkpoint + 1

        records_replayed = 0
        last_applied = checkpoint
        reset_occurred = False
        reset_log_start = 0
        replayed_after_reset = 0
        # Replay terminates when the position reaches the LEO captured
        # above — an empty getmany is NOT a termination signal (spec §7).
        # A position ABOVE the LEO (stale-high checkpoint after a log-end
        # regression) still fetches: the broker answers with OOR, which is
        # what routes it into the reset + clamp path (RH-10).
        #
        # The deadline is checked on EVERY loop iteration — after a
        # materialized batch, after an empty fetch, and after an OOR reset.
        # A fetched batch is ALWAYS materialized + committed before the
        # check (spec §7); but a degraded broker returning {} forever must
        # not hold the rebalance callback past rehydrate_timeout — that is
        # the very rebalance storm the budget exists to prevent (RH-31/32).
        while position != leo:
            try:
                batch_map = await consumer.getmany(
                    changelog_tp, timeout_ms=200, max_records=self._batch_size
                )
            except OffsetOutOfRangeError:
                # Truncated log: WARNING (data-loss-adjacent), reset to
                # earliest, best-effort replay — NO raise (spec §7 step 2).
                log_start: int = (await consumer.beginning_offsets([changelog_tp]))[
                    changelog_tp
                ]
                logger.warning(
                    "checkpoint %s of %s is below the changelog log start %s;"
                    " resetting to earliest",
                    checkpoint,
                    tp,
                    log_start,
                    extra={
                        "event": "truncation_reset",
                        "tp": tp,
                        "checkpoint": checkpoint,
                        "log_start": log_start,
                    },
                )
                await consumer.seek_to_beginning(changelog_tp)
                position = log_start
                reset_occurred = True
                reset_log_start = log_start
                replayed_after_reset = 0
                if self._clock() >= deadline:
                    await self._force_stop(tp, checkpoint=last_applied, leo=leo)
                    return
                continue
            records = batch_map.get(changelog_tp, [])
            if not records:
                if self._clock() >= deadline:
                    await self._force_stop(tp, checkpoint=last_applied, leo=leo)
                    return
                continue
            await self._materialize_batch(tp, records)
            records_replayed += len(records)
            replayed_after_reset += len(records)
            position = records[-1].offset + 1
            last_applied = records[-1].offset
            if position != leo and self._clock() >= deadline:
                await self._force_stop(tp, checkpoint=last_applied, leo=leo)
                return

        if reset_occurred:
            await self._apply_post_reset_clamps(
                tp, leo, reset_log_start, replayed_after_reset
            )

        self._state.mark_ready(tp)
        await self._reveal(tp)
        logger.info(
            "rehydrate end",
            extra={
                "event": "rehydrate_end",
                "tp": tp,
                "records_replayed": records_replayed,
            },
        )

    async def _materialize_batch(self, tp: TopicPartition, records: list[Any]) -> None:
        """One BEGIN IMMEDIATE transaction per fetched batch; the poll always
        happens OUTSIDE the transaction (spec §7 step 3).
        """
        batch_max_offset: int = records[-1].offset

        async def work(conn: aiosqlite.Connection) -> None:
            for record in records:
                decoded = _wire.classify(
                    key=record.key,
                    value=record.value,
                    headers=list(record.headers),
                )
                if isinstance(decoded, _wire.ForeignRecord):
                    # Not KSQLite data (spec §7): skip + log; its offset
                    # still advances the checkpoint (below).
                    logger.info(
                        "skipping foreign record: %s",
                        decoded.reason,
                        extra={
                            "event": "foreign_record_skipped",
                            "tp": tp,
                            "offset": record.offset,
                            "reason": decoded.reason,
                        },
                    )
                    continue
                if isinstance(decoded, _wire.UnknownVersionRecord):
                    # A KSQLite record this reader can't understand (rolling
                    # upgrade/downgrade): fail loud, never drop (spec §7).
                    raise RehydrateError(
                        f"changelog record at offset {record.offset} of {tp} "
                        f"carries format_version="
                        f"{decoded.format_version!r} (reader understands "
                        f"{_wire.FORMAT_VERSION!r})"
                    )
                assert isinstance(decoded, _wire.KsqliteRecord)
                await _schema.apply_materialization(
                    conn,
                    message_id=decoded.message_id,
                    source_topic=tp.topic,
                    source_partition=tp.partition,
                    changelog_offset=record.offset,
                    entity_key=decoded.entity_key,
                    payload=decoded.payload,
                )
            # Skipped-foreign offsets still advance the checkpoint (spec §7),
            # so a foreign tail cannot permanently defeat the fast path.
            await _pool.execute(
                conn,
                _schema.CHECKPOINT_UPSERT_SQL,
                {
                    "source_topic": tp.topic,
                    "source_partition": tp.partition,
                    "changelog_offset": batch_max_offset,
                },
            )

        async with self._pool.connection() as conn:
            await self._txn_runner.run(conn, work)
        # Post-commit in-memory write-back (spec §4).
        self._state.record_applied(tp, batch_max_offset)

    async def _reveal_partial_unreached(
        self, tp: TopicPartition, changelog_name: str
    ) -> None:
        """A partition the budget never reached: revealed READY_PARTIAL at
        its kept-checkpoint state (spec §7 force-stop); LEO cached for lag.
        """
        consumer = await self._get_consumer()
        changelog_tp = TopicPartition(changelog_name, 0)
        leo: int = (await consumer.end_offsets([changelog_tp]))[changelog_tp]
        self._state.record_log_end(tp, leo)
        checkpoint = await self._read_checkpoint(tp)
        if checkpoint is not None:
            self._state.record_applied(tp, checkpoint)
        self._state.mark_ready_partial(tp)
        await self._reveal(tp)
        logger.info(
            "replay budget exhausted before this partition; revealed at its"
            " kept checkpoint",
            extra={
                "event": "rehydrate_force_stop",
                "tp": tp,
                "checkpoint": checkpoint,
                "log_end_offset": leo,
            },
        )

    async def _apply_post_reset_clamps(
        self,
        tp: TopicPartition,
        leo: int,
        log_start: int,
        replayed_after_reset: int,
    ) -> None:
        """The two §7 step-2 clamps — the ONLY non-monotonic checkpoint
        writes. Clamp DOWN when the stored checkpoint sits above LEO-1
        (else every future rehydrate full-replays forever); clamp UP past a
        fully-trimmed gap (else every future rehydrate re-enters the reset
        path). Both log a WARNING (data-loss-adjacent, operator-actionable).
        """
        stored = await self._read_checkpoint(tp)
        target: int | None = None
        if stored is not None and stored > leo - 1:
            target = leo - 1
        elif replayed_after_reset == 0 and log_start == leo:
            target = log_start - 1
        if target is None:
            return
        await self._execute_in_txn(
            _schema.CHECKPOINT_CLAMP_SQL, (tp.topic, tp.partition, target)
        )
        self._state.record_clamped(tp, target)
        logger.warning(
            "checkpoint of %s clamped from %s to %s after truncation reset",
            tp,
            stored,
            target,
            extra={
                "event": "checkpoint_clamped",
                "tp": tp,
                "old_checkpoint": stored,
                "new_checkpoint": target,
            },
        )

    async def _reveal(self, tp: TopicPartition) -> None:
        await self._execute_in_txn(_schema.OWNED_INSERT_SQL, (tp.topic, tp.partition))

    async def _force_stop(
        self, tp: TopicPartition, *, checkpoint: int | None, leo: int
    ) -> None:
        """Reveal the partition READY_PARTIAL at its committed frontier —
        the §7 force-stop taken whenever the budget expires mid-replay.
        """
        self._state.mark_ready_partial(tp)
        await self._reveal(tp)
        logger.info(
            "rehydrate force-stopped at the replay budget",
            extra={
                "event": "rehydrate_force_stop",
                "tp": tp,
                "checkpoint": checkpoint,
                "log_end_offset": leo,
            },
        )

    async def _execute_in_txn(self, sql: str, params: tuple[Any, ...]) -> None:
        """One single-statement write through the TransactionRunner: the same
        bounded SQLITE_BUSY retry and StorageError boundary mapping as every
        other KSQLite write (spec §4/§11).
        """

        async def work(conn: aiosqlite.Connection) -> None:
            await _pool.execute(conn, sql, params)

        async with self._pool.connection() as conn:
            await self._txn_runner.run(conn, work)

    async def _read_checkpoint(self, tp: TopicPartition) -> int | None:
        try:
            async with self._pool.connection() as conn:
                row = await _pool.fetch_one(
                    conn, _schema.SELECT_CHECKPOINT_SQL, (tp.topic, tp.partition)
                )
        except sqlite3.Error as exc:
            # Boundary mapping (spec §4): reads bypass the TransactionRunner
            # (a SELECT needs no write lock), so the mapping lives here.
            raise StorageError(f"checkpoint read for {tp} failed: {exc}") from exc
        return None if row is None else int(row[0])
