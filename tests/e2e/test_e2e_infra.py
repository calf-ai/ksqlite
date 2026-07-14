"""P10 E2E infrastructure scenarios: E2E-04/09/13 (plan §5 P10).

Every container mutation is undone in a ``finally`` — a poisoned session
container fails everything downstream.
"""

import asyncio
import json
import logging
import socket
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

import aiokafka

from ksqlite import KSQLite

from tests.e2e.test_e2e_core import make_store, unique

pytestmark = pytest.mark.e2e


@pytest.mark.timeout(30)
async def test_e2e09_broker_outage(broker: Any, tmp_path: Path) -> None:
    """`docker pause` on the (wrapped) broker: append fails with
    ChangelogProduceError in ~2-4 s, writes nothing locally; appends resume
    after unpause. Must NOT assert the failed record is absent from the
    changelog (the phantom may land — spec §14) nor exact post-unpause
    offsets.
    """
    from ksqlite.errors import ChangelogProduceError

    bootstrap = broker.get_bootstrap_server()
    source = TopicPartition(unique("src"), 0)

    store = make_store(
        tmp_path / "s.db",
        bootstrap,
        producer_config={"request_timeout_ms": 2000},
    )
    await store.start()
    try:
        await store.on_partitions_assigned([source])
        await store.append(source=source, entity_key="ok", payload={"v": 1})

        wrapped = broker.get_wrapped_container()
        wrapped.pause()
        try:
            started = time.monotonic()
            with pytest.raises(ChangelogProduceError):
                await store.append(source=source, entity_key="failed", payload={"v": 2})
            elapsed = time.monotonic() - started
            assert elapsed < 10  # ~2-4 s with request_timeout_ms=2000
        finally:
            wrapped.unpause()

        # No local row for the failed append.
        rows = await store.query("SELECT entity_key FROM records")
        assert {r["entity_key"] for r in rows} == {"ok"}

        # Appends resume after the broker returns (bounded-wait retry loop —
        # the client needs a moment to reconnect).
        deadline = time.monotonic() + 15
        while True:
            try:
                await store.append(
                    source=source, entity_key="resumed", payload={"v": 3}
                )
                break
            except ChangelogProduceError:
                if time.monotonic() >= deadline:  # pragma: no cover
                    raise
                await asyncio.sleep(0.5)

        rows = await store.query("SELECT entity_key FROM records")
        assert {"ok", "resumed"} <= {r["entity_key"] for r in rows}
    finally:
        await store.stop()


@pytest.mark.timeout(180)
async def test_e2e13_broker_restart_persistence(tmp_path: Path) -> None:
    """A DEDICATED container with a fixed host port (the session container
    cannot be restarted: Docker remaps the ephemeral port AND Redpanda
    re-advertises the stale one — plan E2E-13, live-verified).
    """
    from testcontainers.kafka import RedpandaContainer

    from tests.e2e.conftest import REDPANDA_IMAGE

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    fixed_port = probe.getsockname()[1]
    probe.close()

    container = RedpandaContainer(REDPANDA_IMAGE).with_bind_ports(9092, fixed_port)
    container.start(timeout=60)
    try:
        bootstrap = container.get_bootstrap_server()
        source = TopicPartition(unique("src"), 0)
        n = 5

        store = make_store(tmp_path / "s.db", bootstrap)
        await store.start()
        await store.on_partitions_assigned([source])
        for i in range(n):
            await store.append(source=source, entity_key=f"k{i}", payload={"i": i})

        wrapped = container.get_wrapped_container()
        wrapped.stop()
        wrapped.start()
        # Readiness = the SECOND 'Started Kafka API server' log line (pinned).
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            logs = wrapped.logs().decode(errors="replace")
            if logs.count("Started Kafka API server") >= 2:
                break
            await asyncio.sleep(0.5)
        else:  # pragma: no cover - diagnostics
            pytest.fail("Redpanda did not come back after restart")

        # The same store's appends resume.
        from ksqlite.errors import ChangelogProduceError

        append_deadline = time.monotonic() + 30
        while True:
            try:
                await store.append(
                    source=source, entity_key="after-restart", payload={"i": n}
                )
                break
            except ChangelogProduceError:
                if time.monotonic() >= append_deadline:  # pragma: no cover
                    raise
                await asyncio.sleep(0.5)
        await store.stop()

        # A fresh instance cold-rebuilds everything from the restarted broker.
        async with make_store(tmp_path / "fresh.db", bootstrap) as fresh:
            await fresh.on_partitions_assigned([source])
            rows = await fresh.query("SELECT COUNT(*) AS n FROM records")
            assert rows[0]["n"] == n + 1
    finally:
        container.stop()


class _Host:
    """A host application: its OWN consumer + listener wired to the store
    (the §4 integration handshake), consuming the source topic and calling
    ``store.append`` per record.
    """

    def __init__(self, store: KSQLite, bootstrap: str, topic: str, group: str) -> None:
        self.store = store
        self.topic = topic
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=group,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        host = self

        class Listener(aiokafka.ConsumerRebalanceListener):  # type: ignore[misc]
            async def on_partitions_assigned(self, assigned: Any) -> None:
                await host.store.on_partitions_assigned(assigned)

            async def on_partitions_revoked(self, revoked: Any) -> None:
                await host.store.on_partitions_revoked(revoked)

        await self.consumer.start()
        self.consumer.subscribe([self.topic], listener=Listener())
        self._running = True
        self._task = asyncio.ensure_future(self._consume_loop())

    async def _consume_loop(self) -> None:
        while self._running:
            batches = await self.consumer.getmany(timeout_ms=200)
            for tp, records in batches.items():
                for record in records:
                    await self.store.append(
                        source=TopicPartition(record.topic, record.partition),
                        entity_key=record.key.decode(),
                        payload=json.loads(record.value),
                    )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            await self._task
        await self.consumer.stop()

    async def owned_partitions(self) -> set[int]:
        rows = await self.store.query("SELECT DISTINCT source_partition FROM records")
        return {int(r["source_partition"]) for r in rows}


@pytest.mark.timeout(120)
async def test_e2e04_real_consumer_group(
    broker: Any, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Two consumers in a REAL group: quiesce before the membership change
    (at-most-once means in-flight records are legitimately lost otherwise);
    assert disjoint-union assignment + row conservation, never specific
    placements; the eager no-op re-reveal exercises the fast path.
    """
    caplog.set_level(logging.INFO, logger="ksqlite.lifecycle")
    bootstrap = broker.get_bootstrap_server()
    topic = unique("group-src")
    group = f"g{uuid.uuid4().hex[:8]}"
    n = 12

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(topic, num_partitions=2, replication_factor=1)]
        )
    finally:
        await admin.close()

    store1 = make_store(tmp_path / "one.db", bootstrap)
    store2 = make_store(tmp_path / "two.db", bootstrap)
    await store1.start()
    await store2.start()
    host1 = _Host(store1, bootstrap, topic, group)
    host2 = _Host(store2, bootstrap, topic, group)

    try:
        await host1.start()

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            for i in range(n):
                await producer.send_and_wait(
                    topic,
                    key=f"e{i}".encode(),
                    value=json.dumps({"i": i}).encode(),
                    partition=i % 2,
                )
        finally:
            await producer.stop()

        async def total_visible() -> int:
            counts = 0
            for store in (store1, store2):
                rows = await store.query("SELECT COUNT(*) AS n FROM records")
                counts += int(rows[0]["n"])
            return counts

        # QUIESCE: host1 alone holds all N before the membership change.
        deadline = time.monotonic() + 30
        while await total_visible() < n:
            if time.monotonic() >= deadline:  # pragma: no cover
                pytest.fail("quiesce: host1 never materialized all records")
            await asyncio.sleep(0.2)

        # Membership change: host2 joins; the group rebalances eagerly.
        caplog.clear()
        await host2.start()

        deadline = time.monotonic() + 45
        while True:
            owned1 = {
                tp.partition
                for tp, st in store1.partition_states().items()
                if st.state.name in ("READY", "READY_PARTIAL")
            }
            owned2 = {
                tp.partition
                for tp, st in store2.partition_states().items()
                if st.state.name in ("READY", "READY_PARTIAL")
            }
            if (
                len(owned1) == 1  # a real 1+1 split: forces waiting for the
                and len(owned2) == 1  # rebalance (empty sets are vacuously
                and owned1 | owned2 == {0, 1}  # disjoint)
                and await total_visible() == n
            ):
                break
            if time.monotonic() >= deadline:  # pragma: no cover
                pytest.fail(
                    f"rebalance never converged: {owned1=} {owned2=},"
                    f" visible={await total_visible()}"
                )
            await asyncio.sleep(0.3)

        # Disjoint union + row conservation (never specific placements).
        assert owned1 | owned2 == {0, 1}
        assert owned1.isdisjoint(owned2)
        assert await total_visible() == n

        # The eager re-reveal of an unchanged partition is a zero-replay
        # fast path (assert via the structured logs).
        zero_replays = [
            r
            for r in caplog.records
            if getattr(r, "event", None) == "rehydrate_end"
            and getattr(r, "records_replayed", None) == 0
        ]
        observed = [
            (getattr(r, "event", None), getattr(r, "records_replayed", None))
            for r in caplog.records
            if str(getattr(r, "event", "")).startswith("rehydrate")
        ]
        assert zero_replays, f"no fast-path re-reveal after rebalance: {observed}"
    finally:
        await host1.stop()
        await host2.stop()
        await store1.stop()
        await store2.stop()
