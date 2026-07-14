"""Conformance-suite environments (plan §4.3): one abstraction, two
implementations — the in-memory fakes (always) and real aiokafka clients
against the pinned Redpanda container (under ``-m e2e``).
"""

import uuid
from typing import Any

from tests.support import fakes


class FakeKafkaEnv:
    name = "fake"

    def __init__(self) -> None:
        self.kafka = fakes.InMemoryKafka()

    def unique_topic(self, prefix: str) -> str:
        return f"{prefix}.{uuid.uuid4().hex[:12]}"

    async def create_topic(
        self, name: str, *, partitions: int = 1, configs: dict[str, str] | None = None
    ) -> None:
        self.kafka.create_topic(
            name, num_partitions=partitions, configs=dict(configs or {})
        )

    def new_producer(self) -> Any:
        return fakes.FakeProducer(self.kafka)

    def new_consumer(self, **kwargs: Any) -> Any:
        return fakes.FakeConsumer(self.kafka, **kwargs)

    def new_admin(self) -> Any:
        return fakes.FakeAdmin(self.kafka)

    def consumer_type(self) -> Any:
        return fakes.FakeConsumer

    def producer_type(self) -> Any:
        return fakes.FakeProducer


class RealKafkaEnv:
    name = "real"

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers

    def unique_topic(self, prefix: str) -> str:
        return f"{prefix}.{uuid.uuid4().hex[:12]}"

    async def create_topic(
        self, name: str, *, partitions: int = 1, configs: dict[str, str] | None = None
    ) -> None:
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic

        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            await admin.create_topics(
                [
                    NewTopic(
                        name,
                        num_partitions=partitions,
                        replication_factor=1,
                        topic_configs=dict(configs or {}),
                    )
                ]
            )
        finally:
            await admin.close()

    def new_producer(self) -> Any:
        from aiokafka import AIOKafkaProducer

        return AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def new_consumer(self, **kwargs: Any) -> Any:
        from aiokafka import AIOKafkaConsumer

        return AIOKafkaConsumer(bootstrap_servers=self.bootstrap_servers, **kwargs)

    def new_admin(self) -> Any:
        from aiokafka.admin import AIOKafkaAdminClient

        return AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def consumer_type(self) -> Any:
        from aiokafka import AIOKafkaConsumer

        return AIOKafkaConsumer

    def producer_type(self) -> Any:
        from aiokafka import AIOKafkaProducer

        return AIOKafkaProducer
