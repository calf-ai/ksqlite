"""The ChangelogTopics service (plan §3 ``_topics``; spec §7 step 0, §10).

An INSTANCE: the verified-TP cache is instance state and dies with the
store. Implements ``ChangelogNameResolver``. Grown red-first by T-01..T-09.
"""

import logging
from typing import Any

from aiokafka import TopicPartition
from aiokafka.admin import NewTopic
from aiokafka.errors import (
    ClusterAuthorizationFailedError,
    KafkaError,
    TopicAlreadyExistsError,
    TopicAuthorizationFailedError,
)

from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

from . import _sql
from .errors import ConfigError, RehydrateError, TopicNotFoundError

logger = logging.getLogger("ksqlite.topics")


class ChangelogTopics:
    """Step-0 ensure/verify with per-TP caching (spec §7 step 0)."""

    def __init__(
        self,
        *,
        admin: Any,
        template: str,
        create_topics_retention_ms: int | None,
        verify_changelog_policy: bool,
    ) -> None:
        self._admin = admin
        self._template = template
        self._retention = create_topics_retention_ms
        self._verify = verify_changelog_policy
        self._checked: set[TopicPartition] = set()

    def changelog_name(self, source: TopicPartition) -> str:
        name = self._template.format(
            source_topic=source.topic, partition=source.partition
        )
        # Resolved names are validated at first resolution (spec §4; S-07).
        _sql.validate_topic_name(name)
        return name

    async def ensure_and_verify(self, source: TopicPartition) -> str:
        """Unconditional on the partition's first assignment; budget-exempt.

        Success is cached per TP for the store's lifetime; failures are NOT
        cached and re-run on the next assignment.
        """
        name = self.changelog_name(source)
        if source in self._checked:
            return name

        try:
            existing = await self._admin.list_topics()
            if name not in existing:
                if self._retention is None:
                    raise TopicNotFoundError(
                        f"changelog topic {name!r} does not exist and "
                        "create_topics_retention_ms is None (spec §7 step 0)"
                    )
                await self._create(name)

            if self._verify:
                await self._verify_policy(name)
        except KafkaError as exc:
            # Any other broker failure here (describe/create timeout,
            # connection loss) raises RehydrateError; NOT cached (spec §7).
            raise RehydrateError(f"step 0 failed for {name!r}: {exc}") from exc

        self._checked.add(source)
        return name

    async def _verify_policy(self, name: str) -> None:
        """Read ``cleanup.policy`` via describe_configs — walk the
        response's ``.resources`` config entries and substring-match
        ``compact`` (the value may be ``compact,delete``; spec §10).
        """
        try:
            responses = await self._admin.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, name)]
            )
        except (TopicAuthorizationFailedError, ClusterAuthorizationFailedError):
            # Missing DESCRIBE_CONFIGS ACL: "could not verify" != "policy is
            # wrong" — warn and proceed; cached-as-attempted (spec §7/§10).
            logger.warning(
                "cannot verify cleanup.policy of %r (missing DESCRIBE_CONFIGS "
                "ACL); proceeding unverified",
                name,
                extra={"event": "policy_verify_unauthorized", "topic": name},
            )
            return
        for response in responses:
            for resource in response.resources:
                _error_code, _msg, _rtype, _rname, entries = resource
                for entry in entries:
                    if entry[0] == "cleanup.policy" and "compact" in str(entry[1]):
                        logger.error(
                            "changelog topic %r has cleanup.policy=%r",
                            name,
                            entry[1],
                            extra={
                                "event": "compact_rejected",
                                "topic": name,
                                "policy": entry[1],
                            },
                        )
                        raise ConfigError(
                            f"changelog topic {name!r} has cleanup.policy="
                            f"{entry[1]!r}; compaction would collapse an "
                            "entity to its last message (spec §10)"
                        )
        logger.info(
            "verified cleanup.policy of %r",
            name,
            extra={"event": "topic_verified", "topic": name},
        )

    async def _create(self, name: str) -> None:
        topic = NewTopic(
            name,
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                "cleanup.policy": "delete",
                "retention.ms": str(self._retention),
            },
        )
        try:
            await self._admin.create_topics([topic])
        except TopicAlreadyExistsError:
            # A concurrent create counts as success (spec §7 step 0).
            return
        logger.info(
            "created changelog topic %r",
            name,
            extra={"event": "topic_created", "topic": name},
        )
