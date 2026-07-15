"""The ChangelogTopics service (plan §3 ``_topics``; spec §7 step 0, §10).

An INSTANCE: the verified-TP cache is instance state and dies with the
store. Implements ``ChangelogNameResolver``.
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
    for_code,
)

from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

from . import _sql
from .errors import RehydrateError, TopicNotFoundError

logger = logging.getLogger("ksqlite.topics")

# describe_configs reports a missing DESCRIBE_CONFIGS ACL as one of these
# per-resource error codes (it does NOT raise the exceptions).
_AUTHZ_ERROR_CODES = frozenset(
    {TopicAuthorizationFailedError.errno, ClusterAuthorizationFailedError.errno}
)


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
                        "create_topics_retention_ms is None"
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

        Best-effort (spec §10): a per-resource error code means "could not
        verify" (warn + proceed, never claim verified), and a compact policy
        is reported loudly (ERROR) but never fails the assignment — topic
        config is ops' domain. Connection-level broker failures still raise
        (``KafkaError`` → ``RehydrateError`` in ``ensure_and_verify``).
        """
        responses = await self._admin.describe_configs(
            [ConfigResource(ConfigResourceType.TOPIC, name)]
        )
        for response in responses:
            for resource in response.resources:
                error_code, error_message, _rtype, _rname, entries = resource
                if error_code != 0:
                    # aiokafka reports per-resource failures (authorization
                    # denials included) as error codes with EMPTY config
                    # entries — it never raises them. "Could not verify" !=
                    # "verified": warn and proceed, cached-as-attempted.
                    event = (
                        "policy_verify_unauthorized"
                        if error_code in _AUTHZ_ERROR_CODES
                        else "policy_verify_failed"
                    )
                    logger.warning(
                        "cannot verify cleanup.policy of %r (error code %s:"
                        " %s); proceeding unverified",
                        name,
                        error_code,
                        error_message,
                        extra={
                            "event": event,
                            "topic": name,
                            "error_code": error_code,
                        },
                    )
                    return
                for entry in entries:
                    if entry[0] == "cleanup.policy" and "compact" in str(entry[1]):
                        logger.error(
                            "changelog topic %r has cleanup.policy=%r;"
                            " compaction collapses an entity to its last"
                            " message — rebuilds from this changelog can"
                            " lose history",
                            name,
                            entry[1],
                            extra={
                                "event": "compact_policy_detected",
                                "topic": name,
                                "policy": entry[1],
                            },
                        )
                        return
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
        response = await self._admin.create_topics([topic])
        # Per-topic outcomes are error codes ON THE RESPONSE — aiokafka's
        # create_topics never raises them (verified against 0.14; contrast
        # its create_partitions, which does walk topic_errors). Discarding
        # them would cache a nonexistent changelog as created, never to be
        # retried — a silent durability hole in the source of truth.
        # Entries are (topic, error_code) in response v0, (topic, error_code,
        # error_message) in v1+.
        for entry in response.topic_errors:
            error_code = entry[1]
            if error_code == 0:
                continue
            if error_code == TopicAlreadyExistsError.errno:
                # A concurrent create counts as success (spec §7 step 0).
                return
            message = entry[2] if len(entry) > 2 else None
            raise for_code(error_code)(
                f"creating changelog topic {name!r} failed: {message}"
            )
        logger.info(
            "created changelog topic %r",
            name,
            extra={"event": "topic_created", "topic": name},
        )
