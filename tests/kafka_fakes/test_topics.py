"""P8 `_topics` tests: T-01..T-09 (plan §5 P8). Fakes + real semantics.

T-07 (fast path still runs step 0) lives with the RH suite — it needs the
lifecycle's assign path.
"""

from aiokafka import TopicPartition

from ksqlite import _topics
from tests.support.fakes import FakeAdmin, InMemoryKafka

SOURCE = TopicPartition("messages", 3)
TEMPLATE = "{source_topic}.p{partition}.changelog"


def make_service(
    admin: FakeAdmin,
    *,
    create_topics_retention_ms: int | None = None,
    verify_changelog_policy: bool = True,
    template: str = TEMPLATE,
) -> _topics.ChangelogTopics:
    return _topics.ChangelogTopics(
        admin=admin,
        template=template,
        create_topics_retention_ms=create_topics_retention_ms,
        verify_changelog_policy=verify_changelog_policy,
    )


async def test_t01_step0_creates_missing_topic() -> None:
    kafka = InMemoryKafka()
    service = make_service(FakeAdmin(kafka), create_topics_retention_ms=-1)

    name = await service.ensure_and_verify(SOURCE)

    assert name == "messages.p3.changelog"
    assert kafka.topic_exists(name)
    assert kafka.topic_partitions(name) == 1  # single-partition (spec §10)
    config = kafka.topic_config(name)
    assert config["cleanup.policy"] == "delete"
    assert config["retention.ms"] == "-1"


async def test_t02_step0_missing_topic_without_autocreate() -> None:
    import pytest

    from ksqlite.errors import TopicNotFoundError

    kafka = InMemoryKafka()
    service = make_service(FakeAdmin(kafka), create_topics_retention_ms=None)

    with pytest.raises(TopicNotFoundError):
        await service.ensure_and_verify(SOURCE)
    assert not kafka.topic_exists("messages.p3.changelog")


async def test_t03_step0_compact_policy_rejected() -> None:
    """Compaction would collapse an entity to its last message (spec §10);
    the value may be `compact,delete`, hence the substring match.
    """
    import pytest

    from ksqlite.errors import ConfigError

    for policy in ("compact", "compact,delete"):
        kafka = InMemoryKafka()
        kafka.create_topic(
            "messages.p3.changelog",
            num_partitions=1,
            configs={"cleanup.policy": policy},
        )
        service = make_service(FakeAdmin(kafka))
        with pytest.raises(ConfigError):
            await service.ensure_and_verify(SOURCE)

    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p3.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "delete"},
    )
    await make_service(FakeAdmin(kafka)).ensure_and_verify(SOURCE)  # passes


def _acl_denied_service() -> tuple[FakeAdmin, "_topics.ChangelogTopics"]:
    from aiokafka.errors import TopicAuthorizationFailedError

    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p3.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "compact"},  # unverifiable without the ACL
    )
    admin = FakeAdmin(kafka, describe_fail_with=TopicAuthorizationFailedError("no ACL"))
    return admin, make_service(admin)


async def test_t04_step0_acl_degrade_warns_and_proceeds(
    caplog: object,
) -> None:
    import logging

    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)
    _admin, service = _acl_denied_service()

    with caplog.at_level(logging.WARNING, logger="ksqlite.topics"):
        await service.ensure_and_verify(SOURCE)  # "could not verify" != wrong

    events = [(r.name, getattr(r, "event", None)) for r in caplog.records]
    assert ("ksqlite.topics", "policy_verify_unauthorized") in events


async def test_t04b_acl_degrade_cached_as_attempted(caplog: object) -> None:
    import logging

    import pytest

    assert isinstance(caplog, pytest.LogCaptureFixture)
    admin, service = _acl_denied_service()
    await service.ensure_and_verify(SOURCE)
    calls_after_first = admin.describe_calls

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="ksqlite.topics"):
        await service.ensure_and_verify(SOURCE)  # second assignment

    assert admin.describe_calls == calls_after_first  # no second describe
    assert not caplog.records  # warn once per TP per store lifetime


async def test_t05_success_cached_per_tp() -> None:
    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p3.changelog", num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    admin = FakeAdmin(kafka)
    service = make_service(admin)

    await service.ensure_and_verify(SOURCE)
    assert admin.describe_calls == 1
    await service.ensure_and_verify(SOURCE)  # second assignment of the SAME TP
    assert admin.describe_calls == 1  # no re-verify


async def test_t05b_cache_is_per_tp_not_global() -> None:
    import pytest

    from ksqlite.errors import ConfigError

    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p0.changelog", num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    kafka.create_topic(
        "messages.p1.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "compact"},
    )
    service = make_service(FakeAdmin(kafka))

    await service.ensure_and_verify(TopicPartition("messages", 0))  # verified

    with pytest.raises(ConfigError):  # (messages,1) must still be checked
        await service.ensure_and_verify(TopicPartition("messages", 1))


async def test_t06_verify_false_skips_describe() -> None:
    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p3.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "compact"},
    )
    admin = FakeAdmin(kafka)
    service = make_service(admin, verify_changelog_policy=False)

    await service.ensure_and_verify(SOURCE)  # proceeds despite compact
    assert admin.describe_calls == 0  # LOCKED opt-out exercised


async def test_t08_failure_not_cached() -> None:
    import pytest

    from ksqlite.errors import ConfigError

    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p3.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "compact"},
    )
    service = make_service(FakeAdmin(kafka))

    with pytest.raises(ConfigError):
        await service.ensure_and_verify(SOURCE)

    # Ops fixes the policy; the next assignment re-verifies and succeeds.
    kafka.create_topic(
        "messages.p3.changelog",
        num_partitions=1,
        configs={"cleanup.policy": "delete"},
    )
    await service.ensure_and_verify(SOURCE)


async def test_t09_resolution_time_name_validation() -> None:
    import pytest

    from ksqlite.errors import ConfigError

    kafka = InMemoryKafka()
    illegal = make_service(
        FakeAdmin(kafka), template="{source_topic}!!.p{partition}.changelog"
    )
    with pytest.raises(ConfigError):
        await illegal.ensure_and_verify(SOURCE)

    too_long = make_service(
        FakeAdmin(kafka), template="x" * 250 + ".{source_topic}.p{partition}"
    )
    with pytest.raises(ConfigError):
        await too_long.ensure_and_verify(SOURCE)


async def test_t_extra_broker_failure_maps_to_rehydrate_error() -> None:
    """Spec §7 step 0: any non-authz broker failure (describe/create timeout,
    connection loss) raises RehydrateError.
    """
    import pytest
    from aiokafka.errors import KafkaError

    from ksqlite.errors import RehydrateError

    kafka = InMemoryKafka()
    kafka.create_topic(
        "messages.p3.changelog", num_partitions=1, configs={"cleanup.policy": "delete"}
    )
    admin = FakeAdmin(kafka, describe_fail_with=KafkaError("connection lost"))
    service = make_service(admin)

    with pytest.raises(RehydrateError):
        await service.ensure_and_verify(SOURCE)

    # And a failed step 0 is NOT cached: a later attempt re-runs it.
    admin._describe_fail_with = None
    await service.ensure_and_verify(SOURCE)
    assert admin.describe_calls == 2
