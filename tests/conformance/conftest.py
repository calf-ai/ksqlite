"""Conformance fixtures (plan §4.3): the parametrized fake/real environment.

The `broker` session fixture lives in the top-level tests/conftest.py; it is
only requested lazily in real mode, so plain (non-e2e) runs never touch
Docker.
"""

from collections.abc import AsyncIterator

import pytest

from tests.support.kafka_env import FakeKafkaEnv, RealKafkaEnv


@pytest.fixture(params=["fake", pytest.param("real", marks=pytest.mark.e2e)])
async def kafka_env(
    request: pytest.FixtureRequest,
) -> AsyncIterator[FakeKafkaEnv | RealKafkaEnv]:
    if request.param == "fake":
        yield FakeKafkaEnv()
    else:  # pragma: no cover - exercised under -m e2e
        broker = request.getfixturevalue("broker")
        yield RealKafkaEnv(broker.get_bootstrap_server())
