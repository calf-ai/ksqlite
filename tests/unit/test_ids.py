"""P1 `_ids` tests: L-05, L-06 (plan §5 P1)."""

import sys
import uuid

import pytest

from ksqlite import _ids


def test_l05_new_message_id_is_a_36_char_uuidv7_string() -> None:
    mid = _ids.new_message_id()
    assert isinstance(mid, str)
    assert len(mid) == 36
    assert uuid.UUID(mid).version == 7


def test_l05_new_message_id_unique_across_100k_calls() -> None:
    n = 100_000
    assert len({_ids.new_message_id() for _ in range(n)}) == n


def test_l06_backport_is_the_source_below_3_14() -> None:
    # The backport is only installed below 3.14 (dependency marker), so this
    # half of L-06 can only run there.
    uuid6 = pytest.importorskip("uuid6", reason="uuid6 is only installed below 3.14")
    assert _ids._select_uuid7((3, 10)) is uuid6.uuid7
    assert _ids._select_uuid7((3, 13)) is uuid6.uuid7


def test_l06_stdlib_is_the_source_at_3_14(monkeypatch: pytest.MonkeyPatch) -> None:
    # uuid.uuid7 does not exist on older runtimes, so pin it via monkeypatch
    # (raising=False); on a real >=3.14 runtime the patch simply replaces the
    # genuine attribute.
    def sentinel() -> uuid.UUID:
        return uuid.UUID(int=0)

    monkeypatch.setattr(uuid, "uuid7", sentinel, raising=False)
    assert _ids._select_uuid7((3, 14)) is sentinel


def test_l06_module_binding_matches_the_running_interpreter() -> None:
    expected_module = "uuid" if sys.version_info >= (3, 14) else "uuid6"
    assert _ids._uuid7.__module__ == expected_module
