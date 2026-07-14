"""P1 `_ids` tests: L-05, L-06 (plan §5 P1)."""

import sys
import uuid

import pytest
import uuid6

from ksqlite import _ids


def test_l05_new_message_id_is_a_36_char_uuidv7_string() -> None:
    mid = _ids.new_message_id()
    assert isinstance(mid, str)
    assert len(mid) == 36
    assert uuid.UUID(mid).version == 7


def test_l05_new_message_id_unique_across_100k_calls() -> None:
    n = 100_000
    assert len({_ids.new_message_id() for _ in range(n)}) == n


def test_l06_uuid7_source_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    select = getattr(_ids, "_select_uuid7", None)
    assert select is not None, "_select_uuid7 missing"

    # Below 3.14 the pinned uuid6 backport is the source.
    assert select((3, 10)) is uuid6.uuid7
    assert select((3, 13)) is uuid6.uuid7

    # At/after 3.14 the stdlib generator is the source. It does not exist on
    # older runtimes, so pin it via monkeypatch (raising=False); on a real
    # >=3.14 runtime the patch simply replaces the genuine attribute.
    def sentinel() -> uuid.UUID:
        return uuid.UUID(int=0)

    monkeypatch.setattr(uuid, "uuid7", sentinel, raising=False)
    assert select((3, 14)) is sentinel

    # The module-level binding matches the interpreter actually running.
    expected_module = "uuid" if sys.version_info >= (3, 14) else "uuid6"
    assert _ids._uuid7.__module__ == expected_module
