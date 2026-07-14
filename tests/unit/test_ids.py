"""P1 `_ids` tests: L-05 (plan §5 P1)."""

import uuid

from ksqlite import _ids


def test_l05_new_message_id_is_a_36_char_uuidv7_string() -> None:
    mid = _ids.new_message_id()
    assert isinstance(mid, str)
    assert len(mid) == 36
    parsed = uuid.UUID(mid)
    assert parsed.version == 7  # uuidv7 (RFC 9562)
    assert isinstance(parsed, uuid.UUID)  # stdlib UUID (uuid_utils.compat)


def test_l05_new_message_id_unique_across_100k_calls() -> None:
    n = 100_000
    assert len({_ids.new_message_id() for _ in range(n)}) == n
