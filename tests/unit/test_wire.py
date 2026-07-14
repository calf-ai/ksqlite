"""P5 `_wire` tests: WF-01..WF-07 (plan §5 P5). Pure; no I/O."""

import json
from collections.abc import Sequence

import pytest

from ksqlite import _wire

MID = "01890a5d-ac96-774b-bcce-b302099a8057"  # a valid 36-char uuidv7 string


def test_wf01_encode() -> None:
    record = _wire.encode(entity_key="task-42", payload={"a": 1}, message_id=MID)

    assert record.key == b"task-42"
    assert isinstance(record.value, bytes)
    assert json.loads(record.value.decode("utf-8")) == {"a": 1}
    # Header keys are str, values bytes (aiokafka carries only bytes; the
    # fakes and CF-05 pin the same shape). A list, as the producer requires.
    assert record.headers == [
        ("format_version", b"1"),
        ("message_id", MID.encode("utf-8")),
    ]

    unicode_record = _wire.encode(entity_key="tâsk-🚀", payload={}, message_id=MID)
    assert unicode_record.key == "tâsk-🚀".encode()


def test_wf02_decode_round_trip() -> None:
    record = _wire.encode(entity_key="tâsk-🚀", payload={"x": "y"}, message_id=MID)

    result = _wire.classify(key=record.key, value=record.value, headers=record.headers)

    assert isinstance(result, _wire.KsqliteRecord)
    assert result.message_id == MID
    assert result.entity_key == "tâsk-🚀"
    assert json.loads(result.payload) == {"x": "y"}


def test_wf03_classification_foreign() -> None:
    """It can't be a KSQLite record — a foreign producer wrote it. Skipped
    (and, at the lifecycle layer, logged + checkpointed; spec §7).
    """
    good = _wire.encode(entity_key="k", payload={}, message_id=MID)

    cases: list[tuple[str, bytes | None, bytes | None, Sequence[tuple[str, bytes]]]] = [
        ("no headers at all", good.key, good.value, []),
        (
            "message_id decodes but is not a UUID",
            good.key,
            good.value,
            [("format_version", b"1"), ("message_id", b"not-a-uuid")],
        ),
        (
            "message_id is not UTF-8",
            good.key,
            good.value,
            [("format_version", b"1"), ("message_id", b"\xff\xfe")],
        ),
        ("null key", None, good.value, good.headers),
        ("undecodable key", b"\xff\xfe", good.value, good.headers),
        ("null value", good.key, None, good.headers),
        ("undecodable value", good.key, b"\xff\xfe", good.headers),
    ]
    for label, key, value, headers in cases:
        result = _wire.classify(key=key, value=value, headers=headers)
        assert isinstance(result, _wire.ForeignRecord), label


def test_wf04_classification_unknown_version() -> None:
    """A valid message_id marks it as a KSQLite record; an unrecognized
    format_version means this reader can't understand it (rolling upgrade/
    downgrade) — never silently dropped (spec §7).
    """
    good = _wire.encode(entity_key="k", payload={}, message_id=MID)

    result = _wire.classify(
        key=good.key,
        value=good.value,
        headers=[("format_version", b"2"), ("message_id", MID.encode("utf-8"))],
    )

    assert isinstance(result, _wire.UnknownVersionRecord)
    assert result.format_version == "2"
    assert result.message_id == MID


def test_wf07_missing_version_with_valid_message_id_is_unknown_version() -> None:
    """It carries the KSQLite marker (valid message_id), so a missing
    format_version must fail loud too — an inline classifier that skips it
    as foreign silently loses data (spec §7; plan RH-06 re-pins at P8).
    """
    good = _wire.encode(entity_key="k", payload={}, message_id=MID)

    result = _wire.classify(
        key=good.key,
        value=good.value,
        headers=[("message_id", MID.encode("utf-8"))],  # no format_version
    )

    assert isinstance(result, _wire.UnknownVersionRecord)
    assert result.format_version is None
    assert result.message_id == MID


def test_wf05_payload_validated_pre_produce() -> None:
    """An unmaterializable payload durably in the changelog with valid
    headers would poison every future replay — validate BEFORE any produce
    (spec §6); the SQL-level ``json(?)`` is defense-in-depth only.
    """
    with pytest.raises(ValueError):
        _wire.encode(entity_key="k", payload=object(), message_id=MID)

    with pytest.raises(ValueError):
        _wire.encode(entity_key="k", payload="{not json", message_id=MID)

    assert _wire.encode(entity_key="k", payload={}, message_id=MID).value == b"{}"

    nested = _wire.encode(entity_key="k", payload={"a": {"b": None}}, message_id=MID)
    assert json.loads(nested.value) == {"a": {"b": None}}

    # A JSON-text str is validated by parse, then produced AS-IS — the value
    # is the pure host payload (spec §10; decisions log item 8).
    text = '{"a":   1}'
    as_is = _wire.encode(entity_key="k", payload=text, message_id=MID)
    assert as_is.value == text.encode("utf-8")


def test_wf05b_nan_and_infinity_rejected() -> None:
    """Python's defaults emit/accept non-standard NaN tokens: SQLite < 3.42
    rejects them on every replay (poisoned partition) and SQLite >= 3.42
    silently coerces to null (payload corruption). Rejected pre-produce in
    BOTH object and JSON-text forms (spec §6, live-verified).
    """
    for bad_object in (
        {"x": float("nan")},
        {"x": float("inf")},
        {"x": float("-inf")},
    ):
        with pytest.raises(ValueError):
            _wire.encode(entity_key="k", payload=bad_object, message_id=MID)

    for bad_text in ('{"x": NaN}', '{"x": Infinity}', '{"x": -Infinity}'):
        with pytest.raises(ValueError):
            _wire.encode(entity_key="k", payload=bad_text, message_id=MID)


def test_wf06_header_lookup_by_name() -> None:
    """Order-insensitive; unknown extra headers (e.g. tracing) are ignored
    (spec §7)."""
    good = _wire.encode(entity_key="k", payload={"x": 1}, message_id=MID)

    shuffled_with_extras = [
        ("trace_id", b"abc123"),  # unknown extra
        ("message_id", MID.encode("utf-8")),
        ("b3", b"\x00\xff"),  # unknown extra, not even UTF-8
        ("format_version", b"1"),
    ]

    result = _wire.classify(
        key=good.key, value=good.value, headers=shuffled_with_extras
    )
    assert isinstance(result, _wire.KsqliteRecord)
    assert result.message_id == MID
