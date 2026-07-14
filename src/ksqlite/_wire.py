"""Wire encode/decode + record classification (plan §3 ``_wire``; spec §6,
§7, §10). Pure module, grown red-first by WF-01..WF-07.
"""

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

FORMAT_VERSION = "1"


@dataclass(frozen=True)
class KsqliteRecord:
    """A well-formed KSQLite changelog record (spec §7 step 3)."""

    message_id: str
    entity_key: str
    payload: str


@dataclass(frozen=True)
class UnknownVersionRecord:
    """Carries a valid ``message_id`` (so it IS a KSQLite record) but a
    ``format_version`` this reader can't understand — fails loud as
    ``RehydrateError`` at the lifecycle layer, never silently dropped
    (spec §7).
    """

    message_id: str
    format_version: str | None


@dataclass(frozen=True)
class ForeignRecord:
    """Not a KSQLite record — a foreign producer wrote it. Skip + log; the
    skipped offset still advances the checkpoint (spec §7).
    """

    reason: str


def classify(
    *,
    key: bytes | None,
    value: bytes | None,
    headers: Sequence[tuple[str, bytes]],
) -> KsqliteRecord | ForeignRecord | UnknownVersionRecord:
    """Reconstruct a replayed record per the spec §7 classification rules.

    Headers are looked up BY NAME (order-insensitive; unknown extras
    ignored).
    """
    header_map = dict(headers)

    message_id_bytes = header_map.get("message_id")
    if message_id_bytes is None:
        return ForeignRecord("missing message_id header")
    try:
        message_id = message_id_bytes.decode("utf-8")
        uuid.UUID(message_id)
    except ValueError:  # UnicodeDecodeError is a ValueError
        return ForeignRecord("malformed message_id header")

    version_bytes = header_map.get("format_version")
    if version_bytes is None:
        # Valid message_id marks it as ours; a missing version still fails
        # loud (WF-07) — skipping it as foreign would silently lose data.
        return UnknownVersionRecord(message_id=message_id, format_version=None)
    format_version = version_bytes.decode("utf-8", errors="replace")
    if format_version != FORMAT_VERSION:
        return UnknownVersionRecord(
            message_id=message_id, format_version=format_version
        )

    if key is None:
        return ForeignRecord("null record key")
    try:
        entity_key = key.decode("utf-8")
    except UnicodeDecodeError:
        return ForeignRecord("undecodable record key")

    if value is None:
        return ForeignRecord("null record value")
    try:
        payload = value.decode("utf-8")
    except UnicodeDecodeError:
        return ForeignRecord("undecodable record value")

    return KsqliteRecord(
        message_id=message_id,
        entity_key=entity_key,
        payload=payload,
    )


@dataclass(frozen=True)
class EncodedRecord:
    """What ``append`` produces to the changelog (spec §10 wire format)."""

    key: bytes
    value: bytes
    headers: list[tuple[str, bytes]]


def serialize_payload(payload: object) -> str:
    """Serialize + validate the payload BEFORE any produce (spec §6).

    Accepted: a JSON-serializable object, or a ``str`` that must parse as
    JSON — validated, then passed through AS-IS (the changelog value is the
    pure host payload, spec §10). ``ValueError`` on anything else.
    """
    if isinstance(payload, str):
        try:
            # parse_constant fires exactly on NaN/Infinity/-Infinity, which
            # Python otherwise ACCEPTS despite them not being JSON (WF-05b).
            json.loads(payload, parse_constant=_reject_nonstandard_constant)
        except ValueError as exc:
            raise ValueError(f"payload string is not valid JSON: {exc}") from exc
        return payload
    try:
        # allow_nan=False: Python otherwise EMITS non-standard NaN tokens.
        return json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"payload is not JSON-serializable: {exc}") from exc


def _reject_nonstandard_constant(token: str) -> None:
    raise ValueError(f"non-standard JSON constant {token!r} is not accepted")


def encode(*, entity_key: str, payload: object, message_id: str) -> EncodedRecord:
    """Encode one changelog record: key = ``entity_key`` bytes, value = the
    pure host payload as UTF-8 JSON text, headers = format_version +
    message_id (spec §10).
    """
    value = serialize_payload(payload)
    return EncodedRecord(
        key=entity_key.encode("utf-8"),
        value=value.encode("utf-8"),
        headers=[
            ("format_version", FORMAT_VERSION.encode("utf-8")),
            ("message_id", message_id.encode("utf-8")),
        ],
    )
