"""uuidv7 ``message_id`` source (spec §6; plan §3 ``_ids``).

Only *uniqueness* is load-bearing; v7 is for index locality. Sourced from
uuid-utils' stdlib-compatible generator — Rust-backed (faster than a
pure-Python backport), returns a stdlib ``uuid.UUID``, and ships wheels for
every supported Python (3.10–3.14), so no version branching is needed.
"""

from uuid_utils.compat import uuid7


def new_message_id() -> str:
    """Return a new uuidv7 as its canonical 36-char string (spec §10)."""
    return str(uuid7())
