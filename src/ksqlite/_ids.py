"""uuidv7 ``message_id`` source (spec §6; plan §3 ``_ids``).

Only *uniqueness* is load-bearing; v7 is for index locality. The stdlib
generator exists from Python 3.14; the pinned ``uuid6`` backport covers
older runtimes (and is only installed there — see its dependency marker).
"""

import sys
import uuid
from collections.abc import Callable


def _select_uuid7(version_info: tuple[int, int]) -> Callable[[], uuid.UUID]:
    if version_info >= (3, 14):
        # getattr: typeshed and older runtimes gate uuid.uuid7 behind 3.14.
        stdlib_uuid7: Callable[[], uuid.UUID] = getattr(uuid, "uuid7")
        return stdlib_uuid7
    from uuid6 import uuid7

    return uuid7


_uuid7 = _select_uuid7((sys.version_info[0], sys.version_info[1]))


def new_message_id() -> str:
    """Return a new uuidv7 as its canonical 36-char string (spec §10)."""
    return str(_uuid7())
