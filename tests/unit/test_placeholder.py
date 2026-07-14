"""P0 placeholder test (plan §5 P0).

Ensures the suite always collects at least one test (plain pytest exits 5 on
zero collected tests) and pins the sole P0 production behavior: the library
attaches a NullHandler to its root logger so hosts without logging config
never see "No handlers could be found" warnings.
"""

import logging

import ksqlite  # noqa: F401  (importing the package wires the handler)


def test_package_root_logger_has_null_handler() -> None:
    handlers = logging.getLogger("ksqlite").handlers
    assert any(isinstance(h, logging.NullHandler) for h in handlers)
