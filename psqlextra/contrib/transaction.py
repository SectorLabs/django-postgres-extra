from contextlib import contextmanager
from typing import Optional

from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db import DEFAULT_DB_ALIAS, connections


def _is_in_test():
    return (
        getattr(settings, "TEST_MODE", False)
        or getattr(settings, "TESTING", False)
        or getattr(settings, "TEST", False)
    )


@contextmanager
def no_transaction(*, why: str, using: Optional[str] = None):
    """Prevents a method or a block from running in a database transaction."""

    # During tests, allow one level of transaction.atomic(..) nesting
    # because tests themselves run in a transaction. If there's only
    # one level of nesting, it's from the test itself and the code
    # would actually run without a transaction outside the test.

    connection = connections[using or DEFAULT_DB_ALIAS]

    if connection.in_atomic_block and not (
        _is_in_test() and len(connection.savepoint_ids) <= 1
    ):
        raise SuspiciousOperation(f"Unexpected database transaction: {why}")

    yield
