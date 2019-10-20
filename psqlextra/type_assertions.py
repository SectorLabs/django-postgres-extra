from collections.abc import Iterable
from typing import Any

from django.db.models.query import QuerySet


def is_query_set(value: Any) -> bool:
    """Gets whether the specified value is a :see:QuerySet."""

    return isinstance(value, QuerySet)


def is_sql(value: Any) -> bool:
    """Gets whether the specified value could be a raw SQL query."""

    return isinstance(value, str)


def is_sql_with_params(value: Any) -> bool:
    """Gets whether the specified value is a tuple of a SQL query (as a string)
    and a tuple of bind parameters."""

    return (
        isinstance(value, tuple)
        and len(value) == 2
        and is_sql(value[0])
        and isinstance(value[1], Iterable)
        and not isinstance(value[1], (str, bytes, bytearray))
    )
