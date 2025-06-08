from contextlib import contextmanager
from typing import Generator, List, Optional, Union

from django.core.exceptions import SuspiciousOperation
from django.db import DEFAULT_DB_ALIAS, connections


@contextmanager
def postgres_set_local(
    *,
    using: str = DEFAULT_DB_ALIAS,
    **options: Optional[Union[str, int, float, List[str]]],
) -> Generator[None, None, None]:
    """Sets the specified PostgreSQL options using SET LOCAL so that they apply
    to the current transacton only.

    The effect is undone when the context manager exits.

    See
    https://www.postgresql.org/docs/current/runtime-config-client.html
    for an overview of all available options.
    """

    connection = connections[using]
    qn = connection.ops.quote_name

    if not connection.in_atomic_block:
        raise SuspiciousOperation(
            "SET LOCAL makes no sense outside a transaction. Start a transaction first."
        )

    sql = []
    params: List[Union[str, int, float, List[str]]] = []
    for name, value in options.items():
        if value is None:
            sql.append(f"SET LOCAL {qn(name)} TO DEFAULT")
            continue

        # Settings that accept a list of values are actually
        # stored as string lists. We cannot just pass a list
        # of values. We have to create the comma separated
        # string ourselves.
        if isinstance(value, list) or isinstance(value, tuple):
            placeholder = ", ".join(["%s" for _ in value])
            params.extend(value)
        else:
            placeholder = "%s"
            params.append(value)

        sql.append(f"SET LOCAL {qn(name)} = {placeholder}")

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT name, setting FROM pg_settings WHERE name = ANY(%s)",
            (list(options.keys()),),
        )
        original_values = dict(cursor.fetchall())
        cursor.execute("; ".join(sql), params)

    yield

    # Put everything back to how it was. DEFAULT is
    # not good enough as a outer SET LOCAL might
    # have set a different value.
    with connection.cursor() as cursor:
        sql = []
        params = []

        for name, value in options.items():
            original_value = original_values.get(name)
            if original_value:
                sql.append(f"SET LOCAL {qn(name)} = {original_value}")
            else:
                sql.append(f"SET LOCAL {qn(name)} TO DEFAULT")

        cursor.execute("; ".join(sql), params)


@contextmanager
def postgres_set_local_search_path(
    search_path: List[str], *, using: str = DEFAULT_DB_ALIAS
) -> Generator[None, None, None]:
    """Sets the search path to the specified schemas."""

    with postgres_set_local(search_path=search_path, using=using):
        yield


@contextmanager
def postgres_prepend_local_search_path(
    search_path: List[str], *, using: str = DEFAULT_DB_ALIAS
) -> Generator[None, None, None]:
    """Prepends the current local search path with the specified schemas."""

    connection = connections[using]

    with connection.cursor() as cursor:
        cursor.execute("SHOW search_path")
        [
            original_search_path,
        ] = cursor.fetchone()

        placeholders = ", ".join(["%s" for _ in search_path])
        cursor.execute(
            f"SET LOCAL search_path = {placeholders}, {original_search_path}",
            tuple(search_path),
        )

        yield

        cursor.execute(f"SET LOCAL search_path = {original_search_path}")


@contextmanager
def postgres_reset_local_search_path(
    *, using: str = DEFAULT_DB_ALIAS
) -> Generator[None, None, None]:
    """Resets the local search path to the default."""

    with postgres_set_local(search_path=None, using=using):
        yield
