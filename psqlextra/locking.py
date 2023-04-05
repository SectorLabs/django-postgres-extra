from enum import Enum
from typing import Optional, Type

from django.db import DEFAULT_DB_ALIAS, connections, models


class PostgresTableLockMode(Enum):
    """List of table locking modes.

    See: https://www.postgresql.org/docs/current/explicit-locking.html
    """

    ACCESS_SHARE = "ACCESS SHARE"
    ROW_SHARE = "ROW SHARE"
    ROW_EXCLUSIVE = "ROW EXCLUSIVE"
    SHARE_UPDATE_EXCLUSIVE = "SHARE UPDATE EXCLUSIVE"
    SHARE = "SHARE"
    SHARE_ROW_EXCLUSIVE = "SHARE ROW EXCLUSIVE"
    EXCLUSIVE = "EXCLUSIVE"
    ACCESS_EXCLUSIVE = "ACCESS EXCLUSIVE"

    @property
    def alias(self) -> str:
        return (
            "".join([word.title() for word in self.name.lower().split("_")])
            + "Lock"
        )


def postgres_lock_table(
    table_name: str,
    lock_mode: PostgresTableLockMode,
    *,
    schema_name: Optional[str] = None,
    using: str = DEFAULT_DB_ALIAS,
) -> None:
    """Locks the specified table with the specified mode.

    The lock is held until the end of the current transaction.

    Arguments:
        table_name:
            Unquoted table name to acquire the lock on.

        lock_mode:
            Type of lock to acquire.

        schema_name:
            Optionally, the unquoted name of the schema
            the table to lock is in. If not specified,
            the table name is resolved by PostgreSQL
            using it's ``search_path``.

        using:
            Optional name of the database connection to use.
    """

    connection = connections[using]

    with connection.cursor() as cursor:
        quoted_fqn = connection.ops.quote_name(table_name)
        if schema_name:
            quoted_fqn = (
                connection.ops.quote_name(schema_name) + "." + quoted_fqn
            )

        cursor.execute(f"LOCK TABLE {quoted_fqn} IN {lock_mode.value} MODE")


def postgres_lock_model(
    model: Type[models.Model],
    lock_mode: PostgresTableLockMode,
    *,
    using: str = DEFAULT_DB_ALIAS,
    schema_name: Optional[str] = None,
) -> None:
    """Locks the specified model with the specified mode.

    The lock is held until the end of the current transaction.

    Arguments:
        model:
            The model of which to lock the table.

        lock_mode:
            Type of lock to acquire.

        schema_name:
            Optionally, the unquoted name of the schema
            the table to lock is in. If not specified,
            the table name is resolved by PostgreSQL
            using it's ``search_path``.

            Django models always reside in the default
            ("public") schema. You should not specify
            this unless you're doing something special.

        using:
            Optional name of the database connection to use.
    """

    postgres_lock_table(
        model._meta.db_table, lock_mode, schema_name=schema_name, using=using
    )
