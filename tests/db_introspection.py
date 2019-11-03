"""Thin wrappers over functions in connection.introspection that don't require
creating a cursor.

This makes test code less verbose and easier to read/write.
"""

from django.db import connection


def table_names(include_views: bool = True):
    """Gets a flat list of tables in the default database."""

    with connection.cursor() as cursor:
        introspection = connection.introspection
        return introspection.table_names(cursor, include_views)


def get_partitioned_table(table_name: str):
    """Gets the definition of a partitioned table in the default database."""

    with connection.cursor() as cursor:
        introspection = connection.introspection
        return introspection.get_partitioned_table(cursor, table_name)


def get_partitions(table_name: str):
    """Gets a list of partitions for the specified partitioned table in the
    default database."""

    with connection.cursor() as cursor:
        introspection = connection.introspection
        return introspection.get_partitions(cursor, table_name)


def get_constraints(table_name: str):
    """Gets a complete list of constraints and indexes for the specified
    table."""

    with connection.cursor() as cursor:
        introspection = connection.introspection
        return introspection.get_constraints(cursor, table_name)
