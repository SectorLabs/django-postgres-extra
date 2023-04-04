"""Thin wrappers over functions in connection.introspection that don't require
creating a cursor.

This makes test code less verbose and easier to read/write.
"""

from contextlib import contextmanager
from typing import Optional

from django.db import connection


@contextmanager
def introspect(schema_name: Optional[str] = None):
    default_schema_name = connection.ops.default_schema_name()
    search_path = [schema_name or default_schema_name]

    with connection.introspection.in_search_path(search_path) as introspection:
        with connection.cursor() as cursor:
            yield introspection, cursor


def table_names(
    include_views: bool = True, *, schema_name: Optional[str] = None
):
    """Gets a flat list of tables in the default database."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.table_names(cursor, include_views)


def get_partitioned_table(
    table_name: str,
    *,
    schema_name: Optional[str] = None,
):
    """Gets the definition of a partitioned table in the default database."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_partitioned_table(cursor, table_name)


def get_partitions(
    table_name: str,
    *,
    schema_name: Optional[str] = None,
):
    """Gets a list of partitions for the specified partitioned table in the
    default database."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_partitions(cursor, table_name)


def get_columns(
    table_name: str,
    *,
    schema_name: Optional[str] = None,
):
    """Gets a list of columns for the specified table."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_columns(cursor, table_name)


def get_relations(
    table_name: str,
    *,
    schema_name: Optional[str] = None,
):
    """Gets a list of relations for the specified table."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_relations(cursor, table_name)


def get_constraints(
    table_name: str,
    *,
    schema_name: Optional[str] = None,
):
    """Gets a list of constraints and indexes for the specified table."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_constraints(cursor, table_name)


def get_sequences(
    table_name: str,
    *,
    schema_name: Optional[str] = None,
):
    """Gets a list of sequences own by the specified table."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_sequences(cursor, table_name)


def get_storage_settings(table_name: str, *, schema_name: Optional[str] = None):
    """Gets a list of all storage settings that have been set on the specified
    table."""

    with introspect(schema_name) as (introspection, cursor):
        return introspection.get_storage_settings(cursor, table_name)
