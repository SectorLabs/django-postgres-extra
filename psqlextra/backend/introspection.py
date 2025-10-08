from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from django.db.backends.postgresql.introspection import (  # type: ignore[import]
    DatabaseIntrospection,
)

from psqlextra.types import PostgresPartitioningMethod

from . import base_impl

PARTITIONING_STRATEGY_TO_METHOD = {
    "r": PostgresPartitioningMethod.RANGE,
    "l": PostgresPartitioningMethod.LIST,
    "h": PostgresPartitioningMethod.HASH,
}


@dataclass
class PostgresIntrospectedPartitionTable:
    """Data container for information about a partition."""

    name: str
    full_name: str
    comment: Optional[str]


@dataclass
class PostgresIntrospectedPartitonedTable:
    """Data container for information about a partitioned table."""

    name: str
    method: PostgresPartitioningMethod
    key: List[str]
    partitions: List[PostgresIntrospectedPartitionTable]

    def partition_by_name(
        self, name: str
    ) -> Optional[PostgresIntrospectedPartitionTable]:
        """Finds the partition with the specified name."""

        return next(
            (
                partition
                for partition in self.partitions
                if partition.name == name
            ),
            None,
        )


if TYPE_CHECKING:

    class Introspection(DatabaseIntrospection):
        pass

else:
    Introspection = base_impl.introspection()


class PostgresIntrospection(Introspection):
    """Adds introspection features specific to PostgreSQL."""

    # TODO: This class is a mess, both here and in the
    # the base.
    #
    # Some methods return untyped dicts, some named tuples,
    # some flat lists of strings. It's horribly inconsistent.
    #
    # Most methods are poorly named. For example; `get_table_description`
    # does not return a complete table description. It merely returns
    # the columns.
    #
    # We do our best in this class to stay consistent with
    # the base in Django by respecting its naming scheme
    # and commonly used return types. Creating an API that
    # matches the look&feel from the Django base class
    # is more important than fixing those issues.

    def get_partitioned_tables(
        self, cursor
    ) -> List[PostgresIntrospectedPartitonedTable]:
        """Gets a list of partitioned tables."""

        cursor.execute(
            """
            SELECT
                pg_class.relname,
                pg_partitioned_table.partstrat
            FROM
                pg_partitioned_table
            JOIN
                pg_class
            ON
                pg_class.oid = pg_partitioned_table.partrelid
            ORDER BY
                pg_partitioned_table.partrelid
        """
        )

        return [
            PostgresIntrospectedPartitonedTable(
                name=row[0],
                method=PARTITIONING_STRATEGY_TO_METHOD[row[1]],
                key=self.get_partition_key(cursor, row[0]),
                partitions=self.get_partitions(cursor, row[0]),
            )
            for row in cursor.fetchall()
        ]

    def get_partitioned_table(self, cursor, table_name: str):
        """Gets a single partitioned table."""

        return next(
            (
                table
                for table in self.get_partitioned_tables(cursor)
                if table.name == table_name
            ),
            None,
        )

    def get_partitions(
        self, cursor, table_name
    ) -> List[PostgresIntrospectedPartitionTable]:
        """Gets a list of partitions belonging to the specified partitioned
        table."""

        sql = """
            SELECT
                child.relname,
                pg_description.description
            FROM pg_inherits
            JOIN
                pg_class parent
            ON
                pg_inherits.inhparent = parent.oid
            JOIN
                pg_class child
            ON
                pg_inherits.inhrelid = child.oid
            JOIN
                pg_namespace nmsp_parent
            ON
                nmsp_parent.oid = parent.relnamespace
            JOIN
                pg_namespace nmsp_child
            ON
                nmsp_child.oid = child.relnamespace
            LEFT JOIN
                pg_description
            ON
                pg_description.objoid = child.oid
            WHERE
                parent.relname = %s
            ORDER BY
                child.oid,
                child.relname
        """

        cursor.execute(sql, (table_name,))

        return [
            PostgresIntrospectedPartitionTable(
                name=row[0].replace(f"{table_name}_", ""),
                full_name=row[0],
                comment=row[1] or None,
            )
            for row in cursor.fetchall()
        ]

    def get_partition_key(self, cursor, table_name: str) -> List[str]:
        """Gets the partition key for the specified partitioned table.

        Returns:
            A list of column names that are part of the
            partition key.
        """

        sql = """
            SELECT
                col.column_name
            FROM
                (SELECT partrelid,
                        partnatts,
                        CASE partstrat
                            WHEN 'l' THEN 'list'
                            WHEN 'r' THEN 'range'
                            WHEN 'h' THEN 'hash'
                        END AS partition_strategy,
                        Unnest(partattrs) column_index
                 FROM pg_partitioned_table) pt
            JOIN
                pg_class par
            ON par.oid = pt.partrelid
            JOIN
                information_schema.COLUMNS col
            ON
                col.table_schema = par.relnamespace :: regnamespace :: text
                AND col.table_name = par.relname
                AND ordinal_position = pt.column_index
            WHERE
                table_name = %s
            ORDER BY
                col.ordinal_position,
                col.column_name
        """

        cursor.execute(sql, (table_name,))
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, cursor, table_name: str):
        return self.get_table_description(cursor, table_name)

    def get_schema_list(self, cursor) -> List[str]:
        """A flat list of available schemas."""

        cursor.execute(
            """
            SELECT
                schema_name
            FROM
                information_schema.schemata
            ORDER BY
                schema_name,
                catalog_name
            """,
            tuple(),
        )

        return [name for name, in cursor.fetchall()]

    def get_constraints(self, cursor, table_name: str):
        """Retrieve any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Also retrieve the definition of expression-based indexes.
        """

        constraints = super().get_constraints(cursor, table_name)

        # standard Django implementation does not return the definition
        # for indexes, only for constraints, let's patch that up
        cursor.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s",
            (table_name,),
        )
        for index_name, definition in cursor.fetchall():
            # PostgreSQL 13 or older won't give a definition if the
            # index is actually a primary key.
            constraint = constraints.get(index_name)
            if not constraint:
                continue

            if constraint.get("definition") is None:
                constraint["definition"] = definition

        return constraints

    def get_table_locks(self, cursor) -> List[Tuple[str, str, str]]:
        cursor.execute(
            """
            SELECT
                n.nspname,
                t.relname,
                l.mode
            FROM pg_locks l
            INNER JOIN pg_class t ON t.oid = l.relation
            INNER JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relnamespace >= 2200
            ORDER BY n.nspname, t.relname, l.mode
        """
        )

        return cursor.fetchall()

    def get_storage_settings(self, cursor, table_name: str) -> Dict[str, str]:
        sql = """
            SELECT
                unnest(c.reloptions || array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x))
            FROM
                pg_catalog.pg_class c
            LEFT JOIN
                pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
            LEFT JOIN
                pg_catalog.pg_am am ON (c.relam = am.oid)
            WHERE
                c.relname::text = %s
                AND pg_catalog.pg_table_is_visible(c.oid)
        """

        cursor.execute(sql, (table_name,))

        storage_settings = {}
        for row in cursor.fetchall():
            # It's hard to believe, but storage settings are really
            # represented as `key=value` strings in Postgres.
            # See: https://www.postgresql.org/docs/current/catalog-pg-class.html
            name, value = row[0].split("=")
            storage_settings[name] = value

        return storage_settings

    def get_relations(self, cursor, table_name: str):
        """Gets a dictionary {field_name: (field_name_other_table,
        other_table)} representing all relations in the specified table.

        This is overriden because the query in Django does not handle
        relations between tables in different schemas properly.
        """

        cursor.execute(
            """
            SELECT a1.attname, c2.relname, a2.attname
            FROM pg_constraint con
            LEFT JOIN pg_class c1 ON con.conrelid = c1.oid
            LEFT JOIN pg_class c2 ON con.confrelid = c2.oid
            LEFT JOIN pg_attribute a1 ON c1.oid = a1.attrelid AND a1.attnum = con.conkey[1]
            LEFT JOIN pg_attribute a2 ON c2.oid = a2.attrelid AND a2.attnum = con.confkey[1]
            WHERE
                con.conrelid = %s::regclass AND
                con.contype = 'f' AND
                pg_catalog.pg_table_is_visible(c1.oid)
        """,
            [table_name],
        )
        return {row[0]: (row[2], row[1]) for row in cursor.fetchall()}
