from dataclasses import dataclass

from psqlextra.types import PostgresPartitioningMethod

from . import base_impl


@dataclass
class PostgresIntrospectedPartitonedTable:
    """Data container for informatino about
    a partitioned table."""

    name: str
    method: PostgresPartitioningMethod


class PostgresIntrospection(base_impl.introspection()):
    """Adds introspection features specific to
    PostgreSQL."""

    def get_partition_names(self, cursor, table_name):
        """Gets the names of partitions for the
        specified partitioned table name."""

        sql = """
            SELECT
                child.relname
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
            WHERE
                parent.relname = %s
        """

        cursor.execute(sql, (table_name,))
        return [row[0] for row in cursor.fetchall()]

    def get_partitioned_tables(self, cursor):
        sql = """
            SELECT
                pg_class.relname,
                pg_partitioned_table.partstrat
            FROM
                pg_partitioned_table
            JOIN
                pg_class
            ON
                pg_class.oid = pg_partitioned_table.partrelid
        """

        cursor.execute(sql)

        return [
            PostgresIntrospectedPartitonedTable(
                name=row[0],
                method=PostgresPartitioningMethod.RANGE
                if row[1] == "r"
                else PostgresPartitioningMethod.LIST,
            )
            for row in cursor.fetchall()
        ]
