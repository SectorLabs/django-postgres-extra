from dataclasses import dataclass
from typing import List

from psqlextra.types import PostgresPartitioningMethod

from . import base_impl


@dataclass
class PostgresIntrospectedPartitionTable:
    """Data container for information about a partition."""

    name: str


@dataclass
class PostgresIntrospectedPartitonedTable:
    """Data container for information about a partitioned table."""

    name: str
    method: PostgresPartitioningMethod
    key: List[str]
    partitions: List[PostgresIntrospectedPartitionTable]


class PostgresIntrospection(base_impl.introspection()):
    """Adds introspection features specific to PostgreSQL."""

    def get_partitioned_tables(
        self, cursor
    ) -> PostgresIntrospectedPartitonedTable:
        """Gets a list of partitioned tables."""

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
        return [
            PostgresIntrospectedPartitionTable(name=row[0])
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
        """

        cursor.execute(sql, (table_name,))
        return [row[0] for row in cursor.fetchall()]
