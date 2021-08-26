from dataclasses import dataclass
from typing import List, Optional

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
        """

        cursor.execute(sql, (table_name,))
        return [row[0] for row in cursor.fetchall()]

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
        for index, definition in cursor.fetchall():
            if constraints[index].get("definition") is None:
                constraints[index]["definition"] = definition

        return constraints
