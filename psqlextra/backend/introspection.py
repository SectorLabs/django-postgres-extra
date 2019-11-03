import re

from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from django.db.utils import NotSupportedError, OperationalError

from psqlextra.types import PostgresPartitioningMethod

from . import base_impl


@dataclass
class PostgresIntrospectedPartitionTable:
    """Data container for information about a partition."""

    name: str
    full_name: str
    values: List[Union[str, datetime, int, float]]


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
                child.relname,
                pg_get_expr(child.relpartbound, child.oid)
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
        partitions = []

        for row in cursor.fetchall():
            full_name = row[0]
            name = full_name.replace(table_name + "_", "")
            values_expr = row[1]
            values = []

            # dragons be here!!
            # this is some really ugly stuff... there's no way to get
            # a the range or list of values used for a partition from postgres
            # other than parsing the expression postgres returns :(

            if "FOR VALUES FROM" in values_expr:
                values_matches = re.search(
                    r"\('?(.*)?'\) TO \('?(.*)?'\)", values_expr
                )
                if not values_matches:
                    raise OperationalError(
                        f"Cannot parse partition {table_name}'s boundaries"
                    )

                from_value = values_matches.group(1)
                to_value = values_matches.group(2)

                if not from_value or not to_value:
                    raise OperationalError(
                        f"{table_name}'s appears to be range partition, but cannot parse boundaries"
                    )

                values = [
                    self._parse_partition_value(from_value),
                    self._parse_partition_value(to_value),
                ]

            elif "FOR VALUES IN" in values_expr:
                values_matches = re.search(r"\((.*)\)", values_expr)
                if not values_matches:
                    raise OperationalError(
                        f"Cannot parse partition {table_name}'s boundaries"
                    )

                values = [
                    self._parse_partition_value(value)
                    for value in values_match.group(1)
                    .replace("'", "")
                    .split(",")
                ]
            else:
                raise NotSupportedError(
                    f"Partition {table_name} uses an unsupported partitioning method"
                )

            partitions.append(
                PostgresIntrospectedPartitionTable(
                    name=name, full_name=full_name, values=values
                )
            )

        return partitions

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

    @staticmethod
    def _parse_partition_value(value):
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S-%f")
        except ValueError:
            pass

        try:
            return int(value)
        except (ValueError, TypeError):
            pass

        try:
            return float(value)
        except (ValueError, TypeError):
            pass

        return value
