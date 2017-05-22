from typing import Dict

from django.db import connection, models


class PostgresMaterializedView(models.Model):
    """Base class for PostgreSQL materialized views."""

    class Meta:
        abstract = True

    @classmethod
    def context(cls) -> Dict[str, str]:
        """Gets dictionary to be passed to queries."""
        return dict(
            table_name=connection.ops.quote_name(cls._meta.db_table),
            index_name=connection.ops.quote_name('%s_index' % cls._meta.db_table),
            query=str(cls.queryset.query),
            index_columns=','.join(cls._meta.unique_together[0])
        )

    @classmethod
    def drop(cls) -> None:
        """Drops the materialized view if it exists."""
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP MATERIALIZED VIEW IF EXISTS {table_name}
                """.format(**cls.context())
            )

    @classmethod
    def refresh(cls, recreate: bool=False) -> None:
        """Creates and/or refreshes the materialized view.

        Arguments:
            recreate:
                If set to true, will drop the view if it
                exists and then re-create it.
        """
        if recreate:
            cls.drop()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}
                AS {query}
                """.format(**cls.context())
            )

            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                ON {table_name} ({index_columns})
                """.format(**cls.context())
            )

            cursor.execute(
                """
                REFRESH MATERIALIZED VIEW CONCURRENTLY {table_name}
                """.format(**cls.context())
            )
