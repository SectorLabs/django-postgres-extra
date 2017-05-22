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
            pk_column_name=cls._meta.pk.db_column or cls._meta.pk.name,
            query=str(cls.queryset.query),
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
    def refresh(cls, recreate: bool=False, concurrently: bool=True) -> None:
        """Creates and/or refreshes the materialized view.

        Arguments:
            recreate:
                If set to true, will drop the view if it
                exists and then re-create it.

            concurrently:
                Indicates whether this materialized view
                should be refreshed in the background.
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
                ON {table_name} ({pk_column_name})
                """.format(**cls.context())
            )

            concurrent = 'CONCURRENTLY' if concurrently else ''
            cursor.execute(
                """
                REFRESH MATERIALIZED VIEW {concurrently} {table_name}
                """.format(**cls.context(), concurrently=concurrent)
            )
