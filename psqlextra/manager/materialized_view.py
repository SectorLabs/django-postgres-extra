from typing import Dict

from django.db import connection
from django.db.models.query import QuerySet

from .manager import PostgresManager


class PostgresMaterializedViewManager(PostgresManager):
    """Special manager for materialized views."""

    def context(self) -> Dict[str, str]:
        """Gets dictionary to be passed to queries."""

        return dict(
            table_name=connection.ops.quote_name(self.model._meta.db_table),
            index_name=connection.ops.quote_name('%s_index' % self.model._meta.db_table),
            pk_column_name=self.model._meta.pk.db_column or self.model._meta.pk.name,
            query=self._get_query()
        )

    def create(self, recreate=False) -> None:
        """Creates the materialized view if it doesn't exist yet."""

        if recreate:
            self.drop()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}
                AS {query}
                """.format(**self.context())
            )

            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                ON {table_name} ({pk_column_name})
                """.format(**self.context())
            )

    def drop(self) -> None:
        """Drops the materialized view if it exists."""
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP MATERIALIZED VIEW IF EXISTS {table_name}
                """.format(**self.context())
            )

    def refresh(self, recreate: bool=False, concurrently: bool=True) -> None:
        """Creates and/or refreshes the materialized view.

        Arguments:
            recreate:
                If set to true, will drop the view if it
                exists and then re-create it.

            concurrently:
                Indicates whether this materialized view
                should be refreshed in the background.
        """

        # won't touch it, if it already exists, unless recreate=True
        self.create(recreate=recreate)

        with connection.cursor() as cursor:
            concurrent = 'CONCURRENTLY' if concurrently else ''
            cursor.execute(
                """
                REFRESH MATERIALIZED VIEW {concurrently} {table_name}
                """.format(**self.context(), concurrently=concurrent)
            )

    def _get_query(self) -> str:
        """Gets the raw SQL query to use for this view."""

        view_query = self.model._meta.view_query

        if isinstance(view_query, str):
            return view_query

        if isinstance(view_query, QuerySet):
            return str(view_query.query)

        return view_query
