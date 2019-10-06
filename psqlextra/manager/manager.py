from typing import Optional

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.db.models import Manager

from psqlextra.query import PostgresQuerySet


class PostgresManager(Manager.from_queryset(PostgresQuerySet)):
    """Adds support for PostgreSQL specifics."""

    use_in_migrations = True

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:PostgresManager."""

        super().__init__(*args, **kwargs)

        # make sure our back-end is set and refuse to proceed
        # if it's not set
        db_backend = settings.DATABASES["default"]["ENGINE"]
        if "psqlextra" not in db_backend:
            raise ImproperlyConfigured(
                (
                    "'%s' is not the 'psqlextra.backend'. "
                    "django-postgres-extra cannot function without "
                    "the 'psqlextra.backend'. Set DATABASES.ENGINE."
                )
                % db_backend
            )

    def truncate(
        self, cascade: bool = False, using: Optional[str] = None
    ) -> None:
        """Truncates this model/table using the TRUNCATE statement.

        This DELETES ALL ROWS. No signals will be fired.

        See: https://www.postgresql.org/docs/9.1/sql-truncate.html

        Arguments:
            cascade:
                Whether to delete dependent rows. If set to
                False, an error will be raised if there
                are rows in other tables referencing
                the rows you're trying to delete.
        """

        connection = connections[using or "default"]
        table_name = connection.ops.quote_name(self.model._meta.db_table)

        with connection.cursor() as cursor:
            sql = "TRUNCATE TABLE %s" % table_name
            if cascade:
                sql += " CASCADE"

            cursor.execute(sql)
