from django.db import models
from django.conf import settings
from django.db.models.sql import InsertQuery
from django.core.exceptions import ImproperlyConfigured
import django

from .compiler import PostgresSQLUpsertCompiler


class PostgresManager(models.Manager):
    """Adds support for PostgreSQL specifics."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:PostgresManager."""

        super(PostgresManager, self).__init__(*args, **kwargs)

        # make sure our back-end is set and refuse to proceed
        # if it's not set
        db_backend = settings.DATABASES['default']['ENGINE']
        if 'psqlextra' not in db_backend:
            raise ImproperlyConfigured((
                '\'%s\' is not the \'psqlextra.backend\'. '
                'django-postgres-extra cannot function without '
                'the \'psqlextra.backend\'. Set DATABASES.ENGINE.'
            ) % db_backend)

    def upsert(self, **kwargs) -> int:
        """Creates a new record or updates the existing one
        with the specified data.

        Arguments:
            kwargs:
                Fields to insert/update.

        Returns:
            The primary key of the row that was created/updated.
        """

        fields = [
            field
            for field in self.model._meta.local_concrete_fields
            if field is not self.model._meta.auto_field and field.column in kwargs
        ]

        # indicate this query is going to perform write
        self._for_write = True

        # create an empty object to store the result in
        obj = self.model(**kwargs)

        # build a normal insert query
        query = InsertQuery(self.model)
        query.insert_values(fields, [obj], raw=False)

        # use the upsert query compiler to transform the insert
        # into an upsert so that it will overwrite an existing row
        connection = django.db.connections[self.db]
        compiler = PostgresSQLUpsertCompiler(query, connection, self.db)

        # execute the query to the database
        return compiler.execute_sql(True)

    def upsert_and_get(self, **kwargs):
        """Creates a new record or updates the existing one
        with the specified data and then gets the row.

        Arguments:
            kwargs:
                Fields to insert/update.

        Returns:
            The model instance representing the row
            that was created/updated.
        """

        pk = self.upsert(**kwargs)
        return self.get(pk=pk)
