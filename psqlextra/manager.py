import copy

from django.db import models
from django.conf import settings
from django.db.models.sql import InsertQuery
from django.core.exceptions import ImproperlyConfigured
import django

from .query import PostgresUpsertQuery
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

        compiler = self._build_upsert_compiler(kwargs)
        return compiler.execute_sql(return_id=True)

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

        compiler = self._build_upsert_compiler(kwargs)
        row = compiler.execute_sql(return_id=False)
        field_names = [f.name for f in self.model._meta.concrete_fields]
        return self.model.from_db(self.db, field_names, row)

    def _build_upsert_compiler(self, kwargs):
        """Builds the SQL compiler for a insert/update query.

        Arguments:
            kwargs:
                Field values.

            return_id:
                Indicates whether to return just
                the id or the entire row.

        Returns:
            The SQL compiler for the upsert.
        """

        # create an empty object to store the result in
        obj = self.model(**kwargs)

        # indicate this query is going to perform write
        self._for_write = True

        # get the fields to be used during update/insert
        insert_fields, update_fields = self._get_upsert_fields(kwargs)

        # build a normal insert query
        query = PostgresUpsertQuery(self.model)
        query.values([obj], insert_fields, update_fields)

        # use the upsert query compiler to transform the insert
        # into an upsert so that it will overwrite an existing row
        connection = django.db.connections[self.db]
        compiler = PostgresSQLUpsertCompiler(query, connection, self.db)

        return compiler

    def _is_magical_field(self, model_instance, field, is_insert: bool):
        """Verifies whether this field is gonna modify something
        on its own.

        "Magical" means that a field modifies the field value
        during the pre_save.

        Arguments:
            model_instance:
                The model instance the field is defined on.

            field:
                The field to get of whether the field is
                magical.

            is_insert:
                Pretend whether this is an insert?

        Returns:
            True when this field modifies something.
        """

        # does this field modify someting upon insert?
        old_value = getattr(model_instance, field.name, None)
        field.pre_save(model_instance, is_insert)
        new_value = getattr(model_instance, field.name, None)

        return old_value != new_value

    def _get_upsert_fields(self, kwargs):
        """Gets the fields to use in an upsert.

        This some nice magic. We'll split the fields into
        a group of "insert fields" and "update fields":

        INSERT INTO bla ("val1", "val2") ON CONFLICT DO UPDATE SET val1 = EXCLUDED.val1

                         ^^^^^^^^^^^^^^                            ^^^^^^^^^^^^^^^^^^^^
                         insert_fields                                 update_fields

        Often, fields appear in both lists. But, for example,
        a :see:DateTime field with `auto_now_add=True` set, will
        only appear in "insert_fields", since it won't be set
        on existing rows.

        Other than that, the user specificies a list of fields
        in the upsert() call. That migt not be all fields. The
        user could decide to leave out optional fields. If we
        end up doing an update, we don't want to overwrite
        those non-specified fields.

        We cannot just take the list of fields the user
        specifies, because as mentioned, some fields
        make modifications to the model on their own.

        We'll have to detect which fields make modifications
        and include them in the list of insert/update fields.
        """

        model_instance = self.model(**kwargs)
        insert_fields = []
        update_fields = []

        for field in model_instance._meta.local_concrete_fields:
            if field.name in kwargs:
                insert_fields.append(field)
                update_fields.append(field)
                continue

            if self._is_magical_field(model_instance, field, is_insert=True):
                insert_fields.append(field)

            if self._is_magical_field(model_instance, field, is_insert=False):
                update_fields.append(field)

        return insert_fields, update_fields
