from typing import Dict, List

import django
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import models, transaction
from django.db.models.sql import UpdateQuery
from django.db.models.sql.constants import CURSOR

from . import signals
from .compiler import (PostgresSQLReturningUpdateCompiler,
                       PostgresSQLUpsertCompiler)
from .query import PostgresUpsertQuery


class PostgresQuerySet(models.QuerySet):
    """Adds support for PostgreSQL specifics."""

    def update(self, **fields):
        """Updates all rows that match the filter."""

        # build up the query to execute
        self._for_write = True
        query = self.query.clone(UpdateQuery)
        query._annotations = None
        query.add_update_values(fields)

        # build the compiler for form the query
        connection = django.db.connections[self.db]
        compiler = PostgresSQLReturningUpdateCompiler(query, connection, self.db)

        # execute the query
        with transaction.atomic(using=self.db, savepoint=False):
            rows = compiler.execute_sql(CURSOR)
        self._result_cache = None

        # send out a signal for each row
        for row in rows:
            signals.update.send(self.model, pk=row[0])

        # the original update(..) returns the amount of rows
        # affected, let's do the same
        return len(rows)


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

        # hook into django signals to then trigger our own

        def on_model_save(sender, **kwargs):
            """When a model gets created or updated."""

            created, instance = kwargs['created'], kwargs['instance']

            if created:
                signals.create.send(sender, pk=instance.pk)
            else:
                signals.update.send(sender, pk=instance.pk)

        django.db.models.signals.post_save.connect(
            on_model_save, sender=self.model, weak=False)

        def on_model_delete(sender, **kwargs):
            """When a model gets deleted."""

            instance = kwargs['instance']
            signals.delete.send(sender, pk=instance.pk)

        django.db.models.signals.pre_delete.connect(
            on_model_delete, sender=self.model, weak=False)

    def get_queryset(self):
        """Gets the query set to be used on this manager."""

        return PostgresQuerySet(self.model, using=self._db)

    def upsert(self, conflict_target: List, fields: Dict) -> int:
        """Creates a new record or updates the existing one
        with the specified data.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

        Returns:
            The primary key of the row that was created/updated.
        """

        compiler = self._build_upsert_compiler(conflict_target, fields)
        return compiler.execute_sql(return_id=True)['id']

    def upsert_and_get(self, conflict_target: List, fields: Dict):
        """Creates a new record or updates the existing one
        with the specified data and then gets the row.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

        Returns:
            The model instance representing the row
            that was created/updated.
        """

        compiler = self._build_upsert_compiler(conflict_target, fields)
        column_data = compiler.execute_sql(return_id=False)

        # get a list of columns that are officially part of the model
        model_columns = [
            field.column
            for field in self.model._meta.local_concrete_fields
        ]

        # strip out any columns/fields returned by the db that
        # are not present in the model
        model_init_fields = {}
        for column_name, column_value in column_data.items():
            if column_name not in model_columns:
                continue

            model_init_fields[column_name] = column_value

        return self.model(**model_init_fields)

    def _build_upsert_compiler(self, conflict_target: List, kwargs):
        """Builds the SQL compiler for a insert/update query.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

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
        query.conflict_target = conflict_target
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
            if field.name in kwargs or field.column in kwargs:
                insert_fields.append(field)
                update_fields.append(field)
                continue

            if self._is_magical_field(model_instance, field, is_insert=True):
                insert_fields.append(field)

            if self._is_magical_field(model_instance, field, is_insert=False):
                update_fields.append(field)

        return insert_fields, update_fields
