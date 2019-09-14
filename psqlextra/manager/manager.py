from itertools import chain
from typing import Dict, Iterable, List, Tuple, Union

import django

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, SuspiciousOperation
from django.db import models, transaction
from django.db.models.fields import NOT_PROVIDED
from django.db.models.sql import UpdateQuery
from django.db.models.sql.constants import CURSOR

from psqlextra import signals
from psqlextra.compiler import (
    PostgresInsertCompiler,
    PostgresReturningUpdateCompiler,
)
from psqlextra.query import ConflictAction, PostgresInsertQuery, PostgresQuery


class PostgresQuerySet(models.QuerySet):
    """Adds support for PostgreSQL specifics."""

    def __init__(self, model=None, query=None, using=None, hints=None):
        """Initializes a new instance of :see:PostgresQuerySet."""

        super().__init__(model, query, using, hints)

        self.query = query or PostgresQuery(self.model)

        self.conflict_target = None
        self.conflict_action = None
        self.index_predicate = None

    def annotate(self, **annotations):
        """Custom version of the standard annotate function
        that allows using field names as annotated fields.

        Normally, the annotate function doesn't allow you
        to use the name of an existing field on the model
        as the alias name. This version of the function does
        allow that.
        """

        fields = {field.name: field for field in self.model._meta.get_fields()}

        # temporarily rename the fields that have the same
        # name as a field name, we'll rename them back after
        # the function in the base class ran
        new_annotations = {}
        renames = {}
        for name, value in annotations.items():
            if name in fields:
                new_name = "%s_new" % name
                new_annotations[new_name] = value
                renames[new_name] = name
            else:
                new_annotations[name] = value

        # run the base class's annotate function
        result = super().annotate(**new_annotations)

        # rename the annotations back to as specified
        result.rename_annotations(**renames)
        return result

    def rename_annotations(self, **annotations):
        """Renames the aliases for the specified annotations:

            .annotate(myfield=F('somestuf__myfield'))
            .rename_annotations(myfield='field')

        Arguments:
            annotations:
                The annotations to rename. Mapping the
                old name to the new name.
        """

        self.query.rename_annotations(annotations)
        return self

    def join(self, **conditions):
        """Adds extra conditions to existing joins.

        WARNING: This is an extremely experimental feature.
                 DO NOT USE unless you know what you're doing.
        """

        self.query.add_join_conditions(conditions)
        return self

    def update(self, **fields):
        """Updates all rows that match the filter."""

        # build up the query to execute
        self._for_write = True
        if django.VERSION >= (2, 0):
            query = self.query.chain(UpdateQuery)
        else:
            query = self.query.clone(UpdateQuery)
        query._annotations = None
        query.add_update_values(fields)

        # build the compiler for for the query
        connection = django.db.connections[self.db]
        compiler = PostgresReturningUpdateCompiler(query, connection, self.db)

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

    def on_conflict(
        self,
        fields: List[Union[str, Tuple[str]]],
        action,
        index_predicate: str = None,
    ):
        """Sets the action to take when conflicts arise when attempting
        to insert/create a new row.

        Arguments:
            fields:
                The fields the conflicts can occur in.

            action:
                The action to take when the conflict occurs.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)
        """

        self.conflict_target = fields
        self.conflict_action = action
        self.index_predicate = index_predicate

        return self

    def bulk_insert(self, rows, return_model=False):
        """Creates multiple new records in the database.

        This allows specifying custom conflict behavior using .on_conflict().
        If no special behavior was specified, this uses the normal Django create(..)

        Arguments:
            rows:
                An iterable of dictionaries, where each dictionary
                describes the fields to insert.

            return_model (default: False):
                If model instances should be returned rather than
                just dicts.

        Returns:
            A list of either the dicts of the rows inserted, including the pk or
            the models of the rows inserted with defaults for any fields not specified
        """

        if self.conflict_target or self.conflict_action:
            deduped_rows = rows

            # when we do a ConflictAction.NOTHING, we are actually
            # doing a ON CONFLICT DO UPDATE with a trick to avoid
            # touching conflicting rows... however, ON CONFLICT UPDATE
            # barfs when you specify the exact same row twice:
            #
            # > "cannot affect row a second time"
            #
            # we filter out the duplicates here to make sure we maintain
            # the same behaviour as the real ON CONFLICT DO NOTHING
            if self.conflict_action == ConflictAction.NOTHING:
                deduped_rows = []
                for row in rows:
                    if row in deduped_rows:
                        continue

                    deduped_rows.append(row)

            compiler = self._build_insert_compiler(deduped_rows)
            objs = compiler.execute_sql(return_id=True)
            if return_model:
                return [
                    self.model(**dict(row, **obj))
                    for row, obj in zip(deduped_rows, objs)
                ]
            else:
                return [
                    dict(row, **obj) for row, obj in zip(deduped_rows, objs)
                ]

        # no special action required, use the standard Django bulk_create(..)
        return super().bulk_create([self.model(**fields) for fields in rows])

    def insert(self, **fields):
        """Creates a new record in the database.

        This allows specifying custom conflict behavior using .on_conflict().
        If no special behavior was specified, this uses the normal Django create(..)

        Arguments:
            fields:
                The fields of the row to create.

        Returns:
            The primary key of the record that was created.
        """

        if self.conflict_target or self.conflict_action:
            compiler = self._build_insert_compiler([fields])
            rows = compiler.execute_sql(return_id=True)

            pk_field_name = self.model._meta.pk.name
            return rows[0][pk_field_name]

        # no special action required, use the standard Django create(..)
        return super().create(**fields).pk

    def insert_and_get(self, **fields):
        """Creates a new record in the database and then gets
        the entire row.

        This allows specifying custom conflict behavior using .on_conflict().
        If no special behavior was specified, this uses the normal Django create(..)

        Arguments:
            fields:
                The fields of the row to create.

        Returns:
            The model instance representing the row that was created.
        """

        if not self.conflict_target and not self.conflict_action:
            # no special action required, use the standard Django create(..)
            return super().create(**fields)

        compiler = self._build_insert_compiler([fields])
        rows = compiler.execute_sql(return_id=False)

        if not rows:
            return None

        columns = rows[0]

        # get a list of columns that are officially part of the model and
        # preserve the fact that the attribute name
        # might be different than the database column name
        model_columns = {}
        for field in self.model._meta.local_concrete_fields:
            model_columns[field.column] = field.attname

        # strip out any columns/fields returned by the db that
        # are not present in the model
        model_init_fields = {}
        for column_name, column_value in columns.items():
            try:
                model_init_fields[model_columns[column_name]] = column_value
            except KeyError:
                pass

        return self.model(**model_init_fields)

    def upsert(
        self, conflict_target: List, fields: Dict, index_predicate: str = None
    ) -> int:
        """Creates a new record or updates the existing one
        with the specified data.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)

        Returns:
            The primary key of the row that was created/updated.
        """

        self.on_conflict(
            conflict_target, ConflictAction.UPDATE, index_predicate
        )
        return self.insert(**fields)

    def upsert_and_get(
        self, conflict_target: List, fields: Dict, index_predicate: str = None
    ):
        """Creates a new record or updates the existing one
        with the specified data and then gets the row.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)

        Returns:
            The model instance representing the row
            that was created/updated.
        """

        self.on_conflict(
            conflict_target, ConflictAction.UPDATE, index_predicate
        )
        return self.insert_and_get(**fields)

    def bulk_upsert(
        self,
        conflict_target: List,
        rows: Iterable[Dict],
        index_predicate: str = None,
        return_model: bool = False,
    ):
        """Creates a set of new records or updates the existing
        ones with the specified data.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            rows:
                Rows to upsert.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)

            return_model (default: False):
                If model instances should be returned rather than
                just dicts.

        Returns:
            A list of either the dicts of the rows upserted, including the pk or
            the models of the rows upserted
        """

        def is_empty(r):
            return all([False for _ in r])

        if not rows or is_empty(rows):
            return []

        self.on_conflict(
            conflict_target, ConflictAction.UPDATE, index_predicate
        )
        return self.bulk_insert(rows, return_model)

    def _build_insert_compiler(self, rows: Iterable[Dict]):
        """Builds the SQL compiler for a insert query.

        Arguments:
            rows:
                An iterable of dictionaries, where each entry
                describes a record to insert.

        Returns:
            The SQL compiler for the insert.
        """

        # create model objects, we also have to detect cases
        # such as:
        #   [dict(first_name='swen'), dict(fist_name='swen', last_name='kooij')]
        # we need to be certain that each row specifies the exact same
        # amount of fields/columns
        objs = []
        rows_iter = iter(rows)
        first_row = next(rows_iter)
        field_count = len(first_row)
        for index, row in enumerate(chain([first_row], rows_iter)):
            if field_count != len(row):
                raise SuspiciousOperation(
                    (
                        "In bulk upserts, you cannot have rows with different field "
                        "configurations. Row {0} has a different field config than "
                        "the first row."
                    ).format(index)
                )

            objs.append(self.model(**row))

        # indicate this query is going to perform write
        self._for_write = True

        # get the fields to be used during update/insert
        insert_fields, update_fields = self._get_upsert_fields(first_row)

        # build a normal insert query
        query = PostgresInsertQuery(self.model)
        query.conflict_action = self.conflict_action
        query.conflict_target = self.conflict_target
        query.index_predicate = self.index_predicate
        query.values(objs, insert_fields, update_fields)

        # use the postgresql insert query compiler to transform the insert
        # into an special postgresql insert
        connection = django.db.connections[self.db]
        compiler = PostgresInsertCompiler(query, connection, self.db)

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
            has_default = field.default != NOT_PROVIDED
            if field.name in kwargs or field.column in kwargs:
                insert_fields.append(field)
                update_fields.append(field)
                continue
            elif has_default:
                insert_fields.append(field)
                continue

            # special handling for 'pk' which always refers to
            # the primary key, so if we the user specifies `pk`
            # instead of a concrete field, we have to handle that
            if field.primary_key is True and "pk" in kwargs:
                insert_fields.append(field)
                update_fields.append(field)
                continue

            if self._is_magical_field(model_instance, field, is_insert=True):
                insert_fields.append(field)

            if self._is_magical_field(model_instance, field, is_insert=False):
                update_fields.append(field)

        return insert_fields, update_fields


class PostgresManager(models.Manager):
    """Adds support for PostgreSQL specifics."""

    use_in_migrations = True

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:PostgresManager."""

        super(PostgresManager, self).__init__(*args, **kwargs)

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

        # hook into django signals to then trigger our own

        django.db.models.signals.post_save.connect(
            self._on_model_save, sender=self.model, weak=False
        )

        django.db.models.signals.pre_delete.connect(
            self._on_model_delete, sender=self.model, weak=False
        )

        self._signals_connected = True

    def __del__(self):
        """Disconnects signals."""

        if self._signals_connected is False:
            return

        # django.db.models.signals.post_save.disconnect(
        #     self._on_model_save, sender=self.model)

        # django.db.models.signals.pre_delete.disconnect(
        #     self._on_model_delete, sender=self.model)

    def get_queryset(self):
        """Gets the query set to be used on this manager."""

        return PostgresQuerySet(self.model, using=self._db)

    def on_conflict(
        self,
        fields: List[Union[str, Tuple[str]]],
        action,
        index_predicate: str = None,
    ):
        """Sets the action to take when conflicts arise when attempting
        to insert/create a new row.

        Arguments:
            fields:
                The fields the conflicts can occur in.

            action:
                The action to take when the conflict occurs.

            index_predicate:
                The index predicate to satisfy an arbiter partial index.
        """
        return self.get_queryset().on_conflict(fields, action, index_predicate)

    def upsert(
        self, conflict_target: List, fields: Dict, index_predicate: str = None
    ) -> int:
        """Creates a new record or updates the existing one
        with the specified data.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

            index_predicate:
                The index predicate to satisfy an arbiter partial index.

        Returns:
            The primary key of the row that was created/updated.
        """

        return self.get_queryset().upsert(
            conflict_target, fields, index_predicate
        )

    def upsert_and_get(
        self, conflict_target: List, fields: Dict, index_predicate: str = None
    ):
        """Creates a new record or updates the existing one
        with the specified data and then gets the row.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

            index_predicate:
                The index predicate to satisfy an arbiter partial index.

        Returns:
            The model instance representing the row
            that was created/updated.
        """

        return self.get_queryset().upsert_and_get(
            conflict_target, fields, index_predicate
        )

    def bulk_upsert(
        self,
        conflict_target: List,
        rows: Iterable[Dict],
        index_predicate: str = None,
        return_model: bool = False,
    ):
        """Creates a set of new records or updates the existing
        ones with the specified data.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            index_predicate:
                The index predicate to satisfy an arbiter partial index.

            rows:
                Rows to upsert.

            return_model (default: False):
                If model instances should be returned rather than
                just dicts.

        Returns:
            A list of either the dicts of the rows upserted, including the pk or
            the models of the rows upserted
        """

        return self.get_queryset().bulk_upsert(
            conflict_target, rows, index_predicate, return_model
        )

    @staticmethod
    def _on_model_save(sender, **kwargs):
        """When a model gets created or updated."""

        created, instance = kwargs["created"], kwargs["instance"]

        if created:
            signals.create.send(sender, pk=instance.pk)
        else:
            signals.update.send(sender, pk=instance.pk)

    @staticmethod
    def _on_model_delete(sender, **kwargs):
        """When a model gets deleted."""

        instance = kwargs["instance"]
        signals.delete.send(sender, pk=instance.pk)
