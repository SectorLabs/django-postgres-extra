from collections import OrderedDict
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from django.core.exceptions import SuspiciousOperation
from django.db import models, router
from django.db.backends.utils import CursorWrapper
from django.db.models import Expression, Q, QuerySet
from django.db.models.fields import NOT_PROVIDED

from .expressions import ExcludedCol
from .introspect import model_from_cursor, models_from_cursor
from .sql import PostgresInsertQuery, PostgresQuery
from .types import ConflictAction

if TYPE_CHECKING:
    from django.db.models.constraints import BaseConstraint
    from django.db.models.indexes import Index

ConflictTarget = Union[List[Union[str, Tuple[str]]], "BaseConstraint", "Index"]


TModel = TypeVar("TModel", bound=models.Model, covariant=True)

if TYPE_CHECKING:
    from typing_extensions import Self

    QuerySetBase = QuerySet[TModel]
else:
    QuerySetBase = QuerySet


def peek_iterator(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return list(chain([first], iterable))


class PostgresQuerySet(QuerySetBase, Generic[TModel]):
    """Adds support for PostgreSQL specifics."""

    def __init__(self, model=None, query=None, using=None, hints=None):
        """Initializes a new instance of :see:PostgresQuerySet."""

        super().__init__(model, query, using, hints)

        self.query = query or PostgresQuery(self.model)

        self.conflict_target = None
        self.conflict_action = None
        self.conflict_update_condition = None
        self.index_predicate = None
        self.update_values = None

    def annotate(self, **annotations) -> "Self":  # type: ignore[valid-type, override]
        """Custom version of the standard annotate function that allows using
        field names as annotated fields.

        Normally, the annotate function doesn't allow you to use the
        name of an existing field on the model as the alias name. This
        version of the function does allow that.

        This is done by temporarily renaming the fields in order to
        avoid the check for conflicts that the base class does. We
        rename all fields instead of the ones that already exist because
        the annotations are stored in an OrderedDict. Renaming only the
        conflicts will mess up the order.
        """
        fields = {field.name: field for field in self.model._meta.get_fields()}

        new_annotations = OrderedDict()

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

    def on_conflict(
        self,
        fields: ConflictTarget,
        action: ConflictAction,
        index_predicate: Optional[Union[Expression, Q, str]] = None,
        update_condition: Optional[Union[Expression, Q, str]] = None,
        update_values: Optional[Dict[str, Union[Any, Expression]]] = None,
    ):
        """Sets the action to take when conflicts arise when attempting to
        insert/create a new row.

        Arguments:
            fields:
                The fields the conflicts can occur in.

            action:
                The action to take when the conflict occurs.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)

            update_condition:
                Only update if this SQL expression evaluates to true.

            update_values:
                Optionally, values/expressions to use when rows
                conflict. If not specified, all columns specified
                in the rows are updated with the values you specified.
        """

        self.conflict_target = fields
        self.conflict_action = action
        self.conflict_update_condition = update_condition
        self.index_predicate = index_predicate
        self.update_values = update_values

        return self

    def bulk_insert(
        self,
        rows: Iterable[Dict[str, Any]],
        return_model: bool = False,
        using: Optional[str] = None,
    ):
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

            using:
                Optional name of the database connection to use for
                this query.

        Returns:
            A list of either the dicts of the rows inserted, including the pk or
            the models of the rows inserted with defaults for any fields not specified
        """
        if rows is None:
            return []

        rows = peek_iterator(iter(rows))

        if not rows:
            return []

        if not self.conflict_target and not self.conflict_action:
            # no special action required, use the standard Django bulk_create(..)
            return super().bulk_create(
                [self.model(**fields) for fields in rows]
            )

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

        compiler = self._build_insert_compiler(deduped_rows, using=using)

        with compiler.connection.cursor() as cursor:
            for sql, params in compiler.as_sql(return_id=not return_model):
                cursor.execute(sql, params)

                if return_model:
                    return list(models_from_cursor(self.model, cursor))

                return self._consume_cursor_as_dicts(
                    cursor, original_rows=deduped_rows
                )

    def insert(self, using: Optional[str] = None, **fields):
        """Creates a new record in the database.

        This allows specifying custom conflict behavior using .on_conflict().
        If no special behavior was specified, this uses the normal Django create(..)

        Arguments:
            fields:
                The fields of the row to create.

            using:
                The name of the database connection
                to use for this query.

        Returns:
            The primary key of the record that was created.
        """

        if self.conflict_target or self.conflict_action:
            if not self.model or not self.model.pk:
                return None

            compiler = self._build_insert_compiler([fields], using=using)

            with compiler.connection.cursor() as cursor:
                for sql, params in compiler.as_sql(return_id=True):
                    cursor.execute(sql, params)

                row = cursor.fetchone()
                if not row:
                    return None

            return row[0]

        # no special action required, use the standard Django create(..)
        return super().create(**fields).pk

    def insert_and_get(self, using: Optional[str] = None, **fields):
        """Creates a new record in the database and then gets the entire row.

        This allows specifying custom conflict behavior using .on_conflict().
        If no special behavior was specified, this uses the normal Django create(..)

        Arguments:
            fields:
                The fields of the row to create.

            using:
                The name of the database connection
                to use for this query.

        Returns:
            The model instance representing the row that was created.
        """

        if not self.conflict_target and not self.conflict_action:
            # no special action required, use the standard Django create(..)
            return super().create(**fields)

        compiler = self._build_insert_compiler([fields], using=using)

        with compiler.connection.cursor() as cursor:
            for sql, params in compiler.as_sql(return_id=False):
                cursor.execute(sql, params)

            return model_from_cursor(self.model, cursor)

    def upsert(
        self,
        conflict_target: ConflictTarget,
        fields: dict,
        index_predicate: Optional[Union[Expression, Q, str]] = None,
        using: Optional[str] = None,
        update_condition: Optional[Union[Expression, Q, str]] = None,
        update_values: Optional[Dict[str, Union[Any, Expression]]] = None,
    ) -> int:
        """Creates a new record or updates the existing one with the specified
        data.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)

            using:
                The name of the database connection to
                use for this query.

            update_condition:
                Only update if this SQL expression evaluates to true.

            update_values:
                Optionally, values/expressions to use when rows
                conflict. If not specified, all columns specified
                in the rows are updated with the values you specified.

        Returns:
            The primary key of the row that was created/updated.
        """

        self.on_conflict(
            conflict_target,
            ConflictAction.UPDATE
            if (update_condition or update_condition is None)
            else ConflictAction.NOTHING,
            index_predicate=index_predicate,
            update_condition=update_condition,
            update_values=update_values,
        )

        kwargs = {**fields, "using": using}
        return self.insert(**kwargs)

    def upsert_and_get(
        self,
        conflict_target: ConflictTarget,
        fields: dict,
        index_predicate: Optional[Union[Expression, Q, str]] = None,
        using: Optional[str] = None,
        update_condition: Optional[Union[Expression, Q, str]] = None,
        update_values: Optional[Dict[str, Union[Any, Expression]]] = None,
    ):
        """Creates a new record or updates the existing one with the specified
        data and then gets the row.

        Arguments:
            conflict_target:
                Fields to pass into the ON CONFLICT clause.

            fields:
                Fields to insert/update.

            index_predicate:
                The index predicate to satisfy an arbiter partial index (i.e. what partial index to use for checking
                conflicts)

            using:
                The name of the database connection to
                use for this query.

            update_condition:
                Only update if this SQL expression evaluates to true.

            update_values:
                Optionally, values/expressions to use when rows
                conflict. If not specified, all columns specified
                in the rows are updated with the values you specified.

        Returns:
            The model instance representing the row
            that was created/updated.
        """

        self.on_conflict(
            conflict_target,
            ConflictAction.UPDATE,
            index_predicate=index_predicate,
            update_condition=update_condition,
            update_values=update_values,
        )

        kwargs = {**fields, "using": using}
        return self.insert_and_get(**kwargs)

    def bulk_upsert(
        self,
        conflict_target: ConflictTarget,
        rows: Iterable[Dict],
        index_predicate: Optional[Union[Expression, Q, str]] = None,
        return_model: bool = False,
        using: Optional[str] = None,
        update_condition: Optional[Union[Expression, Q, str]] = None,
        update_values: Optional[Dict[str, Union[Any, Expression]]] = None,
    ):
        """Creates a set of new records or updates the existing ones with the
        specified data.

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

            using:
                The name of the database connection to use
                for this query.

            update_condition:
                Only update if this SQL expression evaluates to true.

            update_values:
                Optionally, values/expressions to use when rows
                conflict. If not specified, all columns specified
                in the rows are updated with the values you specified.

        Returns:
            A list of either the dicts of the rows upserted, including the pk or
            the models of the rows upserted
        """

        self.on_conflict(
            conflict_target,
            ConflictAction.UPDATE,
            index_predicate=index_predicate,
            update_condition=update_condition,
            update_values=update_values,
        )

        return self.bulk_insert(rows, return_model, using=using)

    @staticmethod
    def _consume_cursor_as_dicts(
        cursor: CursorWrapper, *, original_rows: Iterable[Dict[str, Any]]
    ) -> List[dict]:
        cursor_description = cursor.description

        return [
            {
                **original_row,
                **{
                    column.name: row[column_index]
                    for column_index, column in enumerate(cursor_description)
                    if row
                },
            }
            for original_row, row in zip(original_rows, cursor)
        ]

    def _build_insert_compiler(
        self, rows: Iterable[Dict], using: Optional[str] = None
    ):
        """Builds the SQL compiler for a insert query.

        Arguments:
            rows:
                An iterable of dictionaries, where each entry
                describes a record to insert.

            using:
                The name of the database connection to use
                for this query.

        Returns:
            The SQL compiler for the insert.
        """

        # ask the db router which connection to use
        using = (
            using or self._db or router.db_for_write(self.model, **self._hints)  # type: ignore[attr-defined]
        )

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

            obj = self.model(**row.copy())
            obj._state.db = using
            obj._state.adding = False
            objs.append(obj)

        # get the fields to be used during update/insert
        insert_fields, update_values = self._get_upsert_fields(first_row)

        # allow the user to override what should happen on update
        if self.update_values is not None:
            update_values = self.update_values

        # build a normal insert query
        query = PostgresInsertQuery(self.model)
        query.conflict_action = self.conflict_action
        query.conflict_target = self.conflict_target
        query.conflict_update_condition = self.conflict_update_condition
        query.index_predicate = self.index_predicate
        query.insert_on_conflict_values(objs, insert_fields, update_values)

        compiler = query.get_compiler(using)
        return compiler

    def _pre_save_field(
        self,
        model_instance: models.Model,
        field: models.Field,
        *,
        is_insert: bool
    ):
        """Pre-saves the model and gets whether the :see:pre_save method makes
        any modifications to the field value.

        Arguments:
            model_instance:
                The model instance the field is defined on.

            field:
                The field to pre-save.

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

        insert_fields = []
        update_values = {}

        insert_model_instance = self.model(**kwargs)
        update_model_instance = self.model(**kwargs)
        for field in insert_model_instance._meta.local_concrete_fields:
            has_default = field.default != NOT_PROVIDED
            if field.name in kwargs or field.column in kwargs:
                insert_fields.append(field)
                update_values[field.name] = ExcludedCol(field)
                continue
            elif has_default:
                insert_fields.append(field)
                continue

            # special handling for 'pk' which always refers to
            # the primary key, so if we the user specifies `pk`
            # instead of a concrete field, we have to handle that
            if field.primary_key is True and "pk" in kwargs:
                insert_fields.append(field)
                update_values[field.name] = ExcludedCol(field)
                continue

            if self._pre_save_field(
                insert_model_instance, field, is_insert=True
            ):
                insert_fields.append(field)

            if self._pre_save_field(
                update_model_instance, field, is_insert=False
            ):
                update_values[field.name] = ExcludedCol(field)

        return insert_fields, update_values
