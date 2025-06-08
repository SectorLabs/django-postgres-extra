from collections import OrderedDict
from collections.abc import Iterable
from typing import Any, Dict, List, Optional, Tuple, Union

import django

from django.core.exceptions import SuspiciousOperation
from django.db import connections, models
from django.db.models import Expression, sql
from django.db.models.constants import LOOKUP_SEP
from django.db.models.expressions import Ref

from .compiler import PostgresInsertOnConflictCompiler
from .compiler import SQLUpdateCompiler as PostgresUpdateCompiler
from .expressions import HStoreColumn
from .fields import HStoreField
from .types import ConflictAction


class PostgresQuery(sql.Query):
    select: Tuple[Expression, ...]

    def chain(self, klass=None):
        """Chains this query to another.

        We override this so that we can make sure our subclassed query
        classes are used.
        """

        if klass == sql.UpdateQuery:
            return super().chain(PostgresUpdateQuery)

        if klass == sql.InsertQuery:
            return super().chain(PostgresInsertQuery)

        return super().chain(klass)

    def rename_annotations(self, annotations) -> None:
        """Renames the aliases for the specified annotations:

            .annotate(myfield=F('somestuf__myfield'))
            .rename_annotations(myfield='field')

        Arguments:
            annotations:
                The annotations to rename. Mapping the
                old name to the new name.
        """

        # safety check only, make sure there are no renames
        # left that cannot be mapped back to the original name
        for old_name, new_name in annotations.items():
            annotation = self.annotations.get(old_name)
            if not annotation:
                raise SuspiciousOperation(
                    (
                        'Cannot rename annotation "{old_name}" to "{new_name}", because there'
                        ' is no annotation named "{old_name}".'
                    ).format(old_name=old_name, new_name=new_name)
                )

        # rebuild the annotations according to the original order
        new_annotations = OrderedDict()
        for old_name, annotation in self.annotations.items():
            new_name = annotations.get(old_name)
            new_annotations[new_name or old_name] = annotation

            if new_name and self.annotation_select_mask:
                # It's a set in all versions prior to Django 5.x
                # and a list in Django 5.x and newer.
                # https://github.com/django/django/commit/d6b6e5d0fd4e6b6d0183b4cf6e4bd4f9afc7bf67
                if isinstance(self.annotation_select_mask, set):
                    updated_annotation_select_mask = set(
                        self.annotation_select_mask
                    )
                    updated_annotation_select_mask.discard(old_name)
                    updated_annotation_select_mask.add(new_name)
                    self.set_annotation_mask(updated_annotation_select_mask)
                elif isinstance(self.annotation_select_mask, list):
                    self.annotation_select_mask.remove(old_name)
                    self.annotation_select_mask.append(new_name)

        if isinstance(self.group_by, Iterable):
            for statement in self.group_by:
                if not isinstance(statement, Ref):
                    continue

                if statement.refs in annotations:  # type: ignore[attr-defined]
                    statement.refs = annotations[statement.refs]  # type: ignore[attr-defined]

        self.annotations.clear()
        self.annotations.update(new_annotations)

    def add_fields(self, field_names, *args, **kwargs) -> None:
        """Adds the given (model) fields to the select set.

        The field names are added in the order specified. This overrides
        the base class's add_fields method. This is called by the
        .values() or .values_list() method of the query set. It
        instructs the ORM to only select certain values. A lot of
        processing is neccesarry because it can be used to easily do
        joins. For example, `my_fk__name` pulls in the `name` field in
        foreign key `my_fk`. In our case, we want to be able to do
        `title__en`, where `title` is a HStoreField and `en` a key. This
        doesn't really involve a join. We iterate over the specified
        field names and filter out the ones that refer to HStoreField
        and compile it into an expression which is added to the list of
        to be selected fields using `self.add_select`.
        """

        # django knows how to do all of this natively from v2.1
        # see: https://github.com/django/django/commit/20bab2cf9d02a5c6477d8aac066a635986e0d3f3
        if django.VERSION >= (2, 1):
            return super().add_fields(field_names, *args, **kwargs)

        select = []
        field_names_without_hstore = []

        for name in field_names:
            parts = name.split(LOOKUP_SEP)

            # it cannot be a special hstore thing if there's no __ in it
            if len(parts) > 1:
                column_name, hstore_key = parts[:2]
                is_hstore, field = self._is_hstore_field(column_name)
                if self.model and is_hstore:
                    select.append(
                        HStoreColumn(
                            self.model._meta.db_table
                            or self.model.__class__.__name__,
                            field,
                            hstore_key,
                        )
                    )
                    continue

            field_names_without_hstore.append(name)

        super().add_fields(field_names_without_hstore, *args, **kwargs)

        if len(select) > 0:
            self.set_select(list(self.select + tuple(select)))

    def _is_hstore_field(
        self, field_name: str
    ) -> Tuple[bool, Optional[models.Field]]:
        """Gets whether the field with the specified name is a HStoreField.

        Returns     A tuple of a boolean indicating whether the field
        with the specified name is a HStoreField, and the     field
        instance.
        """

        if not self.model:
            return (False, None)

        field_instance = None
        for field in self.model._meta.local_concrete_fields:  # type: ignore[attr-defined]
            if field.name == field_name or field.column == field_name:
                field_instance = field
                break

        return isinstance(field_instance, HStoreField), field_instance


class PostgresInsertQuery(sql.InsertQuery):
    """Insert query using PostgreSQL."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance :see:PostgresInsertQuery."""

        super(PostgresInsertQuery, self).__init__(*args, **kwargs)

        self.conflict_target = []
        self.conflict_action = ConflictAction.UPDATE
        self.conflict_update_condition = None
        self.index_predicate = None
        self.update_values = {}

    def insert_on_conflict_values(
        self,
        objs: List,
        insert_fields: List,
        update_values: Dict[str, Union[Any, Expression]] = {},
    ):
        """Sets the values to be used in this query.

        Insert fields are fields that are definitely
        going to be inserted, and if an existing row
        is found, are going to be overwritten with the
        specified value.

        Update fields are fields that should be overwritten
        in case an update takes place rather than an insert.
        If we're dealing with a INSERT, these will not be used.

        Arguments:
            objs:
                The objects to apply this query to.

            insert_fields:
                The fields to use in the INSERT statement

            update_values:
                Expressions/values to use when a conflict
                occurs and an UPDATE is performed.
        """

        self.insert_values(insert_fields, objs, raw=False)
        self.update_values = update_values

    def get_compiler(self, using=None, connection=None):
        if using:
            connection = connections[using]
        return PostgresInsertOnConflictCompiler(self, connection, using)


class PostgresUpdateQuery(sql.UpdateQuery):
    """Update query using PostgreSQL."""

    def get_compiler(self, using=None, connection=None):
        if using:
            connection = connections[using]
        return PostgresUpdateCompiler(self, connection, using)
