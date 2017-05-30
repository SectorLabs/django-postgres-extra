from typing import List, Tuple, Optional, Dict, Any
from enum import Enum

from django.db import models
from django.db.models import sql
from django.db.models.constants import LOOKUP_SEP
from django.core.exceptions import SuspiciousOperation

from .fields import HStoreField
from .expressions import HStoreColumn
from .datastructures import ConditionalJoin


class ConflictAction(Enum):
    """Possible actions to take on a conflict."""

    NOTHING = 'NOTHING'
    UPDATE = 'UPDATE'


class PostgresQuery(sql.Query):
    def rename_annotations(self, annotations) -> None:
        """Renames the aliases for the specified annotations:

            .annotate(myfield=F('somestuf__myfield'))
            .rename_annotations(myfield='field')

        Arguments:
            annotations:
                The annotations to rename. Mapping the
                old name to the new name.
        """

        for old_name, new_name in annotations.items():
            annotation = self.annotations.get(old_name)

            if not annotation:
                raise SuspiciousOperation((
                    'Cannot rename annotation "{old_name}" to "{new_name}", because there'
                    ' is no annotation named "{old_name}".'
                ).format(old_name=old_name, new_name=new_name))

            del self.annotations[old_name]
            self.annotations[new_name] = annotation

    def add_join_conditions(self, conditions: Dict[str, Any]) -> None:
        """Adds an extra condition to an existing JOIN.

        This allows you to for example do:

            INNER JOIN othertable ON (mytable.id = othertable.other_id AND [extra conditions])

        This does not work if nothing else in your query doesn't already generate the
        initial join in the first place.
        """

        alias = self.get_initial_alias()
        opts = self.get_meta()

        for name, value in conditions.items():
            parts = name.split(LOOKUP_SEP)
            _, targets, _, joins, path = self.setup_joins(parts, opts, alias, allow_many=True)
            self.trim_joins(targets, joins, path)

            target_table = joins[-1]
            field = targets[-1]
            join = self.alias_map.get(target_table)

            if not join:
                raise SuspiciousOperation((
                    'Cannot add an extra join condition for "%s", there\'s no'
                    ' existing join to add it to.'
                ) % target_table)

            # convert the Join object into a ConditionalJoin object, which
            # allows us to add the extra condition
            if not isinstance(join, ConditionalJoin):
                self.alias_map[target_table] = ConditionalJoin.from_join(join)
                join = self.alias_map[target_table]

            join.add_condition(field, value)

    def add_fields(self, field_names: List[str], allow_m2m: bool=True) -> bool:
        """
        Adds the given (model) fields to the select set. The field names are
        added in the order specified.

        This overrides the base class's add_fields method. This is called by
        the .values() or .values_list() method of the query set. It instructs
        the ORM to only select certain values. A lot of processing is neccesarry
        because it can be used to easily do joins. For example, `my_fk__name` pulls
        in the `name` field in foreign key `my_fk`.

        In our case, we want to be able to do `title__en`, where `title` is a HStoreField
        and `en` a key. This doesn't really involve a join. We iterate over the specified
        field names and filter out the ones that refer to HStoreField and compile it into
        an expression which is added to the list of to be selected fields using `self.add_select`.
        """

        alias = self.get_initial_alias()
        opts = self.get_meta()

        for name in field_names:
            parts = name.split(LOOKUP_SEP)

            # it cannot be a special hstore thing if there's no __ in it
            if len(parts) > 1:
                column_name, hstore_key = parts[:2]
                is_hstore, field = self._is_hstore_field(column_name)
                if is_hstore:
                    self.add_select(
                        HStoreColumn(self.model._meta.db_table or self.model.name, field, hstore_key)
                    )
                    continue

            _, targets, _, joins, path = self.setup_joins(parts, opts, alias, allow_many=allow_m2m)
            targets, final_alias, joins = self.trim_joins(targets, joins, path)

            for target in targets:
                self.add_select(target.get_col(final_alias))

    def _is_hstore_field(self, field_name: str) -> Tuple[bool, Optional[models.Field]]:
        """Gets whether the field with the specified name is a
        HStoreField.

        Returns
            A tuple of a boolean indicating whether the field
            with the specified name is a HStoreField, and the
            field instance.
        """

        field_instance = None
        for field in self.model._meta.local_concrete_fields:
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

        self.update_fields = []

    def values(self, objs: List, insert_fields: List, update_fields: List=[]):
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

            update_fields:
                The fields to only use in the UPDATE statement.
        """

        self.insert_values(insert_fields, objs, raw=False)
        self.update_fields = update_fields
