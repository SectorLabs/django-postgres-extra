from typing import List

from django.core.exceptions import SuspiciousOperation
from django.db import connections
from django.db.models import sql

from .compiler import PostgresInsertCompiler, PostgresUpdateCompiler
from .types import ConflictAction


class PostgresQuery(sql.Query):
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

        for old_name, new_name in annotations.items():
            annotation = self.annotations.get(old_name)

            if not annotation:
                raise SuspiciousOperation(
                    (
                        'Cannot rename annotation "{old_name}" to "{new_name}", because there'
                        ' is no annotation named "{old_name}".'
                    ).format(old_name=old_name, new_name=new_name)
                )

            self.annotations[new_name] = annotation
            del self.annotations[old_name]


class PostgresInsertQuery(sql.InsertQuery):
    """Insert query using PostgreSQL."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance :see:PostgresInsertQuery."""

        super(PostgresInsertQuery, self).__init__(*args, **kwargs)

        self.conflict_target = []
        self.conflict_action = ConflictAction.UPDATE

        self.update_fields = []

    def values(self, objs: List, insert_fields: List, update_fields: List = []):
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

    def get_compiler(self, using=None, connection=None):
        if using:
            connection = connections[using]
        return PostgresInsertCompiler(self, connection, using)


class PostgresUpdateQuery(sql.UpdateQuery):
    """Update query using PostgreSQL."""

    def get_compiler(self, using=None, connection=None):
        if using:
            connection = connections[using]
        return PostgresUpdateCompiler(self, connection, using)
