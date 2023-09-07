import inspect
import os
import sys

from collections.abc import Iterable
from typing import TYPE_CHECKING, Tuple, Union, cast

import django

from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db.models import Expression, Model, Q
from django.db.models.fields.related import RelatedField
from django.db.models.sql import compiler as django_compiler

from .expressions import HStoreValue
from .types import ConflictAction

if TYPE_CHECKING:
    from .sql import PostgresInsertQuery


def append_caller_to_sql(sql):
    """Append the caller to SQL queries.

    Adds the calling file and function as an SQL comment to each query.
    Examples:
     INSERT INTO "tests_47ee19d1" ("id", "title")
     VALUES (1, 'Test')
     RETURNING "tests_47ee19d1"."id"
     /* 998020 test_append_caller_to_sql_crud .../django-postgres-extra/tests/test_append_caller_to_sql.py 55 */

     SELECT "tests_47ee19d1"."id", "tests_47ee19d1"."title"
     FROM "tests_47ee19d1"
     WHERE "tests_47ee19d1"."id" = 1
     LIMIT 1
     /* 998020 test_append_caller_to_sql_crud .../django-postgres-extra/tests/test_append_caller_to_sql.py 69 */

     UPDATE "tests_47ee19d1"
     SET "title" = 'success'
     WHERE "tests_47ee19d1"."id" = 1
     /* 998020 test_append_caller_to_sql_crud .../django-postgres-extra/tests/test_append_caller_to_sql.py 64 */

     DELETE FROM "tests_47ee19d1"
     WHERE "tests_47ee19d1"."id" IN (1)
     /* 998020 test_append_caller_to_sql_crud .../django-postgres-extra/tests/test_append_caller_to_sql.py 74 */

    Slow and blocking queries could be easily tracked down to their originator
    within the source code using the "pg_stat_activity" table.

    Enable "POSTGRES_EXTRA_ANNOTATE_SQL" within the database settings to enable this feature.
    """

    if not getattr(settings, "POSTGRES_EXTRA_ANNOTATE_SQL", None):
        return sql

    try:
        # Search for the first non-Django caller
        stack = inspect.stack()
        for stack_frame in stack[1:]:
            frame_filename = stack_frame[1]
            frame_line = stack_frame[2]
            frame_function = stack_frame[3]
            if "/django/" in frame_filename or "/psqlextra/" in frame_filename:
                continue

            return f"{sql} /* {os.getpid()} {frame_function} {frame_filename} {frame_line} */"

        # Django internal commands (like migrations) end up here
        return f"{sql} /* {os.getpid()} {sys.argv[0]} */"
    except Exception:
        # Don't break anything because this convinence function runs into an unexpected situation
        return sql


class SQLCompiler(django_compiler.SQLCompiler):  # type: ignore [attr-defined]
    def as_sql(self, *args, **kwargs):
        sql, params = super().as_sql(*args, **kwargs)
        return append_caller_to_sql(sql), params


class SQLDeleteCompiler(django_compiler.SQLDeleteCompiler):  # type: ignore [name-defined]
    def as_sql(self, *args, **kwargs):
        sql, params = super().as_sql(*args, **kwargs)
        return append_caller_to_sql(sql), params


class SQLAggregateCompiler(django_compiler.SQLAggregateCompiler):  # type: ignore [name-defined]
    def as_sql(self, *args, **kwargs):
        sql, params = super().as_sql(*args, **kwargs)
        return append_caller_to_sql(sql), params


class SQLUpdateCompiler(django_compiler.SQLUpdateCompiler):  # type: ignore [name-defined]
    """Compiler for SQL UPDATE statements that allows us to use expressions
    inside HStore values.

    Like:

        .update(name=dict(en=F('test')))
    """

    def as_sql(self, *args, **kwargs):
        self._prepare_query_values()
        sql, params = super().as_sql(*args, **kwargs)
        return append_caller_to_sql(sql), params

    def _prepare_query_values(self):
        """Extra prep on query values by converting dictionaries into
        :see:HStoreValue expressions.

        This allows putting expressions in a dictionary. The
        :see:HStoreValue will take care of resolving the expressions
        inside the dictionary.
        """

        if not self.query.values:
            return

        new_query_values = []
        for field, model, val in self.query.values:
            if not isinstance(val, dict):
                new_query_values.append((field, model, val))
                continue

            if not self._does_dict_contain_expression(val):
                new_query_values.append((field, model, val))
                continue

            expression = HStoreValue(dict(val))
            new_query_values.append((field, model, expression))

        self.query.values = new_query_values

    @staticmethod
    def _does_dict_contain_expression(data: dict) -> bool:
        """Gets whether the specified dictionary contains any expressions that
        need to be resolved."""

        for value in data.values():
            if hasattr(value, "resolve_expression"):
                return True

            if hasattr(value, "as_sql"):
                return True

        return False


class SQLInsertCompiler(django_compiler.SQLInsertCompiler):  # type: ignore [name-defined]
    """Compiler for SQL INSERT statements."""

    def as_sql(self, *args, **kwargs):
        """Builds the SQL INSERT statement."""
        queries = [
            (append_caller_to_sql(sql), params)
            for sql, params in super().as_sql(*args, **kwargs)
        ]

        return queries


class PostgresInsertOnConflictCompiler(django_compiler.SQLInsertCompiler):  # type: ignore [name-defined]
    """Compiler for SQL INSERT statements."""

    query: "PostgresInsertQuery"

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of
        :see:PostgresInsertOnConflictCompiler."""
        super().__init__(*args, **kwargs)
        self.qn = self.connection.ops.quote_name

    def as_sql(self, return_id=False, *args, **kwargs):
        """Builds the SQL INSERT statement."""

        queries = [
            self._rewrite_insert(sql, params, return_id)
            for sql, params in super().as_sql(*args, **kwargs)
        ]

        return queries

    def _rewrite_insert(self, sql, params, return_id=False):
        """Rewrites a formed SQL INSERT query to include the ON CONFLICT
        clause.

        Arguments:
            sql:
                The SQL INSERT query to rewrite.

            params:
                The parameters passed to the query.

            return_id:
                Whether to only return the ID or all
                columns.

        Returns:
            A tuple of the rewritten SQL query and new params.
        """

        returning = (
            self.qn(self.query.model._meta.pk.attname) if return_id else "*"
        )

        (sql, params) = self._rewrite_insert_on_conflict(
            sql, params, self.query.conflict_action.value, returning
        )

        return append_caller_to_sql(sql), params

    def _rewrite_insert_on_conflict(
        self, sql, params, conflict_action: ConflictAction, returning
    ):
        """Rewrites a normal SQL INSERT query to add the 'ON CONFLICT'
        clause."""

        # build the conflict target, the columns to watch
        # for conflicts
        on_conflict_clause = self._build_on_conflict_clause()
        index_predicate = self.query.index_predicate  # type: ignore[attr-defined]
        update_condition = self.query.conflict_update_condition  # type: ignore[attr-defined]

        rewritten_sql = f"{sql} {on_conflict_clause}"

        if index_predicate:
            expr_sql, expr_params = self._compile_expression(index_predicate)
            rewritten_sql += f" WHERE {expr_sql}"
            params += tuple(expr_params)

        # Fallback in case the user didn't specify any update values. We can still
        # make the query work if we switch to ConflictAction.NOTHING
        if (
            conflict_action == ConflictAction.UPDATE.value
            and not self.query.update_values
        ):
            conflict_action = ConflictAction.NOTHING

        rewritten_sql += f" DO {conflict_action}"

        if conflict_action == ConflictAction.UPDATE.value:
            set_sql, sql_params = self._build_set_statement()

            rewritten_sql += f" SET {set_sql}"
            params += sql_params

            if update_condition:
                expr_sql, expr_params = self._compile_expression(
                    update_condition
                )
                rewritten_sql += f" WHERE {expr_sql}"
                params += tuple(expr_params)

        rewritten_sql += f" RETURNING {returning}"

        return (rewritten_sql, params)

    def _build_set_statement(self) -> Tuple[str, tuple]:
        """Builds the SET statement for the ON CONFLICT DO UPDATE clause.

        This uses the update compiler to provide full compatibility with
        the standard Django's `update(...)`.
        """

        # Local import to work around the circular dependency between
        # the compiler and the queries.
        from .sql import PostgresUpdateQuery

        query = cast(PostgresUpdateQuery, self.query.chain(PostgresUpdateQuery))
        query.add_update_values(self.query.update_values)

        sql, params = query.get_compiler(self.connection.alias).as_sql()
        return sql.split("SET")[1].split(" WHERE")[0], tuple(params)

    def _build_on_conflict_clause(self):
        if django.VERSION >= (2, 2):
            from django.db.models.constraints import BaseConstraint
            from django.db.models.indexes import Index

            if isinstance(
                self.query.conflict_target, BaseConstraint
            ) or isinstance(self.query.conflict_target, Index):
                return "ON CONFLICT ON CONSTRAINT %s" % self.qn(
                    self.query.conflict_target.name
                )

        conflict_target = self._build_conflict_target()
        return f"ON CONFLICT {conflict_target}"

    def _build_conflict_target(self):
        """Builds the `conflict_target` for the ON CONFLICT clause."""

        if not isinstance(self.query.conflict_target, Iterable):
            raise SuspiciousOperation(
                (
                    "%s is not a valid conflict target, specify "
                    "a list of column names, or tuples with column "
                    "names and hstore key."
                )
                % str(self.query.conflict_target)
            )

        conflict_target = self._build_conflict_target_by_index()
        if conflict_target:
            return conflict_target

        return self._build_conflict_target_by_fields()

    def _build_conflict_target_by_fields(self):
        """Builds the `conflict_target` for the ON CONFLICT clauses by matching
        the fields specified in the specified conflict target against the
        model's fields.

        This requires some special handling because the fields names
        might not be same as the column names.
        """

        conflict_target = []

        for field_name in self.query.conflict_target:
            self._assert_valid_field(field_name)

            # special handling for hstore keys
            if isinstance(field_name, tuple):
                conflict_target.append(
                    "(%s->'%s')"
                    % (self._format_field_name(field_name), field_name[1])
                )
            else:
                conflict_target.append(self._format_field_name(field_name))

        return "(%s)" % ",".join(conflict_target)

    def _build_conflict_target_by_index(self):
        """Builds the `conflict_target` for the ON CONFLICT clause by trying to
        find an index that matches the specified conflict target on the query.

        Conflict targets must match some unique constraint, usually this
        is a `UNIQUE INDEX`.
        """

        matching_index = next(
            (
                index
                for index in self.query.model._meta.indexes
                if list(index.fields) == list(self.query.conflict_target)
            ),
            None,
        )

        if not matching_index:
            return None

        with self.connection.schema_editor() as schema_editor:
            stmt = matching_index.create_sql(self.query.model, schema_editor)
            return "(%s)" % stmt.parts["columns"]

    def _get_model_field(self, name: str):
        """Gets the field on a model with the specified name.

        Arguments:
            name:
                The name of the field to look for.

                This can be both the actual field name, or
                the name of the column, both will work :)

        Returns:
            The field with the specified name or None if
            no such field exists.
        """

        field_name = self._normalize_field_name(name)

        if not self.query.model:
            return None

        # 'pk' has special meaning and always refers to the primary
        # key of a model, we have to respect this de-facto standard behaviour
        if field_name == "pk" and self.query.model._meta.pk:
            return self.query.model._meta.pk

        for field in self.query.model._meta.local_concrete_fields:  # type: ignore[attr-defined]
            if field.name == field_name or field.column == field_name:
                return field

        return None

    def _format_field_name(self, field_name) -> str:
        """Formats a field's name for usage in SQL.

        Arguments:
            field_name:
                The field name to format.

        Returns:
            The specified field name formatted for
            usage in SQL.
        """

        field = self._get_model_field(field_name)
        return self.qn(field.column)

    def _format_field_value(self, field_name) -> str:
        """Formats a field's value for usage in SQL.

        Arguments:
            field_name:
                The name of the field to format
                the value of.

        Returns:
            The field's value formatted for usage
            in SQL.
        """

        field_name = self._normalize_field_name(field_name)
        field = self._get_model_field(field_name)

        value = getattr(self.query.objs[0], field.attname)

        if isinstance(field, RelatedField) and isinstance(value, Model):
            value = value.pk

        return django_compiler.SQLInsertCompiler.prepare_value(  # type: ignore[attr-defined]
            self,
            field,
            # Note: this deliberately doesn't use `pre_save_val` as we don't
            # want things like auto_now on DateTimeField (etc.) to change the
            # value. We rely on pre_save having already been done by the
            # underlying compiler so that things like FileField have already had
            # the opportunity to save out their data.
            value,
        )

    def _compile_expression(
        self, expression: Union[Expression, Q, str]
    ) -> Tuple[str, Union[tuple, list]]:
        """Compiles an expression, Q object or raw SQL string into SQL and
        tuple of parameters."""

        if isinstance(expression, Q):
            if django.VERSION < (3, 1):
                raise SuspiciousOperation(
                    "Q objects in psqlextra can only be used with Django 3.1 and newer"
                )

            return self.query.build_where(expression).as_sql(
                self, self.connection
            )

        elif isinstance(expression, Expression):
            return self.compile(expression)

        return expression, tuple()

    def _assert_valid_field(self, field_name: str):
        """Asserts that a field with the specified name exists on the model and
        raises :see:SuspiciousOperation if it does not."""

        field_name = self._normalize_field_name(field_name)
        if self._get_model_field(field_name):
            return

        raise SuspiciousOperation(
            (
                "%s is not a valid conflict target, specify "
                "a list of column names, or tuples with column "
                "names and hstore key."
            )
            % str(field_name)
        )

    @staticmethod
    def _normalize_field_name(field_name: str) -> str:
        """Normalizes a field name into a string by extracting the field name
        if it was specified as a reference to a HStore key (as a tuple).

        Arguments:
            field_name:
                The field name to normalize.

        Returns:
            The normalized field name.
        """

        if isinstance(field_name, tuple):
            field_name, _ = field_name

        return field_name
