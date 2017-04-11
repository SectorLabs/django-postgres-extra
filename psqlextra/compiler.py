from django.core.exceptions import SuspiciousOperation
from django.db.models.sql.compiler import SQLInsertCompiler, SQLUpdateCompiler


class PostgresReturningUpdateCompiler(SQLUpdateCompiler):
    """Compiler for SQL UPDATE statements that return
    the primary keys of the affected rows."""

    def execute_sql(self, _result_type):
        sql, params = self.as_sql()
        sql += self._form_returning()

        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            primary_keys = cursor.fetchall()

        return primary_keys

    def _form_returning(self):
        """Builds the RETURNING part of the query."""

        qn = self.connection.ops.quote_name
        return 'RETURNING %s' % qn(self.query.model._meta.pk.name)


class PostgresInsertCompiler(SQLInsertCompiler):
    """Compiler for SQL INSERT statements."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:PostgresInsertCompiler."""

        super().__init__(*args, **kwargs)
        self.qn = self.connection.ops.quote_name

    def as_sql(self, return_id=False):
        """Builds the SQL INSERT statement."""

        queries = [
            self._rewrite_insert(sql, params, return_id)
            for sql, params in super().as_sql()
        ]

        return queries

    def execute_sql(self, return_id=False):
        # execute all the generate queries
        with self.connection.cursor() as cursor:
            rows = []
            for sql, params in self.as_sql(return_id):
                cursor.execute(sql, params)
                rows.append(cursor.fetchone())

        # create a mapping between column names and column value
        return [
            {
                column.name: row[column_index]
                for column_index, column in enumerate(cursor.description) if row
            }
            for row in rows
        ]

    def _rewrite_insert(self, sql, params, return_id=False):
        """Rewrites a formed SQL INSERT query to include
        the ON CONFLICT clause.

        Arguments:
            sql:
                The SQL INSERT query to rewrite.

            params:
                The parameters passed to the query.

            returning:
                What to put in the `RETURNING` clause
                of the resulting query.

        Returns:
            A tuple of the rewritten SQL query and new params.
        """

        returning = self.qn(self.query.model._meta.pk.name) if return_id else '*'

        if self.query.conflict_action.value == 'UPDATE':
            return self._rewrite_insert_update(sql, params, returning)
        elif self.query.conflict_action.value == 'NOTHING':
            return self._rewrite_insert_nothing(sql, params, returning)

        raise SuspiciousOperation((
            '%s is not a valid conflict action, specify '
            'ConflictAction.UPDATE or ConflictAction.NOTHING.'
        ) % str(self.query.conflict_action))

    def _rewrite_insert_update(self, sql, params, returning):
        """Rewrites a formed SQL INSERT query to include
        the ON CONFLICT DO UPDATE clause."""

        update_columns = ', '.join([
            '{0} = EXCLUDED.{0}'.format(self.qn(field.column))
            for field in self.query.update_fields
        ])

        # build the conflict target, the columns to watch
        # for conflicts
        conflict_target = self._build_conflict_target()

        return (
            (
                '{insert} ON CONFLICT {conflict_target} DO UPDATE'
                ' SET {update_columns} RETURNING {returning}'
            ).format(
                insert=sql,
                conflict_target=conflict_target,
                update_columns=update_columns,
                returning=returning
            ),
            params
        )

    def _rewrite_insert_nothing(self, sql, params, returning):
        """Rewrites a formed SQL INSERT query to include
        the ON CONFLICT DO NOTHING clause."""

        # build the conflict target, the columns to watch
        # for conflicts
        conflict_target = self._build_conflict_target()

        where_clause = ' AND '.join([
            '{0} = %s'.format(self._format_field_name(field_name))
            for field_name in self.query.conflict_target
        ])

        where_clause_params = [
            self._format_field_value(field_name)
            for field_name in self.query.conflict_target
        ]

        params = params + tuple(where_clause_params)

        # this looks complicated, and it is, but it is for a reason... a normal
        # ON CONFLICT DO NOTHING doesn't return anything if the row already exists
        # so we do DO UPDATE instead that never executes to lock the row, and then
        # select from the table in case we're dealing with an existing row..
        return (
            (
                'WITH insdata AS ('
                '{insert} ON CONFLICT {conflict_target} DO UPDATE'
                ' SET id = NULL WHERE FALSE RETURNING {returning})'
                ' SELECT * FROM insdata UNION ALL'
                ' SELECT {returning} FROM {table} WHERE {where_clause} LIMIT 1;'
            ).format(
                insert=sql,
                conflict_target=conflict_target,
                returning=returning,
                table=self.query.objs[0]._meta.db_table,
                where_clause=where_clause
            ),
            params
        )

    def _build_conflict_target(self):
        """Builds the `conflict_target` for the ON CONFLICT
        clause."""

        conflict_target = []

        if not isinstance(self.query.conflict_target, list):
            raise SuspiciousOperation((
                '%s is not a valid conflict target, specify '
                'a list of column names, or tuples with column '
                'names and hstore key.'
            ) % str(self.query.conflict_target))

        def _assert_valid_field(field_name):
            field_name = self._normalize_field_name(field_name)
            if self._get_model_field(field_name):
                return

            raise SuspiciousOperation((
                '%s is not a valid conflict target, specify '
                'a list of column names, or tuples with column '
                'names and hstore key.'
            ) % str(field_name))

        for field_name in self.query.conflict_target:
            _assert_valid_field(field_name)

            # special handling for hstore keys
            if isinstance(field_name, tuple):
                conflict_target.append(
                    '(%s->\'%s\')' % (
                        self._format_field_name(field_name),
                        field_name[1]
                    )
                )
            else:
                conflict_target.append(
                    self._format_field_name(field_name))

        return '(%s)' % ','.join(conflict_target)

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

        for field in self.query.model._meta.local_concrete_fields:
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

        return SQLInsertCompiler.prepare_value(
            self,
            field,
            getattr(self.query.objs[0], field_name)
        )

    def _normalize_field_name(self, field_name) -> str:
        """Normalizes a field name into a string by
        extracting the field name if it was specified
        as a reference to a HStore key (as a tuple).

        Arguments:
            field_name:
                The field name to normalize.

        Returns:
            The normalized field name.
        """

        if isinstance(field_name, tuple):
            field_name, _ = field_name

        return field_name
