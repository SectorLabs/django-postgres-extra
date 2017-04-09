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

    def as_sql(self, return_id=False):
        """Builds the SQL INSERT statement."""

        queries = [
            (self._rewrite_insert(sql, return_id), params)
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

    def _rewrite_insert(self, sql, return_id=False):
        """Rewrites a formed SQL INSERT query to include
        the ON CONFLICT clause.

        Arguments:
            sql:
                The SQL INSERT query to rewrite.

            returning:
                What to put in the `RETURNING` clause
                of the resulting query.

        Returns:
            The specified SQL INSERT query rewritten
            to include the ON CONFLICT clause.
        """

        qn = self.connection.ops.quote_name
        returning = qn(self.query.model._meta.pk.name) if return_id else '*'

        if self.query.conflict_action.value == 'UPDATE':
            return self._rewrite_insert_update(sql, returning)
        elif self.query.conflict_action.value == 'NOTHING':
            return self._rewrite_insert_nothing(sql, returning)

        raise SuspiciousOperation((
            '%s is not a valid conflict action, specify '
            'ConflictAction.UPDATE or ConflictAction.NOTHING.'
        ) % str(self.query.conflict_action))

    def _rewrite_insert_update(self, sql, returning):
        qn = self.connection.ops.quote_name

        update_columns = ', '.join([
            '{0} = EXCLUDED.{0}'.format(qn(field.column))
            for field in self.query.update_fields
        ])

        # build the conflict target, the columns to watch
        # for conflicts
        conflict_target = self._build_conflict_target()

        return (
            '{insert} ON CONFLICT ({conflict_target}) DO UPDATE'
            ' SET {update_columns} RETURNING {returning}'
        ).format(
            insert=sql,
            conflict_target=conflict_target,
            update_columns=update_columns,
            returning=returning
        )

    def _rewrite_insert_nothing(self, sql, returning):
        qn = self.connection.ops.quote_name

        # build the conflict target, the columns to watch
        # for conflicts
        conflict_target = self._build_conflict_target()

        select_columns = ', '.join([
            '{0} = \'{1}\''.format(qn(column), getattr(self.query.objs[0], column))
            for column in self.query.conflict_target
        ])

        # this looks complicated, and it is, but it is for a reason... a normal
        # ON CONFLICT DO NOTHING doesn't return anything if the row already exists
        # so we do DO UPDATE instead that never executes to lock the row, and then
        # select from the table in case we're dealing with an existing row..
        return (
            'WITH insdata AS ('
            '{insert} ON CONFLICT ({conflict_target}) DO UPDATE'
            ' SET id = NULL WHERE FALSE RETURNING {returning})'
            ' SELECT * FROM insdata UNION ALL'
            ' SELECT {returning} FROM {table} WHERE {select_columns} LIMIT 1;'
        ).format(
            insert=sql,
            conflict_target=conflict_target,
            returning=returning,
            table=self.query.objs[0]._meta.db_table,
            select_columns=select_columns
        )

    def _build_conflict_target(self):
        """Builds the `conflict_target` for the ON CONFLICT
        clause."""

        qn = self.connection.ops.quote_name
        conflict_target = []

        if not isinstance(self.query.conflict_target, list):
            raise SuspiciousOperation((
                '%s is not a valid conflict target, specify '
                'a list of column names, or tuples with column '
                'names and hstore key.'
            ) % str(self.query.conflict_target))

        def _assert_valid_field(field_name):
            for field in self.query.objs[0]._meta.local_concrete_fields:
                if field.column == field_name:
                    return

            raise SuspiciousOperation((
                '%s is not a valid conflict target, specify '
                'a list of column names, or tuples with column '
                'names and hstore key.'
            ) % str(field))

        for field in self.query.conflict_target:
            if isinstance(field, str):
                _assert_valid_field(field)
                conflict_target.append(qn(field))
                continue

            if isinstance(field, tuple):
                field, key = field
                _assert_valid_field(field)
                conflict_target.append(
                    '(%s -> \'%s\')' % (qn(field), key))
                continue

            raise SuspiciousOperation((
                '%s is not a valid conflict target, specify '
                'a list of column names, or tuples with column '
                'names and hstore key.'
            ) % str(field))

        return ','.join(conflict_target)
