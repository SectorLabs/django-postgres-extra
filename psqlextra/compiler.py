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

        # build the conflict target, the columns to watch
        # for conflict basically
        conflict_target = self._build_conflict_target()

        # form the new sql query that does the insert
        new_sql = (
            '{insert} ON CONFLICT ({conflict_target}) DO {conflict_action}'
        ).format(
            insert=sql,
            conflict_target=conflict_target,
            conflict_action=self._build_conflict_action(return_id)
        )

        return new_sql

    def _build_conflict_action(self, return_id=False):
        """Builds the `conflict_action` for the DO clause."""

        returning = 'id' if return_id else '*'

        qn = self.connection.ops.quote_name

        # construct a list of columns to update when there's a conflict
        if self.query.conflict_action.value == 'UPDATE':
            update_columns = ', '.join([
                '{0} = EXCLUDED.{0}'.format(qn(field.column))
                for field in self.query.update_fields
            ])

            return (
                'UPDATE SET {update_columns} RETURNING {returning}'
            ).format(
                update_columns=update_columns,
                returning=returning
            )
        elif self.query.conflict_action.value == 'NOTHING':
            return (
                'NOTHING RETURNING {returning}'
            ).format(returning=returning)

        raise SuspiciousOperation((
            '%s is not a valid conflict action, specify '
            'ConflictAction.UPDATE or ConflictAction.NOTHING.'
        ) % str(self.query.conflict_action))

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

        for field in self.query.conflict_target:
            if isinstance(field, str):
                conflict_target.append(qn(field))
                continue

            if isinstance(field, tuple):
                field, key = field
                conflict_target.append(
                    '(%s -> \'%s\')' % (qn(field), key))
                continue

            raise SuspiciousOperation((
                '%s is not a valid conflict target, specify '
                'a list of column names, or tuples with column '
                'names and hstore key.'
            ) % str(field))

        return ','.join(conflict_target)
