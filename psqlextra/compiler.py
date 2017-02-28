from django.core.exceptions import SuspiciousOperation
from django.db.models.sql.compiler import SQLInsertCompiler


class PostgresSQLUpsertCompiler(SQLInsertCompiler):
    """Compiler for SQL INSERT statements."""

    def as_sql(self, returning='id'):
        """Builds the SQL INSERT statement."""

        queries = [
            (self._rewrite_insert(sql, returning), params)
            for sql, params in super().as_sql()
        ]

        return queries

    def execute_sql(self, return_id=False):
        returning = 'id' if return_id else '*'
        returning = '*'

        # execute all the generate queries
        with self.connection.cursor() as cursor:
            rows = []
            for sql, params in self.as_sql(returning):
                cursor.execute(sql, params)
                rows.append(cursor.fetchone())

        # return the primary key, which is stored in
        # the first column that is returned
        if return_id:
            return dict(id=rows[0][0])

        # create a mapping between column names and column value
        return {
            column.name: rows[0][column_index]
            for column_index, column in enumerate(cursor.description)
        }

    def _rewrite_insert(self, sql, returning='id'):
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

        # construct a list of columns to update when there's a conflict
        update_columns = ', '.join([
            '{0} = EXCLUDED.{0}'.format(qn(field.column))
            for field in self.query.update_fields
        ])

        # build the conflict target, the columns to watch
        # for conflict basically
        conflict_target = self._build_conflict_target()

        # form the new sql query that does the insert
        new_sql = (
            '{insert} ON CONFLICT ({conflict_target}) '
            'DO UPDATE SET {update_columns} RETURNING {returning}'
        ).format(
            insert=sql,
            conflict_target=conflict_target,
            update_columns=update_columns,
            returning=returning
        )

        return new_sql

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
