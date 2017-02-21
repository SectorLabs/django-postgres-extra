from django.db.models.sql.compiler import SQLInsertCompiler

from .fields import HStoreField


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
            return rows[0][0]

        # return the entire row instead
        return rows[0]

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

        # ON CONFLICT requires a list of columns to operate on, form
        # a list of columns to pass in
        unique_columns = ', '.join(self._get_unique_columns())
        if len(unique_columns) == 0:
            return sql

        # construct a list of columns to update when there's a conflict
        update_columns = ', '.join([
            '{0} = EXCLUDED.{0}'.format(qn(field.column))
            for field in self.query.update_fields
        ])

        # form the new sql query that does the insert
        new_sql = (
            '{insert} ON CONFLICT ({unique_columns}) '
            'DO UPDATE SET {update_columns} RETURNING {returning}'
        ).format(
            insert=sql,
            unique_columns=unique_columns,
            update_columns=update_columns,
            returning=returning
        )

        return new_sql

    def _get_unique_columns(self):
        """Gets a list of columns that are marked as 'UNIQUE'.

        This is used in the ON CONFLICT clause. This also
        works for :see:HStoreField."""

        qn = self.connection.ops.quote_name
        unique_columns = []

        for field in self.query.fields:
            if field.unique is True:
                unique_columns.append(qn(field.column))
                continue

            # we must also go into possible tuples since those
            # are used to indicate "unique together"
            if isinstance(field, HStoreField):
                uniqueness = getattr(field, 'uniqueness', None)
                if not uniqueness:
                    continue
                for key in uniqueness:
                    if isinstance(key, tuple):
                        for sub_key in key:
                            unique_columns.append(
                                '(%s->\'%s\')' % (qn(field.column), sub_key))
                    else:
                        unique_columns.append(
                            '(%s->\'%s\')' % (qn(field.column), key))

                continue

        return unique_columns
