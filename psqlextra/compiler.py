from django.db.models.sql.compiler import SQLInsertCompiler

from .fields import HStoreField


class PostgresSQLUpsertCompiler(SQLInsertCompiler):
    """Compiler for SQL INSERT statements."""

    def as_sql(self):
        """Builds the SQL INSERT statement."""

        queries = [
            (self._rewrite_insert(sql), params)
            for sql, params in super().as_sql()
        ]

        return queries

    def _rewrite_insert(self, sql):
        """Rewrites a formed SQL INSERT query to include
        the ON CONFLICT clause.

        Arguments:
            sql:
                The SQL INSERT query to rewrite.

        Returns:
            The specified SQL INSERT query rewritten
            to include the ON CONFLICT clause.
        """
        qn = self.connection.ops.quote_name

        # remove the RETURNING part, it will be become part of
        # the ON CONFLICT part
        insert, _ = sql.split(' RETURNING ')

        # ON CONFLICT requires a list of columns to operate on, form
        # a list of columns to pass in
        unique_columns = ', '.join(self._get_unique_columns())
        if len(unique_columns) == 0:
            return sql

        # construct a list of columns to update when there's a conflict
        update_columns = ', '.join([
            '{0} = EXCLUDED.{0}'.format(qn(field.column))
            for field in self.query.fields
        ])

        # form the new sql query that does the insert
        new_sql = (
            '{insert} ON CONFLICT ({unique_columns}) '
            'DO UPDATE SET {update_columns} RETURNING id'
        ).format(
            insert=insert,
            unique_columns=unique_columns,
            update_columns=update_columns
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
