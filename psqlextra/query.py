from django.db.models.sql import InsertQuery


class PostgresUpsertQuery(InsertQuery):
    """Upsert query (insert/update) query using PostgreSQL."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance :see:PostgresUpsertQuery."""

        super(PostgresUpsertQuery, self).__init__(*args, **kwargs)

        self.update_fields = []

    def values(self, objs, insert_fields, update_fields):
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
