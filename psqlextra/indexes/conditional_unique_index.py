from django.db.models.indexes import Index


class ConditionalUniqueIndex(Index):
    """
    Creates a partial unique index based on a given condition.

    Useful, for example, if you need unique combination of foreign keys, but you might want to include
    NULL as a valid value. In that case, you can just use:
    >>> class Meta:
    ...    indexes = [
    ...        ConditionalUniqueIndex(fields=['a', 'b', 'c'], condition='"c" IS NOT NULL'),
    ...        ConditionalUniqueIndex(fields=['a', 'b'], condition='"c" IS NULL')
    ...    ]
    """

    sql_create_index = "CREATE UNIQUE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s WHERE %(condition)s"

    def __init__(self, condition: str, fields=[], name=None):
        """Initializes a new instance of :see:ConditionalUniqueIndex."""

        super().__init__(fields=fields, name=name)
        self.condition = condition

    def create_sql(self, model, schema_editor, using=''):
        """Creates the actual SQL used when applying the migration."""
        statement = super().create_sql(model, schema_editor, using)
        statement.template = self.sql_create_index
        statement.parts['condition'] = self.condition
        return statement

    def deconstruct(self):
        """Serializes the :see:ConditionalUniqueIndex for the migrations file."""
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        path = path.replace('django.db.models.indexes', 'django.db.models')
        return path, (), {'fields': self.fields, 'name': self.name, 'condition': self.condition}
