import django

from django.db.models.indexes import Index


class ConditionalUniqueIndex(Index):
    """Creates a partial unique index based on a given condition.

    Useful, for example, if you need unique combination of foreign keys, but you might want to include
    NULL as a valid value. In that case, you can just use:

    >>> class Meta:
    >>>    indexes = [
    >>>        ConditionalUniqueIndex(fields=['a', 'b', 'c'], condition='"c" IS NOT NULL'),
    >>>        ConditionalUniqueIndex(fields=['a', 'b'], condition='"c" IS NULL')
    >>>    ]
    """

    sql_create_index = "CREATE UNIQUE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s WHERE %(condition)s"

    def __init__(self, condition: str, fields=[], name=None):
        """Initializes a new instance of :see:ConditionalUniqueIndex."""

        super().__init__(fields=fields, name=name)

        self._condition = condition

    def create_sql(self, model, schema_editor, using="", **kwargs):
        """Creates the actual SQL used when applying the migration."""
        if django.VERSION >= (2, 0):
            statement = super().create_sql(model, schema_editor, using)
            statement.template = self.sql_create_index
            statement.parts["condition"] = self._condition
            return statement
        else:
            sql_create_index = self.sql_create_index
            sql_parameters = {
                **Index.get_sql_create_template_values(
                    self, model, schema_editor, using
                ),
                "condition": self._condition,
            }
            return sql_create_index % sql_parameters

    def deconstruct(self):
        """Serializes the :see:ConditionalUniqueIndex for the migrations
        file."""
        path = "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        path = path.replace("django.db.models.indexes", "django.db.models")
        return (
            path,
            (),
            {
                "fields": self.fields,
                "name": self.name,
                "condition": self._condition,
            },
        )
