from django.db.models import expressions


class HStoreColumn(expressions.Col):
    """HStoreColumn expression.

    Generates expressions like:

    [db table].[column]->'[hstore key]'
    """

    contains_column_references = True

    def __init__(self, alias, target, hstore_key):
        """Initializes a new instance of :see:HStoreColumn.

        Arguments:
            alias:
                The table name.

            target:
                The field instance.

            hstore_key
                The name of the hstore key to include
                in the epxression.
        """

        super().__init__(alias, target, output_field=target)
        self.alias, self.target, self.hstore_key = alias, target, hstore_key

    def __repr__(self):
        """Gets a textual representation of this expresion."""

        return "{}({}, {}->'{}')".format(
            self.__class__.__name__,
            self.alias,
            self.target,
            self.hstore_key
        )

    def as_sql(self, compiler, connection):
        """Compiles this expression into SQL."""

        qn = compiler.quote_name_unless_alias
        return "%s.%s->'%s'" % (qn(self.alias), qn(self.target.column), self.hstore_key), []

    def relabeled_clone(self, relabels):
        """Gets a re-labeled clone of this expression."""

        return self.__class__(
            relabels.get(self.alias, self.alias),
            self.target,
            self.hstore_key,
            self.output_field
        )
