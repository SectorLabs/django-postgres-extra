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


class HStoreRef(expressions.F):
    """Inline reference to a HStore key.

    Allows selecting individual keys in annotations.
    """

    def __init__(self, name: str, key: str):
        """Initializes a new instance of :see:HStoreRef.

        Arguments:
            name:
                The name of the column/field to resolve.

            key:
                The name of the HStore key to select.
        """

        super().__init__(name)
        self.key = key

    def resolve_expression(self, *args, **kwargs) -> HStoreColumn:
        """Resolves the expression into a :see:HStoreColumn expression."""

        original_expression = super().resolve_expression(*args, **kwargs)
        expression = HStoreColumn(
            original_expression.alias,
            original_expression.target,
            self.key
        )
        return expression

class NonGroupableFunc(expressions.Func):
    """A version of Django's :see:Func expression that
    is _never_ included in the GROUP BY clause."""

    def get_group_by_cols(self):
        """Gets the columns to be included in the GROUP BY clause.

        We have to override this because Django's default behavior
        is to include function calls in GROUP by clauses."""
        return []


class Min(NonGroupableFunc):
    """Exposes PostgreSQL's MIN(..) func."""

    def __init__(self, expression):
        super().__init__(expression, function='MIN')


class Max(NonGroupableFunc):
    """Exposes PostgreSQL's Max(..) func."""

    def __init__(self, expression):
        super().__init__(expression, function='Max')
