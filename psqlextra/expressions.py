from django.db.models import CharField, expressions


class HStoreValue(expressions.Expression):
    """Represents a HStore value.

    The base PostgreSQL implementation Django provides, always
    represents HStore values as dictionaries, but this doesn't work if
    you want to use expressions inside hstore values.
    """

    def __init__(self, value):
        """Initializes a new instance."""

        self.value = value

    def resolve_expression(self, *args, **kwargs):
        """Resolves expressions inside the dictionary."""

        result = dict()
        for key, value in self.value.items():
            if hasattr(value, "resolve_expression"):
                result[key] = value.resolve_expression(*args, **kwargs)
            else:
                result[key] = value

        return HStoreValue(result)

    def as_sql(self, compiler, connection):
        """Compiles the HStore value into SQL.

        Compiles expressions contained in the values
        of HStore entries as well.

        Given a dictionary like:

            dict(key1='val1', key2='val2')

        The resulting SQL will be:

            hstore(hstore('key1', 'val1'), hstore('key2', 'val2'))
        """

        sql = []
        params = []

        for key, value in self.value.items():
            if hasattr(value, "as_sql"):
                inner_sql, inner_params = value.as_sql(compiler, connection)
                sql.append(f"hstore(%s, {inner_sql})")
                params.append(key)
                params.extend(inner_params)
            elif value is not None:
                sql.append("hstore(%s, %s)")
                params.append(key)
                params.append(str(value))
            else:
                sql.append("hstore(%s, NULL)")
                params.append(key)

        return " || ".join(sql), params


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
            self.__class__.__name__, self.alias, self.target, self.hstore_key
        )

    def as_sql(self, compiler, connection):
        """Compiles this expression into SQL."""

        qn = compiler.quote_name_unless_alias
        return (
            "%s.%s->'%s'"
            % (qn(self.alias), qn(self.target.column), self.hstore_key),
            [],
        )

    def relabeled_clone(self, relabels):
        """Gets a re-labeled clone of this expression."""

        return self.__class__(
            relabels.get(self.alias, self.alias),
            self.target,
            self.hstore_key,
            self.output_field,
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

    def resolve_expression(self, *args, **kwargs):
        """Resolves the expression into a :see:HStoreColumn expression."""

        original_expression: expressions.Col = super().resolve_expression(
            *args, **kwargs
        )
        expression = HStoreColumn(
            original_expression.alias, original_expression.target, self.key
        )
        return expression


class DateTimeEpochColumn(expressions.Col):
    """Gets the date/time column as a UNIX epoch timestamp."""

    contains_column_references = True

    def as_sql(self, compiler, connection):
        """Compiles this expression into SQL."""

        sql, params = super().as_sql(compiler, connection)
        return "EXTRACT(epoch FROM {})".format(sql), params

    def get_group_by_cols(self):
        return []


class DateTimeEpoch(expressions.F):
    """Gets the date/time column as a UNIX epoch timestamp."""

    contains_aggregate = False

    def resolve_expression(self, *args, **kwargs):
        original_expression = super().resolve_expression(*args, **kwargs)
        expression = DateTimeEpochColumn(
            original_expression.alias, original_expression.target
        )
        return expression


def IsNotNone(*fields, default=None):
    """Selects whichever field is not None, in the specified order.

    Arguments:
        fields:
            The fields to attempt to get a value from,
            in order.

        default:
            The value to return in case all values are None.

    Returns:
        A Case-When expression that tries each field and
        returns the specified default value when all of
        them are None.
    """

    when_clauses = [
        expressions.When(
            ~expressions.Q(**{field: None}), then=expressions.F(field)
        )
        for field in reversed(fields)
    ]

    return expressions.Case(
        *when_clauses,
        default=expressions.Value(default),
        output_field=CharField(),
    )


class ExcludedCol(expressions.Expression):
    """References a column in PostgreSQL's special EXCLUDED column, which is
    used in upserts to refer to the data about to be inserted/updated.

    See: https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
    """

    def __init__(self, name: str):
        self.name = name

    def as_sql(self, compiler, connection):
        quoted_name = connection.ops.quote_name(self.name)
        return f"EXCLUDED.{quoted_name}", tuple()
