from typing import List, Tuple, Any

from django.db.models.sql.datastructures import Join


class ConditionalJoin(Join):
    """A custom JOIN statement that allows attaching
    extra conditions."""

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:ConditionalJoin."""

        super().__init__(*args, **kwargs)
        self.join_type = 'LEFT OUTER JOIN'
        self.extra_conditions = []

    def add_condition(self, field, value: Any) -> None:
        """Adds an extra condition to this join.

        Arguments:
            field:
                The field that the condition will apply to.

            value:
                The value to compare.
        """

        self.extra_conditions.append((field, value))

    def as_sql(self, compiler, connection) -> Tuple[str, List[Any]]:
        """Compiles this JOIN into a SQL string."""

        sql, params = super().as_sql(compiler, connection)
        qn = compiler.quote_name_unless_alias

        # generate the extra conditions
        extra_conditions = ' AND '.join([
            '{}.{} = %s'.format(
                qn(self.table_name),
                qn(field.column)
            )
            for field, value in self.extra_conditions
        ])

        # add to the existing params, so the connector will
        # actually nicely format the value for us
        for _, value in self.extra_conditions:
            params.append(value)

        # rewrite the sql to include the extra conditions
        rewritten_sql = sql.replace(')', ' AND {})'.format(extra_conditions))
        return rewritten_sql, params

    @classmethod
    def from_join(cls, join: Join) -> 'ConditionalJoin':
        """Creates a new :see:ConditionalJoin from the
        specified :see:Join object.

        Arguments:
            join:
                The :see:Join object to create the
                :see:ConditionalJoin object from.

        Returns:
            A :see:ConditionalJoin object created from
            the :see:Join object.
        """

        return cls(
            join.table_name,
            join.parent_alias,
            join.table_alias,
            join.join_type,
            join.join_field,
            join.nullable
        )
