from typing import Any, List, Optional, Tuple, Type, TypeVar, cast

from django.db import DEFAULT_DB_ALIAS, connections, models
from django.db.models.expressions import Value
from django.db.models.query import RawQuerySet
from django.db.models.sql import Query
from django.db.models.sql.compiler import SQLCompiler

TModel = TypeVar("TModel", bound=models.Model)


class StaticRowQueryCompiler(SQLCompiler):
    has_extra_select = False

    def as_sql(self, *args, **kwargs):
        cols = []
        params = []

        select, _, _ = self.get_select()

        for _, (s_sql, s_params), s_alias in select:
            cols.append(
                "%s AS %s"
                % (
                    s_sql,
                    self.connection.ops.quote_name(s_alias),
                )
            )

            params.extend(s_params)

        return f"SELECT {', '.join(cols)}", tuple(params)


class StaticRowQuery(Query):
    def __init__(
        self, model: Type[models.Model], using: str = DEFAULT_DB_ALIAS
    ):
        self.using = using

        super().__init__(model)

    def get_columns(self):
        return list(self.annotations.keys())

    def get_compiler(
        self, using: Optional[str] = None, connection=None, elide_empty=True
    ):
        using = using or self.using

        compiler = StaticRowQueryCompiler(
            self, connection or connections[using], using
        )
        compiler.setup_query()

        return compiler

    def __iter__(self):
        compiler = self.get_compiler()

        cursor = compiler.connection.cursor()
        cursor.execute(*compiler.as_sql())

        return iter(cursor)


class StaticRowQuerySet(RawQuerySet):
    """Query set that compiles queries that don't select from anything and have
    their values hard-coded.

    Example:

        >>> SELECT 'mystring' AS something, -1 AS somethingelse;

    This is used when you want to add some rows to a result
    set using UNION in SQL.
    """

    def __init__(
        self,
        model: Type[models.Model],
        row: List[Tuple[str, Value]],
        using: str = DEFAULT_DB_ALIAS,
    ) -> None:
        query = StaticRowQuery(model, using)
        query.default_cols = False
        query.annotations = dict(row)

        sql, params = query.sql_with_params()

        # cast(Tuple[Any], params) because `RawQuerySet.__init_` is mistyped
        super().__init__(
            raw_query=sql,
            model=model,
            query=query,
            params=cast(Tuple[Any], params),
        )
