import django

from django.db.models.indexes import Index


class UniqueIndex(Index):
    def create_sql(self, *args, **kwargs):
        if django.VERSION >= (2, 0):
            statement = super().create_sql(*args, **kwargs)
            statement.template = self._rewrite_sql(statement.template)
            return statement

        sql = super().create_sql(*args, **kwargs)
        return self._rewrite_sql(sql)

    @staticmethod
    def _rewrite_sql(sql: str) -> str:
        return sql.replace("CREATE INDEX", "CREATE UNIQUE INDEX")
