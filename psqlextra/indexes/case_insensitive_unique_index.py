from django.db.models.indexes import Index


class CaseInsensitiveUniqueIndex(Index):
    sql_create_unique_index = (
        "CREATE UNIQUE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s"
    )

    def create_sql(self, model, schema_editor, using="", **kwargs):
        statement = super().create_sql(model, schema_editor, using)
        statement.template = self.sql_create_unique_index

        column_collection = statement.parts["columns"]
        statement.parts["columns"] = ", ".join(
            [
                "LOWER(%s)" % self._quote_column(column_collection, column, idx)
                for idx, column in enumerate(column_collection.columns)
            ]
        )

        return statement

    def deconstruct(self):
        """Serializes the :see:CaseInsensitiveUniqueIndex for the migrations
        file."""
        _, args, kwargs = super().deconstruct()
        path = "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        path = path.replace("django.db.models.indexes", "django.db.models")

        return path, args, kwargs

    @staticmethod
    def _quote_column(column_collection, column, idx):
        quoted_name = column_collection.quote_name(column)
        try:
            return quoted_name + column_collection.col_suffixes[idx]
        except IndexError:
            return column_collection.quote_name(column)
