from ..fields import HStoreField


class HStoreRequiredSchemaEditorMixin:
    sql_hstore_unique_create = "CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table}{using} ({columns}){extra}"
    sql_hstore_unique_drop = "DROP INDEX IF EXISTS {name}"

    def _alter_field(self, model, old_field, new_field, *args, **kwargs):
        """Ran when the configuration on a field changed."""

        super()._alter_field(
            model, old_field, new_field,
            *args, **kwargs
        )

        isinstance(old_field, HStoreField)
        isinstance(new_field, HStoreField)

    def create_model(self, model):
        """Ran when a new model is created."""

        super().create_model(model)

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue

    def delete_model(self, model):
        """Ran when a model is being deleted."""

        super().delete_model(model)

        for field in model._meta.local_fields:
            if not isinstance(field, HStoreField):
                continue
