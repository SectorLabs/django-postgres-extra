class MaterializedViewSchemaEditorMixin:
    def create_model(self, base, model):
        """Ran when a new model is created."""

        if not self._is_materialized_view(model):
            return base.create_model(model)

        model.view.create()

    def delete_model(self, base, model):
        """Ran when a model is deleted."""

        if not self._is_materialized_view(model):
            return base.delete_model(model)

        model.view.drop()

    def _is_materialized_view(self, model) -> bool:
        """Gets whether the specified model is a materialized view."""

        return hasattr(model._meta, 'view_query')
