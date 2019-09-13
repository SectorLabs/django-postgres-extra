from django.db.migrations.operations.models import DeleteModel


class DeletePartitionedModel(DeleteModel):
    """Deletes the partitioned model and all of its partitions."""

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.drop_partitioned_model(model)
