from django.db.migrations.operations.models import CreateModel


class CreatePartitionedModel(CreateModel):
    """Creates the model as a natively PostgreSQL 11.x partitioned table."""

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.create_partitioned_model(model)
