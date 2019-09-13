from .partition import PostgresPartitionOperation


class PostgresDeletePartition(PostgresPartitionOperation):
    """Deletes a partition that's part of a :see:PartitionedPostgresModel."""

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)
