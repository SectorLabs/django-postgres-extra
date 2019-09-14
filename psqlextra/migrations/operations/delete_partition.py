from .partition import PostgresPartitionOperation


class PostgresDeletePartition(PostgresPartitionOperation):
    """Deletes a partition that's part of a :see:PartitionedPostgresModel."""

    def state_forwards(self, app_label, state):
        model = state.models[(app_label, self.model_name)]
        model.delete_partition(self.name)

        state.reload_model(app_label, self.model_name)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)

    def describe(self) -> str:
        return "Deletes partition %s on %s" % (self.name, self.model_name)
