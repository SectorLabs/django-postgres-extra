from .partition import PostgresPartitionOperation


class PostgresDeletePartition(PostgresPartitionOperation):
    """Deletes a partition that's part of a :see:PartitionedPostgresModel."""

    def state_forwards(self, app_label, state):
        model = state.models[(app_label, self.model_name_lower)]
        model.delete_partition(self.name)

        state.reload_model(app_label, self.model_name_lower)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = from_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        model = to_state.apps.get_model(app_label, self.model_name)
        model_state = to_state.models[(app_label, self.model_name)]

        if self.allow_migrate_model(schema_editor.connection.alias, model):
            partition_state = model_state.partitions[self.name]
            schema_editor.add_default_partition(model, partition_state.name)

    def describe(self) -> str:
        return "Deletes partition %s on %s" % (self.name, self.model_name)
