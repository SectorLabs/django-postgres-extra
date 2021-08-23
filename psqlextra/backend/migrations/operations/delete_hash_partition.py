from .delete_partition import PostgresDeletePartition


class PostgresDeleteHashPartition(PostgresDeletePartition):
    """Deletes a hash partition that's part of a.

    :see:PartitionedPostgresModel.
    """

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        model = to_state.apps.get_model(app_label, self.model_name)
        model_state = to_state.models[(app_label, self.model_name_lower)]

        if self.allow_migrate_model(schema_editor.connection.alias, model):
            partition_state = model_state.partitions[self.name]
            schema_editor.add_hash_partition(
                model,
                partition_state.name,
                partition_state.modulus,
                partition_state.remainder,
            )

    def describe(self) -> str:
        return "Deletes hash partition '%s' on %s" % (
            self.name,
            self.model_name,
        )
