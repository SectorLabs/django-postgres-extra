from .partition import PostgresPartitionOperation


class PostgresAddListPartition(PostgresPartitionOperation):
    """Adds a new list partition to a :see:PartitionedPostgresModel."""

    def __init__(self, model_name, name, values):
        """Initializes new instance of :see:AddListPartition.

        Arguments:
            model_name:
                The name of the :see:PartitionedPostgresModel.

            name:
                The name to give to the new partition table.

            values:
                Partition key values that should be
                stored in this partition.
        """

        super().__init__(model_name, name)

        self.values = values

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.add_list_partition(model, self.name, self.values)

    def deconstruct(self):
        name, args, kwargs = super().deconstruct()
        kwargs["values"] = self.values

        return name, args, kwargs
