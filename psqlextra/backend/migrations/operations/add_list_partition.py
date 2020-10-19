from psqlextra.backend.migrations.state import PostgresListPartitionState

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

    def state_forwards(self, app_label, state):
        model = state.models[(app_label, self.model_name_lower)]
        model.add_partition(
            PostgresListPartitionState(
                app_label=app_label,
                model_name=self.model_name,
                name=self.name,
                values=self.values,
            )
        )

        state.reload_model(app_label, self.model_name_lower)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.add_list_partition(model, self.name, self.values)

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        model = from_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)

    def deconstruct(self):
        name, args, kwargs = super().deconstruct()
        kwargs["values"] = self.values

        return name, args, kwargs

    def describe(self) -> str:
        return "Creates list partition %s on %s" % (self.name, self.model_name)
