from django.db.migrations.operations.base import Operation


class AddRangePartition(Operation):
    """Adds a new range partition to a :see:PartitionedPostgresModel."""

    def __init__(self, model_name, name, from_values, to_values):
        """Initializes new instance of :see:AddPartition.

        Arguments:
            model_name:
                The name of the :see:PartitionedPostgresModel.

            name:
                The name to give to the new partition table.

            from_values:
                Start of the partitioning key range of
                values that need to be stored in this
                partition.

            to_values:
                End of the partitioning key range of
                values that need to be stored in this
                partition.
        """

        self.model_name = model_name
        self.name = name
        self.from_values = from_values
        self.to_values = to_values

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.add_range_partition(
                model, self.name, self.from_values, self.to_values
            )

    def deconstruct(self):
        kwargs = {
            "model_name": self.model_name,
            "name": self.name,
            "from_values": self.from_values,
            "to_values": self.to_values,
        }

        return (self.__class__.__qualname__, [], kwargs)
