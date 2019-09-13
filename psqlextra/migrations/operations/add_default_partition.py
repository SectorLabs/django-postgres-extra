from django.db.migrations.operations.base import Operation


class AddDefaultPartition(Operation):
    """Adds a new default partition to a :see:PartitionedPostgresModel."""

    def __init__(self, model_name, name, from_values, to_values):
        """Initializes new instance of :see:AddDefaultPartition.

        Arguments:
            model_name:
                The name of the :see:PartitionedPostgresModel.

            name:
                The name to give to the new partition table.
        """

        self.model_name = model_name
        self.name = name

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.add_default_partition(model, self.name)

    def deconstruct(self):
        kwargs = {"model_name": self.model_name, "name": self.name}

        return (self.__class__.__qualname__, [], kwargs)
