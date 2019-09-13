from django.db.migrations.operations.models import DeleteModel


class PostgresDeletePartition(DeleteModel):
    """Deletes a partition that's part of a :see:PartitionedPostgresModel."""

    def __init__(self, model_name, name):
        """Initializes new instance of :see:AddListPartition.

        Arguments:
            model_name:
                The name of the :see:PartitionedPostgresModel.

            name:
                The name of the partition to delete.
        """

        self.model_name = model_name
        self.name = name

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)

    def deconstruct(self):
        kwargs = {"model_name": self.model_name, "name": self.name}

        return (self.__class__.__qualname__, [], kwargs)
