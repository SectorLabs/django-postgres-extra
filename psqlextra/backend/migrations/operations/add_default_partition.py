from psqlextra.backend.migrations.state import PostgresPartitionState

from .partition import PostgresPartitionOperation


class PostgresAddDefaultPartition(PostgresPartitionOperation):
    """Adds a new default partition to a :see:PartitionedPostgresModel."""

    def state_forwards(self, app_label, state):
        model_state = state.models[(app_label, self.model_name_lower)]
        model_state.add_partition(
            PostgresPartitionState(
                app_label=app_label, model_name=self.model_name, name=self.name
            )
        )

        state.reload_model(app_label, self.model_name_lower)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.add_default_partition(model, self.name)

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        model = from_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)

    def describe(self) -> str:
        return "Creates default partition '%s' on %s" % (
            self.name,
            self.model_name,
        )
