from psqlextra.backend.migrations.state import PostgresHashPartitionState

from .partition import PostgresPartitionOperation


class PostgresAddHashPartition(PostgresPartitionOperation):
    """Adds a new hash partition to a :see:PartitionedPostgresModel.

    Each partition will hold the rows for which the hash value of the
    partition key divided by the specified modulus will produce the
    specified remainder.
    """

    def __init__(
        self, model_name: str, name: str, modulus: int, remainder: int
    ):
        """Initializes new instance of :see:AddHashPartition.
        Arguments:
            model_name:
                The name of the :see:PartitionedPostgresModel.

            name:
                The name to give to the new partition table.

            modulus:
                Integer value by which the key is divided.

            remainder:
                The remainder of the hash value when divided by modulus.
        """

        super().__init__(model_name, name)

        self.modulus = modulus
        self.remainder = remainder

    def state_forwards(self, app_label, state):
        model = state.models[(app_label, self.model_name_lower)]
        model.add_partition(
            PostgresHashPartitionState(
                app_label=app_label,
                model_name=self.model_name,
                name=self.name,
                modulus=self.modulus,
                remainder=self.remainder,
            )
        )

        state.reload_model(app_label, self.model_name_lower)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.add_hash_partition(
                model, self.name, self.modulus, self.remainder
            )

    def database_backwards(
        self, app_label, schema_editor, from_state, to_state
    ):
        model = from_state.apps.get_model(app_label, self.model_name)
        if self.allow_migrate_model(schema_editor.connection.alias, model):
            schema_editor.delete_partition(model, self.name)

    def deconstruct(self):
        name, args, kwargs = super().deconstruct()

        kwargs["modulus"] = self.modulus
        kwargs["remainder"] = self.remainder

        return name, args, kwargs

    def describe(self) -> str:
        return "Creates hash partition %s on %s" % (self.name, self.model_name)
