from django.db.migrations.operations.base import Operation


class PostgresPartitionOperation(Operation):
    def __init__(self, model_name: str, name: str) -> None:
        """Initializes new instance of :see:AddDefaultPartition.

        Arguments:
            model_name:
                The name of the :see:PartitionedPostgresModel.

            name:
                The name to give to the new partition table.
        """

        self.model_name = model_name
        self.model_name_lower = model_name.lower()
        self.name = name

    def deconstruct(self):
        kwargs = {"model_name": self.model_name, "name": self.name}
        return (self.__class__.__qualname__, [], kwargs)

    def state_forwards(self, *args, **kwargs):
        pass

    def state_backwards(self, *args, **kwargs):
        pass

    def reduce(self, *args, **kwargs):
        # PartitionOperation doesn't break migrations optimizations
        return True
