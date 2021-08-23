from typing import Dict, List, Type

from psqlextra.models import PostgresPartitionedModel

from .model import PostgresModelState


class PostgresPartitionState:
    """Represents the state of a partition for a :see:PostgresPartitionedModel
    during a migration."""

    def __init__(self, app_label: str, model_name: str, name: str) -> None:
        self.app_label = app_label
        self.model_name = model_name
        self.name = name


class PostgresRangePartitionState(PostgresPartitionState):
    """Represents the state of a range partition for a
    :see:PostgresPartitionedModel during a migration."""

    def __init__(
        self, app_label: str, model_name: str, name: str, from_values, to_values
    ):
        super().__init__(app_label, model_name, name)

        self.from_values = from_values
        self.to_values = to_values


class PostgresListPartitionState(PostgresPartitionState):
    """Represents the state of a list partition for a
    :see:PostgresPartitionedModel during a migration."""

    def __init__(self, app_label: str, model_name: str, name: str, values):
        super().__init__(app_label, model_name, name)

        self.values = values


class PostgresHashPartitionState(PostgresPartitionState):
    """Represents the state of a hash partition for a
    :see:PostgresPartitionedModel during a migration."""

    def __init__(
        self,
        app_label: str,
        model_name: str,
        name: str,
        modulus: int,
        remainder: int,
    ):
        super().__init__(app_label, model_name, name)

        self.modulus = modulus
        self.remainder = remainder


class PostgresPartitionedModelState(PostgresModelState):
    """Represents the state of a :see:PostgresPartitionedModel in the
    migrations."""

    def __init__(
        self,
        *args,
        partitions: List[PostgresPartitionState] = [],
        partitioning_options={},
        **kwargs
    ):
        """Initializes a new instance of :see:PostgresPartitionedModelState.

        Arguments:
            partitioning_options:
                Dictionary of options for partitioning.

                See: PostgresPartitionedModelMeta for a list.
        """

        super().__init__(*args, **kwargs)

        self.partitions: Dict[str, PostgresPartitionState] = {
            partition.name: partition for partition in partitions
        }
        self.partitioning_options = dict(partitioning_options)

    def add_partition(self, partition: PostgresPartitionState):
        """Adds a partition to this partitioned model state."""

        self.partitions[partition.name] = partition

    def delete_partition(self, name: str):
        """Deletes a partition from this partitioned model state."""

        del self.partitions[name]

    @classmethod
    def _pre_new(
        cls,
        model: PostgresPartitionedModel,
        model_state: "PostgresPartitionedModelState",
    ) -> "PostgresPartitionedModelState":
        """Called when a new model state is created from the specified
        model."""

        model_state.partitions = dict()
        model_state.partitioning_options = dict(
            model._partitioning_meta.original_attrs
        )
        return model_state

    def _pre_clone(
        self, model_state: "PostgresPartitionedModelState"
    ) -> "PostgresPartitionedModelState":
        """Called when this model state is cloned."""

        model_state.partitions = dict(self.partitions)
        model_state.partitioning_options = dict(self.partitioning_options)
        return model_state

    def _pre_render(self, name: str, bases, attributes):
        """Called when this model state is rendered into a model."""

        partitioning_meta = type(
            "PartitioningMeta", (), dict(self.partitioning_options)
        )
        return (
            name,
            bases,
            {**attributes, "PartitioningMeta": partitioning_meta},
        )

    @classmethod
    def _get_base_model_class(self) -> Type[PostgresPartitionedModel]:
        """Gets the class to use as a base class for rendered models."""

        return PostgresPartitionedModel
