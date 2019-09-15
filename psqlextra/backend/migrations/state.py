from typing import Dict, List

from django.db.migrations.state import ModelState
from django.db.models import Model

from psqlextra.models import PostgresPartitionedModel


class PostgresPartitionState:
    """Represents the state of a partition for a.

    :see:PostgresPartitionedModel during a migration.
    """

    def __init__(self, app_label: str, model_name: str, name: str) -> None:
        self.app_label = app_label
        self.model_name = model_name
        self.name = name


class PostgresRangePartitionState(PostgresPartitionState):
    """Represents the state of a range partition for a.

    :see:PostgresPartitionedModel during a migration.
    """

    def __init__(
        self, app_label: str, model_name: str, name: str, from_values, to_values
    ):
        super().__init__(app_label, model_name, name)

        self.from_values = from_values
        self.to_values = to_values


class PostgresListPartitionState(PostgresPartitionState):
    """Represents the state of a list partition for a.

    :see:PostgresPartitionedModel during a migration.
    """

    def __init__(self, app_label: str, model_name: str, name: str, values):
        super().__init__(app_label, model_name, name)

        self.values = values


class PostgresPartitionedModelState(ModelState):
    """Represents a :see:PostgresPartitionedModel.

    We don't use the actual model class to represent models in migration
    state as its not designed to have its options changed over time.

    Instead, this state is used and after applying all migrations/
    mutations, this gets rendered into a model.
    """

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
    def from_model(
        cls, model: PostgresPartitionedModel, *args, **kwargs
    ) -> "PostgresPartitionedModelState":
        """Creates a new :see:PartitionedModelState object from the specified
        model."""

        model_state = super().from_model(model, *args, **kwargs)
        model_state.partitions = dict()
        model_state.partitioning_options = dict(
            model._partitioning_meta.original_attrs
        )

        # django does not add abstract bases as a base in migrations
        # because it assumes the base does not add anything important
        # in a migration.. but it does, so we replace the Model
        # base with the actual base: PostgresPartitionedModel
        bases = tuple()
        for base in model_state.bases:
            if issubclass(base, Model):
                bases += (PostgresPartitionedModel,)
            else:
                bases += (base,)

        model_state.bases = bases
        return model_state

    def clone(self) -> "PostgresPartitionedModelState":
        """Gets an exact copy of this :see:PostgresPartitionedModelState."""

        model_state = super().clone()
        model_state.partitions = dict(self.partitions)
        model_state.partitioning_options = dict(self.partitioning_options)

        return model_state

    def render(self, apps):
        """Renders this state into an actual model."""

        # TODO: figure out a way to do this witout pretty much
        #       copying the base class's implementation
        #       ---
        #       all we need is to add `PartitioningMeta` to
        #       the class that is being declared

        try:
            bases = tuple(
                (apps.get_model(base) if isinstance(base, str) else base)
                for base in self.bases
            )
        except LookupError:
            # TODO: this should be a InvalidBaseError
            raise ValueError(
                "Cannot resolve one or more bases from %r" % (self.bases,)
            )

        fields = {name: field.clone() for name, field in self.fields}
        meta = type(
            "Meta",
            (),
            {"app_label": self.app_label, "apps": apps, **self.options},
        )
        partitioning_meta = type(
            "PartitioningMeta", (), dict(self.partitioning_options)
        )

        attributes = {
            **fields,
            "Meta": meta,
            "PartitioningMeta": partitioning_meta,
            "__module__": "__fake__",
            **dict(self.construct_managers()),
        }

        return type(self.name, bases, attributes)
