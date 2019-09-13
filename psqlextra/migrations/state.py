from django.db.migrations.state import ModelState

from psqlextra.models import PostgresPartitionedModel


class PostgresPartitionedModelState(ModelState):
    """Represents a :see:PostgresPartitionedModel.

    We don't use the actual model class to represent models in migration
    state as its not designed to have its options changed over time.

    Instead, this state is used and after applying all migrations/
    mutations, this gets rendered into a model.
    """

    def __init__(self, *args, partitioning_options={}, **kwargs):
        """Initializes a new instance of :see:PostgresPartitionedModelState.

        Arguments:
            partitioning_options:
                Dictionary of options for partitioning.

                See: PostgresPartitionedModelMeta for a list.
        """

        super().__init__(*args, **kwargs)

        self.partitioning_options = dict(partitioning_options)

    @classmethod
    def from_model(
        cls, model: PostgresPartitionedModel, *args, **kwargs
    ) -> "PostgresPartitionedModelState":
        """Creates a new :see:PartitionedModelState object from the specified
        model."""

        model_state = super().from_model(model, *args, **kwargs)
        model_state.partitioning_options = dict(
            model._partitioning_meta.original_attrs
        )

        return model_state

    def clone(self) -> "PostgresPartitionedModelState":
        """Gets an exact copy of this :see:PostgresPartitionedModelState."""

        model_state = super().clone()
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
