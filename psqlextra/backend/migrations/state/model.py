from collections.abc import Mapping
from typing import Type

from django.db.migrations.state import ModelState
from django.db.models import Model

from psqlextra.models import PostgresModel


class PostgresModelState(ModelState):
    """Base for custom model states.

    We need this base class to create some hooks into rendering models,
    creating new states and cloning state. Most of the logic resides
    here in the base class. Our derived classes implement the `_pre_*`
    methods.
    """

    @classmethod
    def from_model(
        cls, model: PostgresModel, *args, **kwargs
    ) -> "PostgresModelState":
        """Creates a new :see:PostgresModelState object from the specified
        model.

        We override this so derived classes get the chance to attach
        additional information to the newly created model state.

        We also need to patch up the base class for the model.
        """

        model_state = super().from_model(model, *args, **kwargs)
        model_state = cls._pre_new(model, model_state)

        # django does not add abstract bases as a base in migrations
        # because it assumes the base does not add anything important
        # in a migration.. but it does, so we replace the Model
        # base with the actual base
        bases = tuple()
        for base in model_state.bases:
            if issubclass(base, Model):
                bases += (cls._get_base_model_class(),)
            else:
                bases += (base,)

        model_state.bases = bases
        return model_state

    def clone(self) -> "PostgresModelState":
        """Gets an exact copy of this :see:PostgresModelState."""

        model_state = super().clone()
        return self._pre_clone(model_state)

    def render(self, apps):
        """Renders this state into an actual model."""

        # TODO: figure out a way to do this witout pretty much
        #       copying the base class's implementation

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

        if isinstance(self.fields, Mapping):
            # In Django 3.1 `self.fields` became a `dict`
            fields = {
                name: field.clone() for name, field in self.fields.items()
            }
        else:
            # In Django < 3.1 `self.fields` is a list of (name, field) tuples
            fields = {name: field.clone() for name, field in self.fields}

        meta = type(
            "Meta",
            (),
            {"app_label": self.app_label, "apps": apps, **self.options},
        )

        attributes = {
            **fields,
            "Meta": meta,
            "__module__": "__fake__",
            **dict(self.construct_managers()),
        }

        return type(*self._pre_render(self.name, bases, attributes))

    @classmethod
    def _pre_new(
        cls, model: PostgresModel, model_state: "PostgresModelState"
    ) -> "PostgresModelState":
        """Called when a new model state is created from the specified
        model."""

        return model_state

    def _pre_clone(
        self, model_state: "PostgresModelState"
    ) -> "PostgresModelState":
        """Called when this model state is cloned."""

        return model_state

    def _pre_render(self, name: str, bases, attributes):
        """Called when this model state is rendered into a model."""

        return name, bases, attributes

    @classmethod
    def _get_base_model_class(self) -> Type[PostgresModel]:
        """Gets the class to use as a base class for rendered models."""

        return PostgresModel
