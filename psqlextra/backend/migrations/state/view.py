from typing import Type

from psqlextra.models import PostgresViewModel

from .model import PostgresModelState


class PostgresViewModelState(PostgresModelState):
    """Represents the state of a :see:PostgresViewModel in the migrations."""

    def __init__(self, *args, view_options={}, **kwargs):
        """Initializes a new instance of :see:PostgresViewModelState.

        Arguments:
            view_options:
                Dictionary of options for views.
                See: PostgresViewModelMeta for a list.
        """

        super().__init__(*args, **kwargs)

        self.view_options = dict(view_options)

    @classmethod
    def _pre_new(
        cls, model: PostgresViewModel, model_state: "PostgresViewModelState"
    ) -> "PostgresViewModelState":
        """Called when a new model state is created from the specified
        model."""

        model_state.view_options = dict(model._view_meta.original_attrs)
        return model_state

    def _pre_clone(
        self, model_state: "PostgresViewModelState"
    ) -> "PostgresViewModelState":
        """Called when this model state is cloned."""

        model_state.view_options = dict(self.view_options)
        return model_state

    def _pre_render(self, name: str, bases, attributes):
        """Called when this model state is rendered into a model."""

        view_meta = type("ViewMeta", (), dict(self.view_options))
        return name, bases, {**attributes, "ViewMeta": view_meta}

    @classmethod
    def _get_base_model_class(self) -> Type[PostgresViewModel]:
        """Gets the class to use as a base class for rendered models."""

        return PostgresViewModel
