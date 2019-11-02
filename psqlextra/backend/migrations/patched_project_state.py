from contextlib import contextmanager
from unittest import mock

from django.db.migrations.state import ProjectState

from psqlextra.models import (
    PostgresMaterializedViewModel,
    PostgresPartitionedModel,
    PostgresViewModel,
)

from .state import (
    PostgresMaterializedViewModelState,
    PostgresPartitionedModelState,
    PostgresViewModelState,
)

# original `ProjectState.from_apps` function,
# saved here so the patched version can call
# the original
original_from_apps = ProjectState.from_apps


def project_state_from_apps(apps):
    """Creates a :see:ProjectState instance from the specified list of apps."""

    project_state = original_from_apps(apps)
    for model in apps.get_models(include_swapped=True):
        model_state = None

        # for some of our custom models, use the more specific model
        # state.. for everything else, business as usual
        if issubclass(model, PostgresPartitionedModel):
            model_state = PostgresPartitionedModelState.from_model(model)
        elif issubclass(model, PostgresMaterializedViewModel):
            model_state = PostgresMaterializedViewModelState.from_model(model)
        elif issubclass(model, PostgresViewModel):
            model_state = PostgresViewModelState.from_model(model)
        else:
            continue

        model_state_key = (model_state.app_label, model_state.name_lower)
        project_state.models[model_state_key] = model_state

    return project_state


@contextmanager
def patched_project_state():
    """Patches the standard Django :see:ProjectState.from_apps for the duration
    of the context.

    The patch intercepts the `from_apps` function to control
    how model state is creatd. We want to use our custom
    model state classes for certain types of models.

    We have to do this because there is no way in Django
    to extend the project state otherwise.
    """

    from_apps_module_path = "django.db.migrations.state"
    from_apps_class_path = f"{from_apps_module_path}.ProjectState"
    from_apps_path = f"{from_apps_class_path}.from_apps"

    with mock.patch(from_apps_path, new=project_state_from_apps):
        yield
