from typing import Type

from psqlextra.models import PostgresMaterializedViewModel

from .view import PostgresViewModelState


class PostgresMaterializedViewModelState(PostgresViewModelState):
    """Represents the state of a :see:PostgresMaterializedViewModel in the
    migrations."""

    @classmethod
    def _get_base_model_class(self) -> Type[PostgresMaterializedViewModel]:
        """Gets the class to use as a base class for rendered models."""

        return PostgresMaterializedViewModel
