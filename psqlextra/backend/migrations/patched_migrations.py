from contextlib import contextmanager

from .patched_autodetector import patched_autodetector
from .patched_project_state import patched_project_state


@contextmanager
def postgres_patched_migrations():
    """Patches migration related classes/functions to extend how Django
    generates and applies migrations.

    This adds support for automatically detecting changes in Postgres
    specific models.
    """

    with patched_project_state():
        with patched_autodetector():
            yield
