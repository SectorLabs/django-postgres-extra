from contextlib import contextmanager
from typing import Generator, Type

from django.db import models

from .manager import PostgresManager


@contextmanager
def postgres_manager(
    model: Type[models.Model],
) -> Generator[PostgresManager, None, None]:
    """Allows you to use the :see:PostgresManager with the specified model
    instance on the fly.

    Arguments:
        model:
            The model or model instance to use this on.
    """

    manager = PostgresManager()
    manager.model = model

    yield manager
