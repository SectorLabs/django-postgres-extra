from django.db import models

from .manager import PostgresManager, PostgresMaterializedViewManager


class PostgresModel(models.Model):
    """Base class for for taking advantage of PostgreSQL specific features."""

    class Meta:
        abstract = True
        base_manager_name = 'objects'

    objects = PostgresManager()


class PostgresMaterializedViewModel(PostgresModel):
    """Base class for defining a PostgreSQL materialized view model."""

    class Meta:
        abstract = True
        base_manager_name = 'objects'

    view = PostgresMaterializedViewManager()
