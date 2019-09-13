from django.db import models

from psqlextra.manager import PostgresManager


class PostgresModel(models.Model):
    """Base class for for taking advantage of PostgreSQL specific features."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    objects = PostgresManager()
