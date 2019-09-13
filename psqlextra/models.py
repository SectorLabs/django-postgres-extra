from django.db import models

from .manager import PostgresManager
from .types import PostgresPartitioningMethod


class PostgresModel(models.Model):
    """Base class for for taking advantage of PostgreSQL specific features."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    objects = PostgresManager()


class PostgresPartitionedModel(PostgresModel):
    """Base class for taking advantage of PostgreSQL's 11.x native support for
    table partitioning."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    partitioning_method = PostgresPartitioningMethod.RANGE
    partitioning_key = []
