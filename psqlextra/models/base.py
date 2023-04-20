from typing import Any

from django.db import models
from django.db.models import Manager

from psqlextra.manager import PostgresManager


class PostgresModel(models.Model):
    """Base class for for taking advantage of PostgreSQL specific features."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    objects: "Manager[Any]" = PostgresManager()
