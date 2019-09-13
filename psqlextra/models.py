from dataclasses import dataclass, field
from typing import List

from django.db import models
from django.db.models.base import ModelBase

from .manager import PostgresManager
from .types import PostgresPartitioningMethod


class PostgresModel(models.Model):
    """Base class for for taking advantage of PostgreSQL specific features."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    objects = PostgresManager()


@dataclass
class PostgresPartitioningModelOptions:
    """Instance of the :see:PartitioningMeta class declared on
    :see:PostgresPartitioningModel.

    This is where attributes copied from the model's `PartitioningMeta`
    are held.
    """

    method: PostgresPartitioningMethod = PostgresPartitioningMethod.RANGE
    key: List[str] = field(default_factory=list)


class PostgresPartitionedModelMeta(ModelBase):
    """Custom meta class for :see:PostgresPartitionedModel.

    This meta class extracts attributes from the inner
    `PartitioningMeta` class and copies it onto a `_partitioning_meta`
    attribute. This is similar to how Django's `_meta` works.
    """

    def __new__(cls, name, bases, attrs, **kwargs):
        new_class = super().__new__(cls, name, bases, attrs, **kwargs)

        attr_partitioning_meta = attrs.pop("PartitioningMeta", None)

        method = (
            getattr(attr_partitioning_meta, "method", None)
            or PostgresPartitioningMethod.RANGE
        )
        key = getattr(attr_partitioning_meta, "key", None) or []

        patitioning_meta = PostgresPartitioningModelOptions(
            method=method, key=key
        )
        new_class.add_to_class("_partitioning_meta", patitioning_meta)

        return new_class


class PostgresPartitionedModel(
    PostgresModel, metaclass=PostgresPartitionedModelMeta
):
    """Base class for taking advantage of PostgreSQL's 11.x native support for
    table partitioning."""

    class Meta:
        abstract = True
        base_manager_name = "objects"

    class PartitioningMeta:
        method: PostgresPartitioningMethod = PostgresPartitioningMethod.RANGE
        key: List[str] = []
