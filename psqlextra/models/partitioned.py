from django.db.models.base import ModelBase

from psqlextra.types import PostgresPartitioningMethod

from .base import PostgresModel
from .options import PostgresPartitionedModelOptions


class PostgresPartitionedModelMeta(ModelBase):
    """Custom meta class for :see:PostgresPartitionedModel.

    This meta class extracts attributes from the inner
    `PartitioningMeta` class and copies it onto a `_partitioning_meta`
    attribute. This is similar to how Django's `_meta` works.
    """

    default_method = PostgresPartitioningMethod.RANGE
    default_key = []

    def __new__(cls, name, bases, attrs, **kwargs):
        new_class = super().__new__(cls, name, bases, attrs, **kwargs)
        meta_class = attrs.pop("PartitioningMeta", None)

        method = getattr(meta_class, "method", None)
        key = getattr(meta_class, "key", None)

        patitioning_meta = PostgresPartitionedModelOptions(
            method=method or cls.default_method, key=key or cls.default_key
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
