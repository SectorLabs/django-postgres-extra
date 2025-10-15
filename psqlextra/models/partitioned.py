from typing import Iterable, List, Optional, Tuple

from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.db.models.base import ModelBase
from django.db.models.options import Options

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
    default_key: Iterable[str] = []

    def __new__(cls, name, bases, attrs, **kwargs):
        partitioning_meta_class = attrs.pop("PartitioningMeta", None)

        partitioning_method = getattr(partitioning_meta_class, "method", None)
        partitioning_key = getattr(partitioning_meta_class, "key", None)
        special = getattr(partitioning_meta_class, "special", None)

        if special:
            cls._create_primary_key(attrs, partitioning_key)

        patitioning_meta = PostgresPartitionedModelOptions(
            method=partitioning_method or cls.default_method,
            key=partitioning_key or cls.default_key,
        )

        new_class = super().__new__(cls, name, bases, attrs, **kwargs)
        new_class.add_to_class("_partitioning_meta", patitioning_meta)
        return new_class

    @classmethod
    def _create_primary_key(cls, attrs, partitioning_key: Optional[List[str]]):
        pk = cls._find_primary_key(attrs)
        if pk and isinstance(pk[1], CompositePrimaryKey):
            return

        if not pk:
            attrs["id"] = attrs.get("id") or cls._create_auto_field(attrs)
            pk_fields = ["id"]
        else:
            pk_fields = [pk[0]]

        unique_pk_fields = set(pk_fields + (partitioning_key or []))
        if len(unique_pk_fields) <= 1:
            return

        auto_generated_pk = CompositePrimaryKey(*sorted(unique_pk_fields))
        attrs["pk"] = auto_generated_pk

    @classmethod
    def _create_auto_field(cls, attrs):
        app_label = attrs.get("app_label")
        meta_class = attrs.get("Meta", None)

        pk_class = Options(meta_class, app_label)._get_default_pk_class()
        return pk_class(verbose_name="ID", primary_key=True, auto_created=True)

    @classmethod
    def _find_primary_key(cls, attrs) -> Optional[Tuple[str, models.Field]]:
        """Gets the field that has been marked by the user as the primary key
        field for this model.

        This is quite complex because Django allows a variety of options:

        1. No PK at all. In this case, Django generates one named `id`
           as an auto-increment integer (AutoField)

        2. One field that has `primary_key=True`. Any field can have
           this attribute, but Django would error if there were more.

        3. One field named `pk`.

        4. One field that has `primary_key=True` and a field that
           is of type `CompositePrimaryKey` that includes that
           field.

        Since a table can only have one primary key, our goal here
        is to find the field (if any) that is going to become
        the primary key of the table.

        Our logic is straight forward:

        1. If there is a `CompositePrimaryKey`, that field becomes the primary key.

        2. If there is a field with `primary_key=True`, that field becomes the primary key.

        3. There is no primary key.
        """
        from django.db.models.fields.composite import CompositePrimaryKey

        fields = {
            name: value
            for name, value in attrs.items()
            if isinstance(value, models.Field)
        }

        fields_marked_as_pk = {
            name: value for name, value in fields.items() if value.primary_key
        }

        # We cannot let the user define a field named `pk` that is not a CompositePrimaryKey
        # already because when we generate a primary key, we want to name it `pk`.
        field_named_pk = attrs.get("pk")
        if field_named_pk and not field_named_pk.primary_key:
            raise ImproperlyConfigured(
                "You cannot define a field named `pk` that is not a primary key."
            )

        if field_named_pk:
            if not isinstance(field_named_pk, CompositePrimaryKey):
                raise ImproperlyConfigured(
                    "You cannot define a field named `pk` that is not a composite primary key on a partitioned model. Either make `pk` a CompositePrimaryKey or rename it."
                )

            return ("pk", field_named_pk)

        if not fields_marked_as_pk:
            return None

        # Make sure the user didn't define N primary keys. Django would also warn
        # about this.
        #
        # One exception is a set up such as:
        #
        # >>> id = models.AutoField(primary_key=True)
        # >>> timestamp = models.DateTimeField()
        # >>> pk = models.CompositePrimaryKey("id", "timestamp")
        #
        # In this case, both `id` and `pk` are marked as primary key. Django
        # allows this and just ignores the `primary_key=True` attribute
        # on all the other fields except the composite one.
        #
        # We also handle this as expected and treat the CompositePrimaryKey
        # as the primary key.
        sorted_fields_marked_as_pk = sorted(
            list(fields_marked_as_pk.items()),
            key=lambda pair: 0
            if isinstance(pair[1], CompositePrimaryKey)
            else 1,
        )
        if len(sorted_fields_marked_as_pk[1:]) > 1:
            raise ImproperlyConfigured(
                "You cannot mark more than one fields as a primary key."
            )

        return sorted_fields_marked_as_pk[0]


class PostgresPartitionedModel(
    PostgresModel, metaclass=PostgresPartitionedModelMeta
):
    """Base class for taking advantage of PostgreSQL's 11.x native support for
    table partitioning."""

    _partitioning_meta: PostgresPartitionedModelOptions

    class Meta:
        abstract = True
        base_manager_name = "objects"
