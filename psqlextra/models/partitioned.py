from typing import Iterable, List, Optional, Tuple

import django

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
        partitioning_sub_method = getattr(partitioning_meta_class, "sub_method", None)
        partitioning_sub_key = getattr(partitioning_meta_class, "sub_key", None)


        if django.VERSION >= (5, 2):
            for base in bases:
                cls._delete_auto_created_fields(base)

            cls._create_primary_key(attrs, partitioning_key, partitioning_sub_key)

        patitioning_meta = PostgresPartitionedModelOptions(
            method=partitioning_method or cls.default_method,
            key=partitioning_key or cls.default_key,
            sub_method=partitioning_sub_method,
            sub_key=partitioning_sub_key,
        )

        new_class = super().__new__(cls, name, bases, attrs, **kwargs)
        new_class.add_to_class("_partitioning_meta", patitioning_meta)
        return new_class

    @classmethod
    def _create_primary_key(
        cls, attrs, partitioning_key: Optional[List[str]], partitioning_sub_key: Optional[List[str]]
    ) -> None:
        from django.db.models.fields.composite import CompositePrimaryKey

        # Find any existing primary key the user might have declared.
        #
        # If it is a composite primary key, we will do nothing and
        # keep it as it is. You're own your own.
        pk = cls._find_primary_key(attrs)
        if pk and isinstance(pk[1], CompositePrimaryKey):
            return

        # Create an `id` field (auto-incrementing) if there is no
        # primary key yet.
        #
        # This matches standard Django behavior.
        if not pk:
            attrs["id"] = attrs.get("id") or cls._create_auto_field(attrs)
            pk_fields = ["id"]
        else:
            pk_fields = [pk[0]]

        partitioning_keys = (
            partitioning_key
            if isinstance(partitioning_key, list)
            else list(filter(None, [partitioning_key]))
        )

        partitioning_sub_key = (
            partitioning_sub_key
            if isinstance(partitioning_sub_key, list)
            else list(filter(None, [partitioning_sub_key]))
        )

        unique_pk_fields = set(pk_fields + (partitioning_keys or []) + (partitioning_sub_key or []))
        if len(unique_pk_fields) <= 1:
            if "id" in attrs:
                attrs["id"].primary_key = True
            return

        # You might have done something like this:
        #
        # id = models.AutoField(primary_key=True)
        # pk = CompositePrimaryKey("id", "timestamp")
        #
        # The `primary_key` attribute has to be removed
        # from the `id` field in the example above to
        # avoid having two primary keys.
        #
        # Without this, the generated schema will
        # have two primary keys, which is an error.
        for field in attrs.values():
            is_pk = getattr(field, "primary_key", False)
            if is_pk:
                field.primary_key = False

        auto_generated_pk = CompositePrimaryKey(*sorted(unique_pk_fields))
        attrs["pk"] = auto_generated_pk

    @classmethod
    def _create_auto_field(cls, attrs):
        app_label = attrs.get("app_label")
        meta_class = attrs.get("Meta", None)

        pk_class = Options(meta_class, app_label)._get_default_pk_class()
        return pk_class(verbose_name="ID", auto_created=True)

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

    @classmethod
    def _delete_auto_created_fields(cls, model: models.Model):
        """Base classes might be injecting an auto-generated `id` field before
        we even have the chance of doing this ourselves.

        Delete any auto generated fields from the base class so that we
        can declare our own. If there is no auto-generated field, one
        will be added anyways by our own logic
        """

        fields = model._meta.local_fields + model._meta.local_many_to_many
        for field in fields:
            auto_created = getattr(field, "auto_created", False)
            if auto_created:
                if field in model._meta.local_fields:
                    model._meta.local_fields.remove(field)

                if field in model._meta.fields:
                    model._meta.fields.remove(field)  # type: ignore [attr-defined]

                if hasattr(model, field.name):
                    delattr(model, field.name)


class PostgresPartitionedModel(
    PostgresModel, metaclass=PostgresPartitionedModelMeta
):
    """Base class for taking advantage of PostgreSQL's 11.x native support for
    table partitioning."""

    _partitioning_meta: PostgresPartitionedModelOptions

    class Meta:
        abstract = True
        base_manager_name = "objects"
