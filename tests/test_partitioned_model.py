import django
import pytest

from django.core.exceptions import ImproperlyConfigured
from django.db import models

from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import define_fake_model, define_fake_partitioned_model


def test_partitioned_model_abstract():
    """Tests whether :see:PostgresPartitionedModel is abstract."""

    assert PostgresPartitionedModel._meta.abstract


def test_partitioning_model_options_meta():
    """Tests whether the `_partitioning_meta` attribute is available on the
    class (created by the meta class) and not just creating when the model is
    instantiated."""

    assert PostgresPartitionedModel._partitioning_meta


def test_partitioned_model_default_options():
    """Tests whether the default partitioning options are set as expected on.

    :see:PostgresPartitionedModel.
    """

    model = define_fake_partitioned_model()

    assert model._partitioning_meta.method == PostgresPartitioningMethod.RANGE
    assert model._partitioning_meta.key == []


def test_partitioned_model_method_option():
    """Tests whether the `method` partitioning option is properly copied onto
    the options object."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(method=PostgresPartitioningMethod.LIST)
    )

    assert model._partitioning_meta.method == PostgresPartitioningMethod.LIST


def test_partitioned_model_method_option_none():
    """Tests whether setting the `method` partitioning option results in the
    default being set."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(method=None)
    )

    assert model._partitioning_meta.method == PostgresPartitioningMethod.RANGE


def test_partitioned_model_key_option():
    """Tests whether the `key` partitioning option is properly copied onto the
    options object."""

    model = define_fake_partitioned_model(
        partitioning_options=dict(key=["timestamp"])
    )

    assert model._partitioning_meta.key == ["timestamp"]


def test_partitioned_model_key_option_none():
    """Tests whether setting the `key` partitioning option results in the
    default being set."""

    model = define_fake_partitioned_model(partitioning_options=dict(key=None))

    assert model._partitioning_meta.key == []


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_custom_composite_primary_key_with_auto_field():
    model = define_fake_partitioned_model(
        fields={
            "auto_id": models.AutoField(primary_key=True),
            "my_custom_pk": models.CompositePrimaryKey("auto_id", "timestamp"),
            "timestamp": models.DateTimeField(),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    assert isinstance(model._meta.pk, models.CompositePrimaryKey)
    assert model._meta.pk.name == "my_custom_pk"
    assert model._meta.pk.columns == ("auto_id", "timestamp")


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_custom_composite_primary_key_with_id_field():
    model = define_fake_partitioned_model(
        fields={
            "id": models.IntegerField(),
            "my_custom_pk": models.CompositePrimaryKey("id", "timestamp"),
            "timestamp": models.DateTimeField(),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    assert isinstance(model._meta.pk, models.CompositePrimaryKey)
    assert model._meta.pk.name == "my_custom_pk"
    assert model._meta.pk.columns == ("id", "timestamp")


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_custom_composite_primary_key_named_id():
    model = define_fake_partitioned_model(
        fields={
            "other_field": models.TextField(),
            "id": models.CompositePrimaryKey("other_field", "timestamp"),
            "timestamp": models.DateTimeField(),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    assert isinstance(model._meta.pk, models.CompositePrimaryKey)
    assert model._meta.pk.name == "id"
    assert model._meta.pk.columns == ("other_field", "timestamp")


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_field_named_pk_not_composite_not_primary():
    with pytest.raises(ImproperlyConfigured):
        define_fake_partitioned_model(
            fields={
                "pk": models.TextField(),
                "id": models.CompositePrimaryKey("other_field", "timestamp"),
                "timestamp": models.DateTimeField(),
            },
            partitioning_options=dict(key=["timestamp"]),
        )


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_field_named_pk_not_composite():
    with pytest.raises(ImproperlyConfigured):
        define_fake_partitioned_model(
            fields={
                "pk": models.AutoField(primary_key=True),
                "timestamp": models.DateTimeField(),
            },
            partitioning_options=dict(key=["timestamp"]),
        )


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_field_multiple_pks():
    with pytest.raises(ImproperlyConfigured):
        define_fake_partitioned_model(
            fields={
                "id": models.AutoField(primary_key=True),
                "another_pk": models.TextField(primary_key=True),
                "timestamp": models.DateTimeField(),
                "real_pk": models.CompositePrimaryKey("id", "timestamp"),
            },
            partitioning_options=dict(key=["timestamp"]),
        )


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_no_pk_defined():
    model = define_fake_partitioned_model(
        fields={
            "timestamp": models.DateTimeField(),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    assert isinstance(model._meta.pk, models.CompositePrimaryKey)
    assert model._meta.pk.name == "pk"
    assert model._meta.pk.columns == ("id", "timestamp")

    id_field = model._meta.get_field("id")
    assert id_field.name == "id"
    assert id_field.column == "id"
    assert isinstance(id_field, models.AutoField)
    assert id_field.primary_key is False


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_composite_primary_key():
    model = define_fake_partitioned_model(
        fields={
            "id": models.AutoField(primary_key=True),
            "pk": models.CompositePrimaryKey("id", "timestamp"),
            "timestamp": models.DateTimeField(),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    assert isinstance(model._meta.pk, models.CompositePrimaryKey)
    assert model._meta.pk.name == "pk"
    assert model._meta.pk.columns == ("id", "timestamp")


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_composite_primary_key_foreign_key():
    model = define_fake_partitioned_model(
        fields={
            "timestamp": models.DateTimeField(),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    define_fake_model(
        fields={
            "model": models.ForeignKey(model, on_delete=models.CASCADE),
        },
    )


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
def test_partitioned_model_custom_composite_primary_key_foreign_key():
    model = define_fake_partitioned_model(
        fields={
            "id": models.TextField(primary_key=True),
            "timestamp": models.DateTimeField(),
            "custom": models.CompositePrimaryKey("id", "timestamp"),
        },
        partitioning_options=dict(key=["timestamp"]),
    )

    define_fake_model(
        fields={
            "model": models.ForeignKey(model, on_delete=models.CASCADE),
        },
    )
