import pytest

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .util import define_fake_model


def test_schema_editor_create_partitioned_model_range():
    """Tests whether creating a partitioned model
    and adding a list partition to it using the
    :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.RANGE
    key = ["timestamp"]

    model = define_fake_model(
        {
            "name": models.TextField(),
            "timestamp": models.DateTimeField(),
            "partitioning_method": method,
            "partitioning_key": key,
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    partition1_name = model._meta.db_table + "_pt1"
    schema_editor.add_range_partition(
        model, partition1_name, "2019-01-01", "2019-02-01"
    )

    with connection.cursor() as cursor:
        introspection = connection.introspection

        pt = introspection.get_partitioned_table(cursor, model._meta.db_table)
        assert pt.name == model._meta.db_table
        assert pt.method == method
        assert pt.key == key
        assert pt.partitions[0].name == partition1_name


def test_schema_editor_create_partitioned_model_list():
    """Tests whether creating a partitioned model
    and adding a range partition to it using the
    :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_model(
        {
            "name": models.TextField(),
            "category": models.TextField(),
            "partitioning_method": method,
            "partitioning_key": key,
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    partition1_name = model._meta.db_table + "_pt1"
    schema_editor.add_list_partition(model, partition1_name, ["car", "boat"])

    with connection.cursor() as cursor:
        introspection = connection.introspection

        pt = introspection.get_partitioned_table(cursor, model._meta.db_table)
        assert pt.name == model._meta.db_table
        assert pt.method == method
        assert pt.key == key
        assert pt.partitions[0].name == partition1_name


def test_schema_editor_create_partitioned_model_default():
    """Tests whether creating a partitioned model
    and adding a default partition to it using the
    :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_model(
        {
            "name": models.TextField(),
            "category": models.TextField(),
            "partitioning_method": method,
            "partitioning_key": key,
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    partition1_name = model._meta.db_table + "_pt1"
    schema_editor.add_default_partition(model, partition1_name)

    with connection.cursor() as cursor:
        introspection = connection.introspection

        pt = introspection.get_partitioned_table(cursor, model._meta.db_table)
        assert pt.name == model._meta.db_table
        assert pt.method == method
        assert pt.key == key
        assert pt.partitions[0].name == partition1_name


def test_schema_editor_create_partitioned_model_no_method():
    """Tests whether its possible to create a partitioned
    model without explicitly setting a partitioning
    method. The default is "range" so setting one
    explicitely should not be needed."""

    model = define_fake_model(
        {
            "name": models.CharField(max_length=255),
            "timestamp": models.DateTimeField(),
            "partitioning_key": ["timestamp"],
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    with connection.cursor() as cursor:
        introspection = connection.introspection

        pt = introspection.get_partitioned_table(cursor, model._meta.db_table)
        assert pt.method == PostgresPartitioningMethod.RANGE
        assert len(pt.partitions) == 0


def test_schema_editor_create_partitioned_model_no_key():
    """Tests whether trying to create a partitioned model
    without a partitioning key raises
    :see:ImproperlyConfigured as its not possible to create
    a partitioned model without one and we cannot
    have a sane default."""

    model = define_fake_model(
        {
            "name": models.CharField(max_length=255),
            "timestamp": models.DateTimeField(),
            "partitioning_method": PostgresPartitioningMethod.RANGE,
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)

    with pytest.raises(ImproperlyConfigured):
        schema_editor.create_partitioned_model(model)
