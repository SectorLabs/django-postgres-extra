import pytest

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .util import define_fake_model


@pytest.mark.parametrize("method", [PostgresPartitioningMethod.RANGE])
def test_schema_editor_create_partitioned_model_range(method):
    """Tests whether creating a partitioned model
    and adding a list partition to it using the
    :see:PostgresSchemaEditor works."""

    model = define_fake_model(
        {
            "name": models.TextField(),
            "timestamp": models.DateTimeField(),
            "partitioning_method": method,
            "partitioning_key": ["timestamp"],
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)
    schema_editor.add_range_partition(
        model, model._meta.db_table + "_pt1", "2019-01-01", "2019-02-01"
    )

    with connection.cursor() as cursor:
        introspection = connection.introspection

        ptt = introspection.get_partitioned_tables(cursor)
        assert ptt[0].name == model._meta.db_table
        assert ptt[0].method == method

        pn = introspection.get_partition_names(cursor, model._meta.db_table)
        assert pn[0] == model._meta.db_table + "_pt1"


@pytest.mark.parametrize("method", [PostgresPartitioningMethod.LIST])
def test_schema_editor_create_partitioned_model_list(method):
    """Tests whether creating a partitioned model
    and adding a range partition to it using the
    :see:PostgresSchemaEditor works."""

    model = define_fake_model(
        {
            "name": models.TextField(),
            "category": models.TextField(),
            "partitioning_method": method,
            "partitioning_key": ["category"],
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)
    schema_editor.add_list_partition(
        model, model._meta.db_table + "_pt1", ["car", "boat"]
    )

    with connection.cursor() as cursor:
        introspection = connection.introspection

        ptt = introspection.get_partitioned_tables(cursor)
        assert ptt[0].name == model._meta.db_table
        assert ptt[0].method == method

        pn = introspection.get_partition_names(cursor, model._meta.db_table)
        assert pn[0] == model._meta.db_table + "_pt1"


def test_schema_editor_create_partitioned_model_default():
    """Tests whether creating a partitioned model
    and adding a default partition to it using the
    :see:PostgresSchemaEditor works."""

    model = define_fake_model(
        {
            "name": models.TextField(),
            "category": models.TextField(),
            "partitioning_method": PostgresPartitioningMethod.LIST,
            "partitioning_key": ["category"],
        },
        PostgresPartitionedModel,
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)
    schema_editor.add_default_partition(model, model._meta.db_table + "_pt1")

    with connection.cursor() as cursor:
        introspection = connection.introspection

        pn = introspection.get_partition_names(cursor, model._meta.db_table)
        assert pn[0] == model._meta.db_table + "_pt1"


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

        ptt = introspection.get_partitioned_tables(cursor)
        assert ptt[0].method == PostgresPartitioningMethod.RANGE


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
