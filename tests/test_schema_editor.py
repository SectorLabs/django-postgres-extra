import pytest

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.types import PostgresPartitioningMethod

from .fake_model import define_fake_partitioning_model


def test_schema_editor_create_delete_partitioned_model_range():
    """Tests whether creating a partitioned model and adding a list partition
    to it using the.

    :see:PostgresSchemaEditor works.
    """

    method = PostgresPartitioningMethod.RANGE
    key = ["timestamp"]

    model = define_fake_partitioning_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"method": method, "key": key},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_range_partition(model, "pt1", "2019-01-01", "2019-02-01")

    with connection.cursor() as cursor:
        introspection = connection.introspection

        table = introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )
        assert table.name == model._meta.db_table
        assert table.method == method
        assert table.key == key
        assert table.partitions[0].name == model._meta.db_table + "_pt1"

    schema_editor.delete_partitioned_model(model)

    with connection.cursor() as cursor:
        introspection = connection.introspection

        table = introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )
        assert not table

        partitions = introspection.get_partitions(cursor, model._meta.db_table)
        assert len(partitions) == 0


def test_schema_editor_create_delete_partitioned_model_list():
    """Tests whether creating a partitioned model and adding a range partition
    to it using the.

    :see:PostgresSchemaEditor works.
    """

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_partitioning_model(
        {"name": models.TextField(), "category": models.TextField()},
        {"method": method, "key": key},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_list_partition(model, "pt1", ["car", "boat"])

    with connection.cursor() as cursor:
        introspection = connection.introspection

        table = introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )
        assert table.name == model._meta.db_table
        assert table.method == method
        assert table.key == key
        assert table.partitions[0].name == model._meta.db_table + "_pt1"

    schema_editor.delete_partitioned_model(model)

    with connection.cursor() as cursor:
        introspection = connection.introspection

        table = introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )
        assert not table

        partitions = introspection.get_partitions(cursor, model._meta.db_table)
        assert len(partitions) == 0


def test_schema_editor_create_delete_partitioned_model_default():
    """Tests whether creating a partitioned model and adding a default
    partition to it using the.

    :see:PostgresSchemaEditor works.
    """

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_partitioning_model(
        {"name": models.TextField(), "category": models.TextField()},
        {"method": method, "key": key},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_default_partition(model, "default")

    with connection.cursor() as cursor:
        introspection = connection.introspection

        table = introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )
        assert table.name == model._meta.db_table
        assert table.method == method
        assert table.key == key
        assert table.partitions[0].name == model._meta.db_table + "_default"

    schema_editor.delete_partitioned_model(model)

    with connection.cursor() as cursor:
        introspection = connection.introspection

        table = introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )
        assert not table

        partitions = introspection.get_partitions(cursor, model._meta.db_table)
        assert len(partitions) == 0


def test_schema_editor_create_partitioned_model_no_method():
    """Tests whether its possible to create a partitioned model without
    explicitly setting a partitioning method.

    The default is "range" so setting one explicitely should not be
    needed.
    """

    model = define_fake_partitioning_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"key": ["timestamp"]},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    with connection.cursor() as cursor:
        introspection = connection.introspection

        pt = introspection.get_partitioned_table(cursor, model._meta.db_table)
        assert pt.method == PostgresPartitioningMethod.RANGE
        assert len(pt.partitions) == 0


def test_schema_editor_create_partitioned_model_no_key():
    """Tests whether trying to create a partitioned model without a
    partitioning key raises.

    :see:ImproperlyConfigured as its not possible to create
    a partitioned model without one and we cannot
    have a sane default.
    """

    model = define_fake_partitioning_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"method": PostgresPartitioningMethod.RANGE},
    )

    schema_editor = PostgresSchemaEditor(connection)

    with pytest.raises(ImproperlyConfigured):
        schema_editor.create_partitioned_model(model)
