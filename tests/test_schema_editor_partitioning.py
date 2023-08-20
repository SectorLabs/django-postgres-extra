import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connection, models
from django.db.models import UniqueConstraint, CheckConstraint, Q
from django.utils.timezone import now

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.types import PostgresPartitioningMethod
from . import db_introspection
from .fake_model import define_fake_partitioned_model


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_create_delete_partitioned_model_range():
    """Tests whether creating a partitioned model and adding a list partition
    to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.RANGE
    key = ["timestamp"]

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {
            "method": method,
            "key": key,
            "per_partition_constraints": (
                UniqueConstraint(fields=("name",), name="test_unique_constraint"),
                CheckConstraint(check=Q(timestamp__gt=now()), name="test_check_constraint")
            ),
        },
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_range_partition(model, "pt1", "2019-01-01", "2019-02-01")

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_pt1"
    assert len(table.partitions[0].constraints) == 3
    for constraint in model._partitioning_meta.per_partition_constraints:
        assert constraint.name in table.partitions[0].constraints

    schema_editor.delete_partitioned_model(model)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert not table

    partitions = db_introspection.get_partitions(model._meta.db_table)
    assert len(partitions) == 0


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_create_delete_partitioned_model_list():
    """Tests whether creating a partitioned model and adding a range partition
    to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "category": models.TextField(), "timestamp": models.DateTimeField()},
        {
            "method": method,
            "key": key,
            "per_partition_constraints": (
                UniqueConstraint(fields=("name",), name="test_unique_constraint"),
                CheckConstraint(check=Q(timestamp__gt=now()), name="test_check_constraint")
            ),
        },
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_list_partition(model, "pt1", ["car", "boat"])

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_pt1"
    assert len(table.partitions[0].constraints) == 3
    for constraint in model._partitioning_meta.per_partition_constraints:
        assert constraint.name in table.partitions[0].constraints

    schema_editor.delete_partitioned_model(model)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert not table

    partitions = db_introspection.get_partitions(model._meta.db_table)
    assert len(partitions) == 0


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize("key", [["name"], ["id", "name"]])
def test_schema_editor_create_delete_partitioned_model_hash(key):
    """Tests whether creating a partitioned model and adding a hash partition
    to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.HASH

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {
            "method": method,
            "key": key,
            "per_partition_constraints": (
                UniqueConstraint(fields=("name",), name="test_unique_constraint"),
                CheckConstraint(check=Q(timestamp__gt=now()), name="test_check_constraint")
            ),
        },
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_hash_partition(model, "pt1", modulus=1, remainder=0)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_pt1"
    assert len(table.partitions[0].constraints) == 3
    for constraint in model._partitioning_meta.per_partition_constraints:
        assert constraint.name in table.partitions[0].constraints

    schema_editor.delete_partitioned_model(model)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert not table

    partitions = db_introspection.get_partitions(model._meta.db_table)
    assert len(partitions) == 0


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_create_delete_partitioned_model_default():
    """Tests whether creating a partitioned model and adding a default
    partition to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "category": models.TextField()},
        {"method": method, "key": key},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_default_partition(model, "default")

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_default"

    schema_editor.delete_partitioned_model(model)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert not table

    partitions = db_introspection.get_partitions(model._meta.db_table)
    assert len(partitions) == 0


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_create_partitioned_model_no_method():
    """Tests whether its possible to create a partitioned model without
    explicitly setting a partitioning method.

    The default is "range" so setting one explicitely should not be
    needed.
    """

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"key": ["timestamp"]},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    pt = db_introspection.get_partitioned_table(model._meta.db_table)
    assert pt.method == PostgresPartitioningMethod.RANGE
    assert len(pt.partitions) == 0


def test_schema_editor_create_partitioned_model_no_key():
    """Tests whether trying to create a partitioned model without a
    partitioning key raises :see:ImproperlyConfigured as its not possible to
    create a partitioned model without one and we cannot have a sane
    default."""

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"method": PostgresPartitioningMethod.RANGE},
    )

    schema_editor = PostgresSchemaEditor(connection)

    with pytest.raises(ImproperlyConfigured):
        schema_editor.create_partitioned_model(model)


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_add_range_partition():
    """Tests whether adding a range partition works."""

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {
            "key": ["timestamp"],
            "per_partition_constraints": (
                UniqueConstraint(fields=("name",), name="test_unique_constraint"),
                CheckConstraint(check=Q(timestamp__gt=now()), name="test_check_constraint")
            ),
        },
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_range_partition(
        model,
        name="mypartition",
        from_values="2019-1-1",
        to_values="2019-2-1",
        comment="test",
    )

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 1
    assert table.partitions[0].name == "mypartition"
    assert (
        table.partitions[0].full_name == f"{model._meta.db_table}_mypartition"
    )
    assert table.partitions[0].comment == "test"
    assert len(table.partitions[0].constraints) == 3
    for constraint in model._partitioning_meta.per_partition_constraints:
        assert constraint.name in table.partitions[0].constraints

    schema_editor.delete_partition(model, "mypartition")
    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 0


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_add_list_partition():
    """Tests whether adding a list partition works."""

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {
            "method": PostgresPartitioningMethod.LIST,
            "key": ["name"],
            "per_partition_constraints": (
                UniqueConstraint(fields=("name",), name="test_unique_constraint"),
                CheckConstraint(check=Q(timestamp__gt=now()), name="test_check_constraint")
            ),
        },
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_list_partition(
        model, name="mypartition", values=["1"], comment="test"
    )

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 1
    assert table.partitions[0].name == "mypartition"
    assert (
        table.partitions[0].full_name == f"{model._meta.db_table}_mypartition"
    )
    assert table.partitions[0].comment == "test"
    assert len(table.partitions[0].constraints) == 3
    for constraint in model._partitioning_meta.per_partition_constraints:
        assert constraint.name in table.partitions[0].constraints

    schema_editor.delete_partition(model, "mypartition")
    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 0


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "method,key",
    [
        (PostgresPartitioningMethod.RANGE, ["timestamp"]),
        (PostgresPartitioningMethod.LIST, ["name"]),
    ],
)
def test_schema_editor_add_default_partition(method, key):
    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {
            "method": method,
            "key": key,
            "per_partition_constraints": (
                UniqueConstraint(fields=("name",), name="test_unique_constraint"),
                CheckConstraint(check=Q(timestamp__gt=now()), name="test_check_constraint")
            ),
        },
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_default_partition(
        model, name="mypartition", comment="test"
    )

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 1
    assert table.partitions[0].name == "mypartition"
    assert (
        table.partitions[0].full_name == f"{model._meta.db_table}_mypartition"
    )
    assert table.partitions[0].comment == "test"
    assert len(table.partitions[0].constraints) == 3
    for constraint in model._partitioning_meta.per_partition_constraints:
        assert constraint.name in table.partitions[0].constraints

    schema_editor.delete_partition(model, "mypartition")
    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 0
