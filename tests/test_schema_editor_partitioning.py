import django
import pytest

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, models, transaction
from django.db.transaction import TransactionManagementError

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.types import PostgresPartitioningMethod

from . import db_introspection
from .fake_model import define_fake_partitioned_model


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "in_custom_tablespace",
    [False, True],
    ids=["default_tablespace", "custom_tablespace"],
)
def test_schema_editor_create_delete_partitioned_model_range(
    custom_tablespace, in_custom_tablespace
):
    """Tests whether creating a partitioned model and adding a list partition
    to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.RANGE
    key = ["timestamp"]

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"method": method, "key": key},
        {"db_tablespace": custom_tablespace if in_custom_tablespace else None},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_range_partition(model, "pt1", "2019-01-01", "2019-02-01")

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_pt1"

    schema_editor.delete_partitioned_model(model)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert not table

    partitions = db_introspection.get_partitions(model._meta.db_table)
    assert len(partitions) == 0


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "in_custom_tablespace",
    [False, True],
    ids=["default_tablespace", "custom_tablespace"],
)
def test_schema_editor_create_delete_partitioned_model_list(
    custom_tablespace, in_custom_tablespace
):
    """Tests whether creating a partitioned model and adding a range partition
    to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.LIST
    key = ["category"]

    model = define_fake_partitioned_model(
        {"name": models.TextField(), "category": models.TextField()},
        {"method": method, "key": key},
        {"db_tablespace": custom_tablespace if in_custom_tablespace else None},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_list_partition(model, "pt1", ["car", "boat"])

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_pt1"

    schema_editor.delete_partitioned_model(model)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert not table

    partitions = db_introspection.get_partitions(model._meta.db_table)
    assert len(partitions) == 0


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize("key", [["name"], ["id", "name"]])
@pytest.mark.parametrize(
    "in_custom_tablespace",
    [False, True],
    ids=["default_tablespace", "custom_tablespace"],
)
def test_schema_editor_create_delete_partitioned_model_hash(
    key, custom_tablespace, in_custom_tablespace
):
    """Tests whether creating a partitioned model and adding a hash partition
    to it using the :see:PostgresSchemaEditor works."""

    method = PostgresPartitioningMethod.HASH

    model = define_fake_partitioned_model(
        {"name": models.TextField()},
        {"method": method, "key": key},
        {"db_tablespace": custom_tablespace if in_custom_tablespace else None},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    schema_editor.add_hash_partition(model, "pt1", modulus=1, remainder=0)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert table.name == model._meta.db_table
    assert table.method == method
    assert table.key == key
    assert table.partitions[0].full_name == model._meta.db_table + "_pt1"

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
        {"key": ["timestamp"]},
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

    schema_editor.delete_partition(model, "mypartition")
    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 0


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_add_list_partition():
    """Tests whether adding a list partition works."""

    model = define_fake_partitioned_model(
        {"name": models.TextField()},
        {"method": PostgresPartitioningMethod.LIST, "key": ["name"]},
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
        {"method": method, "key": key},
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

    schema_editor.delete_partition(model, "mypartition")
    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 0


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "in_custom_tablespace",
    [False, True],
    ids=["default_tablespace", "custom_tablespace"],
)
def test_schema_editor_create_partitioned_custom_primary_key(
    custom_tablespace, in_custom_tablespace
):
    model = define_fake_partitioned_model(
        {
            "custom_pk": models.IntegerField(primary_key=True),
            "name": models.TextField(),
            "timestamp": models.DateTimeField(),
        },
        {"method": PostgresPartitioningMethod.RANGE, "key": ["timestamp"]},
        {"db_tablespace": custom_tablespace if in_custom_tablespace else None},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    constraints = db_introspection.get_constraints(model._meta.db_table)
    primary_key_constraint = next(
        (
            constraint
            for constraint in constraints.values()
            if constraint["primary_key"]
        ),
        None,
    )

    assert primary_key_constraint
    assert primary_key_constraint["columns"] == ["custom_pk", "timestamp"]


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "in_custom_tablespace",
    [False, True],
    ids=["default_tablespace", "custom_tablespace"],
)
def test_schema_editor_create_partitioned_partioning_key_is_primary_key(
    custom_tablespace, in_custom_tablespace
):
    model = define_fake_partitioned_model(
        {
            "name": models.TextField(),
            "timestamp": models.DateTimeField(primary_key=True),
        },
        {"method": PostgresPartitioningMethod.RANGE, "key": ["timestamp"]},
        {"db_tablespace": custom_tablespace if in_custom_tablespace else None},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    constraints = db_introspection.get_constraints(model._meta.db_table)
    primary_key_constraint = next(
        (
            constraint
            for constraint in constraints.values()
            if constraint["primary_key"]
        ),
        None,
    )

    assert primary_key_constraint
    assert primary_key_constraint["columns"] == ["timestamp"]


@pytest.mark.skipif(
    django.VERSION < (5, 2),
    reason="Django < 5.2 doesn't implement composite primary keys",
)
@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "in_custom_tablespace",
    [False, True],
    ids=["default_tablespace", "custom_tablespace"],
)
def test_schema_editor_create_partitioned_custom_composite_primary_key(
    custom_tablespace, in_custom_tablespace
):
    model = define_fake_partitioned_model(
        {
            "pk": models.CompositePrimaryKey("name", "timestamp"),
            "name": models.TextField(),
            "timestamp": models.DateTimeField(),
        },
        {"method": PostgresPartitioningMethod.RANGE, "key": ["timestamp"]},
        {"db_tablespace": custom_tablespace if in_custom_tablespace else None},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)

    constraints = db_introspection.get_constraints(model._meta.db_table)
    primary_key_constraint = next(
        (
            constraint
            for constraint in constraints.values()
            if constraint["primary_key"]
        ),
        None,
    )

    assert primary_key_constraint
    assert primary_key_constraint["columns"] == ["name", "timestamp"]


def _count_rows(table_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {connection.ops.quote_name(table_name)}"
        )
        return cursor.fetchone()[0]


def _create_model_with_partitions():
    model = define_fake_partitioned_model(
        {"name": models.TextField(), "timestamp": models.DateTimeField()},
        {"key": ["timestamp"]},
    )

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model)
    schema_editor.add_range_partition(
        model, name="jan", from_values="2019-1-1", to_values="2019-2-1"
    )
    schema_editor.add_range_partition(
        model, name="feb", from_values="2019-2-1", to_values="2019-3-1"
    )

    model.objects.create(name="a", timestamp="2019-01-15T00:00:00Z")
    model.objects.create(name="b", timestamp="2019-01-20T00:00:00Z")
    model.objects.create(name="c", timestamp="2019-02-15T00:00:00Z")

    return model


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_detach_partition():
    """Tests whether detaching a partition removes it from the partitioned
    table but keeps the partition's table and rows."""

    model = _create_model_with_partitions()
    table_name = model._meta.db_table
    partition_table_name = f"{table_name}_jan"

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.detach_partition(model, "jan")

    table = db_introspection.get_partitioned_table(table_name)
    assert [partition.name for partition in table.partitions] == ["feb"]

    assert partition_table_name in db_introspection.table_names()
    assert _count_rows(partition_table_name) == 2
    assert model.objects.count() == 1


@pytest.mark.postgres_version(lt=140000)
@pytest.mark.django_db(transaction=True)
def test_schema_editor_detach_partition_concurrently():
    """Tests whether a partition can be detached concurrently outside of a
    transaction and whether its table and rows are kept."""

    model = _create_model_with_partitions()
    table_name = model._meta.db_table
    partition_table_name = f"{table_name}_jan"

    with connection.schema_editor(atomic=False) as schema_editor:
        schema_editor.detach_partition(model, "jan", concurrently=True)

    with transaction.atomic():
        table = db_introspection.get_partitioned_table(table_name)
    assert [partition.name for partition in table.partitions] == ["feb"]

    assert _count_rows(partition_table_name) == 2
    assert model.objects.count() == 1

    with connection.schema_editor(atomic=False) as schema_editor:
        schema_editor.delete_partitioned_model(model)
        schema_editor.execute(
            f"DROP TABLE {connection.ops.quote_name(partition_table_name)}"
        )


@pytest.mark.postgres_version(lt=110000)
def test_schema_editor_detach_partition_concurrently_in_transaction():
    """Tests whether detaching concurrently inside a transaction raises a clear
    error instead of failing in PostgreSQL."""

    model = _create_model_with_partitions()

    schema_editor = PostgresSchemaEditor(connection)
    with pytest.raises(TransactionManagementError):
        schema_editor.detach_partition(model, "jan", concurrently=True)

    table = db_introspection.get_partitioned_table(model._meta.db_table)
    assert len(table.partitions) == 2
