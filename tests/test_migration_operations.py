import pytest

from django.apps import apps
from django.db import connection, migrations, models
from django.db.migrations.operations.models import DeleteModel

from psqlextra.manager import PostgresManager
from psqlextra.migrations import operations
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .migrations import apply_migration


@pytest.fixture
def create_model():
    """Factory for creating a :see:PostgresCreatePartitionedModel operation."""

    def _create_model(method):
        fields = [("name", models.TextField())]

        key = []

        if method == PostgresPartitioningMethod.RANGE:
            key.append("timestamp")
            fields.append(("timestamp", models.DateTimeField()))
        else:
            key.append("category")
            fields.append(("category", models.TextField()))

        return operations.PostgresCreatePartitionedModel(
            "test",
            fields=fields,
            bases=(PostgresPartitionedModel,),
            managers=[("objects", PostgresManager())],
            partitioning_options={"method": method, "key": key},
        )

    return _create_model


@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
def test_migration_operations_create_partitioned_table(method, create_model):
    """Tests whether the see :PostgresCreatePartitionedModel operation works as
    expected in a migration."""

    create_operation = create_model(method)
    apply_migration(connection.schema_editor(), [create_operation])

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, f"tests_{create_operation.name}"
        )

        part_options = create_operation.partitioning_options
        assert table.method == part_options["method"]
        assert table.key == part_options["key"]


@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
def test_migration_operations_delete_partitioned_table(method, create_model):
    """Tests whether a :see:PostgresDeletePartitionedModel can be deleted using
    the standard :see:DeleteModel operation."""

    create_operation = create_model(method)

    apply_migration(
        connection.schema_editor(),
        [create_operation, DeleteModel(create_operation.name)],
    )

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, f"tests_{create_operation.name}"
        )
        assert not table


@pytest.mark.parametrize(
    "method,operation",
    [
        (
            PostgresPartitioningMethod.LIST,
            operations.PostgresAddDefaultPartition(
                model_name="test", name="pt1"
            ),
        ),
        (
            PostgresPartitioningMethod.RANGE,
            operations.PostgresAddRangePartition(
                model_name="test",
                name="pt1",
                from_values="2019-01-01",
                to_values="2019-02-01",
            ),
        ),
        (
            PostgresPartitioningMethod.LIST,
            operations.PostgresAddListPartition(
                model_name="test", name="pt1", values=["car", "boat"]
            ),
        ),
    ],
)
def test_migration_operations_add_delete_partition(
    method, operation, create_model
):
    """Tests whether adding partitions and then removing them works as
    expected."""

    project = migrations.state.ProjectState.from_apps(apps)

    # apply migration to create model and one partition
    create_operation = create_model(method)
    apply_migration(
        connection.schema_editor(), [create_operation, operation], project
    )

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, f"tests_{create_operation.name}"
        )

        assert len(table.partitions) == 1
        assert table.partitions[0].name == f"{table.name}_{operation.name}"

    # apply migration to delete the partition
    apply_migration(
        connection.schema_editor(),
        [
            operations.PostgresDeletePartition(
                model_name=create_operation.name, name=operation.name
            )
        ],
        project,
    )

    with connection.cursor() as cursor:
        partitions = connection.introspection.get_partitions(
            cursor, f"tests_{create_operation.name}"
        )

        assert len(partitions) == 0
