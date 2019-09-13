import pytest

from django.db import connection, models
from django.db.migrations.operations.models import DeleteModel

from psqlextra.manager import PostgresManager
from psqlextra.migrations import operations
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from .migrations import execute_migration


@pytest.fixture
def create_model():
    return lambda method: operations.PostgresCreatePartitionedModel(
        "test",
        fields=[
            ("name", models.TextField()),
            ("timestamp", models.DateTimeField()),
        ],
        bases=(PostgresPartitionedModel,),
        managers=[("objects", PostgresManager())],
        partitioning_options={"method": method, "key": ["timestamp"]},
    )


@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
def test_migration_operations_create_partitioned_table(method, create_model):
    """Tests whether the see :PostgresCreatePartitionedModel operation works as
    expected in a migration."""

    operation = create_model(method)
    execute_migration(connection.schema_editor(), [operation])

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, f"tests_{operation.name}"
        )
        assert table.method == operation.partitioning_options["method"]
        assert table.key == operation.partitioning_options["key"]


@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
def test_migration_operations_delete_partitioned_table(method, create_model):
    """Tests whether a :see:PostgresDeletePartitionedModel can be deleted using
    the standard :see:DeleteModel operation."""

    create_operation = create_model(method)

    execute_migration(
        connection.schema_editor(),
        [create_operation, DeleteModel(create_operation.name)],
    )

    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, f"tests_{create_operation.name}"
        )
        assert not table
