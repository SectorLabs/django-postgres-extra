import pytest

from django.apps import apps
from django.db import connection, migrations, models

from psqlextra.backend.migrations import operations
from psqlextra.manager import PostgresManager
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

from . import db_introspection
from .migrations import apply_migration


def _partitioned_table_exists(op: operations.PostgresCreatePartitionedModel):
    """Checks whether the specified partitioned model operation was succesfully
    applied."""

    model_table_name = f"tests_{op.name}"

    table = db_introspection.get_partitioned_table(model_table_name)
    if not table:
        return False

    part_options = op.partitioning_options
    return (
        table.method == part_options["method"]
        and table.key == part_options["key"]
    )


def _partition_exists(model_op, op):
    """Checks whether the parttitioned model and partition operations were
    succesfully applied."""

    model_table_name = f"tests_{model_op.name}"

    table = db_introspection.get_partitioned_table(model_table_name)
    if not table:
        return False

    partition = next(
        (
            partition
            for partition in table.partitions
            if partition.full_name == f"{model_table_name}_{op.name}"
        ),
        None,
    )

    return bool(partition)


@pytest.fixture
def create_model():
    """Factory for creating a :see:PostgresCreatePartitionedModel operation."""

    def _create_model(method):
        fields = [("name", models.TextField())]

        key = []

        if method == PostgresPartitioningMethod.RANGE:
            key.append("timestamp")
            fields.append(("timestamp", models.DateTimeField()))
        elif method == PostgresPartitioningMethod.LIST:
            key.append("category")
            fields.append(("category", models.TextField()))
        elif method == PostgresPartitioningMethod.HASH:
            key.append("artist_id")
            fields.append(("artist_id", models.IntegerField()))
        else:
            raise NotImplementedError

        return operations.PostgresCreatePartitionedModel(
            "test",
            fields=fields,
            bases=(PostgresPartitionedModel,),
            managers=[("objects", PostgresManager())],
            partitioning_options={"method": method, "key": key},
        )

    return _create_model


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
def test_migration_operations_create_partitioned_table(method, create_model):
    """Tests whether the see :PostgresCreatePartitionedModel operation works as
    expected in a migration."""

    create_operation = create_model(method)
    state = migrations.state.ProjectState.from_apps(apps)

    # migrate forwards, is the table there?
    apply_migration([create_operation], state)
    assert _partitioned_table_exists(create_operation)

    # migrate backwards, is the table there?
    apply_migration([create_operation], state=state, backwards=True)
    assert not _partitioned_table_exists(create_operation)


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize("method", PostgresPartitioningMethod.all())
def test_migration_operations_delete_partitioned_table(method, create_model):
    """Tests whether the see :PostgresDeletePartitionedModel operation works as
    expected in a migration."""

    create_operation = create_model(method)
    delete_operation = operations.PostgresDeletePartitionedModel(
        create_operation.name
    )

    state = migrations.state.ProjectState.from_apps(apps)

    # migrate forwards, create model
    apply_migration([create_operation], state)
    assert _partitioned_table_exists(create_operation)

    # record intermediate state, the state we'll
    # migrate backwards to
    intm_state = state.clone()

    # migrate forwards, delete model
    apply_migration([delete_operation], state)
    assert not _partitioned_table_exists(create_operation)

    # migrate backwards, undelete model
    delete_operation.database_backwards(
        "tests", connection.schema_editor(), state, intm_state
    )
    assert _partitioned_table_exists(create_operation)


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "method,add_partition_operation",
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
        (
            PostgresPartitioningMethod.HASH,
            operations.PostgresAddHashPartition(
                model_name="test", name="pt1", modulus=3, remainder=0
            ),
        ),
    ],
)
def test_migration_operations_add_partition(
    method, add_partition_operation, create_model
):
    """Tests whether adding partitions and then rolling them back works as
    expected."""

    create_operation = create_model(method)
    state = migrations.state.ProjectState.from_apps(apps)

    # migrate forwards
    apply_migration([create_operation, add_partition_operation], state)
    assert _partition_exists(create_operation, add_partition_operation)

    # rollback
    apply_migration(
        [create_operation, add_partition_operation], state, backwards=True
    )

    assert not _partition_exists(create_operation, add_partition_operation)


@pytest.mark.postgres_version(lt=110000)
@pytest.mark.parametrize(
    "method,add_partition_operation,delete_partition_operation",
    [
        (
            PostgresPartitioningMethod.LIST,
            operations.PostgresAddDefaultPartition(
                model_name="test", name="pt1"
            ),
            operations.PostgresDeleteDefaultPartition(
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
            operations.PostgresDeleteRangePartition(
                model_name="test", name="pt1"
            ),
        ),
        (
            PostgresPartitioningMethod.LIST,
            operations.PostgresAddListPartition(
                model_name="test", name="pt1", values=["car", "boat"]
            ),
            operations.PostgresDeleteListPartition(
                model_name="test", name="pt1"
            ),
        ),
        (
            PostgresPartitioningMethod.HASH,
            operations.PostgresAddHashPartition(
                model_name="test", name="pt1", modulus=3, remainder=0
            ),
            operations.PostgresDeleteHashPartition(
                model_name="test", name="pt1"
            ),
        ),
    ],
)
def test_migration_operations_add_delete_partition(
    method, add_partition_operation, delete_partition_operation, create_model
):
    """Tests whether adding partitions and then removing them works as
    expected."""

    create_operation = create_model(method)
    state = migrations.state.ProjectState.from_apps(apps)

    # migrate forwards, create model and partition
    apply_migration([create_operation, add_partition_operation], state)
    assert _partition_exists(create_operation, add_partition_operation)

    # record intermediate state, the state we'll
    # migrate backwards to
    intm_state = state.clone()

    # migrate forwards, delete the partition
    apply_migration([delete_partition_operation], state)
    assert not _partition_exists(create_operation, add_partition_operation)

    # migrate backwards, undelete the partition
    delete_partition_operation.database_backwards(
        "tests", connection.schema_editor(), state, intm_state
    )
    assert _partition_exists(create_operation, add_partition_operation)
