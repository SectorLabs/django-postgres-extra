import pytest

from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.partitioning import (
    PostgresPartitioningError,
    PostgresPartitioningManager,
    partition_by_current_time,
)

from .fake_model import define_fake_partitioned_model, get_fake_model


def test_partitioning_manager_duplicate_model():
    """Tests whether it is not possible to have more than one partitioning
    config per model."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        PostgresPartitioningManager(
            [
                partition_by_current_time(model, years=1, count=3),
                partition_by_current_time(model, years=1, count=3),
            ]
        )


def test_partitioning_manager_find_config_for_model():
    """Tests that finding a partitioning config by the model works as
    expected."""

    model1 = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    config1 = partition_by_current_time(model1, years=1, count=3)

    model2 = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    config2 = partition_by_current_time(model2, months=1, count=2)

    manager = PostgresPartitioningManager([config1, config2])
    assert manager.find_config_for_model(model1) == config1
    assert manager.find_config_for_model(model2) == config2


def test_partitioning_manager_plan_specific_model_names():
    """Tests that only planning for specific models works as expected."""

    model1 = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    config1 = partition_by_current_time(model1, years=1, count=3)

    model2 = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    config2 = partition_by_current_time(model2, months=1, count=2)

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_partitioned_model(model1)
    schema_editor.create_partitioned_model(model2)

    manager = PostgresPartitioningManager([config1, config2])

    plan = manager.plan()
    assert len(plan.model_plans) == 2

    plan = manager.plan(model_names=[model2.__name__])
    assert len(plan.model_plans) == 1
    assert plan.model_plans[0].config.model == model2

    # make sure casing is irrelevant
    plan = manager.plan(model_names=[model2.__name__.lower()])
    assert len(plan.model_plans) == 1


def test_partitioning_manager_plan_not_partitioned_model():
    """Tests that the auto partitioner does not try to auto partition for non-
    partitioned models/tables."""

    model = get_fake_model({"timestamp": models.DateTimeField()})

    with pytest.raises(PostgresPartitioningError):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=2)]
        )
        manager.plan()


def test_partitioning_manager_plan_non_existent_model():
    """Tests that the auto partitioner does not try to partition for non-
    existent partitioned tables."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        manager = PostgresPartitioningManager(
            [partition_by_current_time(model, months=1, count=2)]
        )
        manager.plan()
