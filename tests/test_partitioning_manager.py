from datetime import date

import pytest

from django.db import models

from psqlextra.partitioning import (
    PostgresPartitioningError,
    PostgresPartitioningManager,
    partition_by_time,
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
                partition_by_time(
                    model, years=1, count=3, start_from=date(1337, 1, 1)
                ),
                partition_by_time(
                    model, years=1, count=3, start_from=date(1337, 1, 1)
                ),
            ]
        )


def test_partitioning_manager_find_by_model():
    """Tests that finding a partitioning config by the model works as
    expected."""

    model1 = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    config1 = partition_by_time(
        model1, years=1, count=3, start_from=date(1337, 1, 1)
    )

    model2 = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    config2 = partition_by_time(
        model2, months=1, count=2, start_from=date(1337, 1, 1)
    )

    manager = PostgresPartitioningManager([config1, config2])
    assert manager.find_by_model(model1) == config1
    assert manager.find_by_model(model2) == config2


def test_partitioning_manager_apply_not_partitioned_model():
    """Tests that the auto partitioner does not try to auto partition for non-
    partitioned models/tables."""

    model = get_fake_model({"timestamp": models.DateTimeField()})

    with pytest.raises(PostgresPartitioningError):
        manager = PostgresPartitioningManager(
            [
                partition_by_time(
                    model, months=1, count=2, start_from=date(1337, 1, 1)
                )
            ]
        )
        manager.apply()


def test_partitioning_manager_apply_non_existent_model():
    """Tests that the auto partitioner does not try to partition for non-
    existent partitioned tables."""

    model = define_fake_partitioned_model(
        {"timestamp": models.DateTimeField()}, {"key": ["timestamp"]}
    )

    with pytest.raises(PostgresPartitioningError):
        manager = PostgresPartitioningManager(
            [
                partition_by_time(
                    model, months=1, count=2, start_from=date(1337, 1, 1)
                )
            ]
        )
        manager.apply()
