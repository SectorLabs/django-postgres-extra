from typing import Generator

from psqlextra.partitioning import (
    PostgresPartitioningStrategy,
    PostgresPartition,
    PostgresTimePartition,
    PostgresRangePartition,
)
from psqlextra.partitioning.delete_on_condition_strategy import (
    PostgresDeleteOnConditionPartitioningStrategy,
)


class TestStrategy(PostgresPartitioningStrategy):
    def to_create(self,) -> Generator[PostgresRangePartition, None, None]:
        """Generates a list of partitions to be created."""

    def to_delete(self,) -> Generator[PostgresRangePartition, None, None]:
        """Generates a list of partitions to be deleted."""
        for i in range(0, 100, 10):
            yield PostgresRangePartition(from_values=i, to_values=i + 9)


def test_delete_on_condition():
    def test_condition(partition: PostgresRangePartition):
        return partition.to_values < 10

    strategy = PostgresDeleteOnConditionPartitioningStrategy(
        delegate=TestStrategy(), delete_condition=test_condition
    )
    partitions_to_delete = list(strategy.to_delete())
    assert len(partitions_to_delete) == 1
    assert partitions_to_delete[0].from_values == 0
    assert partitions_to_delete[0].to_values == 9
