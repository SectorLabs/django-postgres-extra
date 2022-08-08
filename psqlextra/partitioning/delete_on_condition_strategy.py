from typing import Callable, Generator

from psqlextra.partitioning import (
    PostgresPartition,
    PostgresPartitioningStrategy,
)


class PostgresDeleteOnConditionPartitioningStrategy(
    PostgresPartitioningStrategy
):
    def __init__(
        self,
        delegate: PostgresPartitioningStrategy,
        delete_condition: Callable[[PostgresPartition], bool],
    ):
        self._delegate = delegate
        self._delete_condition = delete_condition

    def to_create(self,) -> Generator[PostgresPartition, None, None]:
        return self._delegate.to_delete()

    def to_delete(self,) -> Generator[PostgresPartition, None, None]:
        for partition in self._delegate.to_delete():
            if self._delete_condition(partition):
                yield partition
