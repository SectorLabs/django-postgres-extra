from abc import abstractmethod
from typing import Generator

from .range_partition import PostgresRangePartition


class PostgresRangePartitioningStrategy:
    """Base class for implementing a partitioning strategy for a range
    partitioned table."""

    @abstractmethod
    def generate(self) -> Generator[PostgresRangePartition, None, None]:
        """Generates a list of partitions to be created."""


__all__ = ["PostgresRangePartitioningStrategy"]
