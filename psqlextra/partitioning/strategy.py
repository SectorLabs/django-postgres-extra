from abc import abstractmethod
from typing import Generator

from .partition import PostgresPartition


class PostgresPartitioningStrategy:
    """Base class for implementing a partitioning strategy for a partitioned
    table."""

    @abstractmethod
    def generate(self) -> Generator[PostgresPartition, None, None]:
        """Generates a list of partitions to be created."""


__all__ = ["PostgresRangePartitioningStrategy"]
