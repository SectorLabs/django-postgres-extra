from abc import abstractmethod
from typing import Generator

from .partition import PostgresPartition


class PostgresPartitioningStrategy:
    """Base class for implementing a partitioning strategy for a partitioned
    table."""

    @abstractmethod
    def to_create(
        self,
    ) -> Generator[PostgresPartition, None, None]:
        """Generates a list of partitions to be created."""

    @abstractmethod
    def to_delete(
        self,
    ) -> Generator[PostgresPartition, None, None]:
        """Generates a list of partitions to be deleted."""


__all__ = ["PostgresPartitioningStrategy"]
