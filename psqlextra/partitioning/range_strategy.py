from .strategy import PostgresPartitioningStrategy


class PostgresRangePartitioningStrategy(PostgresPartitioningStrategy):
    """Base class for implementing a partitioning strategy for a range
    partitioned table."""


__all__ = ["PostgresRangePartitioningStrategy"]
