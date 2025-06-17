from typing import Type

from psqlextra.models import PostgresPartitionedModel

from .strategy import PostgresPartitioningStrategy


class PostgresPartitioningConfig:
    """Configuration for partitioning a specific model according to the
    specified strategy."""

    def __init__(
        self,
        model: Type[PostgresPartitionedModel],
        strategy: PostgresPartitioningStrategy,
        atomic: bool = True,
    ) -> None:
        self.model = model
        self.strategy = strategy
        self.atomic = atomic


__all__ = ["PostgresPartitioningConfig"]
