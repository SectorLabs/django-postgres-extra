from psqlextra.models import PostgresPartitionedModel

from .strategy import PostgresPartitioningStrategy


class PostgresPartitioningConfig:
    """Configuration for partitioning a specific model according to the
    specified strategy."""

    def __init__(
        self,
        model: PostgresPartitionedModel,
        strategy: PostgresPartitioningStrategy,
        substrategy: PostgresPartitioningStrategy | None = None,
    ) -> None:
        self.model = model
        self.strategy = strategy
        self.substrategy = substrategy


__all__ = ["PostgresPartitioningConfig"]
