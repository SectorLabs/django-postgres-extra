from psqlextra.models import PostgresPartitionedModel

from .strategy import PostgresPartitioningStrategy


class PostgresPartitioningConfig:
    """Configuration for partitioning a specific model according to the
    specified strategy."""

    def __init__(
        self,
        model: PostgresPartitionedModel,
        strategy: PostgresPartitioningStrategy,
    ) -> None:
        self.model = model
        self.strategy = strategy


__all__ = ["PostgresPartitioningConfig"]
