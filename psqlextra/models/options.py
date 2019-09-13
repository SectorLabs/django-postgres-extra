from typing import List

from psqlextra.types import PostgresPartitioningMethod


class PostgresPartitionedModelOptions:
    """Container for :see:PostgresPartitionedModel options.

    This is where attributes copied from the model's `PartitioningMeta`
    are held.
    """

    def __init__(self, method: PostgresPartitioningMethod, key: List[str]):
        self.method = method
        self.key = key
        self.original_attrs = dict(method=method, key=key)
