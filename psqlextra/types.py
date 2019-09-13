from enum import Enum
from typing import List


class PostgresPartitioningMethod(str, Enum):
    """Methods of partitioning supported by PostgreSQL
    11.x native support for table partitioning."""

    RANGE = "range"
    LIST = "list"

    @classmethod
    def all(cls) -> List["PostgresPartitioningMethod"]:
        return [choice for choice in cls]

    def __str__(self) -> str:
        return str(self.value)
