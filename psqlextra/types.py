from enum import Enum
from typing import List


class ConflictAction(Enum):
    """Possible actions to take on a conflict."""

    NOTHING = "NOTHING"
    UPDATE = "UPDATE"

    @classmethod
    def all(cls) -> List["ConflictAction"]:
        return [choice for choice in cls]


class PostgresPartitioningMethod(str, Enum):
    """Methods of partitioning supported by PostgreSQL 11.x native support for
    table partitioning."""

    RANGE = "range"
    LIST = "list"

    @classmethod
    def all(cls) -> List["PostgresPartitioningMethod"]:
        return [choice for choice in cls]

    def __str__(self) -> str:
        return str(self.value)
