from enum import Enum
from typing import Any, Dict, List, Tuple, Union

SQL = str
SQLWithParams = Tuple[str, Union[Tuple[Any, ...], Dict[str, Any]]]


class StrEnum(str, Enum):
    @classmethod
    def all(cls) -> List["StrEnum"]:
        return [choice for choice in cls]

    @classmethod
    def values(cls) -> List[str]:
        return [choice.value for choice in cls]

    def __str__(self) -> str:
        return str(self.value)


class ConflictAction(Enum):
    """Possible actions to take on a conflict."""

    NOTHING = "NOTHING"
    UPDATE = "UPDATE"

    @classmethod
    def all(cls) -> List["ConflictAction"]:
        return [choice for choice in cls]


class PostgresPartitioningMethod(StrEnum):
    """Methods of partitioning supported by PostgreSQL 11.x native support for
    table partitioning."""

    RANGE = "range"
    LIST = "list"
    HASH = "hash"
