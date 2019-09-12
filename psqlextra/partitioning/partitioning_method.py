from enum import Enum


class PartitioningMethod(str, Enum):
    """Methods by which a table/model can be partitioned."""

    RANGE = "range"
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
