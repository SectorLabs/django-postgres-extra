from datetime import datetime
from typing import Optional

from .error import PostgresPartitioningError
from .range_partition import PostgresRangePartition
from .time_partition_size import (
    PostgresTimePartitionSize,
    PostgresTimePartitionUnit,
)


class PostgresTimePartition(PostgresRangePartition):
    """Time-based range table partition.

    :see:PostgresTimePartitioningStrategy for more info.
    """

    _unit_name_format = {
        PostgresTimePartitionUnit.YEARS: "%Y",
        PostgresTimePartitionUnit.MONTHS: "%Y_%b",
        PostgresTimePartitionUnit.WEEKS: "%Y_week_%W",
        PostgresTimePartitionUnit.DAYS: "%Y_%b_%d",
    }

    def __init__(
        self,
        size: PostgresTimePartitionSize,
        start_datetime: datetime,
        name_format: Optional[str] = None,
    ) -> None:
        end_datetime = start_datetime + size.as_delta()

        super().__init__(
            from_values=start_datetime.strftime("%Y-%m-%d"),
            to_values=end_datetime.strftime("%Y-%m-%d"),
        )

        self.size = size
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.name_format = name_format

    def name(self) -> str:
        name_format = self.name_format or self._unit_name_format.get(
            self.size.unit
        )
        if not name_format:
            raise PostgresPartitioningError("Unknown size/unit")

        return self.start_datetime.strftime(name_format).lower()

    def deconstruct(self) -> dict:
        return {
            **super().deconstruct(),
            "size_unit": self.size.unit.value,
            "size_value": self.size.value,
        }


__all__ = ["PostgresTimePartition"]
