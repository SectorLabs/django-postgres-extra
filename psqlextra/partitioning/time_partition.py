from datetime import datetime

from .range_partition import PostgresRangePartition
from .time_partition_size import (
    PostgresTimePartitionSize,
    PostgresTimePartitionUnit,
)


class PostgresTimePartition(PostgresRangePartition):
    """Time-based range table partition.

    :see:PostgresTimePartitioningStrategy for more info.
    """

    def __init__(
        self, size: PostgresTimePartitionSize, start_datetime: datetime
    ) -> None:
        end_datetime = start_datetime + size.as_delta()

        super().__init__(
            from_values=start_datetime.strftime("%Y-%m-%d"),
            to_values=end_datetime.strftime("%Y-%m-%d"),
        )

        self.size = size
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    def name(self) -> str:
        if self.size.unit == PostgresTimePartitionUnit.YEARS:
            return self.start_datetime.strftime("%Y").lower()

        if self.size.unit == PostgresTimePartitionUnit.MONTHS:
            return self.start_datetime.strftime("%Y_%b").lower()

        if self.size.unit == PostgresTimePartitionUnit.WEEKS:
            return self.start_datetime.strftime("%Y_week_%W").lower()

        return self.start_datetime.strftime("%Y_%b_%d").lower()


__all__ = ["PostgresTimePartition"]
