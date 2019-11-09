from datetime import datetime
from typing import Generator, Optional

from .range_strategy import PostgresRangePartitioningStrategy
from .time_partition import PostgresTimePartition
from .time_partition_size import PostgresTimePartitionSize


class PostgresTimePartitioningStrategy(PostgresRangePartitioningStrategy):
    """Implments a time based partitioning strategy where each partition
    contains values for a specific time period.

    All buckets will be equal in size and start at the start of the
    unit. With monthly partitioning, partitions start on the 1st and
    with weekly partitioning, partitions start on monday.
    """

    def __init__(
        self,
        size: PostgresTimePartitionSize,
        count: int,
        start_from: Optional[datetime] = None,
    ) -> None:
        """Initializes a new instance of :see:PostgresTimePartitioningStrategy.

        Arguments:
            size:
                The size of each partition.

            count:
                The amount of partitions to create ahead
                from the current date/time.

            start_from:
                Skip creating any partitions that would
                contain data _before_ this date.
        """

        self.size = size
        self.count = count
        self.start_from = start_from

    def generate(self) -> Generator[PostgresTimePartition, None, None]:
        start_datetime = self.size.start(datetime.now())

        for _ in range(self.count):
            if self.start_from and start_datetime.date() < self.start_from:
                start_datetime += self.size.as_delta()
                continue

            yield PostgresTimePartition(
                start_datetime=start_datetime, size=self.size
            )

            start_datetime += self.size.as_delta()

    def for_datetime(self, dt: datetime) -> PostgresTimePartition:
        """Gets the definition of a partition in which the specified date/time
        belongs."""

        start_datetime = self.size.start(dt)

        return PostgresTimePartition(
            start_datetime=start_datetime, size=self.size
        )
