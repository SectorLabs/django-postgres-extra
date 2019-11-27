from datetime import datetime, timezone
from typing import Generator, Optional

from dateutil.relativedelta import relativedelta

from .range_strategy import PostgresRangePartitioningStrategy
from .time_partition import PostgresTimePartition
from .time_partition_size import PostgresTimePartitionSize


class PostgresCurrentTimePartitioningStrategy(
    PostgresRangePartitioningStrategy
):
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
        max_age: Optional[relativedelta] = None,
    ) -> None:
        """Initializes a new instance of :see:PostgresTimePartitioningStrategy.

        Arguments:
            size:
                The size of each partition.

            count:
                The amount of partitions to create ahead
                from the current date/time.

            max_age:
                Maximum age of a partition. Partitions
                older than this are deleted during
                auto cleanup.
        """

        self.size = size
        self.count = count
        self.max_age = max_age

    def to_create(self) -> Generator[PostgresTimePartition, None, None]:
        current_datetime = self.size.start(self.get_start_datetime())

        for _ in range(self.count):
            yield PostgresTimePartition(
                start_datetime=current_datetime, size=self.size
            )

            current_datetime += self.size.as_delta()

    def to_delete(self) -> Generator[PostgresTimePartition, None, None]:
        if not self.max_age:
            return

        current_datetime = self.size.start(
            self.get_start_datetime() - self.max_age
        )

        while True:
            yield PostgresTimePartition(
                start_datetime=current_datetime, size=self.size
            )

            current_datetime -= self.size.as_delta()

    def get_start_datetime(self) -> datetime:
        return datetime.now(timezone.utc)
