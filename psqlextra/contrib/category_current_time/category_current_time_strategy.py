from datetime import datetime, timezone
from typing import Any, Generator, Optional
from dateutil.relativedelta import relativedelta

from psqlextra.partitioning.strategy import PostgresPartitioningStrategy
from psqlextra.partitioning.time_partition_size import PostgresTimePartitionSize

from .partition import PostgresListPartition, PostgresTimeSubPartition


class PostgresCategoryCurrentTimePartitioningStrategy(PostgresPartitioningStrategy):
    """Implments a category and time based partitioning strategy where
    on the first level a partition is created for each category and on the
    second level each partition contains values for a specific time period.

    All buckets will be equal in size and start at the start of the
    unit. With monthly partitioning, partitions start on the 1st and
    with weekly partitioning, partitions start on monday, with hourly
    partitioning, partitions start at 00:00.
    """

    def __init__(
        self,
        categories: list[Any],
        size: PostgresTimePartitionSize,
        count: int,
        max_age: Optional[relativedelta] = None,
        name_format: Optional[tuple[str, str]] = None,
    ) -> None:
        """Initializes a new instance of :see:PostgresTimePartitioningStrategy.

        Arguments:
            categories:
                The list of categories to create first-level partitions for.

            size:
                The size of each partition.

            count:
                The amount of partitions to create ahead
                from the current date/time.

            max_age:
                Maximum age of a partition. Partitions
                older than this are deleted during
                auto cleanup.

            name_format:
                Optional name format for the partitions supplied as a tuple.  The first value is used
                for the category partitions, the second for the time partitions.
        """

        self.size = size
        self.count = count
        self.max_age = max_age
        self.name_format = name_format or (None, None)
        self.categories = categories

    def to_create(self) -> Generator[PostgresTimeSubPartition, None, None]:
        for category in self.categories:
            lp = PostgresListPartition(
                values=[category], name_format=self.name_format[0]
            )
            yield lp

            current_datetime = self.size.start(self.get_start_datetime())

            for _ in range(self.count):
                yield PostgresTimeSubPartition(
                    parent_partition=lp,
                    start_datetime=current_datetime,
                    size=self.size,
                    name_format=self.name_format[1],
                )

                current_datetime += self.size.as_delta()

    def to_delete(self) -> Generator[PostgresTimeSubPartition, None, None]:
        if not self.max_age:
            return

        current_datetime = self.size.start(self.get_start_datetime() - self.max_age)

        while True:
            for category in self.categories:
                lp = PostgresListPartition(
                    values=[category], name_format=self.name_format[0]
                )

                yield PostgresTimeSubPartition(
                    parent_partition=lp,
                    start_datetime=current_datetime,
                    size=self.size,
                    name_format=self.name_format[1],
                )

            current_datetime -= self.size.as_delta()

    def get_start_datetime(self) -> datetime:
        return datetime.now(timezone.utc)
