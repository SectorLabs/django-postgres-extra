from datetime import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta

from psqlextra.models import PostgresPartitionedModel

from .config import PostgresPartitioningConfig
from .current_time_strategy import PostgresCurrentTimePartitioningStrategy
from .time_partition_size import PostgresTimePartitionSize


def partition_by_time(
    model: PostgresPartitionedModel,
    count: int,
    years: Optional[int] = None,
    months: Optional[int] = None,
    weeks: Optional[int] = None,
    days: Optional[int] = None,
    max_age: Optional[relativedelta] = None,
    from_datetime: Optional[datetime] = None,
) -> PostgresPartitioningConfig:
    """Short-hand for generating a partitioning config that partitions the
    specified model by time.

    One specifies one of the `years`, `months`, `weeks`
    or `days` parameter to indicate the size of each
    partition. These parameters cannot be combined.

    Arguments:
        count:
            The amount of partitions to create ahead of
            the current date/time.

        years:
            The amount of years each partition should contain.

        months:
            The amount of months each partition should contain.

        weeks:
            The amount of weeks each partition should contain.

        days:
            The amount of days each partition should contain.

        max_age:
            The maximum age of a partition (calculated from the
            start of the partition).

            Partitions older than this are deleted when running
            a delete/cleanup run.

        from_datetime:
            Skip creating any partitions that would
            contain data _before_ this date.

            Use this when switching partitioning
            interval. Useful when you've already partitioned
            ahead using the original interval and want
            to avoid creating overlapping partitioninig.

            Set this to the _end date_ for the
            last partition that was created.

            Only delete partitions newer than this
            (but older than :paramref:max_age).
    """

    size = PostgresTimePartitionSize(
        years=years, months=months, weeks=weeks, days=days
    )

    return PostgresPartitioningConfig(
        model=model,
        strategy=PostgresCurrentTimePartitioningStrategy(
            size=size, count=count, max_age=max_age, from_datetime=from_datetime
        ),
    )


__all_ = ["partition_by_time"]
