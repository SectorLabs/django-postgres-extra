from typing import Optional, Type

from dateutil.relativedelta import relativedelta

from psqlextra.models import PostgresPartitionedModel

from psqlextra.partitioning.config import PostgresPartitioningConfig
from psqlextra.partitioning import PostgresTimePartitionSize

from .category_current_time_strategy import (
    PostgresCategoryCurrentTimePartitioningStrategy,
)


def partition_by_category_and_current_time(
    model: Type[PostgresPartitionedModel],
    count: int,
    categories: list[int] = None,
    years: Optional[int] = None,
    months: Optional[int] = None,
    weeks: Optional[int] = None,
    days: Optional[int] = None,
    hours: Optional[int] = None,
    max_age: Optional[relativedelta] = None,
    name_format: Optional[str] = None,
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

        categories:
            The list of categories to create first-level partitions for.

        years:
            The amount of years each partition should contain.

        months:
            The amount of months each partition should contain.

        weeks:
            The amount of weeks each partition should contain.

        days:
            The amount of days each partition should contain.

        hours:
            The amount of hours each partition should contain.

        max_age:
            The maximum age of a partition (calculated from the
            start of the partition).

            Partitions older than this are deleted when running
            a delete/cleanup run.

        name_format:
            The datetime format supplied as a tuple.
            The first value creates the first level partition names
            and the second value is passed to datetime.strftime to generate the
            second level partition name.
    """

    size = PostgresTimePartitionSize(
        years=years, months=months, weeks=weeks, days=days, hours=hours
    )

    return PostgresPartitioningConfig(
        model=model,
        strategy=PostgresCategoryCurrentTimePartitioningStrategy(
            categories=categories or [],
            size=size,
            count=count,
            max_age=max_age,
            name_format=name_format,
        ),
    )


__all_ = ["partition_by_category_and_current_time"]
