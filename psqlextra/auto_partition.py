from datetime import date, datetime
from typing import List, Optional

import structlog

from dateutil.relativedelta import relativedelta
from django.db import connections

from psqlextra.backend.introspection import PostgresIntrospectedPartitionTable
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod, StrEnum

LOGGER = structlog.get_logger(__name__)


class PostgresAutoPartitioningError(RuntimeError):
    """Raised when a fatal error is encountered during automatic
    partitioning."""


class PostgresAutoPartitioningIntervalUnit(StrEnum):
    """Interval units that can auto partitioned with."""

    MONTH = "month"
    WEEK = "week"


def postgres_auto_partition(
    model: PostgresPartitionedModel,
    count: int,
    interval_unit: PostgresAutoPartitioningIntervalUnit,
    interval: int,
    start_from: Optional[date] = None,
    dry_run: bool = False,
    using: str = "default",
) -> List[PostgresIntrospectedPartitionTable]:
    """Pre-create N partitions ahead of time according to the specified
    interval unit and interval.

    Arguments:
        model:
            The model to auto partition for.

        count:
            The amount of partitions for the specified interval
            to create ahead (from the current date).

        interval_unit:
            Date/time unit to partition by.

        interval:
            Amount of specified units to partition by.

        start_from:
            Skip creating any partitions that would
            contain data _before_ this date.

            Use this when switching partitioning
            interval. Useful when you've already partitioned
            ahead using the original interval and want
            to avoid creating overlapping partitioninig.
            Set this to the _end date_ for the
            last partition that was created.

            If the specified start date is in the past,
            it is ignored.

        dry_run:
            If set to True, partitions aren't actually
            created, just detected and returned.

        using:
            Database connection name to use.

    Returns:
        A list of partitions that were created (or
        "would be created" if dry_run=True).

    Example:
        Partition by month, 2 months ahead:
            count=2, interval_unit=MONTH, interval=1

        Partition by week, 3 weeks ahead:
            count=3, interval_unit=WEEK, interval=1

        Partion by 2 weeks, 4 weeks ahead
            count=2, interval_unit=WEEK, interval=2
    """

    connection = connections[using]
    table = _get_partitioned_table(model, connection)
    schema_editor = connection.schema_editor()

    start_datetime = datetime.now()
    if interval_unit == PostgresAutoPartitioningIntervalUnit.MONTH:
        start_datetime = start_datetime.replace(day=1)
    elif interval_unit == PostgresAutoPartitioningIntervalUnit.WEEK:
        start_datetime = start_datetime - relativedelta(
            days=start_datetime.weekday()
        )

    created_partitions = []

    for _ in range(count):
        if interval_unit == PostgresAutoPartitioningIntervalUnit.MONTH:
            end_datetime = start_datetime + relativedelta(months=+interval)
            partition_name = start_datetime.strftime("%Y_%b").lower()
        elif interval_unit == PostgresAutoPartitioningIntervalUnit.WEEK:
            end_datetime = start_datetime + relativedelta(weeks=+interval)
            partition_name = start_datetime.strftime("%Y_week_%W").lower()

        from_values = start_datetime.strftime("%Y-%m-%d")
        to_values = end_datetime.strftime("%Y-%m-%d")

        logger = LOGGER.bind(
            model_name=model.__name__,
            name=partition_name,
            from_values=from_values,
            to_values=to_values,
            dry_run=dry_run,
        )

        if start_from and start_datetime.date() < start_from:
            start_datetime = end_datetime
            logger.info(
                "Skipping creation of partition, before specified start date",
                start_from=start_from,
            )
            continue

        partition_full_name = schema_editor.create_partition_full_name(
            model, partition_name
        )

        existing_partition = next(
            (
                partition
                for partition in table.partitions
                if partition.full_name == partition_full_name
            ),
            None,
        )

        if existing_partition:
            start_datetime = end_datetime
            logger.info("Skipping creation of partition, already exists")
            continue

        created_partitions.append(
            PostgresIntrospectedPartitionTable(
                name=partition_name,
                full_name=partition_full_name,
                values=[start_datetime, end_datetime],
            )
        )

        if not dry_run:
            schema_editor.add_range_partition(
                model=model,
                name=partition_name,
                from_values=from_values,
                to_values=to_values,
            )

        logger.info("Created partition")
        start_datetime = end_datetime

    return created_partitions


def postgres_auto_delete_partitions(
    model: PostgresPartitionedModel,
    max_age: relativedelta,
    dry_run: bool = False,
    using: str = "default",
) -> List[PostgresIntrospectedPartitionTable]:
    """Deletes partitions that have a starting date older than
    :paramref:max_age.

    Arguments:
        model:
            The model to auto partition for.

        max_age:
            The maximum age of a partition before it
            is PERMANENTLY deleted.

            "age" is computed from the partition's
            start date/time.

        dry_run:
            If set to True, partitions aren't actually
            deleted, just detected and returned.

        using:
            Database connection name to use.

    Returns:
        A list of partitions that were deleted (or "would
        be deleted" if dry_run=True).
    """

    connection = connections[using]
    table = _get_partitioned_table(model, connection)
    schema_editor = connection.schema_editor()

    deleted_partitions = []

    for partition in table.partitions:
        from_values, to_values = partition.values
        expiry_date = from_values + max_age

        logger = LOGGER.bind(
            from_values=from_values.strftime("%Y-%m-%d"),
            to_values=to_values.strftime("%Y-%m-%d"),
            name=partition.name,
            expiry_date=expiry_date.strftime("%Y-%m-%d"),
            max_age=max_age,
            dry_run=dry_run,
        )

        if datetime.now() > expiry_date:
            deleted_partitions.append(partition)
            if not dry_run:
                schema_editor.delete_partition(model, partition.name)

            logger.info("Deleted partition")

    return deleted_partitions


def _get_partitioned_table(
    model: PostgresPartitionedModel, connection
) -> List[PostgresIntrospectedPartitionTable]:
    with connection.cursor() as cursor:
        table = connection.introspection.get_partitioned_table(
            cursor, model._meta.db_table
        )

    if not table:
        raise PostgresAutoPartitioningError(
            f"Model {model.__name__}, with table {model._meta.db_table} "
            "does not exists in the database. Did you run "
            "`python manage.py migrate`?"
        )

    if table.method != PostgresPartitioningMethod.RANGE:
        raise PostgresAutoPartitioningError(
            f"Table {table.name} is not partitioned by a range. Auto partitioning "
            "only supports partitioning by range."
        )

    return table


__all__ = [
    "postgres_auto_partition",
    "postgres_auto_delete_partitions",
    "PostgresAutoPartitioningError",
    "PostgresAutoPartitioningIntervalUnit",
]
