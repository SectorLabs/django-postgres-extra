from datetime import datetime

import structlog

from dateutil.relativedelta import relativedelta
from django.db import connections

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
    using="default",
):
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

        using:
            Database connection name to use.

    Example:
        Partition by month, 2 months ahead:
            count=2, interval_unit=MONTH, interval=1

        Partition by week, 3 weeks ahead:
            count=3, interval_unit=WEEK, interval=1

        Partion by 2 weeks, 4 weeks ahead
            count=2, interval_unit=WEEK, interval=2
    """

    connection = connections[using]

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

    schema_editor = connection.schema_editor()

    start_datetime = datetime.now()
    if interval_unit == PostgresAutoPartitioningIntervalUnit.MONTH:
        start_datetime = start_datetime.replace(day=1)
    elif interval_unit == PostgresAutoPartitioningIntervalUnit.WEEK:
        start_datetime = start_datetime - relativedelta(
            days=start_datetime.weekday()
        )

    for _ in range(count):
        if interval_unit == PostgresAutoPartitioningIntervalUnit.MONTH:
            end_datetime = start_datetime + relativedelta(months=+interval)
            partition_name = start_datetime.strftime("%Y_%b").lower()
        elif interval_unit == PostgresAutoPartitioningIntervalUnit.WEEK:
            end_datetime = start_datetime + relativedelta(weeks=+interval)
            partition_name = start_datetime.strftime("%Y_week_%W").lower()

        partition_table_name = schema_editor.create_partition_table_name(
            model, partition_name
        )

        existing_partition = next(
            (
                table_partition
                for table_partition in table.partitions
                if table_partition.name == partition_table_name
            ),
            None,
        )

        if existing_partition:
            start_datetime = end_datetime
            LOGGER.info(
                "Skipping creation of partition, already exists",
                model_name=model.__name__,
                name=partition_name,
            )
            continue

        from_values = start_datetime.strftime("%Y-%m-%d")
        to_values = end_datetime.strftime("%Y-%m-%d")

        LOGGER.info(
            "Creating partition",
            name=partition_name,
            from_values=from_values,
            to_values=to_values,
        )

        schema_editor.add_range_partition(
            model=model,
            name=partition_name,
            from_values=from_values,
            to_values=to_values,
        )

        start_datetime = end_datetime
