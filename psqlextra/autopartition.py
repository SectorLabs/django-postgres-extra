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


def postgres_auto_partition(
    model: PostgresPartitionedModel,
    count: int,
    interval_unit: PostgresAutoPartitioningIntervalUnit,
    interval: int,
    using="default",
):
    """Pre-create N partitions ahead of time according to the specified
    interval unit and interval."""

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

    start_datetime = datetime.now().replace(day=1)
    for _ in range(count):
        end_datetime = start_datetime + relativedelta(months=+interval)
        partition_name = start_datetime.strftime("%Y_%b").lower()
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
