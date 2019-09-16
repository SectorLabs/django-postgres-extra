from datetime import datetime

import structlog

from dateutil.relativedelta import relativedelta
from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import connection

from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod

LOGGER = structlog.get_logger(__name__)


class PostgresAutoPartitioningError(RuntimeError):
    pass


class Command(BaseCommand):
    help = "Automatically create PostgreSQL 11.x partitions ahead."

    def add_arguments(self, parser):
        parser.add_argument(
            "--app-label",
            "-a",
            type=str,
            help="Label of the app the partitioned model is in.",
            required=True,
        )
        parser.add_argument(
            "model_name",
            type=str,
            help="Name of the partitioned model to auto partition for.",
        )
        parser.add_argument(
            "--count",
            "-c",
            type=int,
            help="Amount of partitions to create in ahead of the current date.",
            default=1,
        )
        parser.add_argument(
            "--interval-unit",
            "-u",
            type=str,
            choices=["month"],
            help="Unit in which to express the interval (months/weeks/days).",
            default="month",
        )
        parser.add_argument(
            "--interval",
            "-i",
            type=int,
            help="Amount of time in-between partitions (see --interval-unit).",
            default=1,
        )

    def handle(self, *app_labels, **options):
        app_label = options.get("app_label")
        model_name = options.get("model_name")

        model = apps.get_model(app_label, model_name)
        if not issubclass(model, PostgresPartitionedModel):
            raise PostgresAutoPartitioningError(
                f"Model {model.__name__} is not a `PostgresPartitionedModel`"
            )

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

        interval = options.get("interval")
        count = options.get("count")
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
