import structlog

from django.apps import apps
from django.core.management.base import BaseCommand

from psqlextra.auto_partition import (
    PostgresAutoPartitioningIntervalUnit,
    postgres_auto_partition,
)
from psqlextra.models import PostgresPartitionedModel

LOGGER = structlog.get_logger(__name__)


class PostgresAutoPartitioningError(RuntimeError):
    pass


class Command(BaseCommand):
    """Creates partitions for future dates for tables that use date/time range
    partitioning."""

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
            choices=PostgresAutoPartitioningIntervalUnit.values(),
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
        count = options.get("count")
        interval_unit = PostgresAutoPartitioningIntervalUnit(
            options.get("interval_unit")
        )
        interval = options.get("interval")

        model = apps.get_model(app_label, model_name)
        if not issubclass(model, PostgresPartitionedModel):
            raise PostgresAutoPartitioningError(
                f"Model {model.__name__} is not a `PostgresPartitionedModel`"
            )

        postgres_auto_partition(
            model, count=count, interval_unit=interval_unit, interval=interval
        )
