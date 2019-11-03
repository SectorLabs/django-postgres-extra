from django.apps import apps
from django.core.management.base import BaseCommand
from django.db.utils import NotSupportedError, OperationalError

from psqlextra.models import PostgresMaterializedViewModel


class Command(BaseCommand):
    """Refreshes a :see:PostgresMaterializedViewModel."""

    help = "Refreshes the specified materialized view."

    def add_arguments(self, parser):
        parser.add_argument(
            "app_label",
            type=str,
            help="Label of the app the materialized view model is in.",
        )

        parser.add_argument(
            "model_name",
            type=str,
            help="Name of the materialized view model to refresh.",
        )

        parser.add_argument(
            "--concurrently",
            "-c",
            action="store_true",
            help="Whether to refresh the materialized view model concurrently.",
            required=False,
            default=False,
        )

    def handle(self, *app_labels, **options):
        app_label = options.get("app_label")
        model_name = options.get("model_name")
        concurrently = options.get("concurrently")

        model = apps.get_model(app_label, model_name)
        if not model:
            raise OperationalError(f"Cannot find a model named '{model_name}'")

        if not issubclass(model, PostgresMaterializedViewModel):
            raise NotSupportedError(
                f"Model {model.__name__} is not a `PostgresMaterializedViewModel`"
            )

        model.refresh(concurrently=concurrently)
