from django.apps import AppConfig


class PostgresExtraAppConfig(AppConfig):
    name = "psqlextra"
    verbose_name = "PostgreSQL Extra"

    def ready(self) -> None:
        from .lookups import InValuesLookup  # noqa
