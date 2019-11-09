from typing import List, Optional

import structlog

from django.db import connections

from psqlextra.models import PostgresPartitionedModel

from .config import PostgresPartitioningConfig
from .error import PostgresPartitioningError


class PostgresPartitioningManager:
    """Helps managing partitions by automatically creating new partitions and
    deleting old ones according to the configuration."""

    def __init__(self, configs: List[PostgresPartitioningConfig]) -> None:
        self.configs = configs
        self._validate_configs(self.configs)

        self._logger = structlog.get_logger(__name__)

    def auto_create(
        self,
        model: Optional[PostgresPartitionedModel] = None,
        using: Optional[str] = None,
    ) -> None:
        """Automatically creates new partitions for the configuration
        associated with the specified model.

        Arguments:
            model:
                The partitioned model to automatically create
                new partitions for.

                If not specified, partitions will automatically
                be created for all configured models.

            using:
                Database connection name to use.
        """

        found_model = False
        for config in self.configs:
            if model and config.model != model:
                continue

            self._auto_create(config, using=using)
            found_model = True

        if not found_model:
            raise PostgresPartitioningError(
                "Cannot find partitioning config for %s" % model.__name__
            )

    def find_by_model(
        self, model: PostgresPartitionedModel
    ) -> Optional[PostgresPartitioningConfig]:
        """Finds the partitioning config for the specified model."""

        return next(
            (config for config in self.configs if config.model == model), None
        )

    def _auto_create(
        self, config: PostgresPartitioningConfig, using: Optional[str] = None
    ) -> None:
        connection = connections[using or "default"]

        with connection.cursor() as cursor:
            table = connection.introspection.get_partitioned_table(
                cursor, config.model._meta.db_table
            )

        if not table:
            raise PostgresPartitioningError(
                f"Model {config.model.__name__}, with table "
                "{config.model._meta.db_table} does not exists in the "
                "database. Did you run `python manage.py migrate`?"
            )

        with connection.schema_editor() as schema_editor:
            for partition in config.strategy.generate():
                if table.partition_by_name(name=partition.name()):
                    self._logger.info(
                        "Skipping creation of partition, already exists",
                        name=partition.name(),
                    )
                    continue

                partition.create(config.model, schema_editor)

    @staticmethod
    def _validate_configs(configs: List[PostgresPartitioningConfig]):
        """Ensures there is only one config per model."""

        models = set([config.model.__name__ for config in configs])
        if len(models) != len(configs):
            raise PostgresPartitioningError(
                "Only one partitioning config per model is allowed"
            )
