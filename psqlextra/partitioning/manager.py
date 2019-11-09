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
        self, dry_run: bool = False, using: Optional[str] = None
    ) -> None:
        """Automatically creates new partitions for the configuration
        associated with the specified model.

        Arguments:
            dry_run:
                When set to True, partitions won't actually be
                created. This allows you to see what partitions
                _would_ be created.

            using:
                Database connection name to use.
        """

        for config in self.configs:
            self._auto_create(config, dry_run=dry_run, using=using)

    def auto_delete(
        self, dry_run: bool = False, using: Optional[str] = None
    ) -> None:
        """Automatically creates new partitions for the configuration
        associated with the specified model.

        Arguments:
            dry_run:
                When set to True, partitions won't actually be
                created. This allows you to see what partitions
                _would_ be created.

            using:
                Database connection name to use.
        """

        for config in self.configs:
            self._auto_delete(config, dry_run=dry_run, using=using)

    def find_by_model(
        self, model: PostgresPartitionedModel
    ) -> Optional[PostgresPartitioningConfig]:
        """Finds the partitioning config for the specified model."""

        return next(
            (config for config in self.configs if config.model == model), None
        )

    def _auto_create(
        self,
        config: PostgresPartitioningConfig,
        dry_run: bool = False,
        using: Optional[str] = None,
    ) -> None:
        logger = self._logger.bind(dry_run=dry_run, using=using)

        connection = connections[using or "default"]
        table = self._get_partitioned_table(connection, config.model)

        with connection.schema_editor() as schema_editor:
            for partition in config.strategy.to_create():
                if table.partition_by_name(name=partition.name()):
                    logger.info(
                        "Skipping creation of partition, already exists",
                        name=partition.name(),
                    )
                    continue

                logger.info("Creating partition", name=partition.name())
                if not dry_run:
                    partition.create(config.model, schema_editor)

    def _auto_delete(
        self,
        config: PostgresPartitioningConfig,
        dry_run: bool = False,
        using: Optional[str] = None,
    ) -> None:
        logger = self._logger.bind(dry_run=dry_run, using=using)

        connection = connections[using or "default"]
        table = self._get_partitioned_table(connection, config.model)

        with connection.schema_editor() as schema_editor:
            for partition in config.strategy.to_delete():
                if not table.partition_by_name(name=partition.name()):
                    continue

                logger.info("Deleting partition", name=partition.name())
                if not dry_run:
                    partition.delete(config.model, schema_editor)

    @staticmethod
    def _get_partitioned_table(connection, model: PostgresPartitionedModel):
        with connection.cursor() as cursor:
            table = connection.introspection.get_partitioned_table(
                cursor, model._meta.db_table
            )

        if not table:
            raise PostgresPartitioningError(
                f"Model {model.__name__}, with table "
                "{model._meta.db_table} does not exists in the "
                "database. Did you run `python manage.py migrate`?"
            )

        return table

    @staticmethod
    def _validate_configs(configs: List[PostgresPartitioningConfig]):
        """Ensures there is only one config per model."""

        models = set([config.model.__name__ for config in configs])
        if len(models) != len(configs):
            raise PostgresPartitioningError(
                "Only one partitioning config per model is allowed"
            )
