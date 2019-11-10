from dataclasses import dataclass
from typing import List, Optional, Tuple

from ansimarkup import ansiprint
from django.db import connections

from psqlextra.models import PostgresPartitionedModel

from .config import PostgresPartitioningConfig
from .error import PostgresPartitioningError
from .partition import PostgresPartition

PartitionList = List[Tuple[PostgresPartitionedModel, List[PostgresPartition]]]


@dataclass
class PostgresModelPartitionPlan:
    model: PostgresPartitionedModel
    created_partitions: List[PostgresPartition]
    deleted_partitions: List[PostgresPartition]

    def print(self) -> None:
        if (
            len(self.created_partitions) == 0
            and len(self.deleted_partitions) == 0
        ):
            return

        ansiprint(f"<b><white>{self.model.__name__}:</white></b>")

        for partition in self.deleted_partitions:
            ansiprint("<b><red>  - %s</red></b>" % partition.name())
            for key, value in partition.deconstruct().items():
                ansiprint(f"<white>     <b>{key}</b>: {value}</white>")

        for partition in self.created_partitions:
            ansiprint("<b><green>  + %s</green></b>" % partition.name())
            for key, value in partition.deconstruct().items():
                ansiprint(f"<white>     <b>{key}</b>: {value}</white>")


class PostgresPartitioningManager:
    """Helps managing partitions by automatically creating new partitions and
    deleting old ones according to the configuration."""

    # comment placed on partition tables created by the partitioner
    # partition tables that do not have this comment will _never_
    # be deleted by the partitioner, this is a safety mechanism so
    # manually created partitions aren't accidently cleaned up
    _partition_table_comment: str = "psqlextra_auto_partitioned"

    def __init__(self, configs: List[PostgresPartitioningConfig]) -> None:
        self.configs = configs
        self._validate_configs(self.configs)

    def apply(
        self,
        no_delete: bool = False,
        no_create: bool = False,
        dry_run: bool = False,
        using: Optional[str] = None,
    ) -> None:
        """Applies the partitioning plan by computing which partitions to
        create and which ones to delete.

        Arguments:
            no_delete:
                If set to True, no partitions will be marked
                for deletion, regardless of the configuration.

            no_create:
                If set to True, no partitions will be marked
                for creation, regardless of the configuration.

            dry_run:
                If set to True, the partitions will not be
                created/deleted. The return value remains
                the same.

                Use this to discover what partitions would
                be created/deleted.

            using:
                Name of the database connection to use.

        Returns:
            A plan of which partitons have been created/deleted
            or will be created/deleted if dry_run=True.
        """

        plans = []

        for config in self.configs:
            created_partitions = (
                self._auto_create(config, dry_run=dry_run, using=using)
                if not no_create
                else []
            )

            deleted_partitions = (
                self._auto_delete(config, dry_run=dry_run, using=using)
                if not no_delete
                else []
            )

            plans.append(
                PostgresModelPartitionPlan(
                    model=config.model,
                    created_partitions=created_partitions,
                    deleted_partitions=deleted_partitions,
                )
            )

        return plans

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
    ) -> List[PostgresPartition]:
        connection = connections[using or "default"]
        table = self._get_partitioned_table(connection, config.model)

        created_partitions = []

        with connection.schema_editor() as schema_editor:
            for partition in config.strategy.to_create():
                if table.partition_by_name(name=partition.name()):
                    continue

                created_partitions.append(partition)

                if not dry_run:
                    partition.create(
                        config.model,
                        schema_editor,
                        comment=self._partition_table_comment,
                    )

        return created_partitions

    def _auto_delete(
        self,
        config: PostgresPartitioningConfig,
        dry_run: bool = False,
        using: Optional[str] = None,
    ) -> List[PostgresPartition]:
        connection = connections[using or "default"]
        table = self._get_partitioned_table(connection, config.model)
        deleted_partitions = []

        with connection.schema_editor() as schema_editor:
            for partition in config.strategy.to_delete():
                introspected_partition = table.partition_by_name(
                    name=partition.name()
                )
                if not introspected_partition:
                    continue

                if (
                    introspected_partition.comment
                    != self._partition_table_comment
                ):
                    continue

                deleted_partitions.append(partition)
                if not dry_run:
                    partition.delete(config.model, schema_editor)

        return deleted_partitions

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
