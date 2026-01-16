from typing import List, Optional, Tuple, Type

from django.db import connections

from psqlextra.models import PostgresPartitionedModel

from .config import PostgresPartitioningConfig
from .constants import AUTO_PARTITIONED_COMMENT
from .error import PostgresPartitioningError
from .partition import PostgresPartition
from .plan import PostgresModelPartitioningPlan, PostgresPartitioningPlan

PartitionList = List[Tuple[PostgresPartitionedModel, List[PostgresPartition]]]


class PostgresPartitioningManager:
    """Helps managing partitions by automatically creating new partitions and
    deleting old ones according to the configuration."""

    def __init__(self, configs: List[PostgresPartitioningConfig]) -> None:
        self.configs = configs
        self._validate_configs(self.configs)

    def plan(
        self,
        skip_create: bool = False,
        skip_delete: bool = False,
        model_names: Optional[List[str]] = None,
        using: Optional[str] = None,
    ) -> PostgresPartitioningPlan:
        """Plans which partitions should be deleted/created.

        Arguments:
            skip_create:
                If set to True, no partitions will be marked
                for creation, regardless of the configuration.

            skip_delete:
                If set to True, no partitions will be marked
                for deletion, regardless of the configuration.

            model_names:
                Optionally, only plan for the models with
                the specified name.

            using:
                Optional name of the database connection to use.

        Returns:
            A plan describing what partitions would be created
            and deleted if the plan is applied.
        """

        model_plans = []

        normalized_model_names = (
            [model_name.lower().strip() for model_name in model_names]
            if model_names
            else []
        )

        for config in self.configs:
            if (
                model_names
                and config.model.__name__.lower() not in normalized_model_names
            ):
                continue

            model_plan = self._plan_for_config(
                config,
                skip_create=skip_create,
                skip_delete=skip_delete,
                using=using,
            )
            if not model_plan:
                continue

            model_plans.append(model_plan)

        return PostgresPartitioningPlan(model_plans)

    def find_config_for_model(
        self, model: PostgresPartitionedModel
    ) -> Optional[PostgresPartitioningConfig]:
        """Finds the partitioning config for the specified model."""

        return next((config for config in self.configs if config.model == model), None)

    def _plan_for_config(
        self,
        config: PostgresPartitioningConfig,
        skip_create: bool = False,
        skip_delete: bool = False,
        using: Optional[str] = None,
    ) -> Optional[PostgresModelPartitioningPlan]:
        """Creates a partitioning plan for one partitioning config."""

        connection = connections[using or "default"]
        model_plan = PostgresModelPartitioningPlan(config)

        if not skip_create:
            for partition in config.strategy.to_create():
                if self._get_partition_from_table(connection, config.model, partition):
                    continue

                model_plan.creations.append(partition)

        if not skip_delete:
            for partition in config.strategy.to_delete():
                introspected_partition = self._get_partition_from_table(
                    connection, config.model, partition
                )
                if not introspected_partition:
                    break

                if introspected_partition.comment != AUTO_PARTITIONED_COMMENT:
                    continue

                model_plan.deletions.append(partition)

        if len(model_plan.creations) == 0 and len(model_plan.deletions) == 0:
            return None

        return model_plan

    @staticmethod
    def _get_partition_from_table(
        connection,
        model: Type[PostgresPartitionedModel],
        search_partition: PostgresPartition,
    ) -> bool:
        """Returns a partition from the table by name.
        Traverses partitions if the model is sub-partitioned"""

        with connection.cursor() as cursor:
            table = connection.introspection.get_partitioned_table(
                cursor, model._meta.db_table
            )

            if not table:
                raise PostgresPartitioningError(
                    f"Model {model.__name__}, with table "
                    f"{model._meta.db_table} does not exists in the "
                    "database. Did you run `python manage.py migrate`?"
                )

            if len(getattr(model._partitioning_meta, "sub_key", [])) > 0:
                partition = table.partition_by_name(name=search_partition.name())
                if partition:
                    return partition
                else:
                    return next(
                        (
                            partition
                            for partition in connection.introspection.get_partitions(
                                cursor, model._meta.db_table
                            )
                            if connection.introspection.get_partitioned_table(
                                cursor, partition.full_name
                            )
                            and connection.introspection.get_partitioned_table(
                                cursor, partition.full_name
                            ).partition_by_name(name=search_partition.name())
                        ),
                        None,
                    )
            else:
                return table.partition_by_name(name=search_partition.name())

    @staticmethod
    def _validate_configs(configs: List[PostgresPartitioningConfig]):
        """Ensures there is only one config per model."""

        models = set([config.model.__name__ for config in configs])
        if len(models) != len(configs):
            raise PostgresPartitioningError(
                "Only one partitioning config per model is allowed"
            )
