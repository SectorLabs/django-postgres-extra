from dataclasses import dataclass, field
from typing import List, Optional

from django.db import connections, transaction

from .config import PostgresPartitioningConfig
from .constants import AUTO_PARTITIONED_COMMENT
from .partition import PostgresPartition


@dataclass
class PostgresModelPartitioningPlan:
    """Describes the partitions that are going to be created/deleted for a
    particular partitioning config.

    A "partitioning config" applies to one model.
    """

    config: PostgresPartitioningConfig
    creations: List[PostgresPartition] = field(default_factory=list)
    deferred_creations: List[PostgresPartition] = field(default_factory=list)
    detachements: List[PostgresPartition] = field(default_factory=list)
    concurrent_detachements: List[PostgresPartition] = field(
        default_factory=list
    )
    deletions: List[PostgresPartition] = field(default_factory=list)

    def apply(self, using: Optional[str]) -> None:
        """Applies this partitioning plan by creating and deleting the planned
        partitions.

        Applying the plan runs in a transaction.

        Arguments:
            using:
                Name of the database connection to use.
        """

        connection = connections[using or "default"]

        if self.creations:
            with transaction.atomic():
                with connection.schema_editor() as schema_editor:
                    for partition in self.creations:
                        partition.create(
                            self.config.model,
                            schema_editor,
                            comment=AUTO_PARTITIONED_COMMENT,
                        )

        if self.deferred_creations:
            with transaction.atomic():
                with connection.schema_editor() as schema_editor:
                    for partition in self.deferred_creations:
                        partition.create(
                            self.config.model,
                            schema_editor,
                            comment=AUTO_PARTITIONED_COMMENT,
                            defer_attach=True,
                        )

        if self.detachements:
            with transaction.atomic():
                with connection.schema_editor() as schema_editor:
                    for partition in self.detachements:
                        partition.detach(
                            self.config.model, schema_editor, concurrently=False
                        )

        if self.concurrent_detachements:
            with connection.schema_editor() as schema_editor:
                for partition in self.concurrent_detachements:
                    partition.detach(
                        self.config.model, schema_editor, concurrently=True
                    )

        with transaction.atomic():
            with connection.schema_editor() as schema_editor:
                for partition in self.deletions:
                    partition.delete(self.config.model, schema_editor)

    def print(self) -> None:
        """Prints this model plan to the terminal in a readable format."""

        print(f"{self.config.model.__name__}:")

        for partition in self.deletions:
            print("  - %s" % partition.name())
            for key, value in partition.deconstruct().items():
                print(f"     {key}: {value}")

        for partition in self.creations:
            print("  + %s" % partition.name())
            for key, value in partition.deconstruct().items():
                print(f"     {key}: {value}")

        for partition in self.deferred_creations:
            print("  + %s" % partition.name())
            for key, value in partition.deconstruct().items():
                print(f"     {key}: {value}")


@dataclass
class PostgresPartitioningPlan:
    """Describes the partitions that are going to be created/deleted."""

    model_plans: List[PostgresModelPartitioningPlan]

    @property
    def creations(self) -> List[PostgresPartition]:
        """Gets a complete flat list of the partitions that are going to be
        created."""

        creations = []
        for model_plan in self.model_plans:
            creations.extend(model_plan.creations)
        return creations

    @property
    def deferred_creations(self) -> List[PostgresPartition]:
        """Gets a complete flat list of the partitions that are going to be
        created."""

        deferred_creations = []
        for model_plan in self.model_plans:
            deferred_creations.extend(model_plan.deferred_creations)
        return deferred_creations

    @property
    def deletions(self) -> List[PostgresPartition]:
        """Gets a complete flat list of the partitions that are going to be
        deleted."""

        deletions = []
        for model_plan in self.model_plans:
            deletions.extend(model_plan.deletions)
        return deletions

    def apply(self, using: Optional[str] = None) -> None:
        """Applies this plan by creating/deleting all planned partitions."""

        for model_plan in self.model_plans:
            model_plan.apply(using=using)

    def print(self) -> None:
        """Prints this plan to the terminal in a readable format."""

        for model_plan in self.model_plans:
            model_plan.print()
            print("")

        create_count = len(self.creations)
        delete_count = len(self.deletions)
        deferred_creations_count = len(self.deferred_creations)

        print(f"{delete_count} partitions will be deleted")
        print(f"{create_count} partitions will be created")
        if deferred_creations_count:
            print(f"{deferred_creations_count} partitions will be deferently created")


__all__ = ["PostgresPartitioningPlan", "PostgresModelPartitioningPlan"]
