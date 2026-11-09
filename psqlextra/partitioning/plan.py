from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, cast

from django.db import connections, transaction

from .config import PostgresPartitioningConfig
from .constants import AUTO_PARTITIONED_COMMENT
from .partition import PostgresPartition

if TYPE_CHECKING:
    from psqlextra.backend.schema import PostgresSchemaEditor


@dataclass
class PostgresModelPartitioningPlan:
    """Describes the partitions that are going to be created/deleted for a
    particular partitioning config.

    A "partitioning config" applies to one model.
    """

    config: PostgresPartitioningConfig
    creations: List[PostgresPartition] = field(default_factory=list)
    deletions: List[PostgresPartition] = field(default_factory=list)

    def apply(
        self,
        using: Optional[str],
        *,
        detach: bool = False,
        concurrently: bool = False,
    ) -> None:
        """Applies this partitioning plan by creating and deleting (or
        detaching) the planned partitions.

        Applying the plan runs in a transaction. When detaching
        concurrently, the partitions are created in a transaction first
        and then detached one by one outside of it, because PostgreSQL
        does not allow ``DETACH PARTITION ... CONCURRENTLY`` in a
        transaction block.

        Arguments:
            using:
                Optional name of the database connection to use.

            detach:
                Detach the partitions that would be deleted instead of
                dropping them. Their tables and rows are kept.

            concurrently:
                Detach with ``DETACH PARTITION ... CONCURRENTLY``
                (PostgreSQL 14 and newer) so that queries against the
                partitioned table are not blocked. Implies ``detach``.
        """

        connection = connections[using or "default"]
        model = self.config.model
        detach = detach or concurrently

        with transaction.atomic(using=using):
            with connection.schema_editor() as schema_editor:
                for partition in self.creations:
                    partition.create(
                        model,
                        cast("PostgresSchemaEditor", schema_editor),
                        comment=AUTO_PARTITIONED_COMMENT,
                    )

                if not concurrently:
                    for partition in self.deletions:
                        if detach:
                            partition.detach(
                                model,
                                cast("PostgresSchemaEditor", schema_editor),
                            )
                        else:
                            partition.delete(
                                model,
                                cast("PostgresSchemaEditor", schema_editor),
                            )

        if concurrently and self.deletions:
            with connection.schema_editor(atomic=False) as schema_editor:
                for partition in self.deletions:
                    partition.detach(
                        model,
                        cast("PostgresSchemaEditor", schema_editor),
                        concurrently=True,
                    )

    def print(self) -> None:
        """Prints this model plan to the terminal in a readable format."""

        print(f"{self.config.model.__name__}: ")

        for partition in self.deletions:
            print("  - %s" % partition.name())
            for key, value in partition.deconstruct().items():
                print(f"     {key}: {value}")

        for partition in self.creations:
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
    def deletions(self) -> List[PostgresPartition]:
        """Gets a complete flat list of the partitions that are going to be
        deleted."""

        deletions = []
        for model_plan in self.model_plans:
            deletions.extend(model_plan.deletions)
        return deletions

    def apply(
        self,
        using: Optional[str] = None,
        *,
        detach: bool = False,
        concurrently: bool = False,
    ) -> None:
        """Applies this plan by creating/deleting all planned partitions.

        See :meth:`PostgresModelPartitioningPlan.apply` for ``detach``
        and ``concurrently``.
        """

        for model_plan in self.model_plans:
            model_plan.apply(
                using=using, detach=detach, concurrently=concurrently
            )

    def print(self, *, detach: bool = False) -> None:
        """Prints this plan to the terminal in a readable format."""

        for model_plan in self.model_plans:
            model_plan.print()
            print("")

        create_count = len(self.creations)
        delete_count = len(self.deletions)

        action = "detached" if detach else "deleted"
        print(f"{delete_count} partitions will be {action}")
        print(f"{create_count} partitions will be created")


__all__ = ["PostgresPartitioningPlan", "PostgresModelPartitioningPlan"]
