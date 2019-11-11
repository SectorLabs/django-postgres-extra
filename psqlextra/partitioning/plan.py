from dataclasses import dataclass, field
from typing import List, Optional

from ansimarkup import ansiprint
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

        with transaction.atomic():
            with connection.schema_editor() as schema_editor:
                for partition in self.creations:
                    partition.create(
                        self.config.model,
                        schema_editor,
                        comment=AUTO_PARTITIONED_COMMENT,
                    )

                for partition in self.deletions:
                    partition.delete(self.config.model, schema_editor)

    def print(self) -> None:
        """Prints this model plan to the terminal in a readable format."""

        ansiprint(f"<b><white>{self.config.model.__name__}:</white></b>")

        for partition in self.deletions:
            ansiprint("<b><red>  - %s</red></b>" % partition.name())
            for key, value in partition.deconstruct().items():
                ansiprint(f"<white>     <b>{key}</b>: {value}</white>")

        for partition in self.creations:
            ansiprint("<b><green>  + %s</green></b>" % partition.name())
            for key, value in partition.deconstruct().items():
                ansiprint(f"<white>     <b>{key}</b>: {value}</white>")


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

        ansiprint(
            f"<b><red>{delete_count} partitions will be deleted</red></b>"
        )
        ansiprint(
            f"<b><green>{create_count} partitions will be created</green></b>"
        )


__all__ = ["PostgresPartitioningPlan", "PostgresModelPartitioningPlan"]
