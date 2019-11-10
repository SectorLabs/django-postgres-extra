from typing import Any, Optional

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.models import PostgresPartitionedModel

from .partition import PostgresPartition


class PostgresRangePartition(PostgresPartition):
    """Base class for a PostgreSQL table partition in a range partitioned
    table."""

    def __init__(self, from_values: Any, to_values: Any) -> None:
        self.from_values = from_values
        self.to_values = to_values

    def deconstruct(self) -> dict:
        return {
            **super().deconstruct(),
            "from_values": self.from_values,
            "to_values": self.to_values,
        }

    def create(
        self,
        model: PostgresPartitionedModel,
        schema_editor: PostgresSchemaEditor,
        comment: Optional[str] = None,
    ) -> None:
        schema_editor.add_range_partition(
            model=model,
            name=self.name(),
            from_values=self.from_values,
            to_values=self.to_values,
            comment=comment,
        )

    def delete(
        self,
        model: PostgresPartitionedModel,
        schema_editor: PostgresSchemaEditor,
    ) -> None:
        schema_editor.delete_partition(model, self.name())


__all__ = ["PostgresRangePartition"]
