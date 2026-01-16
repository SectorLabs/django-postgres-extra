from datetime import datetime
from typing import Any, Optional, Type

from contextlib import contextmanager

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.models import PostgresPartitionedModel

from psqlextra.partitioning.time_partition import (
    PostgresTimePartition,
    PostgresTimePartitionSize,
)

from psqlextra.partitioning import PostgresPartition
from psqlextra.partitioning.error import PostgresPartitioningError

@contextmanager
def patch_model(model: Type[PostgresPartitionedModel], parent_partition_name: str):
    """Context manager that ensures the model's table is created and
    deleted properly.
    """
    original_table = model._meta.db_table
    try:
        model._meta.db_table = original_table + "_" + parent_partition_name
        yield model
    finally:
        model._meta.db_table = original_table

class PostgresTimeSubPartition(PostgresTimePartition):
    """Base class for a PostgreSQL table sub-partition in a range partitioned
    table."""

    def __init__(
        self,
        parent_partition: PostgresPartition,
        size: PostgresTimePartitionSize,
        start_datetime: datetime,
        name_format: Optional[str] = None,
    ) -> None:
        super().__init__(
            size=size,
            start_datetime=start_datetime,
            name_format=name_format,
        )
        self.parent_partition = parent_partition

    def name(self) -> str:
        name_format = self.name_format or self._unit_name_format.get(self.size.unit)
        if not name_format:
            raise PostgresPartitioningError("Unknown size/unit")

        return (
            self.parent_partition.name()
            + "_"
            + self.start_datetime.strftime(name_format).lower()
        )

    def create(
        self,
        model: Type[PostgresPartitionedModel],
        schema_editor: PostgresSchemaEditor,
        comment: Optional[str] = None,
    ) -> None:
        with patch_model(model, self.parent_partition.name()) as managed_model:
            super().create(
                model=managed_model,
                schema_editor=schema_editor,
                comment=comment,
            )

    def delete(
        self,
        model: Type[PostgresPartitionedModel],
        schema_editor: PostgresSchemaEditor,
    ) -> None:
        with patch_model(model, self.parent_partition.name()) as managed_model:
            super().delete(
                model=managed_model,
                schema_editor=schema_editor,
            )

class PostgresListPartition(PostgresPartition):
    """Base class for a PostgreSQL table partition in a list partitioned
    table."""

    def __init__(self, values: list[Any], name_format: Optional[str] = None) -> None:
        self.values = values
        self.name_format = name_format or "%s"

    def name(self) -> str:
        return self.name_format % "_".join(str(v).lower() for v in self.values)

    def deconstruct(self) -> dict:
        return {
            **super().deconstruct(),
            "values": self.values,
        }

    def create(
        self,
        model: Type[PostgresPartitionedModel],
        schema_editor: PostgresSchemaEditor,
        comment: Optional[str] = None,
    ) -> None:
        schema_editor.add_list_partition(
            model=model,
            name=self.name(),
            values=self.values,
            comment=comment,
        )

    def delete(
        self,
        model: Type[PostgresPartitionedModel],
        schema_editor: PostgresSchemaEditor,
    ) -> None:
        schema_editor.delete_partition(model, self.name())


__all__ = ["PostgresListPartition"]
