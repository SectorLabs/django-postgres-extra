from abc import abstractmethod
from typing import List, Optional, Tuple

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.models import PostgresPartitionedModel


class PostgresPartition:
    """Base class for a PostgreSQL table partition."""

    partition_by: Optional[Tuple[str, List[str]]] = None
    parent_partition_name: Optional[str] = None

    @abstractmethod
    def name(self) -> str:
        """Generates/computes the name for this partition."""

    def full_name(self) -> str:
        """Concatenate the name with the parent_partition name (if needed)"""
        if self.parent_partition_name:
            return self.parent_partition_name + "_" + self.name()
        return self.name()

    @abstractmethod
    def create(
        self,
        model: PostgresPartitionedModel,
        schema_editor: PostgresSchemaEditor,
        comment: Optional[str] = None,
    ) -> None:
        """Creates this partition in the database."""

    @abstractmethod
    def delete(
        self,
        model: PostgresPartitionedModel,
        schema_editor: PostgresSchemaEditor,
    ) -> None:
        """Deletes this partition from the database."""

    def deconstruct(self) -> dict:
        """Deconstructs this partition into a dict of attributes/fields."""

        return {"name": self.full_name()}


__all__ = ["PostgresPartition"]
