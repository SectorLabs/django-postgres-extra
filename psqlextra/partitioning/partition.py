from abc import abstractmethod
from typing import Optional

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.models import PostgresPartitionedModel


class PostgresPartition:
    """Base class for a PostgreSQL table partition."""

    @abstractmethod
    def name(self) -> str:
        """Generates/computes the name for this partition."""

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

    @abstractmethod
    def detach(
        self,
        model: PostgresPartitionedModel,
        schema_editor: PostgresSchemaEditor,
        concurrently: bool = False,
    ) -> None:
        """Detaches this partition from the database."""
        if concurrently:
            schema_editor.detach_partition_concurrently(model=model, name=self.name())
        else:
            schema_editor.detach_partition(model=model, name=self.name())

    def deconstruct(self) -> dict:
        """Deconstructs this partition into a dict of attributes/fields."""

        return {"name": self.name()}


__all__ = ["PostgresPartition"]
