from abc import abstractmethod

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
    ) -> None:
        """Creates this partition in the database."""

    @abstractmethod
    def delete(
        self,
        model: PostgresPartitionedModel,
        schema_editor: PostgresSchemaEditor,
    ) -> None:
        """Deletes this partition from the database."""


__all__ = ["PostgresPartition"]
