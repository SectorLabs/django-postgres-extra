from abc import abstractmethod

from psqlextra.backend.schema import PostgresSchemaEditor


class PostgresPartition:
    """Base class for a PostgreSQL table partition."""

    @abstractmethod
    def name(self) -> str:
        """Generates/computes the name for this partition."""

    @abstractmethod
    def create(self, schema_editor: PostgresSchemaEditor) -> None:
        """Creates this partition in the database."""


__all__ = ["PostgresPartition"]
