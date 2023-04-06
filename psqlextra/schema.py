from contextlib import contextmanager

import wrapt

from django.core.exceptions import SuspiciousOperation, ValidationError
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import CursorWrapper
from django.utils import timezone


class PostgresSchemaConnectionWrapper(wrapt.ObjectProxy):
    """Wraps a Django database connection and ensures that each cursor operates
    within the specified schema."""

    def __init__(self, connection, schema) -> None:
        super().__init__(connection)

        self._self_schema = schema

    @contextmanager
    def schema_editor(self):
        with self.__wrapped__.schema_editor() as schema_editor:
            schema_editor.connection = self
            yield schema_editor

    @contextmanager
    def cursor(self) -> CursorWrapper:
        schema = self._self_schema

        with self.__wrapped__.cursor() as cursor:
            quoted_name = self.ops.quote_name(schema.name)
            cursor.execute(f"SET search_path = {quoted_name}")
            try:
                yield cursor
            finally:
                cursor.execute("SET search_path TO DEFAULT")


class PostgresSchema:
    """Represents a Postgres schema.

    See: https://www.postgresql.org/docs/current/ddl-schemas.html
    """

    NAME_MAX_LENGTH = 63

    name: str
    using: str

    default: "PostgresSchema"

    def __init__(self, name: str, *, using: str = DEFAULT_DB_ALIAS) -> None:
        self.name = name
        self.using = using

    @classmethod
    def create(
        cls, name: str, *, using: str = DEFAULT_DB_ALIAS
    ) -> "PostgresSchema":
        """Creates a new schema with the specified name.

        This throws if the schema already exists as that is most likely
        a problem that requires careful handling. Pretending everything
        is ok might cause the caller to overwrite data, thinking it got
        a empty schema.

        Arguments:
            name:
                The name to give to the new schema (max 63 characters).

            using:
                Optional name of the database connection to use.
        """

        if len(name) > cls.NAME_MAX_LENGTH:
            raise ValidationError(
                f"Schema name '{name}' is longer than Postgres's limit of {cls.NAME_MAX_LENGTH} characters"
            )

        with connections[using].schema_editor() as schema_editor:
            schema_editor.create_schema(name)

        return cls(name, using=using)

    @classmethod
    def create_random(
        cls, prefix: str, *, using: str = DEFAULT_DB_ALIAS
    ) -> "PostgresSchema":
        """Creates a new schema with a random (time-based) suffix.

        Arguments:
            prefix:
                Name to prefix the final name with. The name plus
                prefix cannot be longer than 63 characters.

            using:
                Name of the database connection to use.
        """

        name_suffix = timezone.now().strftime("%Y%m%d%H%m%s")
        return cls.create(f"{prefix}_{name_suffix}", using=using)

    @classmethod
    def delete_and_create(
        cls, name: str, *, cascade: bool = False, using: str = DEFAULT_DB_ALIAS
    ) -> "PostgresSchema":
        """Deletes the schema if it exists before re-creating it.

        Arguments:
            name:
                Name of the schema to delete+create (max 63 characters).

            cascade:
                Whether to delete the contents of the schema
                and anything that references it if it exists.

            using:
                Optional name of the database connection to use.
        """

        with transaction.atomic(using=using):
            cls(name, using=using).delete(cascade=cascade)
            return cls.create(name, using=using)

    @classmethod
    def exists(cls, name: str, *, using: str = DEFAULT_DB_ALIAS) -> bool:
        """Gets whether a schema with the specified name exists.

        Arguments:
            name:
                Name of the schema to check of whether it
                exists.

            using:
                Optional name of the database connection to use.
        """

        connection = connections[using]

        with connection.cursor() as cursor:
            return name in connection.introspection.get_schema_list(cursor)

    def delete(self, *, cascade: bool = False) -> None:
        """Deletes the schema and optionally deletes the contents of the schema
        and anything that references it.

        Arguments:
            cascade:
                Cascade the delete to the contents of the schema
                and anything that references it.

                If not set, the schema will refuse to be deleted
                unless it is empty and there are not remaining
                references.
        """

        if self.name == "public":
            raise SuspiciousOperation(
                "Pretty sure you are about to make a mistake by trying to drop the 'public' schema. I have stopped you. Thank me later."
            )

        with connections[self.using].schema_editor() as schema_editor:
            schema_editor.delete_schema(self.name, cascade=cascade)

    @property
    def connection(self) -> BaseDatabaseWrapper:
        """Obtains a database connection scoped to this schema."""

        return PostgresSchemaConnectionWrapper(connections[self.using], self)


PostgresSchema.default = PostgresSchema("public")


@contextmanager
def postgres_temporary_schema(
    prefix: str,
    *,
    cascade: bool = False,
    delete_on_throw: bool = False,
    using: str = DEFAULT_DB_ALIAS,
) -> PostgresSchema:
    """Creates a temporary schema that only lives in the context of this
    context manager.

    Arguments:
        prefix:
            Name to prefix the final name with.

        cascade:
            Whether to cascade the delete when dropping the
            schema. If enabled, the contents of the schema
            are deleted as well as anything that references
            the schema.

        delete_on_throw:
            Whether to automatically drop the schema if
            any error occurs within the context manager.

        using:
            Optional name of the database connection to use.
    """

    schema = PostgresSchema.create_random(prefix, using=using)

    try:
        yield schema
    except Exception as e:
        if delete_on_throw:
            schema.delete(cascade=cascade)

        raise e

    schema.delete(cascade=cascade)
