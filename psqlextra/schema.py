import os

from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator, cast

from django.core.exceptions import SuspiciousOperation, ValidationError
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.utils import timezone

if TYPE_CHECKING:
    from psqlextra.backend.introspection import PostgresIntrospection
    from psqlextra.backend.schema import PostgresSchemaEditor


class PostgresSchema:
    """Represents a Postgres schema.

    See: https://www.postgresql.org/docs/current/ddl-schemas.html
    """

    NAME_MAX_LENGTH = 63

    name: str

    default: "PostgresSchema"

    def __init__(self, name: str) -> None:
        self.name = name

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
            cast("PostgresSchemaEditor", schema_editor).create_schema(name)

        return cls(name)

    @classmethod
    def create_time_based(
        cls, prefix: str, *, using: str = DEFAULT_DB_ALIAS
    ) -> "PostgresSchema":
        """Creates a new schema with a time-based suffix.

        The time is precise up to the second. Creating
        multiple time based schema in the same second
        WILL lead to conflicts.

        Arguments:
            prefix:
                Name to prefix the final name with. The name plus
                prefix cannot be longer than 63 characters.

            using:
                Name of the database connection to use.
        """

        suffix = timezone.now().strftime("%Y%m%d%H%m%S")
        name = cls._create_generated_name(prefix, suffix)

        return cls.create(name, using=using)

    @classmethod
    def create_random(
        cls, prefix: str, *, using: str = DEFAULT_DB_ALIAS
    ) -> "PostgresSchema":
        """Creates a new schema with a random suffix.

        Arguments:
            prefix:
                Name to prefix the final name with. The name plus
                prefix cannot be longer than 63 characters.

            using:
                Name of the database connection to use.
        """

        suffix = os.urandom(4).hex()
        name = cls._create_generated_name(prefix, suffix)

        return cls.create(name, using=using)

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
            cls(name).delete(cascade=cascade, using=using)
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
            return name in cast(
                "PostgresIntrospection", connection.introspection
            ).get_schema_list(cursor)

    def delete(
        self, *, cascade: bool = False, using: str = DEFAULT_DB_ALIAS
    ) -> None:
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

        with connections[using].schema_editor() as schema_editor:
            cast("PostgresSchemaEditor", schema_editor).delete_schema(
                self.name, cascade=cascade
            )

    @classmethod
    def _create_generated_name(cls, prefix: str, suffix: str) -> str:
        separator = "_"
        generated_name = f"{prefix}{separator}{suffix}"
        max_prefix_length = cls.NAME_MAX_LENGTH - len(suffix) - len(separator)

        if len(generated_name) > cls.NAME_MAX_LENGTH:
            raise ValidationError(
                f"Schema prefix '{prefix}' is longer than {max_prefix_length} characters. Together with the separator and generated suffix of {len(suffix)} characters, the name would exceed Postgres's limit of {cls.NAME_MAX_LENGTH} characters."
            )

        return generated_name


PostgresSchema.default = PostgresSchema("public")


@contextmanager
def postgres_temporary_schema(
    prefix: str,
    *,
    cascade: bool = False,
    delete_on_throw: bool = False,
    using: str = DEFAULT_DB_ALIAS,
) -> Generator[PostgresSchema, None, None]:
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
            schema.delete(cascade=cascade, using=using)

        raise e

    schema.delete(cascade=cascade, using=using)
