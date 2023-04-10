import os

from contextlib import contextmanager

from django.core.exceptions import SuspiciousOperation, ValidationError
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.utils import timezone


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
        cls._verify_generated_name_length(prefix, suffix)

        return cls.create(f"{prefix}_{suffix}", using=using)

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
        cls._verify_generated_name_length(prefix, suffix)

        return cls.create(f"{prefix}_{suffix}", using=using)

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

    @classmethod
    def _verify_generated_name_length(cls, prefix: str, suffix: str) -> None:
        max_prefix_length = cls.NAME_MAX_LENGTH - len(suffix)

        if len(prefix) > max_prefix_length:
            raise ValidationError(
                f"Schema prefix '{prefix}' is longer than {max_prefix_length} characters. Together with the generated suffix of {len(suffix)} characters, the name would exceed Postgres's limit of {cls.NAME_MAX_LENGTH} characters."
            )


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
