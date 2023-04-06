import uuid

import freezegun
import pytest

from django.core.exceptions import SuspiciousOperation, ValidationError
from django.db import (
    DEFAULT_DB_ALIAS,
    InternalError,
    ProgrammingError,
    connection,
)
from psycopg2 import errorcodes

from psqlextra.error import extract_postgres_error
from psqlextra.schema import PostgresSchema, postgres_temporary_schema


def _does_schema_exist(name: str) -> bool:
    with connection.cursor() as cursor:
        return name in connection.introspection.get_schema_list(cursor)


def test_postgres_schema_create():
    schema = PostgresSchema.create("myschema")
    assert schema.name == "myschema"
    assert schema.using == DEFAULT_DB_ALIAS

    assert _does_schema_exist(schema.name)


def test_postgres_schema_does_not_overwrite():
    schema = PostgresSchema.create("myschema")

    with pytest.raises(ProgrammingError):
        PostgresSchema.create(schema.name)


def test_postgres_schema_create_max_name_length():
    with pytest.raises(ValidationError) as exc_info:
        PostgresSchema.create(
            "stringthatislongerhtan63charactersforsureabsolutelysurethisislongerthanthat"
        )

    assert "is longer than Postgres's limit" in str(exc_info.value)


def test_postgres_schema_create_name_that_requires_escaping():
    # 'table' needs escaping because it conflicts with
    # the SQL keyword TABLE
    schema = PostgresSchema.create("table")
    assert schema.name == "table"

    assert _does_schema_exist("table")


def test_postgres_schema_create_time_based():
    with freezegun.freeze_time("2023-04-07 13:37:00.0"):
        schema = PostgresSchema.create_time_based("myprefix")

    assert schema.name == "myprefix_2023040713041680892620"
    assert _does_schema_exist(schema.name)


def test_postgres_schema_create_time_based_long_prefix():
    with pytest.raises(ValidationError) as exc_info:
        PostgresSchema.create_time_based("a" * 100)

    assert "is longer than 55 characters" in str(exc_info.value)


def test_postgres_schema_create_random():
    schema = PostgresSchema.create_random("myprefix")

    prefix, suffix = schema.name.split("_")
    assert prefix == "myprefix"
    assert len(suffix) == 8

    assert _does_schema_exist(schema.name)


def test_postgres_schema_create_random_long_prefix():
    with pytest.raises(ValidationError) as exc_info:
        PostgresSchema.create_random("a" * 100)

    assert "is longer than 55 characters" in str(exc_info.value)


def test_postgres_schema_delete_and_create():
    schema = PostgresSchema.create("test")

    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test.bla AS SELECT 'hello'")
        cursor.execute("SELECT * FROM test.bla")

        assert cursor.fetchone() == ("hello",)

    # Should refuse to delete since we added a table to the schema
    with pytest.raises(InternalError) as exc_info:
        schema = PostgresSchema.delete_and_create(schema.name)

    pg_error = extract_postgres_error(exc_info.value)
    assert pg_error.pgcode == errorcodes.DEPENDENT_OBJECTS_STILL_EXIST

    # Verify that the schema and table still exist
    assert _does_schema_exist(schema.name)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM test.bla")
        assert cursor.fetchone() == ("hello",)

    # Dropping the schema should work with cascade=True
    schema = PostgresSchema.delete_and_create(schema.name, cascade=True)
    assert _does_schema_exist(schema.name)

    # Since the schema was deleted and re-created, the `bla`
    # table should not exist anymore.
    with pytest.raises(ProgrammingError) as exc_info:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM test.bla")
            assert cursor.fetchone() == ("hello",)

    pg_error = extract_postgres_error(exc_info.value)
    assert pg_error.pgcode == errorcodes.UNDEFINED_TABLE


def test_postgres_schema_delete():
    schema = PostgresSchema.create("test")
    assert _does_schema_exist(schema.name)

    schema.delete()
    assert not _does_schema_exist(schema.name)


def test_postgres_schema_delete_not_empty():
    schema = PostgresSchema.create("test")
    assert _does_schema_exist(schema.name)

    with schema.connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test.bla AS SELECT 'hello'")

    with pytest.raises(InternalError) as exc_info:
        schema.delete()

    pg_error = extract_postgres_error(exc_info.value)
    assert pg_error.pgcode == errorcodes.DEPENDENT_OBJECTS_STILL_EXIST


def test_postgres_schema_delete_cascade_not_empty():
    schema = PostgresSchema.create("test")
    assert _does_schema_exist(schema.name)

    with schema.connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test.bla AS SELECT 'hello'")

    schema.delete(cascade=True)
    assert not _does_schema_exist(schema.name)


def test_postgres_schema_connection():
    schema = PostgresSchema.create("test")

    with schema.connection.cursor() as cursor:
        # Creating a table without specifying the schema should create
        # it in our schema and we should be able to select from it without
        # specifying the schema.
        cursor.execute("CREATE TABLE myschematable AS SELECT 'myschema'")
        cursor.execute("SELECT * FROM myschematable")
        assert cursor.fetchone() == ("myschema",)

        # Proof that the table was created in our schema even though we
        # never explicitly told it to do so.
        cursor.execute(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = %s",
            ("myschematable",),
        )
        assert cursor.fetchone() == (schema.name,)

        # Creating a table in another schema, we should not be able
        # to select it without specifying the schema since our
        # schema scoped connection only looks at our schema by default.
        cursor.execute(
            "CREATE TABLE public.otherschematable AS SELECT 'otherschema'"
        )
        with pytest.raises(ProgrammingError) as exc_info:
            cursor.execute("SELECT * FROM otherschematable")

        cursor.execute("ROLLBACK")

        pg_error = extract_postgres_error(exc_info.value)
        assert pg_error.pgcode == errorcodes.UNDEFINED_TABLE


def test_postgres_schema_connection_does_not_affect_default():
    schema = PostgresSchema.create("test")

    with schema.connection.cursor() as cursor:
        cursor.execute("SHOW search_path")
        assert cursor.fetchone() == ("test",)

    with connection.cursor() as cursor:
        cursor.execute("SHOW search_path")
        assert cursor.fetchone() == ('"$user", public',)


@pytest.mark.django_db(transaction=True)
def test_postgres_schema_connection_does_not_affect_default_after_throw():
    schema = PostgresSchema.create(str(uuid.uuid4()))

    with pytest.raises(ProgrammingError):
        with schema.connection.cursor() as cursor:
            cursor.execute("COMMIT")
            cursor.execute("SELECT frombadtable")

    with connection.cursor() as cursor:
        cursor.execute("ROLLBACK")
        cursor.execute("SHOW search_path")
        assert cursor.fetchone() == ('"$user", public',)


def test_postgres_schema_connection_schema_editor():
    schema = PostgresSchema.create("test")

    with schema.connection.schema_editor() as schema_editor:
        with schema_editor.connection.cursor() as cursor:
            cursor.execute("SHOW search_path")
            assert cursor.fetchone() == ("test",)

    with connection.cursor() as cursor:
        cursor.execute("SHOW search_path")
        assert cursor.fetchone() == ('"$user", public',)


def test_postgres_schema_connection_does_not_catch():
    schema = PostgresSchema.create("test")

    with pytest.raises(ValueError):
        with schema.connection.cursor():
            raise ValueError("test")


def test_postgres_schema_connection_no_delete_default():
    with pytest.raises(SuspiciousOperation):
        PostgresSchema.default.delete()

    with pytest.raises(SuspiciousOperation):
        PostgresSchema("public").delete()


def test_postgres_temporary_schema():
    with postgres_temporary_schema("temp") as schema:
        name_prefix, name_suffix = schema.name.split("_")
        assert name_prefix == "temp"
        assert len(name_suffix) == 8

        assert _does_schema_exist(schema.name)

    assert not _does_schema_exist(schema.name)


def test_postgres_temporary_schema_not_empty():
    with pytest.raises(InternalError) as exc_info:
        with postgres_temporary_schema("temp") as schema:
            with schema.connection.cursor() as cursor:
                cursor.execute("CREATE TABLE mytable AS SELECT 'hello world'")

    pg_error = extract_postgres_error(exc_info.value)
    assert pg_error.pgcode == errorcodes.DEPENDENT_OBJECTS_STILL_EXIST


def test_postgres_temporary_schema_not_empty_cascade():
    with postgres_temporary_schema("temp", cascade=True) as schema:
        with schema.connection.cursor() as cursor:
            cursor.execute("CREATE TABLE mytable AS SELECT 'hello world'")

    assert not _does_schema_exist(schema.name)


@pytest.mark.parametrize("delete_on_throw", [True, False])
def test_postgres_temporary_schema_no_delete_on_throw(delete_on_throw):
    with pytest.raises(ValueError):
        with postgres_temporary_schema(
            "temp", delete_on_throw=delete_on_throw
        ) as schema:
            raise ValueError("test")

    assert _does_schema_exist(schema.name) != delete_on_throw
