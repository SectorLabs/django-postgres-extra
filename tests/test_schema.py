import freezegun
import pytest

from django.core.exceptions import SuspiciousOperation, ValidationError
from django.db import InternalError, ProgrammingError, connection

from psqlextra.error import extract_postgres_error_code
from psqlextra.schema import PostgresSchema, postgres_temporary_schema


def _does_schema_exist(name: str) -> bool:
    with connection.cursor() as cursor:
        return name in connection.introspection.get_schema_list(cursor)


def test_postgres_schema_create():
    schema = PostgresSchema.create("myschema")
    assert schema.name == "myschema"

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
    with freezegun.freeze_time("2023-04-07 13:37:23.4"):
        schema = PostgresSchema.create_time_based("myprefix")

    assert schema.name == "myprefix_20230407130423"
    assert _does_schema_exist(schema.name)


def test_postgres_schema_create_time_based_long_prefix():
    with pytest.raises(ValidationError) as exc_info:
        with freezegun.freeze_time("2023-04-07 13:37:23.4"):
            PostgresSchema.create_time_based("a" * 49)

    assert "is longer than 48 characters" in str(exc_info.value)


def test_postgres_schema_create_random():
    schema = PostgresSchema.create_random("myprefix")

    prefix, suffix = schema.name.split("_")
    assert prefix == "myprefix"
    assert len(suffix) == 8

    assert _does_schema_exist(schema.name)


def test_postgres_schema_create_random_long_prefix():
    with pytest.raises(ValidationError) as exc_info:
        PostgresSchema.create_random("a" * 55)

    assert "is longer than 54 characters" in str(exc_info.value)


def test_postgres_schema_delete_and_create():
    schema = PostgresSchema.create("test")

    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test.bla AS SELECT 'hello'")
        cursor.execute("SELECT * FROM test.bla")

        assert cursor.fetchone() == ("hello",)

    # Should refuse to delete since we added a table to the schema
    with pytest.raises(InternalError) as exc_info:
        schema = PostgresSchema.delete_and_create(schema.name)

    pg_error = extract_postgres_error_code(exc_info.value)
    assert pg_error == "2BP01"  # DEPENDENT_OBJECTS_STILL_EXIST

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

    pg_error = extract_postgres_error_code(exc_info.value)
    assert pg_error == "42P01"  # UNDEFINED_TABLE


def test_postgres_schema_delete():
    schema = PostgresSchema.create("test")
    assert _does_schema_exist(schema.name)

    schema.delete()
    assert not _does_schema_exist(schema.name)


def test_postgres_schema_delete_not_empty():
    schema = PostgresSchema.create("test")
    assert _does_schema_exist(schema.name)

    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test.bla AS SELECT 'hello'")

    with pytest.raises(InternalError) as exc_info:
        schema.delete()

    pg_error = extract_postgres_error_code(exc_info.value)
    assert pg_error == "2BP01"  # DEPENDENT_OBJECTS_STILL_EXIST


def test_postgres_schema_delete_cascade_not_empty():
    schema = PostgresSchema.create("test")
    assert _does_schema_exist(schema.name)

    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE test.bla AS SELECT 'hello'")

    schema.delete(cascade=True)
    assert not _does_schema_exist(schema.name)


def test_postgres_schema_no_delete_default():
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
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE {schema.name}.mytable AS SELECT 'hello world'"
                )

    pg_error = extract_postgres_error_code(exc_info.value)
    assert pg_error == "2BP01"  # DEPENDENT_OBJECTS_STILL_EXIST


def test_postgres_temporary_schema_not_empty_cascade():
    with postgres_temporary_schema("temp", cascade=True) as schema:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {schema.name}.mytable AS SELECT 'hello world'"
            )

    assert not _does_schema_exist(schema.name)


@pytest.mark.parametrize("delete_on_throw", [True, False])
def test_postgres_temporary_schema_no_delete_on_throw(delete_on_throw):
    with pytest.raises(ValueError):
        with postgres_temporary_schema(
            "temp", delete_on_throw=delete_on_throw
        ) as schema:
            raise ValueError("test")

    assert _does_schema_exist(schema.name) != delete_on_throw
