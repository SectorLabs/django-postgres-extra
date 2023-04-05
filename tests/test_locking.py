import uuid

import pytest

from django.db import connection, models, transaction

from psqlextra.locking import (
    PostgresTableLockMode,
    postgres_lock_model,
    postgres_lock_table,
)

from .fake_model import get_fake_model


@pytest.fixture
def mocked_model():
    return get_fake_model(
        {
            "name": models.TextField(),
        }
    )


def get_table_locks():
    with connection.cursor() as cursor:
        return connection.introspection.get_table_locks(cursor)


@pytest.mark.django_db(transaction=True)
def test_postgres_lock_table(mocked_model):
    lock_signature = (
        "public",
        mocked_model._meta.db_table,
        "AccessExclusiveLock",
    )
    with transaction.atomic():
        postgres_lock_table(
            mocked_model._meta.db_table, PostgresTableLockMode.ACCESS_EXCLUSIVE
        )
        assert lock_signature in get_table_locks()

    assert lock_signature not in get_table_locks()


@pytest.mark.django_db(transaction=True)
def test_postgres_lock_table_in_schema():
    schema_name = str(uuid.uuid4())[:8]
    table_name = str(uuid.uuid4())[:8]
    quoted_schema_name = connection.ops.quote_name(schema_name)
    quoted_table_name = connection.ops.quote_name(table_name)

    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {quoted_schema_name}")
        cursor.execute(
            f"CREATE TABLE {quoted_schema_name}.{quoted_table_name} AS SELECT 'hello world'"
        )

    lock_signature = (schema_name, table_name, "ExclusiveLock")
    with transaction.atomic():
        postgres_lock_table(
            table_name, PostgresTableLockMode.EXCLUSIVE, schema_name=schema_name
        )
        assert lock_signature in get_table_locks()

    assert lock_signature not in get_table_locks()


@pytest.mark.parametrize("lock_mode", list(PostgresTableLockMode))
@pytest.mark.django_db(transaction=True)
def test_postgres_lock_model(mocked_model, lock_mode):
    lock_signature = (
        "public",
        mocked_model._meta.db_table,
        lock_mode.alias,
    )

    with transaction.atomic():
        postgres_lock_model(mocked_model, lock_mode)
        assert lock_signature in get_table_locks()

    assert lock_signature not in get_table_locks()


@pytest.mark.django_db(transaction=True)
def test_postgres_lock_model_in_schema(mocked_model):
    schema_name = str(uuid.uuid4())[:8]
    quoted_schema_name = connection.ops.quote_name(schema_name)
    quoted_table_name = connection.ops.quote_name(mocked_model._meta.db_table)

    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {quoted_schema_name}")
        cursor.execute(
            f"CREATE TABLE {quoted_schema_name}.{quoted_table_name} (LIKE public.{quoted_table_name} INCLUDING ALL)"
        )

    lock_signature = (schema_name, mocked_model._meta.db_table, "ExclusiveLock")
    with transaction.atomic():
        postgres_lock_model(
            mocked_model,
            PostgresTableLockMode.EXCLUSIVE,
            schema_name=schema_name,
        )
        assert lock_signature in get_table_locks()

    assert lock_signature not in get_table_locks()
