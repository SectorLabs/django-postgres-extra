import os

from datetime import timedelta
from unittest.mock import MagicMock

import django
import pytest

from django.db import connection, models, transaction

from psqlextra.contrib.model_data_migrator import PostgresModelDataMigrator
from psqlextra.schema import PostgresSchema
from psqlextra.settings import postgres_prepend_local_search_path

from . import db_introspection
from .fake_model import delete_fake_model, get_fake_model

pytestmark = pytest.mark.skipif(
    django.VERSION < (3, 2),
    reason="The migrator clones models into a separate schema and uses durable transactions, which need Django >= 3.2",
)


@pytest.fixture
def fake_model():
    model = get_fake_model({"name": models.TextField()})

    yield model

    delete_fake_model(model)


@pytest.fixture
def role():
    name = f"psqlextra_{os.urandom(4).hex()}"
    quoted_name = connection.ops.quote_name(name)

    with connection.cursor() as cursor:
        cursor.execute(f"CREATE ROLE {quoted_name}")

    yield name

    with connection.cursor() as cursor:
        cursor.execute(f"DROP OWNED BY {quoted_name}")
        cursor.execute(f"DROP ROLE {quoted_name}")


def _create_migrator(
    model, *, keep_backup_schema=True, fail=False, while_filling=None
):
    class Migrator(PostgresModelDataMigrator):
        operation_timeout = timedelta(minutes=1)

        def fill_cloned_table_lockless(self, work_schema, default_schema):
            with self.atomic():
                with postgres_prepend_local_search_path([work_schema.name]):
                    self.model.objects.create(name="new")

            if while_filling:
                while_filling()

            if fail:
                raise RuntimeError("fill failed")

        def clean_cloned_table(self, work_schema, default_schema):
            pass

        def fill_cloned_table_locked(self, work_schema, default_schema):
            pass

    Migrator.model = model
    Migrator.keep_backup_schema = keep_backup_schema

    return Migrator(MagicMock())


def _list_schemas_of(model):
    with connection.cursor() as cursor:
        schema_names = connection.introspection.get_schema_list(cursor)

    return [
        schema_name
        for schema_name in schema_names
        if model._meta.db_table in schema_name
    ]


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_swaps_in_the_filled_table(fake_model):
    fake_model.objects.create(name="old")

    _create_migrator(fake_model).migrate()

    assert list(fake_model.objects.values_list("name", flat=True)) == ["new"]


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_keeps_the_backup_schema(fake_model):
    fake_model.objects.create(name="old")

    state = _create_migrator(fake_model).migrate()

    assert _list_schemas_of(fake_model) == [state.backup_schema.name]

    with transaction.atomic():
        with postgres_prepend_local_search_path([state.backup_schema.name]):
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT name FROM {connection.ops.quote_name(fake_model._meta.db_table)}"
                )
                assert cursor.fetchall() == [("old",)]

    PostgresSchema(state.backup_schema.name).delete(cascade=True)


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_deletes_the_backup_schema_when_asked(
    fake_model,
):
    _create_migrator(fake_model, keep_backup_schema=False).migrate()

    assert _list_schemas_of(fake_model) == []


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_deletes_its_schemas_when_it_fails(fake_model):
    fake_model.objects.create(name="old")

    with pytest.raises(RuntimeError):
        _create_migrator(fake_model, fail=True).migrate()

    assert _list_schemas_of(fake_model) == []
    assert list(fake_model.objects.values_list("name", flat=True)) == ["old"]


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_keeps_the_storage_settings(fake_model):
    with connection.schema_editor() as schema_editor:
        schema_editor.alter_model_storage_setting(
            fake_model, "fillfactor", "80"
        )

    _create_migrator(fake_model, keep_backup_schema=False).migrate()

    with transaction.atomic():
        assert db_introspection.get_storage_settings(
            fake_model._meta.db_table
        ) == {"fillfactor": "80"}


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_analyzes_the_swapped_in_table(fake_model):
    _create_migrator(fake_model, keep_backup_schema=False).migrate()

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT attname FROM pg_stats WHERE schemaname = current_schema() AND tablename = %s",
            (fake_model._meta.db_table,),
        )
        assert "name" in {attname for attname, in cursor.fetchall()}


def _list_privileges_of(model):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT relacl::text[] FROM pg_class WHERE oid = %s::regclass",
            (connection.ops.quote_name(model._meta.db_table),),
        )
        [acl] = cursor.fetchone()

    return set(acl or [])


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_keeps_the_privileges(fake_model, role):
    quoted_table_name = connection.ops.quote_name(fake_model._meta.db_table)
    quoted_role_name = connection.ops.quote_name(role)

    with connection.cursor() as cursor:
        cursor.execute(f"GRANT SELECT ON {quoted_table_name} TO PUBLIC")
        cursor.execute(
            f"GRANT SELECT, DELETE ON {quoted_table_name} TO {quoted_role_name}"
        )
        cursor.execute(
            f"GRANT UPDATE ON {quoted_table_name} TO {quoted_role_name} WITH GRANT OPTION"
        )

    privileges = _list_privileges_of(fake_model)

    _create_migrator(fake_model, keep_backup_schema=False).migrate()

    assert _list_privileges_of(fake_model) == privileges


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_keeps_the_autovacuum_setting(fake_model):
    with connection.schema_editor() as schema_editor:
        schema_editor.alter_model_storage_setting(
            fake_model, "autovacuum_enabled", "true"
        )

    _create_migrator(fake_model, keep_backup_schema=False).migrate()

    with transaction.atomic():
        assert db_introspection.get_storage_settings(
            fake_model._meta.db_table
        ) == {"autovacuum_enabled": "true"}


@pytest.mark.django_db(transaction=True)
def test_model_data_migrator_keeps_the_privileges_granted_while_filling(
    fake_model, role
):
    quoted_table_name = connection.ops.quote_name(fake_model._meta.db_table)

    def grant():
        with connection.cursor() as cursor:
            cursor.execute(
                f"GRANT SELECT ON {quoted_table_name} TO {connection.ops.quote_name(role)}"
            )

    _create_migrator(
        fake_model, keep_backup_schema=False, while_filling=grant
    ).migrate()

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT has_table_privilege(%s, %s, 'SELECT')",
            (role, quoted_table_name),
        )
        assert cursor.fetchone() == (True,)
