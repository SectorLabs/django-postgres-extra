from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from django.db import connection, models, transaction

from psqlextra.contrib.model_data_migrator import PostgresModelDataMigrator
from psqlextra.schema import PostgresSchema
from psqlextra.settings import postgres_prepend_local_search_path

from .fake_model import delete_fake_model, get_fake_model


@pytest.fixture
def fake_model():
    model = get_fake_model({"name": models.TextField()})

    yield model

    delete_fake_model(model)


def _create_migrator(model, *, keep_backup_schema=True, fail=False):
    class Migrator(PostgresModelDataMigrator):
        operation_timeout = timedelta(minutes=1)

        def fill_cloned_table_lockless(self, work_schema, default_schema):
            with self.atomic():
                with postgres_prepend_local_search_path([work_schema.name]):
                    self.model.objects.create(name="new")

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
