import pytest

from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor

from . import db_introspection
from .fake_model import get_fake_model


@pytest.fixture
def fake_model():
    return get_fake_model(
        {
            "text": models.TextField(),
        }
    )


def test_schema_editor_storage_settings_table_alter_and_reset(fake_model):
    table_name = fake_model._meta.db_table
    schema_editor = PostgresSchemaEditor(connection)

    schema_editor.alter_table_storage_setting(
        table_name, "autovacuum_enabled", "false"
    )
    assert db_introspection.get_storage_settings(table_name) == {
        "autovacuum_enabled": "false"
    }

    schema_editor.reset_table_storage_setting(table_name, "autovacuum_enabled")
    assert db_introspection.get_storage_settings(table_name) == {}


def test_schema_editor_storage_settings_model_alter_and_reset(fake_model):
    table_name = fake_model._meta.db_table
    schema_editor = PostgresSchemaEditor(connection)

    schema_editor.alter_model_storage_setting(
        fake_model, "autovacuum_enabled", "false"
    )
    assert db_introspection.get_storage_settings(table_name) == {
        "autovacuum_enabled": "false"
    }

    schema_editor.reset_model_storage_setting(fake_model, "autovacuum_enabled")
    assert db_introspection.get_storage_settings(table_name) == {}
