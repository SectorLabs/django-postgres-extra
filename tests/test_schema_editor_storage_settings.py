from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor

from . import db_introspection
from .fake_model import get_fake_model


def test_schema_editor_storage_settings_set_and_reset():
    model = get_fake_model(
        {
            "text": models.TextField(),
        }
    )

    table_name = model._meta.db_table

    schema_editor = PostgresSchemaEditor(connection)

    schema_editor.set_table_storage_setting(
        table_name, "autovacuum_enabled", "false"
    )
    assert db_introspection.get_storage_settings(table_name) == {
        "autovacuum_enabled": "false"
    }

    schema_editor.reset_table_storage_setting(table_name, "autovacuum_enabled")
    assert db_introspection.get_storage_settings(table_name) == {}
