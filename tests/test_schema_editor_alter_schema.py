import pytest

from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor

from .fake_model import get_fake_model


@pytest.fixture
def fake_model():
    return get_fake_model(
        {
            "text": models.TextField(),
        }
    )


def test_schema_editor_alter_table_schema(fake_model):
    obj = fake_model.objects.create(text="hello")

    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA target")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.alter_table_schema(fake_model._meta.db_table, "target")

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM target.{fake_model._meta.db_table}")
        assert cursor.fetchall() == [(obj.id, obj.text)]


def test_schema_editor_alter_model_schema(fake_model):
    obj = fake_model.objects.create(text="hello")

    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA target")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.alter_model_schema(fake_model, "target")

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM target.{fake_model._meta.db_table}")
        assert cursor.fetchall() == [(obj.id, obj.text)]
