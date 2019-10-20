from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor

from . import db_introspection
from .fake_model import (
    define_fake_materialized_view_model,
    define_fake_view_model,
    get_fake_model,
)


def test_schema_editor_create_delete_view():
    """Tests whether creating and then deleting a view using the schema editor
    works as expected."""

    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_view_model(
        {"name": models.TextField()},
        {"query": underlying_model.objects.filter(name="test1")},
    )

    underlying_model.objects.create(name="test1")
    underlying_model.objects.create(name="test2")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_view_model(model)

    # view should only show records name="test"1
    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test1"

    # create another record, view should have it right away
    underlying_model.objects.create(name="test1")
    assert model.objects.count() == 2

    # delete the view
    schema_editor.delete_view_model(model)

    # make sure it was actually deleted
    assert model._meta.db_table not in db_introspection.table_names(True)


def test_schema_editor_create_delete_materialized_view():
    """Tests whether creating and then deleting a materialized view using the
    schema editor works as expected."""

    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_materialized_view_model(
        {"name": models.TextField()},
        {"query": underlying_model.objects.filter(name="test1")},
    )

    underlying_model.objects.create(name="test1")
    underlying_model.objects.create(name="test2")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_materialized_view_model(model)

    # materialized view should only show records name="test"1
    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test1"

    # delete the materialized view
    schema_editor.delete_materialized_view_model(model)

    # make sure it was actually deleted
    assert model._meta.db_table not in db_introspection.table_names(True)
