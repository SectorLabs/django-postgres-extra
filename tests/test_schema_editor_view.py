import pytest

from django.db import OperationalError, connection, models

from psqlextra.backend.schema import PostgresSchemaEditor
from psqlextra.error import extract_postgres_error_code

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


def test_schema_editor_replace_view():
    """Tests whether creating a view and then replacing it with another one
    (thus changing the backing query) works as expected."""

    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_view_model(
        {"name": models.TextField()},
        {"query": underlying_model.objects.filter(name="test1")},
    )

    underlying_model.objects.create(name="test1")
    underlying_model.objects.create(name="test2")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_view_model(model)

    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test1"

    model._view_meta.query = underlying_model.objects.filter(
        name="test2"
    ).query.sql_with_params()
    schema_editor.replace_view_model(model)

    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test2"


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


@pytest.mark.django_db(transaction=True)
def test_schema_editor_create_materialized_view_without_data():
    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_materialized_view_model(
        {"name": models.TextField()},
        {"query": underlying_model.objects.filter(name="test1")},
    )

    underlying_model.objects.create(name="test1")
    underlying_model.objects.create(name="test2")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_materialized_view_model(model, with_data=False)

    with pytest.raises(OperationalError) as exc_info:
        list(model.objects.all())

    pg_error = extract_postgres_error_code(exc_info.value)
    assert pg_error == "55000"  # OBJECT_NOT_IN_PREREQUISITE_STATE

    model.refresh()

    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test1"


def test_schema_editor_replace_materialized_view():
    """Tests whether creating a materialized view and then replacing it with
    another one (thus changing the backing query) works as expected."""

    underlying_model = get_fake_model({"name": models.TextField()})

    model = define_fake_materialized_view_model(
        {"name": models.TextField()},
        {"query": underlying_model.objects.filter(name="test1")},
        {"indexes": [models.Index(fields=["name"])]},
    )

    underlying_model.objects.create(name="test1")
    underlying_model.objects.create(name="test2")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_materialized_view_model(model)

    for index in model._meta.indexes:
        schema_editor.add_index(model, index)

    constraints_before = db_introspection.get_constraints(model._meta.db_table)

    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test1"

    model._view_meta.query = underlying_model.objects.filter(
        name="test2"
    ).query.sql_with_params()
    schema_editor.replace_materialized_view_model(model)

    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].name == "test2"

    # make sure all indexes/constraints still exists because
    # replacing a materialized view involves re-creating it
    constraints_after = db_introspection.get_constraints(model._meta.db_table)
    assert constraints_after == constraints_before
