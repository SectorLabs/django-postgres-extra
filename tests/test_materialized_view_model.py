from django.db import connection, models

from psqlextra.backend.schema import PostgresSchemaEditor

from .fake_model import define_fake_materialized_view_model, get_fake_model


def test_materialized_view_model_refresh():
    """Tests whether a materialized view can be refreshed."""

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

    # create another record with "test1" and refresh
    underlying_model.objects.create(name="test1")
    model.refresh()

    objs = list(model.objects.all())
    assert len(objs) == 2
