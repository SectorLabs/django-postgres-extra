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


def test_materialized_view_model_refresh_concurrently():
    """Tests whether a materialized view can be refreshed concurrently."""

    underlying_model = get_fake_model(
        {"first_name": models.TextField(), "last_name": models.TextField()}
    )

    model = define_fake_materialized_view_model(
        {"first_name": models.TextField(), "last_name": models.TextField()},
        {
            "query": underlying_model.objects.filter(first_name="John"),
            "unique_constraint": models.UniqueConstraint(
                fields=["last_name"], name="unique_constraint"
            ),
        },
    )

    underlying_model.objects.create(first_name="John", last_name="Snow")
    underlying_model.objects.create(first_name="Ned", last_name="Stark")

    schema_editor = PostgresSchemaEditor(connection)
    schema_editor.create_materialized_view_model(model)

    # materialized view should only show records first_name="John"
    objs = list(model.objects.all())
    assert len(objs) == 1
    assert objs[0].first_name == "John"
    assert objs[0].last_name == "Snow"

    # create another record with "John" first name and refresh
    underlying_model.objects.create(first_name="John", last_name="Wick")
    model.refresh(concurrently=True)

    objs = list(model.objects.all())
    assert len(objs) == 2
