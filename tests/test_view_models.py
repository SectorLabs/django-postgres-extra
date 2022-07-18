import pytest

from django.core.exceptions import ImproperlyConfigured
from django.db import models
from django.test.utils import override_settings

from psqlextra.models import PostgresMaterializedViewModel, PostgresViewModel

from .fake_model import define_fake_model, define_fake_view_model


@pytest.mark.parametrize(
    "model_base", [PostgresViewModel, PostgresMaterializedViewModel]
)
@override_settings(POSTGRES_EXTRA_ANNOTATE_SQL=True)
def test_view_model_meta_query_set(model_base):
    """Tests whether you can set a :see:QuerySet to be used as the underlying
    query for a view."""

    model = define_fake_model({"name": models.TextField()})

    view_model = define_fake_view_model(
        {"name": models.TextField()},
        model_base=model_base,
        view_options={"query": model.objects.all()},
    )

    expected_sql = 'SELECT "{0}"."id", "{0}"."name" FROM "{0}"'.format(
        model._meta.db_table
    )
    assert view_model._view_meta.query[0].startswith(expected_sql + " /* ")
    assert view_model._view_meta.query[1] == tuple()


@pytest.mark.parametrize(
    "model_base", [PostgresViewModel, PostgresMaterializedViewModel]
)
@pytest.mark.parametrize("bind_params", [("test",), ["test"]])
def test_view_model_meta_sql_with_params(model_base, bind_params):
    """Tests whether you can set a raw SQL query with a tuple of bind params as
    the underlying query for a view."""

    model = define_fake_model({"name": models.TextField()})
    sql = "select * from %s where name = %s" % (model._meta.db_table, "%s")
    sql_with_params = (sql, bind_params)

    view_model = define_fake_view_model(
        {"name": models.TextField()},
        model_base=model_base,
        view_options={"query": sql_with_params},
    )

    assert view_model._view_meta.query == sql_with_params


@pytest.mark.parametrize(
    "model_base", [PostgresViewModel, PostgresMaterializedViewModel]
)
def test_view_model_meta_sql_with_named_params(model_base):
    """Tests whether you can set a raw SQL query with a tuple of bind params as
    the underlying query for a view."""

    model = define_fake_model({"name": models.TextField()})
    sql = "select * from " + model._meta.db_table + " where name = %(name)s"
    sql_with_params = (sql, dict(name="test"))

    view_model = define_fake_view_model(
        {"name": models.TextField()},
        model_base=model_base,
        view_options={"query": sql_with_params},
    )

    assert view_model._view_meta.query == sql_with_params


@pytest.mark.parametrize(
    "model_base", [PostgresViewModel, PostgresMaterializedViewModel]
)
def test_view_model_meta_sql(model_base):
    """Tests whether you can set a raw SQL query without any params."""

    sql = "select 1"

    view_model = define_fake_view_model(
        {"name": models.TextField()},
        model_base=model_base,
        view_options={"query": sql},
    )

    assert view_model._view_meta.query == (sql, tuple())


@pytest.mark.parametrize(
    "model_base", [PostgresViewModel, PostgresMaterializedViewModel]
)
@pytest.mark.parametrize(
    "view_query",
    [
        dict(a=1),
        tuple("test"),
        ("test", None),
        (None, None),
        (1, 2),
        ("select 1", ("a", "b"), "onetoomay"),
    ],
)
def test_view_model_meta_bad_query(model_base, view_query):
    """Tests whether a bad view query configuration raises and error."""

    with pytest.raises(ImproperlyConfigured):
        define_fake_view_model(
            {"name": models.TextField()},
            model_base=model_base,
            view_options={"query": view_query},
        )
