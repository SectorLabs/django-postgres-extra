import uuid

import pytest

from django.db import models
from django.db.models import F
from django.core.exceptions import ImproperlyConfigured

from psqlextra.models import PostgresMaterializedViewModel

from .util import get_fake_model, db_relation_exists


def get_fake_materialized_view():
    """Creates a fake materialized view composed of two
    other models."""

    db_table = str(uuid.uuid4()).replace('-', '')[:8]

    ModelA = get_fake_model({
        'first_name': models.CharField(max_length=255)
    })

    ModelB = get_fake_model({
        'model_a': models.ForeignKey(ModelA),
        'last_name': models.CharField(max_length=255)
    })

    MaterializedViewModel = get_fake_model(
        {
            'id': models.IntegerField(primary_key=True),
            'first_name': models.CharField(max_length=255),
            'last_name': models.CharField(max_length=255),
        },
        PostgresMaterializedViewModel,
        {
            'db_table': db_table,
            'view_query': ModelB.objects.values('id', 'last_name', 'model_a__first_name')
        }
    )

    return ModelA, ModelB, MaterializedViewModel


def test_materialized_view_annotate():
    """Tests whether materialized views work properly
    when the view query has an annotation in it."""

    Model = get_fake_model({
        'name': models.CharField(max_length=255)
    })

    MaterializedViewModel = get_fake_model(
        {
            'id': models.IntegerField(primary_key=True),
            'name': models.CharField(max_length=255),
        },
        PostgresMaterializedViewModel,
        {
            'view_query': Model.objects.annotate(name=F('name')).values('id', 'name')
        }
    )

    table_name = MaterializedViewModel._meta.db_table

    # the view should have been created by the migrations
    assert db_relation_exists(table_name)


def test_materialized_view_create_refresh():
    """Tests whether defining a materialized view,
    creating and refreshing it works correctly."""

    ModelA, ModelB, MaterializedViewModel = get_fake_materialized_view()

    # create the materialized view with one row
    obj_a = ModelA.objects.create(first_name='Swen')
    obj_b = ModelB.objects.create(model_a=obj_a, last_name='Kooij')

    MaterializedViewModel.view.refresh(concurrently=False)

    obj = MaterializedViewModel.objects.first()
    assert obj.first_name == obj_a.first_name
    assert obj.last_name == obj_b.last_name

    # update the source record, and refresh the view
    obj_b.last_name = 'Beer'
    obj_b.save()

    # refresh the materialized view
    MaterializedViewModel.view.refresh(concurrently=False)

    # verify the view was properly updated
    obj.refresh_from_db()
    assert obj.last_name == obj_b.last_name


@pytest.mark.parametrize('view_query_type', ['queryset', 'text'])
def test_materialized_view_query_good(view_query_type):
    """Tests whether specifying legal values for `view_query`
    in a materialized view's `view_query` meta propery works
    properly."""

    Model = get_fake_model({
        'name': models.CharField(max_length=255)
    })

    view_query = None
    if view_query_type == 'queryset':
        view_query = Model.objects.values('id', 'name')
    elif view_query_type == 'text':
        view_query = str(Model.objects.values('id', 'name').query)

    MaterializedViewModel = get_fake_model(
        {
            'id': models.IntegerField(primary_key=True),
            'name': models.CharField(max_length=255),
        },
        PostgresMaterializedViewModel,
        dict(view_query=view_query)
    )

    Model.objects.create(name='swen')
    MaterializedViewModel.view.refresh(concurrently=False)
    assert MaterializedViewModel.objects.first().name == Model.objects.first().name


@pytest.mark.parametrize('view_query', ['', None])
def test_materialized_view_query_bad(view_query):
    """Tests whether specifying illegal values for `view_query`
    in a materialized view's `view_query` meta properly raises
    an exception."""

    with pytest.raises(ImproperlyConfigured):
        get_fake_model(
            dict(),
            PostgresMaterializedViewModel,
            dict(view_query=view_query)
        )


def test_materialized_view_drop():
    """Tests whether dropping a materialize view works
    correctly."""

    ModelA, ModelB, MaterializedViewModel = get_fake_materialized_view()
    table_name = MaterializedViewModel._meta.db_table

    # the view should have been created by the migrations
    assert db_relation_exists(table_name)

    # ensure dropping the model works properly
    MaterializedViewModel.view.drop()
    assert not db_relation_exists(table_name)
