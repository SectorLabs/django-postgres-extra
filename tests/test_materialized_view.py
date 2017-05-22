import uuid

from django.db import connection, models

from psqlextra import PostgresMaterializedView

from .util import get_fake_model, define_fake_model, db_relation_exists


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

    MaterializedViewModel = define_fake_model(
        {
            'queryset': ModelB.objects.values('id', 'last_name', 'model_a__first_name').all(),
            'id': models.IntegerField(primary_key=True),
            'first_name': models.CharField(max_length=255),
            'last_name': models.CharField(max_length=255)
        },
        PostgresMaterializedView,
        {
            'managed': False,
            'db_table': db_table,
        }
    )

    return ModelA, ModelB, MaterializedViewModel


def test_materialized_view_create():
    """Tests whether defining a materialized view and
    creating one works correctly."""

    ModelA, ModelB, MaterializedViewModel = get_fake_materialized_view()

    obj_a = ModelA.objects.create(first_name='Swen')
    obj_b = ModelB.objects.create(model_a=obj_a, last_name='Kooij')

    MaterializedViewModel.refresh()

    obj = MaterializedViewModel.objects.first()
    assert obj.first_name == obj_a.first_name
    assert obj.last_name == obj_b.last_name


def test_materialized_view_drop():
    """Tests whether dropping a materialize view works
    correctly."""

    ModelA, ModelB, MaterializedViewModel = get_fake_materialized_view()
    table_name = MaterializedViewModel._meta.db_table

    MaterializedViewModel.refresh()
    assert db_relation_exists(table_name)

    MaterializedViewModel.drop()
    assert not db_relation_exists(table_name)
