from django.db import models

from psqlextra import PostgresMaterializedView

from .fake_model import get_fake_model, define_fake_model


def test_materialized_view_create():
    """Tests whether defining a materialized view and
    creating one works correctly."""

    ModelA = get_fake_model({
        'first_name': models.CharField(max_length=255)
    })

    ModelB = get_fake_model({
        'model_a': models.ForeignKey(ModelA),
        'last_name': models.CharField(max_length=255)
    })

    obj_a = ModelA.objects.create(first_name='Swen')
    obj_b = ModelB.objects.create(model_a=obj_a, last_name='Kooij')

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
            'db_table': 'test_model_view',
            'unique_together': ('first_name', 'last_name',)
        }
    )

    MaterializedViewModel.refresh()

    obj = MaterializedViewModel.objects.first()
    assert obj.first_name == obj_a.first_name
    assert obj.last_name == obj_b.last_name
