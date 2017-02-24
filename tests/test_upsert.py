from django.test import TestCase

from psqlextra import HStoreField

from .fake_model import get_fake_model
from django.db import models
import pytest


@pytest.mark.django_db
class UpsertTest(TestCase):
    """Tests whether the :see:upsert functionality works correctly."""

    def test_simple(self):
        """Tests whether simple upserts work correctly."""

        model = get_fake_model({
            'title': HStoreField(uniqueness=['key1']),
            'cookies': models.CharField(max_length=255, null=True)
        })

        obj = model.objects.upsert_and_get(
            title={'key1': 'beer'},
            cookies='cheers'
        )

        obj1 = model.objects.upsert_and_get(
            title={'key1': 'beer'}
        )

        obj1.refresh_from_db()

        assert obj1.title['key1'] == 'beer'
        assert obj1.cookies == obj.cookies

    def test_auto_fields(self):
        """Asserts that fields that automatically add something
        to the model automatically still work properly when upserting."""

        model = get_fake_model({
            'title': models.CharField(max_length=255, unique=True),
            'date_added': models.DateTimeField(auto_now_add=True),
            'date_updated': models.DateTimeField(auto_now=True)
        })

        obj1 = model.objects.upsert_and_get(
            title='beer'
        )

        obj2 = model.objects.upsert_and_get(
            title='beer'
        )

        assert obj1.date_added
        assert obj2.date_added

        assert obj1.date_updated
        assert obj2.date_updated

        assert obj1.date_added == obj2.date_added
        assert obj1.date_updated != obj2.date_updated

    def test_foreign_key(self):
        """Asserts that models with foreign key relationships
        can safely be upserted."""

        model1 = get_fake_model({
            'name': models.CharField(max_length=255, unique=True)
        })

        model2 = get_fake_model({
            'name': models.CharField(max_length=255, unique=True),
            'model1': models.ForeignKey(model1)
        })

        model1_row = model1.objects.upsert_and_get(name='item1')

        # upsert by id, that should work
        model2.objects.upsert(name='item1', model1_id=model1_row.id)

        model2_row = model2.objects.get(name='item1')
        assert model2_row.name == 'item1'
        assert model2_row.model1.id == model1_row.id

        # upsert by object, that should also work
        model2.objects.upsert(name='item2', model1=model1_row)

        model2_row = model2.objects.get(name='item2')
        assert model2_row.name == 'item2'
        assert model2_row.model1.id == model1_row.id
