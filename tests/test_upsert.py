from django.test import TestCase

from psqlextra import HStoreField

from .fake_model import get_fake_model
from django.db import models
import pytest

from django.core.exceptions import SuspiciousOperation

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
            conflict_target=[('title', 'key1')],
            fields=dict(
                title={'key1': 'beer'},
                cookies='cheers'
            )
        )

        obj1 = model.objects.upsert_and_get(
            conflict_target=[('title', 'key1')],
            fields=dict(
                title={'key1': 'beer'}
            )
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
            conflict_target=['title'],
            fields=dict(
                title='beer'
            )
        )

        obj2 = model.objects.upsert_and_get(
            conflict_target=['title'],
            fields=dict(
                title='beer'
            )
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

        model1_row = model1.objects.upsert_and_get(
            conflict_target=['name'],
            fields=dict(
                name='item1'
            )
        )

        # upsert by id, that should work
        model2.objects.upsert(
            conflict_target=['name'],
            fields=dict(
                name='item1',
                model1_id=model1_row.id
            )
        )

        model2_row = model2.objects.get(name='item1')
        assert model2_row.name == 'item1'
        assert model2_row.model1.id == model1_row.id

        # upsert by object, that should also work
        model2.objects.upsert(
            conflict_target=['name'],
            fields=dict(
                name='item2',
                model1=model1_row
            )
        )

        model2_row = model2.objects.get(name='item2')
        assert model2_row.name == 'item2'
        assert model2_row.model1.id == model1_row.id

    def test_get_partial(self):
        """Asserts that when doing a upsert_and_get with
        only part of the columns on the model, all fields
        are returned properly."""

        model = get_fake_model({
            'title': models.CharField(max_length=140, unique=True),
            'purpose': models.CharField(max_length=10, null=True),
            'created_at': models.DateTimeField(auto_now_add=True),
            'updated_at': models.DateTimeField(auto_now=True),
        })

        obj1 = model.objects.upsert_and_get(
            conflict_target=['title'],
            fields=dict(
                title='beer',
                purpose='for-sale'
            )
        )

        obj2 = model.objects.upsert_and_get(
            conflict_target=['title'],
            fields=dict(title='beer')
        )

        assert obj2.title == obj1.title
        assert obj2.purpose == obj1.purpose
        assert obj2.created_at == obj2.created_at
        assert obj2.updated_at != obj1.updated_at

    def test_invalid_conflict_target(self):
        """Tests whether specifying a invalid value
        for `conflict_target` raises an error."""

        model = get_fake_model({
            'title': models.CharField(max_length=140, unique=True)
        })

        with self.assertRaises(SuspiciousOperation):
            model.objects.upsert(
                conflict_target='cookie',
                fields=dict(
                    title='beer'
                )
            )

        with self.assertRaises(SuspiciousOperation):
            model.objects.upsert(
                conflict_target=[None],
                fields=dict(
                    title='beer'
                )
            )
