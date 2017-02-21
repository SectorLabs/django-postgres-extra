from django.test import TestCase

from psqlextra import HStoreField

from .fake_model import get_fake_model
from django.db import models
import pytest


@pytest.mark.django_db
class UpsertTest(TestCase):
    """Tests whether the :see:upsert functionality works correctly."""

    def test_simple(self):
        model = get_fake_model({
            'title': HStoreField(uniqueness=['key1']),
            'cookies': models.CharField(max_length=255, null=True)
        })

        obj = model.objects.upsert(
            title={'key1': 'beer'},
            cookies='cheers'
        )

        obj1 = model.objects.upsert_and_get(
            title={'key1': 'beer'}
        )

        assert obj1.title['key1'] == 'beer'
        assert obj1.cookies == 'cheers'

    def test_auto_fields(self):
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
