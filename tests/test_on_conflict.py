from django.test import TestCase

from psqlextra import HStoreField

from .fake_model import get_fake_model
from django.db import models


class OnConflictTest(TestCase):

    def test1(self):
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
