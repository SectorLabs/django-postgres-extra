from django.test import TestCase

from psqlextra import HStoreField
from psqlextra.models import ConflictAction

from .fake_model import define_fake_model, get_fake_model
from django.db import connection


class OnConflictTest(TestCase):

    def test1(self):
        model = get_fake_model({
            'title': HStoreField(uniqueness=['key1'])
        })

        obj = model()
        obj.title = {'key1': 'beer'}
        obj.save(on_conflict=ConflictAction.Update)
        print('INSERT: %d' % obj.pk)

        obj1 = model()
        obj1.title = {'key1': 'beer'}
        obj1.save(on_conflict=ConflictAction.Update)
        print('UPDATE: %d' % obj.pk)
