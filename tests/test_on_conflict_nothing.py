from django.db import models

from psqlextra import HStoreField
from psqlextra.query import ConflictAction

from .fake_model import get_fake_model


def test_on_conflict_nothing():
    """Tests whether simple insert NOTHING works correctly."""

    model = get_fake_model({
        'title': HStoreField(uniqueness=['key1']),
        'cookies': models.CharField(max_length=255, null=True)
    })

    obj1 = (
        model.objects
        .on_conflict([('title', 'key1')], ConflictAction.NOTHING)
        .insert_and_get(title={'key1': 'beer'}, cookies='cheers')
    )

    obj1.refresh_from_db()
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'cheers'

    obj2 = (
        model.objects
        .on_conflict([('title', 'key1')], ConflictAction.NOTHING)
        .insert_and_get(title={'key1': 'beer'}, cookies='choco')
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert that the 'cookies' field didn't change
    assert obj1.id == obj2.id
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'cheers'
    assert obj2.title['key1'] == 'beer'
    assert obj2.cookies == 'cheers'
