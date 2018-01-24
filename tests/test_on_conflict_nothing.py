import pytest
from django.db import models

from psqlextra.fields import HStoreField
from psqlextra.query import ConflictAction

from .util import get_fake_model


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


def test_on_conflict_nothing_foreign_key():
    """
    Tests whether simple insert NOTHING works correctly when the potentially
    conflicting field is a foreign key.
    """

    other_model = get_fake_model({})

    model = get_fake_model({
        'other': models.OneToOneField(
            other_model,
            on_delete=models.CASCADE,
        ),
    })

    other_obj_1 = other_model.objects.create()
    other_obj_2 = other_model.objects.create()

    obj1 = (
        model.objects
        .on_conflict(['other'], ConflictAction.NOTHING)
        .insert_and_get(other=other_obj_1)
    )

    obj1.refresh_from_db()
    assert obj1.other == other_obj_1

    with pytest.raises(ValueError):
        (
            model.objects
            .on_conflict(['other'], ConflictAction.NOTHING)
            .insert_and_get(other=obj1)
        )

    obj2 = (
        model.objects
        .on_conflict(['other'], ConflictAction.NOTHING)
        .insert_and_get(other=other_obj_2)
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert that the 'other' field didn't change
    assert obj1.id == obj2.id
    assert obj1.other == other_obj_1
    assert obj2.other == other_obj_1
