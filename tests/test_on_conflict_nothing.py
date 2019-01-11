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


def test_on_conflict_nothing_foreign_primary_key():
    """
    Tests whether simple insert NOTHING works correctly when the primary key of
    a field is a foreign key with a custom name.
    """

    referenced_model = get_fake_model({})

    model = get_fake_model({
        'parent': models.OneToOneField(
            referenced_model,
            primary_key=True,
            on_delete=models.CASCADE,
        ),
        'cookies': models.CharField(max_length=255),
    })

    referenced_obj = referenced_model.objects.create()

    obj1 = (
        model.objects
        .on_conflict(['parent_id'], ConflictAction.NOTHING)
        .insert_and_get(parent_id=referenced_obj.pk, cookies='cheers')
    )

    obj1.refresh_from_db()
    assert obj1.parent == referenced_obj
    assert obj1.cookies == 'cheers'

    obj2 = (
        model.objects
        .on_conflict(['parent_id'], ConflictAction.NOTHING)
        .insert_and_get(parent_id=referenced_obj.pk, cookies='choco')
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    assert obj1.pk == obj2.pk
    assert obj1.cookies == 'cheers'
    assert obj2.cookies == 'cheers'


def test_on_conflict_nothing_foreign_key_by_object():
    """
    Tests whether simple insert NOTHING works correctly when the potentially
    conflicting field is a foreign key specified as an object.
    """

    other_model = get_fake_model({})

    model = get_fake_model({
        'other': models.OneToOneField(
            other_model,
            on_delete=models.CASCADE,
        ),
        'data': models.CharField(max_length=255),
    })

    other_obj = other_model.objects.create()

    obj1 = (
        model.objects
        .on_conflict(['other'], ConflictAction.NOTHING)
        .insert_and_get(other=other_obj, data="some data")
    )

    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj1.refresh_from_db()
    assert obj1.other == other_obj
    assert obj1.data == "some data"

    with pytest.raises(ValueError):
        (
            model.objects
            .on_conflict(['other'], ConflictAction.NOTHING)
            .insert_and_get(other=obj1)
        )

    obj2 = (
        model.objects
        .on_conflict(['other'], ConflictAction.NOTHING)
        .insert_and_get(other=other_obj, data="different data")
    )

    assert obj2.other == other_obj
    assert obj2.data == "some data"

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert that the 'other' field didn't change
    assert obj1.id == obj2.id
    assert obj1.other == other_obj
    assert obj2.other == other_obj
    assert obj1.data == "some data"
    assert obj2.data == "some data"


def test_on_conflict_nothing_foreign_key_by_id():
    """
    Tests whether simple insert NOTHING works correctly when the potentially
    conflicting field is a foreign key specified as an id.
    """

    other_model = get_fake_model({})

    model = get_fake_model({
        'other': models.OneToOneField(
            other_model,
            on_delete=models.CASCADE,
        ),
        'data': models.CharField(max_length=255),
    })

    other_obj = other_model.objects.create()

    obj1 = (
        model.objects
        .on_conflict(['other_id'], ConflictAction.NOTHING)
        .insert_and_get(other_id=other_obj.pk, data="some data")
    )

    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj1.refresh_from_db()
    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj2 = (
        model.objects
        .on_conflict(['other_id'], ConflictAction.NOTHING)
        .insert_and_get(other_id=other_obj.pk, data="different data")
    )

    assert obj2.other == other_obj
    assert obj2.data == "some data"

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert that the 'other' field didn't change
    assert obj1.id == obj2.id
    assert obj1.other == other_obj
    assert obj2.other == other_obj
    assert obj1.data == "some data"
    assert obj2.data == "some data"
