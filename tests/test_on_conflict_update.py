import pytest
from django.db import models

from psqlextra.fields import HStoreField
from psqlextra.query import ConflictAction

from .util import get_fake_model


def test_on_conflict_update():
    """Tests whether simple upserts works correctly."""

    model = get_fake_model({
        'title': HStoreField(uniqueness=['key1']),
        'cookies': models.CharField(max_length=255, null=True)
    })

    obj1 = (
        model.objects
        .on_conflict([('title', 'key1')], ConflictAction.UPDATE)
        .insert_and_get(title={'key1': 'beer'}, cookies='cheers')
    )

    obj1.refresh_from_db()
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'cheers'

    obj2 = (
        model.objects
        .on_conflict([('title', 'key1')], ConflictAction.UPDATE)
        .insert_and_get(title={'key1': 'beer'}, cookies='choco')
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert both objects are the same
    assert obj1.id == obj2.id
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'choco'
    assert obj2.title['key1'] == 'beer'
    assert obj2.cookies == 'choco'


def test_on_conflict_update_foreign_key_by_object():
    """
    Tests whether simple upsert works correctly when the conflicting field is a
    foreign key specified as an object.
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
        .on_conflict(['other'], ConflictAction.UPDATE)
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
            .on_conflict(['other'], ConflictAction.UPDATE)
            .insert_and_get(other=obj1)
        )

    obj2 = (
        model.objects
        .on_conflict(['other'], ConflictAction.UPDATE)
        .insert_and_get(other=other_obj, data="different data")
    )

    assert obj2.other == other_obj
    assert obj2.data == "different data"

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert that the 'other' field didn't change
    assert obj1.id == obj2.id
    assert obj1.other == other_obj
    assert obj2.other == other_obj
    assert obj1.data == "different data"
    assert obj2.data == "different data"


def test_on_conflict_update_foreign_key_by_id():
    """
    Tests whether simple upsert works correctly when the conflicting field is a
    foreign key specified as an id.
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
        .on_conflict(['other_id'], ConflictAction.UPDATE)
        .insert_and_get(other_id=other_obj.pk, data="some data")
    )

    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj1.refresh_from_db()
    assert obj1.other == other_obj
    assert obj1.data == "some data"

    obj2 = (
        model.objects
        .on_conflict(['other_id'], ConflictAction.UPDATE)
        .insert_and_get(other_id=other_obj.pk, data="different data")
    )

    assert obj2.other == other_obj
    assert obj2.data == "different data"

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert that the 'other' field didn't change
    assert obj1.id == obj2.id
    assert obj1.other == other_obj
    assert obj2.other == other_obj
    assert obj1.data == "different data"
    assert obj2.data == "different data"
