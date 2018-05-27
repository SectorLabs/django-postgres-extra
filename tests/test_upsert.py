from django.db import models

from psqlextra.fields import HStoreField

from .util import get_fake_model


def test_upsert():
    """Tests whether simple upserts works correctly."""

    model = get_fake_model({
        'title': HStoreField(uniqueness=['key1']),
        'cookies': models.CharField(max_length=255, null=True)
    })

    obj1 = (
        model.objects
        .upsert_and_get(
            conflict_target=[('title', 'key1')],
            fields=dict(
                title={'key1': 'beer'},
                cookies='cheers'
            )
        )
    )

    obj1.refresh_from_db()
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'cheers'

    obj2 = (
        model.objects
        .upsert_and_get(
            conflict_target=[('title', 'key1')],
            fields=dict(
                title={'key1': 'beer'},
                cookies='choco'
            )
        )
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert both objects are the same
    assert obj1.id == obj2.id
    assert obj1.title['key1'] == 'beer'
    assert obj1.cookies == 'choco'
    assert obj2.title['key1'] == 'beer'
    assert obj2.cookies == 'choco'


def test_upsert_explicit_pk():
    """Tests whether upserts works when the primary key is explicitly specified."""

    model = get_fake_model({
        'name': models.CharField(max_length=255, primary_key=True),
        'cookies': models.CharField(max_length=255, null=True),
    })

    obj1 = (
        model.objects
        .upsert_and_get(
            conflict_target=[('name')],
            fields=dict(
                name='the-object',
                cookies='first-cheers',
            )
        )
    )

    obj1.refresh_from_db()
    assert obj1.name == 'the-object'
    assert obj1.cookies == 'first-cheers'

    obj2 = (
        model.objects
        .upsert_and_get(
            conflict_target=[('name')],
            fields=dict(
                name='the-object',
                cookies='second-boo',
            )
        )
    )

    obj1.refresh_from_db()
    obj2.refresh_from_db()

    # assert both objects are the same
    assert obj1.pk == obj2.pk
    assert obj1.name == 'the-object'
    assert obj1.cookies == 'second-boo'
    assert obj2.name == 'the-object'
    assert obj2.cookies == 'second-boo'


def test_upsert_bulk():
    """Tests whether bulk_upsert works properly."""

    model = get_fake_model({
        'first_name': models.CharField(max_length=255, null=True, unique=True),
        'last_name': models.CharField(max_length=255, null=True)
    })

    model.objects.bulk_upsert(
        conflict_target=['first_name'],
        rows=[
            dict(first_name='Swen', last_name='Kooij'),
            dict(first_name='Henk', last_name='Test')
        ]
    )

    row_a = model.objects.get(first_name='Swen')
    row_b = model.objects.get(first_name='Henk')

    model.objects.bulk_upsert(
        conflict_target=['first_name'],
        rows=[
            dict(first_name='Swen', last_name='Test'),
            dict(first_name='Henk', last_name='Kooij')
        ]
    )

    row_a.refresh_from_db()
    assert row_a.last_name == 'Test'

    row_b.refresh_from_db()
    assert row_b.last_name == 'Kooij'


def test_upsert_bulk_no_rows():
    """Tests whether bulk_upsert doesn't crash when specifying
    no rows or a falsy value."""

    model = get_fake_model({
        'name': models.CharField(max_length=255, null=True, unique=True)
    })

    model.objects.bulk_upsert(
        conflict_target=['name'],
        rows=[]
    )

    model.objects.bulk_upsert(
        conflict_target=['name'],
        rows=None
    )
