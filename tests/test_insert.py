from django.db import models

from psqlextra.query import ConflictAction
from .util import get_fake_model


def test_insert():
    """Tests whether inserts works when the primary key is explicitly specified."""

    model = get_fake_model({
        'cookies': models.CharField(max_length=255, null=True),
    })

    pk = (
        model.objects.all()
        .insert(
            cookies='some-cookies',
        )
    )

    assert pk is not None

    obj1 = model.objects.get()
    assert obj1.pk == pk
    assert obj1.cookies == 'some-cookies'


def test_insert_explicit_pk():
    """Tests whether inserts works when the primary key is explicitly specified."""

    model = get_fake_model({
        'name': models.CharField(max_length=255, primary_key=True),
        'cookies': models.CharField(max_length=255, null=True),
    })

    pk = (
        model.objects.all()
        .insert(
            name='the-object',
            cookies='some-cookies',
        )
    )

    assert pk == 'the-object'

    obj1 = model.objects.get()
    assert obj1.pk == 'the-object'
    assert obj1.name == 'the-object'
    assert obj1.cookies == 'some-cookies'


def test_insert_on_conflict():
    """Tests whether inserts works when a conflict is anticipated."""

    model = get_fake_model({
        'name': models.CharField(max_length=255, unique=True),
        'cookies': models.CharField(max_length=255, null=True),
    })

    pk = (
        model.objects.on_conflict([('pk')], ConflictAction.NOTHING)
        .insert(
            name='the-object',
            cookies='some-cookies',
        )
    )

    assert pk is not None

    obj1 = model.objects.get()
    assert obj1.pk == pk
    assert obj1.name == 'the-object'
    assert obj1.cookies == 'some-cookies'


def test_insert_on_conflict_explicit_pk():
    """
    Tests whether inserts works when a conflict is anticipated and the primary
    key is explicitly specified.
    """

    model = get_fake_model({
        'name': models.CharField(max_length=255, primary_key=True),
        'cookies': models.CharField(max_length=255, null=True),
    })

    pk = (
        model.objects.on_conflict([('name')], ConflictAction.NOTHING)
        .insert(
            name='the-object',
            cookies='some-cookies',
        )
    )

    assert pk == 'the-object'

    obj1 = model.objects.get()
    assert obj1.pk == 'the-object'
    assert obj1.name == 'the-object'
    assert obj1.cookies == 'some-cookies'


def test_insert_with_different_column_name():
    """Tests whether inserts works when the primary key is explicitly specified."""

    model = get_fake_model({
        'name': models.CharField(max_length=255, primary_key=True),
        'cookies': models.CharField(max_length=255, null=True, db_column='brownies'),
    })

    cookie_string = 'these-are-brownies'

    results = model.objects \
        .on_conflict(['name'], ConflictAction.NOTHING) \
        .insert_and_get(
            name='the-object',
            cookies=cookie_string
        )

    assert results is not None
    assert results.cookies == cookie_string

    obj1 = model.objects.get()
    assert obj1.cookies == cookie_string
