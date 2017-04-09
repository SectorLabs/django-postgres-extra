import pytest
from django.db.utils import IntegrityError

from psqlextra import HStoreField

from . import migrations
from .fake_model import get_fake_model


def test_migration_create_drop_model():
    """Tests whether constraints are properly created
    and dropped when creating and dropping a model."""

    required = ['beer', 'cookies']

    test = migrations.create_drop_model(
        HStoreField(required=required),
        ['ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls['ADD CONSTRAINT']) == len(required)
        assert len(calls['DROP CONSTRAINT']) == len(required)


def test_migration_alter_db_table():
    """Tests whether constraints are renamed properly
    when renaming the database table."""

    test = migrations.alter_db_table(
        HStoreField(required=['beer', 'cookie']),
        ['RENAME CONSTRAINT', 'ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls['RENAME CONSTRAINT']) == 2
        assert len(calls.get('ADD CONSTRAINT', [])) == 0
        assert len(calls.get('DROP CONSTRAINT', [])) == 0


def test_add_field():
    """Tests whether adding a field properly
    creates the constraints."""

    test = migrations.add_field(
        HStoreField(required=['beer']),
        ['ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls.get('ADD CONSTRAINT', [])) == 1
        assert len(calls.get('DROP CONSTRAINT', [])) == 0


def test_remove_field():
    """Tests whether removing a field properly
    removes the constraint."""

    test = migrations.remove_field(
        HStoreField(required=['beer']),
        ['ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls.get('ADD CONSTRAINT', [])) == 0
        assert len(calls.get('DROP CONSTRAINT', [])) == 1


def test_alter_field_nothing():
    """Tests whether no constraints are dropped when not
    changing anything in the required."""

    test = migrations.alter_field(
        HStoreField(required=['beer']),
        HStoreField(required=['beer']),
        ['ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls.get('ADD CONSTRAINT', [])) == 0
        assert len(calls.get('DROP CONSTRAINT', [])) == 0


def test_alter_field_add():
    """Tests whether only one constraint is created when
    adding another key to the required."""

    test = migrations.alter_field(
        HStoreField(required=['beer']),
        HStoreField(required=['beer', 'beer1']),
        ['ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls.get('ADD CONSTRAINT', [])) == 1
        assert len(calls.get('DROP CONSTRAINT', [])) == 0


def test_alter_field_remove():
    """Tests whether one constraint is dropped when removing
    a key from required."""

    test = migrations.alter_field(
        HStoreField(required=['beer']),
        HStoreField(required=[]),
        ['ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls.get('ADD CONSTRAINT', [])) == 0
        assert len(calls.get('DROP CONSTRAINT', [])) == 1


def test_rename_field():
    """Tests whether renaming a field doesn't
    cause the constraint to be re-created."""

    test = migrations.rename_field(
        HStoreField(required=['beer', 'cookies']),
        ['RENAME CONSTRAINT', 'ADD CONSTRAINT', 'DROP CONSTRAINT']
    )

    with test as calls:
        assert len(calls.get('RENAME CONSTRAINT', [])) == 2
        assert len(calls.get('ADD CONSTRAINT', [])) == 0
        assert len(calls.get('DROP CONSTRAINT', [])) == 0


def test_enforcement():
    """Tests whether the constraints are actually
    properly enforced."""

    model = get_fake_model({
        'title': HStoreField(required=['en'])
    })

    with pytest.raises(IntegrityError):
        model.objects.create(title={'ar': 'hello'})
