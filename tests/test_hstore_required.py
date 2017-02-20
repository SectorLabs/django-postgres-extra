from django.test import TestCase
from django.db.utils import IntegrityError

from psqlextra import HStoreField

from . import migrations
from .fake_model import get_fake_model


class HStoreRequiredTest(TestCase):
    """Tests migrations for requiredness on
    the :see:HStoreField."""

    @staticmethod
    def test_migration_create_drop_model():
        """Tests whether indexes are properly created
        and dropped when creating and dropping a model."""

        required = ['beer', 'cookies']

        test = migrations.create_drop_model(
            HStoreField(required=required),
            ['ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls['ADD CONSTRAINT']) == len(required)
            assert len(calls['DROP CONSTRAINT']) == len(required)

    @staticmethod
    def test_migration_alter_db_table():
        """Tests whether indexes are renamed properly
        when renaming the database table."""

        test = migrations.alter_db_table(
            HStoreField(required=['beer', 'cookie']),
            ['RENAME CONSTRAINT', 'ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls['RENAME CONSTRAINT']) == 2
            assert len(calls.get('ADD CONSTRAINT', [])) == 0
            assert len(calls.get('DROP CONSTRAINT', [])) == 0

    @staticmethod
    def test_add_field():
        """Tests whether adding a field properly
        creates the indexes."""

        test = migrations.add_field(
            HStoreField(required=['beer']),
            ['ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls.get('ADD CONSTRAINT', [])) == 1
            assert len(calls.get('DROP CONSTRAINT', [])) == 0

    @staticmethod
    def test_remove_field():
        """Tests whether removing a field properly
        removes the index."""

        test = migrations.remove_field(
            HStoreField(required=['beer']),
            ['ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls.get('ADD CONSTRAINT', [])) == 0
            assert len(calls.get('DROP CONSTRAINT', [])) == 1

    @staticmethod
    def test_alter_field_nothing():
        """Tests whether no indexes are dropped when not
        changing anything in the required."""

        test = migrations.alter_field(
            HStoreField(required=['beer']),
            HStoreField(required=['beer']),
            ['ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls.get('ADD CONSTRAINT', [])) == 0
            assert len(calls.get('DROP CONSTRAINT', [])) == 0

    @staticmethod
    def test_alter_field_add():
        """Tests whether only one index is created when
        adding another key to the required."""

        test = migrations.alter_field(
            HStoreField(required=['beer']),
            HStoreField(required=['beer', 'beer1']),
            ['ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls.get('ADD CONSTRAINT', [])) == 1
            assert len(calls.get('DROP CONSTRAINT', [])) == 0

    @staticmethod
    def test_alter_field_remove():
        """Tests whether one index is dropped when removing
        a key from required."""

        test = migrations.alter_field(
            HStoreField(required=['beer']),
            HStoreField(required=[]),
            ['ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls.get('ADD CONSTRAINT', [])) == 0
            assert len(calls.get('DROP CONSTRAINT', [])) == 1

    @staticmethod
    def test_rename_field():
        """Tests whether renaming a field doesn't
        cause the index to be re-created."""

        test = migrations.rename_field(
            HStoreField(required=['beer', 'cookies']),
            ['RENAME CONSTRAINT', 'ADD CONSTRAINT', 'DROP CONSTRAINT']
        )

        with test as calls:
            assert len(calls.get('RENAME CONSTRAINT', [])) == 2
            assert len(calls.get('ADD CONSTRAINT', [])) == 0
            assert len(calls.get('DROP CONSTRAINT', [])) == 0

    def test_enforcement(self):
        """Tests whether the constraints are actually
        properly enforced."""

        model = get_fake_model({
            'title': HStoreField(required=['en'])
        })

        with self.assertRaises(IntegrityError):
            model.objects.create(title={'ar': 'hello'})
