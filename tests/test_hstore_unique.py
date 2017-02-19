from django.test import TestCase

from psqlextra import HStoreField

from . import migrations


class HStoreUniqueTest(TestCase):
    """Tests migrations for uniqueness on
    the :see:HStoreField."""

    @staticmethod
    def test_migration_create_drop_model():
        """Tests whether indexes are properly created
        and dropped when creating and dropping a model."""

        uniqueness = ['beer', 'cookies']

        test = migrations.create_drop_model(
            HStoreField(uniqueness=uniqueness),
            ['CREATE UNIQUE', 'DROP INDEX']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == len(uniqueness)
            assert len(calls['DROP INDEX']) == len(uniqueness)

    @staticmethod
    def test_migration_alter_db_table():
        """Tests whether indexes are renamed properly
        when renaming the database table."""

        test = migrations.alter_db_table(
            HStoreField(uniqueness=['beer', 'cookie']),
            ['ALTER INDEX']
        )

        with test as calls:
            assert len(calls['ALTER INDEX']) == 2

    @staticmethod
    def test_add_field():
        """Tests whether adding a field properly
        creates the indexes."""

        test = migrations.add_field(
            HStoreField(uniqueness=['beer']),
            ['CREATE UNIQUE']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == 1

    @staticmethod
    def test_remove_field():
        """Tests whether removing a field properly
        removes the index."""

        test = migrations.remove_field(
            HStoreField(uniqueness=['beer']),
            ['DROP INDEX']
        )

        with test as calls:
            assert len(calls['DROP INDEX']) == 1

    @staticmethod
    def test_alter_field_nothing():
        """Tests whether no indexes are dropped when not
        changing anything in the uniqueness."""

        test = migrations.alter_field(
            HStoreField(uniqueness=['beer']),
            HStoreField(uniqueness=['beer']),
            ['CREATE UNIQUE', 'DROP INDEX']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == 0
            assert len(calls['DROP INDEX']) == 0

    @staticmethod
    def test_alter_field_add():
        """Tests whether only one index is created when
        adding another key to the uniqueness."""

        test = migrations.alter_field(
            HStoreField(uniqueness=['beer']),
            HStoreField(uniqueness=['beer', 'beer1']),
            ['CREATE UNIQUE', 'DROP INDEX']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == 1
            assert len(calls['DROP INDEX']) == 0

    @staticmethod
    def test_alter_field_remove():
        """Tests whether one index is dropped when removing
        a key from uniqueness."""

        test = migrations.alter_field(
            HStoreField(uniqueness=['beer']),
            HStoreField(uniqueness=[]),
            ['CREATE UNIQUE', 'DROP INDEX']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == 0
            assert len(calls['DROP INDEX']) == 1

    @staticmethod
    def test_alter_field_add_together():
        """Tests whether adding one index is created
        when adding a "unique together"."""

        test = migrations.alter_field(
            HStoreField(uniqueness=['beer']),
            HStoreField(uniqueness=['beer', ('beer1', 'beer2')]),
            ['CREATE UNIQUE', 'DROP INDEX']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == 1
            assert len(calls['DROP INDEX']) == 0

    @staticmethod
    def test_alter_field_remove_together():
        """Tests whether adding one index is dropped
        when adding a "unique together"."""

        test = migrations.alter_field(
            HStoreField(uniqueness=[('beer1', 'beer2')]),
            HStoreField(uniqueness=[]),
            ['CREATE UNIQUE', 'DROP INDEX']
        )

        with test as calls:
            assert len(calls['CREATE UNIQUE']) == 0
            assert len(calls['DROP INDEX']) == 1
