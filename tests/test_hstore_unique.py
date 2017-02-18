from django.test import TestCase

from psqlextra import HStoreField

from . import migrations


class HStoreUniqueTest(TestCase):
    """Tests the custom database back-end."""

    @classmethod
    def test_migration_create_drop_model(cls):
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

    @classmethod
    def test_alter_field_nothing(cls):
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

    @classmethod
    def test_alter_field_add(cls):
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

    @classmethod
    def test_alter_field_remove(cls):
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

    @classmethod
    def test_alter_field_add_together(cls):
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

    @classmethod
    def test_alter_field_remove_together(cls):
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
