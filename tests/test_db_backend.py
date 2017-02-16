from unittest import mock
import copy
import uuid

from django.db import connection
from psqlextra import HStoreField
from django.apps import apps
from django.test import TestCase
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from .fake_model import define_fake_model


class DBBackendTestCase(TestCase):
    """Tests the custom database back-end."""

    @staticmethod
    def test_hstore_extension_enabled():
        """Tests whether the `hstore` extension was
        enabled automatically."""

        with connection.cursor() as cursor:
            cursor.execute((
                'SELECT count(*) FROM pg_extension '
                'WHERE extname = \'hstore\''
            ))

            assert cursor.fetchone()[0] == 1

    @classmethod
    def test_migration_create_drop_model(cls):
        """Tests whether models containing a :see:LocalizedField
        with a `uniqueness` constraint get created properly,
        with the contraints in the database."""

        uniqueness = ['beer', 'cookies']
        model = define_fake_model('NewModel', {
            'title': HStoreField(uniqueness=uniqueness)
        })

        # create the model in db and verify the indexes are being created
        with mock.patch.object(BaseDatabaseSchemaEditor, 'execute') as execute:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(model)

            create_index_calls = [
                call for call in execute.mock_calls if 'CREATE UNIQUE INDEX' in str(call)
            ]

            assert len(create_index_calls) == len(uniqueness)

        # delete the model in the db and verify the indexes are being deleted
        with mock.patch.object(BaseDatabaseSchemaEditor, 'execute') as execute:
            with connection.schema_editor() as schema_editor:
                schema_editor.delete_model(model)

            drop_index_calls = [
                call for call in execute.mock_calls if 'DROP INDEX' in str(call)
            ]

            assert len(drop_index_calls) == len(uniqueness)

    @classmethod
    def _alter_field_test(cls, old_unique, new_unique):
        """Creates a field with the specified uniqueness and
        then alters it to the specified uniqueness.

        Arguments:
            old_unique:
                The initial uniqueness.

            new_unique:
                The new uniqueness.

        Returns:
            Tuple of:
                - Calls for creating an index
                - Calls for dropping an index
        """

        model_name = str(uuid.uuid4())
        define_fake_model(model_name, {
            'field1': HStoreField(uniqueness=old_unique)
        })

        app_config = apps.get_app_config('tests')

        with mock.patch.object(BaseDatabaseSchemaEditor, 'execute') as execute:
            with connection.schema_editor() as schema_editor:
                dynmodel = app_config.get_model(model_name)

                new_field = copy.deepcopy(dynmodel._meta.fields[1])
                new_field.uniqueness = new_unique

                schema_editor.alter_field(
                    dynmodel,
                    dynmodel._meta.fields[1],
                    new_field
                )

            create_calls = [
                call for call in execute.mock_calls
                if 'CREATE UNIQUE INDEX' in str(call) and 'field1' in str(call)
            ]

            drop_calls = [
                call for call in execute.mock_calls
                if 'DROP INDEX' in str(call) and 'field1' in str(call)
            ]

        return create_calls, drop_calls

    @classmethod
    def test_alter_field_nothing(cls):
        """Tests whether no indexes are dropped when not
        changing anything in the uniqueness."""

        create_calls, drop_calls = cls._alter_field_test(
            ['beer'],
            ['beer']
        )

        assert len(create_calls) == 0
        assert len(drop_calls) == 0

    @classmethod
    def test_alter_field_add(cls):
        """Tests whether only one index is created when
        adding another key to the uniqueness."""

        create_calls, drop_calls = cls._alter_field_test(
            ['beer'],
            ['beer', 'beer1']
        )

        assert len(create_calls) == 1
        assert len(drop_calls) == 0

    @classmethod
    def test_alter_field_remove(cls):
        """Tests whether one index is dropped when removing
        a key from uniqueness."""

        create_calls, drop_calls = cls._alter_field_test(
            ['beer'],
            []
        )

        assert len(create_calls) == 0
        assert len(drop_calls) == 1

    @classmethod
    def test_alter_field_add_together(cls):
        """Tests whether adding one index is created
        when adding a "unique together"."""

        create_calls, drop_calls = cls._alter_field_test(
            ['beer'],
            ['beer', ('beer1', 'beer2')]
        )

        assert len(create_calls) == 1
        assert len(drop_calls) == 0

    @classmethod
    def test_alter_field_remove_together(cls):
        """Tests whether adding one index is dropped
        when adding a "unique together"."""

        create_calls, drop_calls = cls._alter_field_test(
            [('beer1', 'beer2')],
            []
        )

        assert len(create_calls) == 0
        assert len(drop_calls) == 1
