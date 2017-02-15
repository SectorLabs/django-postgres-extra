from unittest import mock

from django.db import connection
from django.apps import apps
from django.test import TestCase
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from psqlextra import HStoreField

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
    def test_migration_alter_field(cls):
        """Tests whether the back-end correctly removes and
        adds `uniqueness` constraints when altering a :see:LocalizedField."""

        uniqueness = ['beer', 'more_cookies']
        define_fake_model('ExistingModel', {
            'title': HStoreField(uniqueness=uniqueness)
        })

        app_config = apps.get_app_config('tests')

        with mock.patch.object(BaseDatabaseSchemaEditor, 'execute') as execute:
            with connection.schema_editor() as schema_editor:
                dynmodel = app_config.get_model('ExistingModel')
                schema_editor.alter_field(
                    dynmodel,
                    dynmodel._meta.fields[1],
                    dynmodel._meta.fields[1]
                )

            index_calls = [
                call for call in execute.mock_calls
                if 'INDEX' in str(call) and 'title' in str(call)
            ]

            assert len(index_calls) == len(uniqueness) * 2
