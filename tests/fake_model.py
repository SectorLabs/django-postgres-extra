import uuid

from django.db import connection, migrations
from django.db.migrations.executor import MigrationExecutor
from django.contrib.postgres.operations import HStoreExtension

from psqlextra.models import PostgresModel


def define_fake_model(fields=None):
    name = str(uuid.uuid4()).replace('-', '')[:8]

    attributes = {
        'app_label': 'tests',
        '__module__': __name__,
        '__name__': name
    }

    if fields:
        attributes.update(fields)
    model = type(name, (PostgresModel,), attributes)

    return model


def get_fake_model(fields=None):
    """Creates a fake model to use during unit tests."""

    model = define_fake_model(fields)

    class TestProject:

        def clone(self, *_args, **_kwargs):
            return self

        @property
        def apps(self):
            return self

    class TestMigration(migrations.Migration):
        operations = [HStoreExtension()]

    with connection.schema_editor() as schema_editor:
        migration_executor = MigrationExecutor(schema_editor.connection)
        migration_executor.apply_migration(
            TestProject(), TestMigration('eh', 'postgres_extra'))

        schema_editor.create_model(model)

    return model
