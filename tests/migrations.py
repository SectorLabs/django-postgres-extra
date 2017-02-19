from typing import List
from unittest import mock
from contextlib import contextmanager

from django.db import connection, migrations
from django.apps import apps
from django.db.migrations.executor import MigrationExecutor
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from .fake_model import define_fake_model


@contextmanager
def migration_test(*filters: List[str]):
    """Assists in testing the custom back-end
    during a migration.

    Arguments:
        filters:
            List of strings to filter SQL
            statements on.
    """

    with connection.schema_editor() as schema_editor:
        wrapper_for = schema_editor.execute
        with mock.patch.object(BaseDatabaseSchemaEditor, 'execute', wraps=wrapper_for) as execute:
            filter_results = {}
            yield schema_editor, filter_results

    for filter_text in filters:
        filter_results[filter_text] = [
            call for call in execute.mock_calls
            if filter_text in str(call)
        ]


@contextmanager
def create_drop_model(field, filters: List[str]):
    """Creates and drops a model with the specified field.

    Arguments:
        field:
            The field to include on the
            model to create and drop.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model()

    class CreateDropModelMigration(migrations.Migration):
        operations = [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', field)
                ]
            ),
            migrations.DeleteModel(
                model.__name__,
            )
        ]

    project = migrations.state.ProjectState.from_apps(apps)

    with migration_test(*filters) as (schema_editor, calls):
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, CreateDropModelMigration('eh', 'postgres_extra'))

    yield calls


@contextmanager
def add_field(field, filters: List[str]):
    """Adds the specified field to a model.

    Arguments:
        field:
            The field to add to a model.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model()

    class CreateModelMigration(migrations.Migration):
        operations = [
            migrations.CreateModel(
                model.__name__,
                fields=[]
            )
        ]

    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, CreateModelMigration('eh', 'postgres_extra'))

    class AddFieldMigration(migrations.Migration):
        operations = [
            migrations.AddField(
                model.__name__,
                'title',
                field
            )
        ]

    with migration_test(*filters) as (schema_editor, calls):
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, AddFieldMigration('eh', 'postgres_extra'))

    yield calls


@contextmanager
def remove_field(field, filters: List[str]):
    """Removes the specified field from a model.

    Arguments:
        field:
            The field to remove from a model.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': field})

    class CreateModelMigration(migrations.Migration):
        operations = [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', field.clone())
                ]
            )
        ]

    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, CreateModelMigration('eh', 'postgres_extra'))

    class RemoveFieldMigration(migrations.Migration):
        operations = [
            migrations.RemoveField(
                model.__name__,
                'title'
            )
        ]

    with migration_test(*filters) as (schema_editor, calls):
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, RemoveFieldMigration('eh', 'postgres_extra'))

    yield calls


@contextmanager
def alter_field(old_field, new_field, filters: List[str]):
    """Alters a field from one state to the other.

    Arguments:
        old_field:
            The field before altering it.

        new_field:
            The field after altering it.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': old_field})

    class CreateModelMigration(migrations.Migration):
        operations = [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', old_field.clone())
                ]
            )
        ]

    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, CreateModelMigration('eh', 'postgres_extra'))

    class AlterFieldMigration(migrations.Migration):
        operations = [
            migrations.AlterField(
                model.__name__,
                'title',
                new_field
            )
        ]

    with migration_test(*filters) as (schema_editor, calls):
        executor = MigrationExecutor(schema_editor.connection)
        executor.apply_migration(
            project, AlterFieldMigration('eh', 'postgres_extra'))

    yield calls
