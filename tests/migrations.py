from typing import List
from unittest import mock
from contextlib import contextmanager

from django.db import connection, migrations
from django.apps import apps
from django.db.migrations.executor import MigrationExecutor
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from .fake_model import define_fake_model


@contextmanager
def filtered_schema_editor(*filters: List[str]):
    """Gets a schema editor, but filters executed SQL
    statements based on the specified text filters.

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


def execute_migration(schema_editor, operations, project=None):
    """Executes the specified migration operations
    using the specified schema editor.

    Arguments:
        schema_editor:
            The schema editor to use to
            execute the migrations.

        operations:
            The migration operations to execute.

        project:
            The project state to use during the
            migrations.
    """

    project = project or migrations.state.ProjectState.from_apps(apps)

    class Migration(migrations.Migration):
        pass

    Migration.operations = operations

    executor = MigrationExecutor(schema_editor.connection)
    executor.apply_migration(
        project, Migration('eh', 'postgres_extra'))


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

    model = define_fake_model({'title': field})

    with filtered_schema_editor(*filters) as (schema_editor, calls):
        execute_migration(schema_editor, [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', field.clone())
                ]
            ),
            migrations.DeleteModel(
                model.__name__,
            )
        ])

    yield calls


@contextmanager
def alter_db_table(field, filters: List[str]):
    """Creates a model with the specified field
    and then renames the database table.

    Arguments:
        field:
            The field to include into the
            model.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model()
    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        execute_migration(schema_editor, [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', field.clone())
                ]
            )
        ], project)

    with filtered_schema_editor(*filters) as (schema_editor, calls):
        execute_migration(schema_editor, [
            migrations.AlterModelTable(
                model.__name__,
                'NewTableName'
            )
        ], project)

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
    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        execute_migration(schema_editor, [
            migrations.CreateModel(
                model.__name__,
                fields=[]
            )
        ], project)

    with filtered_schema_editor(*filters) as (schema_editor, calls):
        execute_migration(schema_editor, [
            migrations.AddField(
                model.__name__,
                'title',
                field
            )
        ], project)

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
    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        execute_migration(schema_editor, [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', field.clone())
                ]
            )
        ], project)

    with filtered_schema_editor(*filters) as (schema_editor, calls):
        execute_migration(schema_editor, [
            migrations.RemoveField(
                model.__name__,
                'title'
            )
        ], project)

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
    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        execute_migration(schema_editor, [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', old_field.clone())
                ]
            )
        ], project)

    with filtered_schema_editor(*filters) as (schema_editor, calls):
        execute_migration(schema_editor, [
            migrations.AlterField(
                model.__name__,
                'title',
                new_field
            )
        ], project)

    yield calls


@contextmanager
def rename_field(field, filters: List[str]):
    """Renames a field from one name to the other.

    Arguments:
        field:
            Field to be renamed.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model({'title': field})
    project = migrations.state.ProjectState.from_apps(apps)

    with connection.schema_editor() as schema_editor:
        execute_migration(schema_editor, [
            migrations.CreateModel(
                model.__name__,
                fields=[
                    ('title', field.clone())
                ]
            )
        ], project)

    with filtered_schema_editor(*filters) as (schema_editor, calls):
        execute_migration(schema_editor, [
            migrations.RenameField(
                model.__name__,
                'title',
                'newtitle'
            )
        ], project)

    yield calls
