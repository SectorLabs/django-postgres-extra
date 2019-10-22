from contextlib import contextmanager
from typing import List
from unittest import mock

from django.apps import apps
from django.db import connection, migrations
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.questioner import NonInteractiveMigrationQuestioner
from django.db.migrations.state import ProjectState

from psqlextra.backend.schema import PostgresSchemaEditor

from .fake_model import define_fake_model


@contextmanager
def filtered_schema_editor(*filters: List[str]):
    """Gets a schema editor, but filters executed SQL statements based on the
    specified text filters.

    Arguments:
        filters:
            List of strings to filter SQL
            statements on.
    """

    with connection.schema_editor() as schema_editor:
        wrapper_for = schema_editor.execute
        with mock.patch.object(
            PostgresSchemaEditor, "execute", wraps=wrapper_for
        ) as execute:
            filter_results = {}
            yield filter_results

    for filter_text in filters:
        filter_results[filter_text] = [
            call for call in execute.mock_calls if filter_text in str(call)
        ]


def apply_migration(operations, state=None, backwards: bool = False):
    """Executes the specified migration operations using the specified schema
    editor.

    Arguments:
        operations:
            The migration operations to execute.

        state:
            The state state to use during the
            migrations.

        backwards:
            Whether to apply the operations
            in reverse (backwards).
    """

    state = state or migrations.state.ProjectState.from_apps(apps)

    class Migration(migrations.Migration):
        pass

    Migration.operations = operations

    migration = Migration("migration", "tests")
    executor = MigrationExecutor(connection)

    if not backwards:
        executor.apply_migration(state, migration)
    else:
        executor.unapply_migration(state, migration)

    return migration


def make_migration(app_label="tests", from_state=None, to_state=None):
    """Generates migrations based on the specified app's state."""

    app_labels = [app_label]

    loader = MigrationLoader(None, ignore_no_migrations=True)
    loader.check_consistent_history(connection)

    questioner = NonInteractiveMigrationQuestioner(
        specified_apps=app_labels, dry_run=False
    )

    autodetector = MigrationAutodetector(
        from_state or loader.project_state(),
        to_state or ProjectState.from_apps(apps),
        questioner,
    )

    changes = autodetector.changes(
        graph=loader.graph,
        trim_to_apps=app_labels or None,
        convert_apps=app_labels or None,
        migration_name="test",
    )

    changes_for_app = changes.get(app_label)
    if not changes_for_app or len(changes_for_app) == 0:
        return None

    return changes_for_app[0]


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

    model = define_fake_model({"title": field})

    with filtered_schema_editor(*filters) as calls:
        apply_migration(
            [
                migrations.CreateModel(
                    model.__name__, fields=[("title", field.clone())]
                ),
                migrations.DeleteModel(model.__name__),
            ]
        )

    yield calls


@contextmanager
def alter_db_table(field, filters: List[str]):
    """Creates a model with the specified field and then renames the database
    table.

    Arguments:
        field:
            The field to include into the
            model.

        filters:
            List of strings to filter
            SQL statements on.
    """

    model = define_fake_model()
    state = migrations.state.ProjectState.from_apps(apps)

    apply_migration(
        [
            migrations.CreateModel(
                model.__name__, fields=[("title", field.clone())]
            )
        ],
        state,
    )

    with filtered_schema_editor(*filters) as calls:
        apply_migration(
            [migrations.AlterModelTable(model.__name__, "NewTableName")], state
        )

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
    state = migrations.state.ProjectState.from_apps(apps)

    apply_migration([migrations.CreateModel(model.__name__, fields=[])], state)

    with filtered_schema_editor(*filters) as calls:
        apply_migration(
            [migrations.AddField(model.__name__, "title", field)], state
        )

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

    model = define_fake_model({"title": field})
    state = migrations.state.ProjectState.from_apps(apps)

    apply_migration(
        [
            migrations.CreateModel(
                model.__name__, fields=[("title", field.clone())]
            )
        ],
        state,
    )

    with filtered_schema_editor(*filters) as calls:
        apply_migration(
            [migrations.RemoveField(model.__name__, "title")], state
        )

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

    model = define_fake_model({"title": old_field})
    state = migrations.state.ProjectState.from_apps(apps)

    apply_migration(
        [
            migrations.CreateModel(
                model.__name__, fields=[("title", old_field.clone())]
            )
        ],
        state,
    )

    with filtered_schema_editor(*filters) as calls:
        apply_migration(
            [migrations.AlterField(model.__name__, "title", new_field)], state
        )

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

    model = define_fake_model({"title": field})
    state = migrations.state.ProjectState.from_apps(apps)

    apply_migration(
        [
            migrations.CreateModel(
                model.__name__, fields=[("title", field.clone())]
            )
        ],
        state,
    )

    with filtered_schema_editor(*filters) as calls:
        apply_migration(
            [migrations.RenameField(model.__name__, "title", "newtitle")], state
        )

    yield calls
