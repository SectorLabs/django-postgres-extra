import time

import pytest

from attr import dataclass
from django.apps import apps
from django.db import (
    InterfaceError,
    OperationalError,
    connection,
    connections,
    migrations,
)
from django.db.migrations.executor import MigrationExecutor

from psqlextra.backend.migrations.patched_migrations import (
    CancellationActions,
    ImproperConfigurationException,
    MigrationTimeoutWithChainActions,
)
from tests.migrations import expectation_judge


@pytest.fixture()
def fix_connection():
    yield
    connection.ensure_connection()


def apply_patched_migration_with_timeout(
    operations,
    timeout,
    wait_python,
    wait_sql,
    cancel_actions=None,
    callback=None,
    state=None,
    backwards: bool = False,
    connection_name: str = "default",
):
    """Executes the specified migration operations using the specified schema
    editor.

    Arguments:
        operations:
            The migration operations to execute.

        timeout:
            The timeout after which the actions start

        wait_python:
            The time to wait for the cancel_python
            action before moving to the next action

        wait_sql:
            The time to wait for the sql cancel
            action before moving on to the next action

        cancel_actions:
            The actions that are performed to stop the migration

        callback:
            A dataclass object which contains information
            on which function to call when this action is
            reached

        state:
            The state state to use during the
            migrations.

        backwards:
            Whether to apply the operations
            in reverse (backwards).

        connection_name:
            Tell the migration executor on which
            database to perform the migration
    """

    state = state or migrations.state.ProjectState.from_apps(apps)

    migration = MigrationTimeoutWithChainActions("migration", "tests")

    migration.timeout = timeout
    migration.operations = operations
    if cancel_actions:
        migration.cancel_actions = cancel_actions
    migration.wait_python = wait_python
    migration.wait_sql = wait_sql
    migration.callback = callback

    executor = MigrationExecutor(
        connection
        if connection_name == "default"
        else connections[connection_name]
    )

    if not backwards:
        executor.apply_migration(state, migration)
    else:
        executor.unapply_migration(state, migration)

    return migration


@pytest.mark.parametrize(
    "timeout, wait_python, wait_sql, stall_python, stall_sql, exception_expected",
    [
        (0.1, 0.1, 0.2, 1, 1, OperationalError),
        (0.1, 1, 1, 1, 1, OperationalError),
        (0.1, 2, 1, 0.5, 0.1, None),
    ],
)
def test_default_cancel_actions_no_callback(
    timeout, wait_python, wait_sql, stall_python, stall_sql, exception_expected
):
    def stall(*args):
        try:
            time.sleep(stall_python)
        except KeyboardInterrupt:
            pass

    expectation_judge(
        exception_expected is not None,
        apply_patched_migration_with_timeout,
        [
            migrations.RunPython(stall),
            migrations.RunSQL(f"SELECT pg_sleep({stall_sql});"),
        ],
        timeout,
        wait_python,
        wait_sql,
        [CancellationActions.CANCEL_PYTHON, CancellationActions.CLOSE_SQL],
        exception_expected=exception_expected,
    )


@pytest.mark.parametrize(
    "test_args, test_kwargs, expected_val1, expected_val2",
    [([1], {}, 1, None), ([1], {"y": 2}, 1, 2)],
)
def test_callback(test_args, test_kwargs, expected_val1, expected_val2):
    val1 = None
    val2 = None

    def callback(x, y=None):
        nonlocal val1
        nonlocal val2
        val1 = x
        val2 = y

    @dataclass
    class DummyCallback:
        func = callback
        args = test_args
        kwargs = test_kwargs

    expectation_judge(
        False,
        apply_patched_migration_with_timeout,
        [migrations.RunSQL("select pg_sleep(1);")],
        0.1,
        0.1,
        0.1,
        exception_expected=None,
        cancel_actions=[CancellationActions.ACTIVATE_CALLBACK],
        callback=DummyCallback,
    )

    assert val1 == expected_val1
    assert val2 == expected_val2


def test_no_callback_raises_exception():
    expectation_judge(
        True,
        apply_patched_migration_with_timeout,
        [migrations.RunSQL("select pg_sleep(1);")],
        0.1,
        0.1,
        0.1,
        exception_expected=ImproperConfigurationException,
        cancel_actions=[CancellationActions.ACTIVATE_CALLBACK],
        callback=None,
    )
