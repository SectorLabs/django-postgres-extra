import time

import pytest

from django.apps import apps
from django.db import connection, connections, migrations
from django.db.migrations.executor import MigrationExecutor

from psqlextra.backend.migrations.patched_migrations import (
    ImproperConfigurationException,
    MigrationWithConfigurableTimeout,
)
from tests.migrations import expectation_judge


def apply_patched_migration_with_timeout(
    operations,
    config: dict,
    on_start: list,
    state=None,
    backwards: bool = False,
    connection_name: str = "default",
):
    """Executes the specified migration operations using the specified schema
    editor.

    Arguments:
        operations:
            The migration operations to execute.

        config:
            The configuration for the migration
            process

        on_start:
            The timers which should start automatically

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

    migration = MigrationWithConfigurableTimeout("migration", "tests")

    migration.operations = operations
    migration.config = config
    migration.on_start = on_start

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
    "on_start, config, config_ok",
    [
        ([], {}, True),
        (["a"], {"a": []}, False),  # config for a is not dict
        (["a"], {"a": {}}, False),  # no timeout and no operation
        (
            ["a"],
            {
                "a": {
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON
                }
            },
            False,
        ),  # missing timeout
        (
            ["a"],
            {
                "a": {
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                    "timeout": None,
                }
            },
            False,
        ),  # no timeout
        (
            ["a"],
            {
                "a": {
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                    "timeout": "1",
                }
            },
            False,
        ),  # timeout not float or int
        (
            ["a"],
            {
                "a": {
                    "timeout": 0,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                }
            },
            False,
        ),  # timeout <= 0
        (["a"], {"a": {"timeout": 1}}, False),  # missing operation
        (
            ["a"],
            {"a": {"timeout": 1, "operation": 0}},
            False,
        ),  # invalid operation
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.ACTIVATE_CALLBACK,
                }
            },
            False,
        ),  # no args for callback
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.ACTIVATE_CALLBACK,
                    "args": {},
                }
            },
            False,
        ),  # invalid type for args
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.ACTIVATE_CALLBACK,
                    "args": [],
                }
            },
            False,
        ),  # empty args
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.ACTIVATE_CALLBACK,
                    "args": [1],
                }
            },
            False,
        ),  # first arg not callable
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.ACTIVATE_CALLBACK,
                    "args": [apply_patched_migration_with_timeout],
                }
            },
            True,
        ),
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.ACTIVATE_CALLBACK,
                    "args": (apply_patched_migration_with_timeout,),
                }
            },
            True,
        ),
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                }
            },
            True,
        ),
        (
            ["a"],
            {
                "a": {
                    "timeout": 1.0,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                }
            },
            True,
        ),
        (
            [],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                }
            },
            False,
        ),  # nothing to start
        (
            ["b"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                }
            },
            False,
        ),  # invalid on_start action_id
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                    "triggers": "b",
                }
            },
            False,
        ),  # invalid triggers
        (
            ["a"],
            {
                "a": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                    "triggers": "b",
                },
                "b": {
                    "timeout": 1,
                    "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                },
            },
            True,
        ),
    ],
)
def test_improper_configuration(on_start, config, config_ok):
    def dummy(*args):
        pass

    operations = [migrations.RunPython(dummy)]

    expectation_judge(
        not config_ok,
        apply_patched_migration_with_timeout,
        operations,
        config,
        on_start,
        exception_expected=ImproperConfigurationException,
    )


@pytest.mark.parametrize(
    "timeout, stalling_time, expected_exception, is_on_start",
    [
        (0.25, 0.5, KeyboardInterrupt, True),
        (0.5, 0.25, None, True),
        (0.25, 0.5, None, False),
    ],
)
def test_stop_python(timeout, stalling_time, expected_exception, is_on_start):
    def stall(*args):
        time.sleep(stalling_time)

    python_action_id = "stop_python"

    expectation_judge(
        expected_exception is not None,
        apply_patched_migration_with_timeout,
        [migrations.RunPython(stall)],
        {
            python_action_id: {
                "timeout": timeout,
                "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
            }
        },
        [python_action_id] if is_on_start else [],
        exception_expected=expected_exception,
    )


@pytest.mark.parametrize(
    "layers, expected_exception",
    [(1, KeyboardInterrupt), (2, KeyboardInterrupt), (3, None)],
)
def test_stop_python_twice(layers, expected_exception):
    def stall(*args):
        nonlocal layers
        if layers > 1:
            layers -= 1
            try:
                stall(*args)
            except KeyboardInterrupt:
                pass
        time.sleep(0.4)

    python_action_id1 = "stop_first_python"
    python_action_id2 = "stop_second_python"

    expectation_judge(
        expected_exception is not None,
        apply_patched_migration_with_timeout,
        [migrations.RunPython(stall)],
        {
            python_action_id1: {
                "timeout": 0.1,
                "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
                "triggers": python_action_id2,
            },
            python_action_id2: {
                "timeout": 0.3,  # if this is a small value, two KeyboardInterrupts will be sent to the same 'except'
                "operation": MigrationWithConfigurableTimeout.Operations.CANCEL_PYTHON,
            },
        },
        [python_action_id1],
        exception_expected=expected_exception,
    )
