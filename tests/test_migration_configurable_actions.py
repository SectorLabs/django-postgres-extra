import time

import pytest

from django.apps import apps
from django.db import (
    InterfaceError,
    OperationalError,
    connection,
    connections,
    migrations,
    transaction,
)
from django.db.migrations.executor import MigrationExecutor

from psqlextra.backend.migrations.patched_migrations import (
    CancellationActions,
    ImproperConfigurationException,
    MigrationTimeoutWithConfigurableActions,
)
from tests.migrations import expectation_judge


@pytest.fixture()
def fix_connection():
    yield
    connection.ensure_connection()


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

    migration = MigrationTimeoutWithConfigurableActions("migration", "tests")

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
            {"a": {"action": CancellationActions.CANCEL_PYTHON}},
            False,
        ),  # missing timeout
        (
            ["a"],
            {"a": {"action": CancellationActions.CANCEL_PYTHON, "wait": None}},
            False,
        ),  # no timeout
        (
            ["a"],
            {"a": {"action": CancellationActions.CANCEL_PYTHON, "wait": "1"}},
            False,
        ),  # timeout not float or int
        (
            ["a"],
            {"a": {"wait": 0, "action": CancellationActions.CANCEL_PYTHON}},
            False,
        ),  # timeout <= 0
        (["a"], {"a": {"wait": 1}}, False),  # missing operation
        (["a"], {"a": {"wait": 1, "action": 0}}, False),  # invalid operation
        (
            ["a"],
            {"a": {"wait": 1, "action": CancellationActions.ACTIVATE_CALLBACK}},
            False,
        ),  # no args for callback
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": {},
                }
            },
            False,
        ),  # invalid type for args
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": [],
                }
            },
            False,
        ),  # empty args
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": [1],
                }
            },
            False,
        ),  # first arg not callable
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": [apply_patched_migration_with_timeout],
                }
            },
            True,
        ),
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": (apply_patched_migration_with_timeout,),
                }
            },
            True,
        ),
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": (apply_patched_migration_with_timeout,),
                    "kwargs": [],
                }
            },
            False,
        ),  # kwargs invalid type
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.ACTIVATE_CALLBACK,
                    "args": (apply_patched_migration_with_timeout,),
                    "kwargs": {},
                }
            },
            True,
        ),
        (
            ["a"],
            {"a": {"wait": 1, "action": CancellationActions.CANCEL_PYTHON}},
            True,
        ),
        (
            ["a"],
            {"a": {"wait": 1.0, "action": CancellationActions.CANCEL_PYTHON}},
            True,
        ),
        (
            [],
            {"a": {"wait": 1, "action": CancellationActions.CANCEL_PYTHON}},
            False,
        ),  # nothing to start
        (
            ["b"],
            {"a": {"wait": 1, "action": CancellationActions.CANCEL_PYTHON}},
            False,
        ),  # invalid on_start action_id
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.CANCEL_PYTHON,
                    "triggers": "b",
                }
            },
            False,
        ),  # invalid triggers
        (
            ["a"],
            {
                "a": {
                    "wait": 1,
                    "action": CancellationActions.CANCEL_PYTHON,
                    "triggers": "b",
                },
                "b": {"wait": 1, "action": CancellationActions.CANCEL_PYTHON},
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
        (0.25, 0.5, ImproperConfigurationException, False),
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
                "wait": timeout,
                "action": CancellationActions.CANCEL_PYTHON,
            }
        },
        [python_action_id] if is_on_start else [],
        exception_expected=expected_exception,
    )


@pytest.mark.parametrize(
    "action",
    [CancellationActions.CANCEL_PYTHON, CancellationActions.CANCEL_SQL],
)
@pytest.mark.parametrize(
    "layers, actions", [(1, 2), (2, 2), (3, 2), (1, 0), (3, 3), (1, 3)]
)
def test_chain_stop(layers, actions, action):
    no_exception = layers > actions

    def python_stall(*args):
        nonlocal layers
        if layers > 1:
            layers -= 1
            try:
                python_stall(*args)
            except KeyboardInterrupt:
                pass
        time.sleep(0.4)

    def sql_stall(app_name, schema_editor):
        nonlocal layers
        if layers > 1:
            layers -= 1
            try:
                sql_stall(app_name, schema_editor)
            except OperationalError:
                pass
        with schema_editor.connection.cursor() as cursor:
            with transaction.atomic():
                cursor.execute("select pg_sleep(1);")

    action_id_template = "action{}"

    config = {
        action_id_template.format(x + 1): {
            "wait": 0.1 if x == 0 else 0.2,
            "action": action,
            "triggers": action_id_template.format(x + 2),
        }
        for x in range(actions)
    }
    if actions > 0:
        del config[action_id_template.format(actions)]["triggers"]

    expectation_judge(
        not no_exception,
        apply_patched_migration_with_timeout,
        [
            migrations.RunPython(python_stall)
            if action == CancellationActions.CANCEL_PYTHON
            else migrations.RunPython(sql_stall)
        ],
        config,
        [action_id_template.format(1)] if actions > 0 else [],
        exception_expected=KeyboardInterrupt
        if action == CancellationActions.CANCEL_PYTHON
        else OperationalError,
    )


@pytest.mark.parametrize(
    "layers, expected_exception",
    [
        (0, None),
        (1, OperationalError),
        (2, InterfaceError),
        (3, InterfaceError),
    ],
)
def test_chain_force_close_sql(layers, expected_exception, fix_connection):
    def sql_stall(app_name, schema_editor):
        nonlocal layers
        if layers == 0:
            return
        if layers > 1:
            layers -= 1
            try:
                sql_stall(app_name, schema_editor)
            except (OperationalError, InterfaceError):
                pass
        with schema_editor.connection.cursor() as cursor:
            with transaction.atomic():
                cursor.execute("select pg_sleep(1);")

    action = "action"

    config = {action: {"wait": 0.1, "action": CancellationActions.CLOSE_SQL}}

    expectation_judge(
        expected_exception is not None,
        apply_patched_migration_with_timeout,
        [migrations.RunPython(sql_stall)],
        config,
        [action],
        exception_expected=expected_exception,
    )


@pytest.mark.parametrize(
    "timeout, stalling_time, expected_result",
    [(0.25, 0.5, True), (0.5, 0.25, None)],
)
def test_callback(timeout, stalling_time, expected_result):
    result = None

    def change_result(*args):
        nonlocal result
        result = True

    def stall(*args):
        time.sleep(stalling_time)

    python_action_id = "stop_python"

    expectation_judge(
        False,
        apply_patched_migration_with_timeout,
        [migrations.RunPython(stall)],
        {
            python_action_id: {
                "wait": timeout,
                "action": CancellationActions.ACTIVATE_CALLBACK,
                "args": [change_result],
            }
        },
        [python_action_id],
        exception_expected=None,
    )

    assert result == expected_result
