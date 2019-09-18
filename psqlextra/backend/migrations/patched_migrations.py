import signal
import threading
import typing

from abc import ABC
from contextlib import contextmanager
from enum import IntEnum

import psycopg2

from django.conf import settings
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations import Migration
from django.db.migrations.state import ProjectState

from .patched_autodetector import patched_autodetector
from .patched_project_state import patched_project_state


@contextmanager
def postgres_patched_migrations():
    """Patches migration related classes/functions to extend how Django
    generates and applies migrations.

    This adds support for automatically detecting changes in Postgres
    specific models.
    """

    with patched_project_state():
        with patched_autodetector():
            yield


class CancellationActions(IntEnum):
    CANCEL_SQL = 0
    CLOSE_SQL = 1
    CANCEL_PYTHON = 2
    ACTIVATE_CALLBACK = 3
    RAISE_EXCEPTION = 4

    def __str__(self):
        return [
            "CANCEL_SQL",
            "CLOSE_SQL",
            "CANCEL_PYTHON",
            "ACTIVATE_CALLBACK",
            "RAISE_EXCEPTION",
        ][self.value]


class ImproperConfigurationException(Exception):
    """Exception for improper configuration of timeout migrations classes."""


class BackendPidNotFoundException(Exception):
    """The backend pid for a connection was not found."""


class MigrationStillRunningException(Exception):
    """After many actions taken, the migration is still running."""


class MonitoredMigration(Migration, ABC):
    def __init__(self, name, app_label):
        super().__init__(name, app_label)
        self.tid = None

    @staticmethod
    def _cancel_python_thread(tid: int):
        """Cancel the current python action on a particular thread."""
        signal.pthread_kill(tid, signal.SIGINT)

    @staticmethod
    def _execute_pg_function_on_pid(
        backend_function: str, pid: int, credentials: dict
    ) -> typing.Union[bool, None]:
        """Execute a postgres function with pid as a parameter using
        credentials from connecting."""
        with psycopg2.connect(
            dbname=credentials["database"],
            **{
                key: value
                for key, value in credentials.items()
                if key in ["user", "password"]
            },
        ) as new_connection:
            with new_connection.cursor() as cursor:
                cursor.execute(f"SELECT pg_{backend_function}({pid});")
                row = cursor.fetchone()
                if row is not None:
                    return row[0]
        return None

    @staticmethod
    def _close_sql(*args) -> bool:
        """Cancel the currently running sql action on a pid on the database by
        using a separate connection to the database."""
        return MonitoredMigration._execute_pg_function_on_pid(
            "terminate_backend", *args
        )

    @staticmethod
    def _cancel_sql(*args) -> bool:
        """Cancel the currently running sql action on a pid on the database by
        using a separate connection to the database."""
        return MonitoredMigration._execute_pg_function_on_pid(
            "cancel_backend", *args
        )

    @staticmethod
    def get_connection_pid(
        connection: BaseDatabaseWrapper
    ) -> typing.Union[int, None]:
        """Returns the pid from the postgresql database which runs the current
        transaction."""
        if connection and connection.is_usable():
            with connection.cursor() as cursor:
                cursor.execute("select pg_backend_pid();")
                row = cursor.fetchone()
                if row:
                    return row[0]
        return None

    @staticmethod
    def get_connection_data(
        connection: BaseDatabaseWrapper
    ) -> typing.Tuple[int, dict]:
        """Returns the pid and the credentials used by the connection."""
        pid = MonitoredMigration.get_connection_pid(connection)
        if not pid:
            raise BackendPidNotFoundException

        credentials = connection.get_connection_params()
        return pid, credentials

    def _should_activate_timer(self) -> bool:
        """Specify the condition to start the timing procedure inside the
        class."""
        raise NotImplementedError

    def _before_start(self, schema_editor: BaseDatabaseSchemaEditor):
        """Perform checks and raise errors if improperly configured."""
        raise NotImplementedError

    def _start(self):
        """Start monitor threads."""
        raise NotImplementedError

    def _stop(self):
        """Close monitor threads."""
        raise NotImplementedError

    def _set_error_state(self):
        """Raise this when many actions were taken but none succeeded."""
        raise MigrationStillRunningException(
            f"None of the cancelling methods succeeded in time!"
        )

    def _action_wrapper(
        self,
        func,
        project_state: ProjectState,
        schema_editor: BaseDatabaseSchemaEditor,
        collect_sql: bool,
    ) -> ProjectState:
        """Wraps apply or unapply and monitors the migration."""
        self.tid = threading.get_ident()
        self._before_start(schema_editor)
        self._start()
        result = project_state
        try:
            result = func(project_state, schema_editor, collect_sql)
        finally:
            self._stop()
        return result

    def apply(
        self,
        project_state: ProjectState,
        schema_editor: BaseDatabaseSchemaEditor,
        collect_sql: bool = False,
    ) -> ProjectState:
        """Apply a migration within a certain timeout (if timeout exists)"""

        if self._should_activate_timer():
            return self._action_wrapper(
                super().apply, project_state, schema_editor, collect_sql
            )
        return super().apply(project_state, schema_editor, collect_sql)

    def unapply(
        self,
        project_state: ProjectState,
        schema_editor: BaseDatabaseSchemaEditor,
        collect_sql: bool = False,
    ) -> ProjectState:
        """Unapply a migration within a certain timeout (if timeout exists)"""

        if self._should_activate_timer():
            return self._action_wrapper(
                super().unapply, project_state, schema_editor, collect_sql
            )
        return super().unapply(project_state, schema_editor, collect_sql)


class MigrationTimeout(MonitoredMigration):
    """Monitoring class which starts a single action after a certain timeout
    has been reached."""

    timeout = getattr(
        settings, "POSTGRES_EXTRA_MIGRATION_DEFAULT_TIMEOUT", None
    )
    cancellation_action = CancellationActions.CLOSE_SQL

    def __init__(self, name: str, app_label: str):
        super().__init__(name, app_label)
        self.migration_timer = None

    def _cancel(self, connection_pid: int, credentials: dict):
        """Timeout has been reached, cancel with the preferred method."""
        if self.cancellation_action == CancellationActions.CANCEL_PYTHON:
            self._cancel_python_thread(self.tid)
        elif self.cancellation_action == CancellationActions.CANCEL_SQL:
            self._cancel_sql(connection_pid, credentials)
        elif self.cancellation_action == CancellationActions.CLOSE_SQL:
            self._close_sql(connection_pid, credentials)

    def _before_start(self, schema_editor: BaseDatabaseSchemaEditor):
        if self.cancellation_action not in [
            CancellationActions.CANCEL_PYTHON,
            CancellationActions.CANCEL_SQL,
            CancellationActions.CLOSE_SQL,
        ]:
            raise ImproperConfigurationException(
                f"Cannot use action {str(self.cancellation_action)} for this migration timeout subclass!"
            )
        self.migration_timer = threading.Timer(
            self.timeout,
            self._cancel,
            args=self.get_connection_data(schema_editor.connection),
        )

    def _start(self):
        self.migration_timer.start()

    def _stop(self):
        self.migration_timer.cancel()

    def _should_activate_timer(self):
        return self.timeout is not None


class MigrationTimeoutWithConfigurableActions(MonitoredMigration):
    """Supports custom configuration of timers for migrations."""

    def __init__(self, name, app_label):
        super().__init__(name, app_label)
        self.config = {}
        self.on_start = []

    def _timer_function(self, action_id, func: callable, *args, **kwargs):
        """Generic function for actions.

        Starts subsequent timers if any
        """
        try:
            func(*args, **kwargs)
        finally:
            if "triggers" in self.config[action_id]:
                self.config[self.config[action_id]["triggers"]]["timer"].start()

    def _check(self):
        """Check if a configuration is valid and raise appropriate exceptions
        if not."""
        if len(self.on_start) == 0 and len(self.config) > 0:
            raise ImproperConfigurationException(
                "Timers were found but none are set to start!"
            )
        for action_id in self.on_start:
            if action_id not in self.config:
                raise ImproperConfigurationException(
                    f"{action_id} cannot be started because it does not exist!"
                )

        for action_id in self.config:
            if not isinstance(self.config[action_id], dict):
                raise ImproperConfigurationException(
                    f"{action_id} does not have a dictionary for config!"
                )

            if "action" not in self.config[action_id]:
                raise ImproperConfigurationException(
                    f"{action_id} does not have an action set!"
                )
            elif self.config[action_id]["action"] not in CancellationActions:
                raise ImproperConfigurationException(
                    f"{action_id} does not have a valid action!"
                )
            elif (
                self.config[action_id]["action"]
                == CancellationActions.ACTIVATE_CALLBACK
            ):
                if "args" not in self.config[action_id]:
                    raise ImproperConfigurationException(
                        f"{action_id} is missing args for callback!"
                    )
                elif not isinstance(
                    self.config[action_id]["args"], tuple
                ) and not isinstance(self.config[action_id]["args"], list):
                    raise ImproperConfigurationException(
                        f"{action_id} does not have a valid type for args!"
                    )
                elif len(self.config[action_id]["args"]) == 0 or not callable(
                    self.config[action_id]["args"][0]
                ):
                    raise ImproperConfigurationException(
                        f"{action_id} does not have a callable function as first argument!"
                    )
                if "kwargs" in self.config[action_id] and not isinstance(
                    self.config[action_id]["kwargs"], dict
                ):
                    raise ImproperConfigurationException(
                        f"{action_id} does not have a valid type for kwargs!"
                    )

            if "triggers" in self.config[action_id]:
                if self.config[action_id]["triggers"] not in self.config:
                    raise ImproperConfigurationException(
                        f"The timer triggered by {action_id} does not exist!"
                    )

            action = self.config[action_id]["action"]
            if "wait" not in self.config[action_id]:
                raise ImproperConfigurationException(
                    f"{action_id} ({str(action)}) does not have a waiting time!"
                )
            elif not self.config[action_id]["wait"] or (
                not isinstance(self.config[action_id]["wait"], float)
                and not isinstance(self.config[action_id]["wait"], int)
            ):
                raise ImproperConfigurationException(
                    f"{action_id} ({str(action)}) does not have a valid waiting time!"
                )
            elif self.config[action_id]["wait"] <= 0:
                raise ImproperConfigurationException(
                    f"{action_id} ({str(action)}) does not have a valid waiting value (must be > 0)"
                )

    def _before_start(self, schema_editor: BaseDatabaseSchemaEditor):
        self._check()

        connection_pid, credentials = self.get_connection_data(
            schema_editor.connection
        )

        args = {
            CancellationActions.CANCEL_SQL: (
                self._cancel_sql,
                connection_pid,
                credentials,
            ),
            CancellationActions.CANCEL_PYTHON: (
                self._cancel_python_thread,
                self.tid,
            ),
            CancellationActions.CLOSE_SQL: (
                self._close_sql,
                connection_pid,
                credentials,
            ),
            CancellationActions.RAISE_EXCEPTION: (self._set_error_state,),
        }

        for action_id in self.config:
            if (
                self.config[action_id]["action"]
                != CancellationActions.ACTIVATE_CALLBACK
            ):
                self.config[action_id]["timer"] = threading.Timer(
                    self.config[action_id]["wait"],
                    function=self._timer_function,
                    args=(action_id,) + args[self.config[action_id]["action"]],
                )
            else:
                self.config[action_id]["timer"] = threading.Timer(
                    self.config[action_id]["wait"],
                    function=self._timer_function,
                    args=(action_id,) + tuple(self.config[action_id]["args"]),
                    kwargs=self.config[action_id]["kwargs"]
                    if "kwargs" in self.config[action_id]
                    else {},
                )

    def _start(self):
        for action_id in self.on_start:
            self.config[action_id]["timer"].start()

    def _stop(self):
        for action_id in self.config:
            self.config[action_id]["timer"].cancel()

    def _should_activate_timer(self):
        return len(self.config) > 0


class MigrationTimeoutWithChainActions(MigrationTimeoutWithConfigurableActions):
    """Preferred class for out-of-the-box configurations with many actions.

    Receives a simple configuration and wraps it in a way that
    MigrationTimeoutWithConfigurableActions understands it and executes
    it
    """

    cancel_actions = [
        CancellationActions.CANCEL_PYTHON,
        CancellationActions.CLOSE_SQL,
        CancellationActions.ACTIVATE_CALLBACK,
        CancellationActions.RAISE_EXCEPTION,
    ]
    callback = None
    timeout = getattr(
        settings, "POSTGRES_EXTRA_MIGRATION_DEFAULT_TIMEOUT", None
    )
    wait_python = 2
    wait_sql = 1

    def _before_start(self, schema_editor: BaseDatabaseSchemaEditor):
        """Wraps into a configuration which the superclass understands."""
        self.config = {
            f"action{index}": {} for index in range(len(self.cancel_actions))
        }
        for index, action in enumerate(self.cancel_actions):
            """Configuring waiting time."""
            if index == 0:
                self.config[f"action{index}"]["wait"] = self.timeout
            else:
                if (
                    self.cancel_actions[index - 1]
                    == CancellationActions.CANCEL_PYTHON
                ):
                    self.config[f"action{index}"]["wait"] = self.wait_python
                elif self.cancel_actions[index - 1] in [
                    CancellationActions.CANCEL_SQL,
                    CancellationActions.CLOSE_SQL,
                ]:
                    self.config[f"action{index}"]["wait"] = self.wait_sql
                elif self.cancel_actions[index - 1] in [
                    CancellationActions.ACTIVATE_CALLBACK
                ]:
                    self.config[f"action{index}"]["wait"] = self.callback.wait
                else:
                    raise ImproperConfigurationException(
                        "Cannot wait for raise Exception, must be last action"
                    )

            """Configuring actions and order"""
            self.config[f"action{index}"]["action"] = action
            if action == CancellationActions.ACTIVATE_CALLBACK:
                if not self.callback:
                    raise ImproperConfigurationException("Callback was specified as an action but no class was provided")
                self.config[f"action{index}"]["args"] = (
                    self.callback.func,
                ) + tuple(self.callback.args)
                self.config[f"action{index}"]["kwargs"] = self.callback.kwargs
            if index < len(self.cancel_actions) - 1:
                self.config[f"action{index}"]["triggers"] = f"action{index + 1}"
        self.on_start = ["action0"]
        super()._before_start(schema_editor)

    def _should_activate_timer(self):
        """Just using the class should automatically start the timing
        monitors."""
        return True
