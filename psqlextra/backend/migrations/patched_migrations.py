import threading

from contextlib import contextmanager
from enum import IntEnum

import psycopg2

from django.conf import settings
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations import Migration
from django.db.migrations.state import ProjectState

import _thread

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


class ImproperConfigurationException(Exception):
    pass


class MonitoredMigration(Migration):
    @staticmethod
    def _cancel_python():
        """Cancel the currently running python operation."""
        _thread.interrupt_main()

    @staticmethod
    def _close_sql_bad(connection):
        """Force close the connection to the postgres database."""
        if connection.is_usable():
            connection.inc_thread_sharing()
            connection.close()
            connection.dec_thread_sharing()

    @staticmethod
    def _execute_pg_function_on_pid(
        backend_function: str, pid: int, credentials: dict
    ):
        if pid is not None:
            with psycopg2.connect(
                dbname=credentials["database"],
                **{
                    key: value
                    for key, value in credentials.items()
                    if key in ["user", "password"]
                },
            ) as new_connection:
                with new_connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                                SELECT pg_{backend_function}({pid});
                                """
                    )
                    row = cursor.fetchone()
                    if row is not None:
                        return row[0]

    @staticmethod
    def _close_sql(*args):
        """Cancel the currently running sql operation on a pid on the database
        by using a separate connection to the database."""
        return MonitoredMigration._execute_pg_function_on_pid(
            "terminate_backend", *args
        )

    @staticmethod
    def _cancel_sql(*args):
        """Cancel the currently running sql operation on a pid on the database
        by using a separate connection to the database."""
        return MonitoredMigration._execute_pg_function_on_pid(
            "cancel_backend", *args
        )

    @staticmethod
    def get_connection_pid(connection=None):
        """Returns the pid from the postgresql database which runs the current
        transaction."""
        if connection is not None:
            if connection.is_usable():
                with connection.cursor() as cursor:
                    cursor.execute("select pg_backend_pid();")
                    row = cursor.fetchone()
                    if row:
                        return row[0]

    def _should_activate_timer(self):
        raise NotImplementedError

    def _before_start(self, schema_editor: BaseDatabaseSchemaEditor):
        raise NotImplementedError

    def _start(self):
        raise NotImplementedError

    def _stop(self):
        raise NotImplementedError

    def _operation_wrapper(
        self,
        func,
        project_state: ProjectState,
        schema_editor: BaseDatabaseSchemaEditor,
        collect_sql: bool,
    ):
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
            return self._operation_wrapper(
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
            return self._operation_wrapper(
                super().unapply, project_state, schema_editor, collect_sql
            )
        return super().unapply(project_state, schema_editor, collect_sql)


class MigrationWithTimeout(MonitoredMigration):
    class CancellationMethods(IntEnum):
        SQL = 0
        PYTHON = 1
        BOTH = 2

    timeout = getattr(
        settings, "POSTGRES_EXTRA_MIGRATION_DEFAULT_TIMEOUT", None
    )
    cancellation_method = CancellationMethods.SQL
    safe_sql_interrupt = True

    def __init__(self, name: str, app_label: str):
        super().__init__(name, app_label)
        self.requires_sql = [
            self.CancellationMethods.SQL,
            self.CancellationMethods.BOTH,
        ]
        self.requires_python = [
            self.CancellationMethods.PYTHON,
            self.CancellationMethods.BOTH,
        ]
        self.migration_timer = None

    def _cancel(self, connection_pid: int, credentials: dict):
        if self.cancellation_method in self.requires_python:
            self._cancel_python()
        if self.cancellation_method in self.requires_sql:
            if self.safe_sql_interrupt:
                self._cancel_sql(connection_pid, credentials)
            else:
                self._close_sql(connection_pid, credentials)

    def _before_start(self, schema_editor):
        connection_pid = None
        credentials = None
        if self.cancellation_method in self.requires_sql:
            connection_pid = self.get_connection_pid(schema_editor.connection)
            credentials = schema_editor.connection.get_connection_params()

        self.migration_timer = threading.Timer(
            self.timeout, self._cancel, args=(connection_pid, credentials)
        )

    def _start(self):
        self.migration_timer.start()

    def _stop(self):
        self.migration_timer.cancel()

    def _should_activate_timer(self):
        return self.timeout is not None


class MigrationWithConfigurableTimeout(MonitoredMigration):
    class Operations(IntEnum):
        CANCEL_SQL = 0
        CLOSE_SQL = 1
        CANCEL_PYTHON = 2
        ACTIVATE_CALLBACK = 3

    config = {}
    on_start = []

    def __init__(self, name, app_label):
        super().__init__(name, app_label)

    def _monitor(self, action_id, func, *args):
        try:
            func(*args)
        finally:
            if "triggers" in self.config[action_id]:
                self.config[self.config[action_id]["triggers"]]["timer"].start()

    def _check(self):
        if len(self.on_start) == 0 and len(self.config) > 0:
            raise ImproperConfigurationException(
                f"Timers were found but none are set to start!"
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
            if "timeout" not in self.config[action_id]:
                raise ImproperConfigurationException(
                    f"{action_id} does not have a timeout!"
                )
            elif self.config[action_id]["timeout"] is None or (
                not isinstance(self.config[action_id]["timeout"], float)
                and not isinstance(self.config[action_id]["timeout"], int)
            ):
                raise ImproperConfigurationException(
                    f"{action_id} does not have a valid timeout type!"
                )
            elif self.config[action_id]["timeout"] <= 0:
                raise ImproperConfigurationException(
                    f"{action_id} does not have a valid timeout value (> 0)"
                )

            if "operation" not in self.config[action_id]:
                raise ImproperConfigurationException(
                    f"{action_id} does not have an operation set!"
                )
            elif self.config[action_id]["operation"] not in self.Operations:
                raise ImproperConfigurationException(
                    f"{action_id} does not have a valid operation!"
                )
            elif (
                self.config[action_id]["operation"]
                == self.Operations.ACTIVATE_CALLBACK
            ):
                if "args" not in self.config[action_id]:
                    raise ImproperConfigurationException(
                        f"{action_id} is missing arguments for callback!"
                    )
                elif not isinstance(
                    self.config[action_id]["args"], tuple
                ) and not isinstance(self.config[action_id]["args"], list):
                    raise ImproperConfigurationException(
                        f"{action_id} does not have a valid list/tuple of args"
                    )
                elif len(self.config[action_id]["args"]) == 0 or not callable(
                    self.config[action_id]["args"][0]
                ):
                    raise ImproperConfigurationException(
                        f"{action_id} does not have a callable function as first argument!"
                    )

            if "triggers" in self.config[action_id]:
                if self.config[action_id]["triggers"] not in self.config:
                    raise ImproperConfigurationException(
                        f"The timer triggered by {action_id} does not exist!"
                    )

    def _before_start(self, schema_editor: BaseDatabaseSchemaEditor):
        self._check()

        connection_pid = self.get_connection_pid(schema_editor.connection)
        credentials = schema_editor.connection.get_connection_params()

        args = {
            self.Operations.CANCEL_SQL: (
                self._cancel_sql,
                connection_pid,
                credentials,
            ),
            self.Operations.CANCEL_PYTHON: (self._cancel_python,),
            self.Operations.CLOSE_SQL: (
                self._close_sql,
                connection_pid,
                credentials,
            ),
        }

        for action_id in self.config:
            if (
                self.config[action_id]["operation"]
                != self.Operations.ACTIVATE_CALLBACK
            ):
                self.config[action_id]["timer"] = threading.Timer(
                    self.config[action_id]["timeout"],
                    function=self._monitor,
                    args=(action_id,)
                    + args[self.config[action_id]["operation"]],
                )
            else:
                self.config[action_id]["timer"] = threading.Timer(
                    self.config[action_id]["timeout"],
                    function=self._monitor,
                    args=(action_id,) + tuple(self.config[action_id]["args"]),
                )

    def _start(self):
        for action_id in self.on_start:
            self.config[action_id]["timer"].start()

    def _stop(self):
        for action_id in self.config:
            self.config[action_id]["timer"].cancel()

    def _should_activate_timer(self):
        return len(self.config) > 0
