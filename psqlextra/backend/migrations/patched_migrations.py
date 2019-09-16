import threading

from contextlib import contextmanager
from enum import Enum

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


class MigrationWithTimeout(Migration):
    class CancellationMethod(Enum):
        SQL = 0
        PYTHON = 1
        BOTH = 2

    timeout = getattr(
        settings, "POSTGRES_EXTRA_MIGRATION_DEFAULT_TIMEOUT", None
    )
    cancellation_method = CancellationMethod.SQL

    def __init__(self, name: str, app_label: str, safe_interrupt: bool = True):
        super().__init__(name, app_label)
        self.safe_interrupt = safe_interrupt

    @staticmethod
    def _close_python_operation():
        _thread.interrupt_main()

    @staticmethod
    def _force_close_sql_operation(schema_editor):
        if schema_editor.connection.is_usable():
            schema_editor.connection.inc_thread_sharing()
            schema_editor.connection.close()
        # TODO: Raise some exception if connection is not usable?

    @staticmethod
    def _close_sql_operation(connection_pid):
        if connection_pid is not None:
            with psycopg2.connect(
                dbname=settings.DATABASES["default"]["NAME"]
            ) as cancelling_connection:
                with cancelling_connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT pg_cancel_backend({connection_pid});
                    """
                    )
        # TODO: Raise some exception if connection_pid is wrong or is None?

    def _force_close_operation(self, schema_editor):
        """Close the connection to the database, forcing the transaction to be
        rolled back on the next connection."""
        if self.cancellation_method in [
            self.CancellationMethod.PYTHON,
            self.CancellationMethod.BOTH,
        ]:
            self._close_python_operation()
        if self.cancellation_method in [
            self.CancellationMethod.SQL,
            self.CancellationMethod.BOTH,
        ]:
            self._force_close_sql_operation(schema_editor)

    def _graceful_close_operation(self, connection_pid: int):
        """Close the current sql operation running without closing the
        connection, django will prompt an error."""
        if self.cancellation_method in [
            self.CancellationMethod.PYTHON,
            self.CancellationMethod.BOTH,
        ]:
            self._close_python_operation()
        if self.cancellation_method in [
            self.CancellationMethod.SQL,
            self.CancellationMethod.BOTH,
        ]:
            self._close_sql_operation(connection_pid)

    @staticmethod
    def get_connection_pid(schema_editor):
        """Returns the pid from the postgresql database which runs the current
        transaction."""
        connection_pid = None
        if schema_editor.connection.is_usable():
            with schema_editor.connection.cursor() as cursor:
                cursor.execute("select pg_backend_pid();")
                connection_pid = cursor.fetchone()[0]
        return connection_pid

    def time_function(
        self,
        func,
        project_state: ProjectState,
        schema_editor: BaseDatabaseSchemaEditor,
        collect_sql: bool,
    ):
        """Starts a timer on a migration function and if not cancelled, aborts
        the operation."""
        migration_timer = None

        if self.safe_interrupt:
            connection_pid = self.get_connection_pid(schema_editor)
            migration_timer = threading.Timer(
                self.timeout,
                self._graceful_close_operation,
                args=(connection_pid,),
            )
        else:
            migration_timer = threading.Timer(
                self.timeout, self._force_close_operation, args=(schema_editor,)
            )

        migration_timer.start()
        result = project_state
        try:
            result = func(project_state, schema_editor, collect_sql)
        finally:
            migration_timer.cancel()
        return result

    def apply(
        self,
        project_state: ProjectState,
        schema_editor: BaseDatabaseSchemaEditor,
        collect_sql: bool = False,
    ) -> ProjectState:
        """Apply a migration within a certain timeout (if timeout exists)"""

        if self.timeout is not None:
            return self.time_function(
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

        if self.timeout is not None:
            return self.time_function(
                super().unapply, project_state, schema_editor, collect_sql
            )
        return super().unapply(project_state, schema_editor, collect_sql)
