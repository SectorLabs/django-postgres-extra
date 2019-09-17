import threading
import time

import pytest

from django.db import OperationalError, connection

from psqlextra.backend.migrations.patched_migrations import MonitoredMigration
from tests.migrations import expectation_judge


@pytest.fixture()
def fix_connection():
    yield
    connection.ensure_connection()


def test_get_connection_pid():
    pid = None
    assert connection.is_usable()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT pg_backend_pid()
        """
        )
        pid = cursor.fetchone()[0]

    assert MonitoredMigration.get_connection_pid(connection) == pid


def test_stop_python():
    with pytest.raises(KeyboardInterrupt):
        threading.Timer(0.1, MonitoredMigration._cancel_python).start()
        time.sleep(0.5)


@pytest.mark.parametrize(
    "force, pid, expected_exception, expected_result",
    [
        (False, None, OperationalError, True),
        (True, None, OperationalError, True),
        (False, 1, None, False),
        (True, 1, None, False),
    ],
)
def test_stop_sql(force, pid, expected_exception, expected_result):
    result = None
    credentials = connection.get_connection_params()
    if pid is None:
        pid = MonitoredMigration.get_connection_pid(connection)

    def do_stop():
        nonlocal result
        if force is False:
            result = MonitoredMigration._cancel_sql(pid, credentials)
        else:
            result = MonitoredMigration._close_sql(pid, credentials)

    def code_for_test():
        threading.Timer(0.2, do_stop).start()
        connection.cursor().execute("SELECT pg_sleep(0.5);")

    assert connection.is_usable()
    assert pid is not None
    expectation_judge(
        expected_exception is not None,
        code_for_test,
        exception_expected=expected_exception,
        with_transaction_wrapper=True,
    )

    assert connection.is_usable() == (not (force and expected_result))
    if result is None:
        time.sleep(0.1)
    assert result is expected_result
