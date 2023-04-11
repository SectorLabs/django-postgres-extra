from typing import Optional, Union

from django import db

try:
    from psycopg2 import Error as Psycopg2Error
except ImportError:
    Psycopg2Error = None

try:
    from psycopg import Error as Psycopg3Error
except ImportError:
    Psycopg3Error = None


def extract_postgres_error(
    error: db.Error,
) -> Optional[Union["Psycopg2Error", "Psycopg3Error"]]:
    """Extracts the underlying :see:psycopg2.Error from the specified Django
    database error.

    As per PEP-249, Django wraps all database errors in its own
    exception. We can extract the underlying database error by examaning
    the cause of the error.
    """

    if (Psycopg2Error and not isinstance(error.__cause__, Psycopg2Error)) and (
        Psycopg3Error and not isinstance(error.__cause__, Psycopg3Error)
    ):
        return None

    return error.__cause__


def extract_postgres_error_code(error: db.Error) -> Optional[str]:
    """Extracts the underlying Postgres error code.

    As per PEP-249, Django wraps all database errors in its own
    exception. We can extract the underlying database error by examaning
    the cause of the error.
    """

    cause = error.__cause__
    if not cause:
        return None

    if Psycopg2Error and isinstance(cause, Psycopg2Error):
        return cause.pgcode

    if Psycopg3Error and isinstance(cause, Psycopg3Error):
        return cause.sqlstate

    return None
