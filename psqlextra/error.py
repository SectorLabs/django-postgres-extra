from typing import TYPE_CHECKING, Optional, Type, Union, cast

from django import db

if TYPE_CHECKING:
    from psycopg2 import Error as _Psycopg2Error

    Psycopg2Error: Optional[Type[_Psycopg2Error]]

    from psycopg import Error as _Psycopg3Error

    Psycopg3Error: Optional[Type[_Psycopg3Error]]

try:
    from psycopg2 import Error as Psycopg2Error  # type: ignore[no-redef]
except ImportError:
    Psycopg2Error = None  # type: ignore[misc]

try:
    from psycopg import Error as Psycopg3Error  # type: ignore[no-redef]
except ImportError:
    Psycopg3Error = None  # type: ignore[misc]


def extract_postgres_error(
    error: db.Error,
) -> Optional[Union["_Psycopg2Error", "_Psycopg3Error"]]:
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

    return cast(Union["_Psycopg2Error", "_Psycopg3Error"], error.__cause__)


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
