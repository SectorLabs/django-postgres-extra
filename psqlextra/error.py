from typing import Optional

import psycopg2

from django import db


def extract_postgres_error(error: db.Error) -> Optional[psycopg2.Error]:
    """Extracts the underlying :see:psycopg2.Error from the specified Django
    database error.

    As per PEP-249, Django wraps all database errors in its own
    exception. We can extract the underlying database error by examaning
    the cause of the error.
    """

    if not isinstance(error.__cause__, psycopg2.Error):
        return None

    return error.__cause__
