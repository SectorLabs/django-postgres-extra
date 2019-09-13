import pytest

from django.contrib.postgres.signals import register_type_handlers
from django.db import connection


@pytest.fixture(scope="function", autouse=True)
def database_access(db):
    """Automatically enable database access for all tests."""

    # enable the hstore extension on our database because
    # our tests rely on it...
    with connection.schema_editor() as schema_editor:
        schema_editor.execute("CREATE EXTENSION IF NOT EXISTS hstore")
        register_type_handlers(schema_editor.connection)
