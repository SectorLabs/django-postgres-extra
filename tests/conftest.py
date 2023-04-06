import pytest

from django.contrib.postgres.signals import register_type_handlers
from django.db import connection

from .fake_model import define_fake_app


@pytest.fixture(scope="function", autouse=True)
def database_access(db):
    """Automatically enable database access for all tests."""

    # enable the hstore extension on our database because
    # our tests rely on it...
    with connection.schema_editor() as schema_editor:
        schema_editor.execute("CREATE EXTENSION IF NOT EXISTS hstore")
        register_type_handlers(schema_editor.connection)


@pytest.fixture
def fake_app():
    """Creates a fake Django app and deletes it at the end of the test."""

    with define_fake_app() as fake_app:
        yield fake_app


@pytest.fixture
def postgres_server_version(db) -> int:
    """Gets the PostgreSQL server version."""

    return connection.cursor().connection.info.server_version


@pytest.fixture(autouse=True)
def _apply_postgres_version_marker(request, postgres_server_version):
    """Skip tests based on Postgres server version number marker condition."""

    marker = request.node.get_closest_marker("postgres_version")
    if not marker:
        return

    lt = marker.kwargs.get("lt")
    if lt and postgres_server_version < lt:
        pytest.skip(
            f"Server version is {postgres_server_version}, the test needs {lt} or newer."
        )
