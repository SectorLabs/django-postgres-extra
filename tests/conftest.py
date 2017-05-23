import pytest

import psqlextra


@pytest.fixture(scope='function', autouse=True)
def database_access(db):
    """Automatically enable database access for all tests."""


@pytest.fixture(scope='session', autouse=True)
def setup_psqlextra():
    """Automatically initializes django-postgres-extra before
    running the tests."""

    psqlextra.setup()
