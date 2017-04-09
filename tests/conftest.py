import pytest


@pytest.fixture(scope='function', autouse=True)
def database_access(db):
    """Automatically enable database access for all tests."""
