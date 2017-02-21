from django.db import connection
from django.test import TestCase
import pytest


@pytest.mark.django_db
class DBBackendTestCase(TestCase):
    """Tests the custom database back-end."""

    @staticmethod
    def test_hstore_extension_enabled():
        """Tests whether the `hstore` extension was
        enabled automatically."""

        with connection.cursor() as cursor:
            cursor.execute((
                'SELECT count(*) FROM pg_extension '
                'WHERE extname = \'hstore\''
            ))

            assert cursor.fetchone()[0] == 1
