from django.db import connection


def test_hstore_extension_enabled():
    """Tests whether the `hstore` extension was
    enabled automatically."""

    with connection.cursor() as cursor:
        cursor.execute((
            'SELECT count(*) FROM pg_extension '
            'WHERE extname = \'hstore\''
        ))

        assert cursor.fetchone()[0] == 1
