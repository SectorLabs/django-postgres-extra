from datetime import timezone

import django

from django.conf import settings

from psqlextra.backend.base import DatabaseWrapper as PSQLExtraDatabaseWrapper


class DatabaseWrapper(PSQLExtraDatabaseWrapper):
    # Works around the compatibility issue of Django <3.0 and psycopg2.9
    # in combination with USE_TZ
    #
    # See: https://github.com/psycopg/psycopg2/issues/1293#issuecomment-862835147
    if django.VERSION < (3, 1):

        def create_cursor(self, name=None):
            cursor = super().create_cursor(name)
            cursor.tzinfo_factory = (
                lambda offset: timezone.utc if settings.USE_TZ else None
            )

            return cursor
