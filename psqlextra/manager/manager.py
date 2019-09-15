from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.models import Manager

from psqlextra.query import PostgresQuerySet


class PostgresManager(Manager.from_queryset(PostgresQuerySet)):
    """Adds support for PostgreSQL specifics."""

    use_in_migrations = True

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:PostgresManager."""

        super().__init__(*args, **kwargs)

        # make sure our back-end is set and refuse to proceed
        # if it's not set
        db_backend = settings.DATABASES["default"]["ENGINE"]
        if "psqlextra" not in db_backend:
            raise ImproperlyConfigured(
                (
                    "'%s' is not the 'psqlextra.backend'. "
                    "django-postgres-extra cannot function without "
                    "the 'psqlextra.backend'. Set DATABASES.ENGINE."
                )
                % db_backend
            )
