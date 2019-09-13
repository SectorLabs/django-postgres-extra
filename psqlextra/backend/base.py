import logging

from django.db import ProgrammingError

from . import base_impl
from .introspection import PostgresIntrospection
from .schema import PostgresSchemaEditor

logger = logging.getLogger(__name__)


class DatabaseWrapper(base_impl.backend()):
    """Wraps the standard PostgreSQL database back-end.

    Overrides the schema editor with our custom
    schema editor and makes sure the `hstore`
    extension is enabled."""

    SchemaEditorClass = PostgresSchemaEditor
    introspection_class = PostgresIntrospection

    def prepare_database(self):
        """Ran to prepare the configured database.

        This is where we enable the `hstore` extension
        if it wasn't enabled yet."""

        super().prepare_database()
        with self.cursor() as cursor:
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS hstore")
            except ProgrammingError:  # permission denied
                logger.warning(
                    'Failed to create "hstore" extension. '
                    "Tables with hstore columns may fail to migrate. "
                    "If hstore is needed, make sure you are connected "
                    "to the database as a superuser "
                    "or add the extension manually.",
                    exc_info=True,
                )
