import logging

from django.conf import settings
from django.db import ProgrammingError

from . import base_impl
from .introspection import PostgresIntrospection
from .operations import PostgresOperations
from .schema import PostgresSchemaEditor

logger = logging.getLogger(__name__)


class DatabaseWrapper(base_impl.backend()):
    """Wraps the standard PostgreSQL database back-end.

    Overrides the schema editor with our custom schema editor and makes
    sure the `hstore` extension is enabled.
    """

    SchemaEditorClass = PostgresSchemaEditor
    introspection_class = PostgresIntrospection
    ops_class = PostgresOperations

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not isinstance(self.ops, PostgresOperations):
            # PostGis replaces the ops object instead of setting the ops_class attribute
            if self.ops.compiler_module != 'django.db.models.sql.compiler':
                raise NotImplementedError(
                    f'''The Django ops object has been replaced by {self.ops} and a custom compiler module {self.ops.compiler_module} has been set.
Replacing both at the same time is incompatible with psqlextra. '''
                )
            self.ops._compiler_cache = None
            self.ops.compiler_module = 'psqlextra.compiler'


def prepare_database(self):
        """Ran to prepare the configured database.

        This is where we enable the `hstore` extension if it wasn't
        enabled yet.
        """

        super().prepare_database()

        setup_ext = getattr(
            settings, "POSTGRES_EXTRA_AUTO_EXTENSION_SET_UP", True
        )
        if not setup_ext:
            return False

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
