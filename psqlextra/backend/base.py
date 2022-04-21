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

        # Some base back-ends such as the PostGIS back-end don't properly
        # set `ops_class` and `introspection_class` and initialize these
        # classes themselves.
        #
        # This can lead to broken functionality. We fix this automatically.

        if not isinstance(self.introspection, self.introspection_class):
            self.introspection = self.introspection_class(self)

        if not isinstance(self.ops, self.ops_class):
            self.ops = self.ops_class(self)

        for expected_compiler_class in self.ops.compiler_classes:
            compiler_class = self.ops.compiler(expected_compiler_class.__name__)

            if not issubclass(compiler_class, expected_compiler_class):
                logger.warning(
                    "Compiler '%s.%s' is not properly deriving from '%s.%s'."
                    % (
                        compiler_class.__module__,
                        compiler_class.__name__,
                        expected_compiler_class.__module__,
                        expected_compiler_class.__name__,
                    )
                )

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
