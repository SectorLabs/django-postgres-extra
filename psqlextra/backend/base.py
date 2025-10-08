import logging

from typing import TYPE_CHECKING

from django import VERSION
from django.conf import settings
from django.contrib.postgres.signals import (
    get_hstore_oids,
    register_type_handlers,
)
from django.db import ProgrammingError

from . import base_impl
from .introspection import PostgresIntrospection
from .operations import PostgresOperations
from .schema import PostgresSchemaEditor

from django.db.backends.postgresql.base import (  # isort:skip
    DatabaseWrapper as PostgresDatabaseWrapper,
)


logger = logging.getLogger(__name__)


if TYPE_CHECKING:

    class Wrapper(PostgresDatabaseWrapper):
        pass

else:
    Wrapper = base_impl.backend()


class DatabaseWrapper(Wrapper):
    """Wraps the standard PostgreSQL database back-end.

    Overrides the schema editor with our custom schema editor and makes
    sure the `hstore` extension is enabled.
    """

    SchemaEditorClass = PostgresSchemaEditor  # type: ignore[assignment]
    introspection_class = PostgresIntrospection
    ops_class = PostgresOperations

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if VERSION >= (5, 0):
            return

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
                return

        # Clear old (non-existent), stale oids.
        get_hstore_oids.cache_clear()

        # Verify that we (and Django) can find the OIDs
        # for hstore.
        oids, _ = get_hstore_oids(self.alias)
        if not oids:
            logger.warning(
                '"hstore" extension was created, but we cannot find the oids'
                "in the database. Something went wrong.",
            )
            return

        # We must trigger Django into registering the type handlers now
        # so that any subsequent code can properly use the newly
        # registered types.
        register_type_handlers(self)
