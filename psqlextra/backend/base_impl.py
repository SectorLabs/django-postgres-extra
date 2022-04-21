import importlib

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections

from django.db.backends.postgresql.base import (  # isort:skip
    DatabaseWrapper as Psycopg2DatabaseWrapper,
)


def base_backend_instance():
    """Gets an instance of the base class for the custom database back-end.

    This should be the Django PostgreSQL back-end. However,
    some people are already using a custom back-end from
    another package. We are nice people and expose an option
    that allows them to configure the back-end we base upon.

    As long as the specified base eventually also has
    the PostgreSQL back-end as a base, then everything should
    work as intended.

    We create an instance to inspect what classes to subclass
    because not all back-ends set properties such as `ops_class`
    properly. The PostGIS back-end is a good example.
    """
    base_class_name = getattr(
        settings,
        "POSTGRES_EXTRA_DB_BACKEND_BASE",
        "django.db.backends.postgresql",
    )

    base_class_module = importlib.import_module(base_class_name + ".base")
    base_class = getattr(base_class_module, "DatabaseWrapper", None)

    if not base_class:
        raise ImproperlyConfigured(
            (
                "'%s' is not a valid database back-end."
                " The module does not define a DatabaseWrapper class."
                " Check the value of POSTGRES_EXTRA_DB_BACKEND_BASE."
            )
            % base_class_name
        )

    if isinstance(base_class, Psycopg2DatabaseWrapper):
        raise ImproperlyConfigured(
            (
                "'%s' is not a valid database back-end."
                " It does inherit from the PostgreSQL back-end."
                " Check the value of POSTGRES_EXTRA_DB_BACKEND_BASE."
            )
            % base_class_name
        )

    base_instance = base_class(connections.databases[DEFAULT_DB_ALIAS])
    if base_instance.connection:
        raise ImproperlyConfigured(
            (
                "'%s' establishes a connection during initialization."
                " This is not expected and can lead to more connections"
                " being established than neccesarry."
            )
            % base_class_name
        )

    return base_instance


def backend():
    """Gets the base class for the database back-end."""

    return base_backend_instance().__class__


def schema_editor():
    """Gets the base class for the schema editor.

    We have to use the configured base back-end's schema editor for
    this.
    """

    return base_backend_instance().SchemaEditorClass


def introspection():
    """Gets the base class for the introspection class.

    We have to use the configured base back-end's introspection class
    for this.
    """

    return base_backend_instance().introspection.__class__


def operations():
    """Gets the base class for the operations class.

    We have to use the configured base back-end's operations class for
    this.
    """

    return base_backend_instance().ops.__class__
