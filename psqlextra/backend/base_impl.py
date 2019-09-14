import importlib

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from django.db.backends.postgresql.base import (  # isort:skip
    DatabaseWrapper as Psycopg2DatabaseWrapper,
)


def backend():
    """Gets the base class for the custom database back-end.

    This should be the Django PostgreSQL back-end. However,
    some people are already using a custom back-end from
    another package. We are nice people and expose an option
    that allows them to configure the back-end we base upon.

    As long as the specified base eventually also has
    the PostgreSQL back-end as a base, then everything should
    work as intended.
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

    return base_class


def schema_editor():
    """Gets the base class for the schema editor.

    We have to use the configured base back-end's schema editor for
    this.
    """

    return backend().SchemaEditorClass


def introspection():
    """Gets the base class for the introspection class.

    We have to use the configured base back-end's introspection class
    for this.
    """

    return backend().introspection_class


def operations():
    """Gets the base class for the operations class.

    We have to use the configured base back-end's operations class for
    this.
    """

    return backend().ops_class
