import importlib

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.postgresql.base import \
    DatabaseWrapper as Psycopg2DatabaseWrapper

from .hstore_unique import HStoreUniqueSchemaEditorMixin
from .hstore_required import HStoreRequiredSchemaEditorMixin


def _get_backend_base():
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
        'POSTGRES_EXTRA_DB_BACKEND_BASE',
        'django.db.backends.postgresql'
    )

    base_class_module = importlib.import_module(base_class_name + '.base')
    base_class = getattr(base_class_module, 'DatabaseWrapper', None)

    if not base_class:
        raise ImproperlyConfigured((
            '\'%s\' is not a valid database back-end.'
            ' The module does not define a DatabaseWrapper class.'
            ' Check the value of POSTGRES_EXTRA_DB_BACKEND_BASE.'
        ) % base_class_name)

    if isinstance(base_class, Psycopg2DatabaseWrapper):
        raise ImproperlyConfigured((
            '\'%s\' is not a valid database back-end.'
            ' It does inherit from the PostgreSQL back-end.'
            ' Check the value of POSTGRES_EXTRA_DB_BACKEND_BASE.'
        ) % base_class_name)

    return base_class


def _get_schema_editor_base():
    """Gets the base class for the schema editor.

    We have to use the configured base back-end's
    schema editor for this."""

    return _get_backend_base().SchemaEditorClass


class SchemaEditor(_get_schema_editor_base()):
    """Custom schema editor, see mixins for implementation."""

    post_processing_mixins = [
        HStoreUniqueSchemaEditorMixin(),
        HStoreRequiredSchemaEditorMixin()
    ]

    def __init__(self, connection, collect_sql=False, atomic=True):
        super(SchemaEditor, self).__init__(
            connection, collect_sql, atomic)

        self.base = super(SchemaEditor, self)

        for mixin in self.post_processing_mixins:
            mixin.execute = self.execute
            mixin.quote_name = self.quote_name

    def create_model(self, model):
        """Ran when a new model is created."""

        super().create_model(model)

        for mixin in self.post_processing_mixins:
            mixin.create_model(model)

    def delete_model(self, model):
        """Ran when a model is being deleted."""

        for mixin in self.post_processing_mixins:
            mixin.delete_model(model)

        super().delete_model(model)

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Ran when the name of a model is changed."""

        super(SchemaEditor, self).alter_db_table(
            model, old_db_table, new_db_table
        )

        for mixin in self.post_processing_mixins:
            mixin.alter_db_table(
                model,
                old_db_table,
                new_db_table
            )

    def add_field(self, model, field):
        """Ran when a field is added to a model."""

        super(SchemaEditor, self).add_field(model, field)

        for mixin in self.post_processing_mixins:
            mixin.add_field(model, field)

    def remove_field(self, model, field):
        """Ran when a field is removed from a model."""

        for mixin in self.post_processing_mixins:
            mixin.remove_field(model, field)

        super(SchemaEditor, self).remove_field(model, field)

    def alter_field(self, model, old_field, new_field, strict=False):
        """Ran when the configuration on a field changed."""

        super(SchemaEditor, self).alter_field(
            model, old_field, new_field, strict
        )

        for mixin in self.post_processing_mixins:
            mixin.alter_field(
                model, old_field, new_field, strict
            )


class DatabaseWrapper(_get_backend_base()):
    """Wraps the standard PostgreSQL database back-end.

    Overrides the schema editor with our custom
    schema editor and makes sure the `hstore`
    extension is enabled."""

    SchemaEditorClass = SchemaEditor

    def prepare_database(self):
        """Ran to prepare the configured database.

        This is where we enable the `hstore` extension
        if it wasn't enabled yet."""

        super().prepare_database()
        with self.cursor() as cursor:
            cursor.execute('CREATE EXTENSION IF NOT EXISTS hstore')
