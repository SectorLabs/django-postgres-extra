import importlib

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.models.sql.compiler import \
    SQLInsertCompiler as BaseSQLInsertCompiler
from django.db.backends.postgresql.base import \
    DatabaseWrapper as Psycopg2DatabaseWrapper

from ..query import PostgresInsertQuery
from ..fields import HStoreField
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


def _get_operations_base():
    """Gets the base class for the operations class.

    We have to use the configured base back-end's
    operations class for this."""

    # the latest django version has `ops_class` as a
    # class attribute, similar to `SchemaEditorClass`
    # but, this hasn't been released yet

    return type(_get_backend_base()({}).ops)


class SQLInsertCompiler(BaseSQLInsertCompiler):
    """Compiler for SQL INSERT statements."""

    def as_sql(self):
        """Builds the SQL INSERT statement."""

        queries = super(SQLInsertCompiler, self).as_sql()

        if isinstance(self.query, PostgresInsertQuery):
            queries = [
                (self._rewrite_insert(sql), params)
                for sql, params in queries
            ]

        return queries

    def _rewrite_insert(self, sql):
        """Rewrites a formed SQL INSERT query to include
        the ON CONFLICT clause.

        Arguments:
            sql:
                The SQL INSERT query to rewrite.

        Returns:
            The specified SQL INSERT query rewritten
            to include the ON CONFLICT clause.
        """

        qn = self.connection.ops.quote_name

        # remove the RETURNING part, it will be become part of
        # the ON CONFLICT part
        insert, _ = sql.split(' RETURNING ')

        # ON CONFLICT requires a list of columns to operate on, form
        # a list of columns to pass in
        unique_columns = '(%s)' % ', '.join(self._get_unique_columns())

        # construct a list of columns to update when there's a conflict
        update_columns = ', '.join([
            '{0} = EXCLUDED.{0}'.format(qn(field.column))
            for field in self.query.fields
        ])

        # form the new sql query that does the insert
        new_sql = (
            '{insert} ON CONFLICT ({unique_columns}) '
            'DO UPDATE SET {update_columns} RETURNING id'
        ).format(
            insert=insert,
            unique_columns=unique_columns,
            update_columns=update_columns
        )

        return new_sql

    def _get_unique_columns(self):
        """Gets a list of columns that are marked as 'UNIQUE'.

        This is used in the ON CONFLICT clause. This also
        works for :see:HStoreField."""

        qn = self.connection.ops.quote_name
        unique_columns = []

        for field in self.query.fields:
            if field.unique is True:
                unique_columns.append(qn(field.column))
                continue

            # we must also go into possible tuples since those
            # are used to indicate "unique together"
            if isinstance(field, HStoreField):
                for key in field.uniqueness:
                    if isinstance(key, tuple):
                        for sub_key in key:
                            unique_columns.append(
                                '(%s->\'%s\')' % (qn(field.column), sub_key))
                    else:
                        unique_columns.append(
                            '(%s->\'%s\')' % (qn(field.column), key))

                continue

        return unique_columns


class DatabaseOperations(_get_operations_base()):
    """Custom database operations."""

    def compiler(self, compiler_name):
        """Gets the SQL compiler with the specified name.

        This is nasty. By default, Django looks in
        `compiler_model.[compiler_name]`. In order to
        just override the SQL compiler, we'd have to
        override everything.

        It's easier to just catch the INSERT compiler."""

        if compiler_name == 'SQLInsertCompiler':
            return SQLInsertCompiler

        return super(DatabaseOperations, self).compiler(compiler_name)


class SchemaEditor(_get_schema_editor_base()):
    """Custom schema editor, see mixins for implementation."""

    mixins = [
        HStoreUniqueSchemaEditorMixin(),
        HStoreRequiredSchemaEditorMixin()
    ]

    def __init__(self, connection, collect_sql=False, atomic=True):
        super(SchemaEditor, self).__init__(
            connection, collect_sql, atomic)

        for mixin in self.mixins:
            mixin.execute = self.execute
            mixin.quote_name = self.quote_name

    def create_model(self, model):
        """Ran when a new model is created."""

        super(SchemaEditor, self).create_model(model)

        for mixin in self.mixins:
            mixin.create_model(model)

    def delete_model(self, model):
        """Ran when a model is being deleted."""

        for mixin in self.mixins:
            mixin.delete_model(model)

        super(SchemaEditor, self).delete_model(model)

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Ran when the name of a model is changed."""

        super(SchemaEditor, self).alter_db_table(
            model, old_db_table, new_db_table
        )

        for mixin in self.mixins:
            mixin.alter_db_table(
                model,
                old_db_table,
                new_db_table
            )

    def add_field(self, model, field):
        """Ran when a field is added to a model."""

        super(SchemaEditor, self).add_field(model, field)

        for mixin in self.mixins:
            mixin.add_field(model, field)

    def remove_field(self, model, field):
        """Ran when a field is removed from a model."""

        for mixin in self.mixins:
            mixin.remove_field(model, field)

        super(SchemaEditor, self).remove_field(model, field)

    def alter_field(self, model, old_field, new_field, strict=False):
        """Ran when the configuration on a field changed."""

        super(SchemaEditor, self).alter_field(
            model, old_field, new_field, strict
        )

        for mixin in self.mixins:
            mixin.alter_field(
                model, old_field, new_field, strict
            )


class DatabaseWrapper(_get_backend_base()):
    """Wraps the standard PostgreSQL database back-end.

    Overrides the schema editor with our custom
    schema editor and makes sure the `hstore`
    extension is enabled."""

    SchemaEditorClass = SchemaEditor

    ops_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        """Initializes a new instance of :see:DatabaseWrapper."""

        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        # remove this line once this change is released:
        # https://github.com/django/django/commit/7ca3b391b611eb710c4c1d613e2f672591097a00
        # this allows django itself to initialize self.ops based on ops_class
        self.ops = self.ops_class(self)

    def prepare_database(self):
        """Ran to prepare the configured database.

        This is where we enable the `hstore` extension
        if it wasn't enabled yet."""

        super().prepare_database()
        with self.cursor() as cursor:
            cursor.execute('CREATE EXTENSION IF NOT EXISTS hstore')
