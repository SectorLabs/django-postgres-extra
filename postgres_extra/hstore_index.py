"""This module is unused, but should be contributed to Django."""

from typing import List

from django.db import models


class HStoreIndex(models.Index):
    """Allows creating a index on a specific HStore index.

    Note: pieces of code in this class have been copied
    from the base class. There was no way around this."""

    def __init__(self, field: str, keys: List[str], unique: bool=False,
                 name: str=''):
        """Initializes a new instance of :see:HStoreIndex.

        Arguments:
            field:
                Name of the hstore field for
                which's keys to create a index for.

            keys:
                The name of the hstore keys to
                create the index on.

            unique:
                Whether this index should
                be marked as UNIQUE.

            name:
                The name of the index. If left
                empty, one will be generated.
        """

        self.field = field
        self.keys = keys
        self.unique = unique

        # this will eventually set self.name
        super(HStoreIndex, self).__init__(
            fields=[field],
            name=name
        )

    def get_sql_create_template_values(self, model, schema_editor, using):
        """Gets the values for the SQL template.

        Arguments:
            model:
                The model this index applies to.

            schema_editor:
                The schema editor to modify the schema.

            using:
                Optional: "USING" statement.

        Returns:
            Dictionary of keys to pass into the SQL template.
        """

        fields = [model._meta.get_field(field_name) for field_name, order in self.fields_orders]
        tablespace_sql = schema_editor._get_index_tablespace_sql(model, fields)
        quote_name = schema_editor.quote_name

        columns = [
            '(%s->\'%s\')' % (self.field, key)
            for key in self.keys
        ]

        return {
            'table': quote_name(model._meta.db_table),
            'name': quote_name(self.name),
            'columns': ', '.join(columns),
            'using': using,
            'extra': tablespace_sql,
        }

    def create_sql(self, model, schema_editor, using=''):
        """Gets the SQL to execute when creating the index.

        Arguments:
            model:
                The model this index applies to.

            schema_editor:
                The schema editor to modify the schema.

            using:
                Optional: "USING" statement.

        Returns:
            SQL string to execute to create this index.
        """

        sql_create_index = schema_editor.sql_create_index
        if self.unique:
            sql_create_index = sql_create_index.replace('CREATE', 'CREATE UNIQUE')
        sql_parameters = self.get_sql_create_template_values(model, schema_editor, using)
        return sql_create_index % sql_parameters

    def remove_sql(self, model, schema_editor):
        """Gets the SQL to execute to remove this index.

        Arguments:
            model:
                The model this index applies to.

            schema_editor:
                The schema editor to modify the schema.

        Returns:
            SQL string to execute to remove this index.
        """
        quote_name = schema_editor.quote_name
        return schema_editor.sql_delete_index % {
            'table': quote_name(model._meta.db_table),
            'name': quote_name(self.name),
        }

    def deconstruct(self):
        """Gets the values to pass to :see:__init__ when
        re-creating this object."""

        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        return (path, (), {
            'field': self.field,
            'keys': self.keys,
            'unique': self.unique,
            'name': self.name
        })
