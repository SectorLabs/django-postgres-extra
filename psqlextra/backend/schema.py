from . import base_impl
from .hstore_required import HStoreRequiredSchemaEditorMixin
from .hstore_unique import HStoreUniqueSchemaEditorMixin


class SchemaEditor(base_impl.schema_editor()):
    """Custom schema editor, see mixins for implementation."""

    post_processing_mixins = [
        HStoreUniqueSchemaEditorMixin(),
        HStoreRequiredSchemaEditorMixin(),
    ]

    def __init__(self, connection, collect_sql=False, atomic=True):
        super(SchemaEditor, self).__init__(connection, collect_sql, atomic)

        self.base = super(SchemaEditor, self)

        for mixin in self.post_processing_mixins:
            mixin.execute = self.execute
            mixin.quote_name = self.quote_name

    def create_model(self, model):
        """Creates a new model."""

        super().create_model(model)

        for mixin in self.post_processing_mixins:
            mixin.create_model(model)

    def delete_model(self, model):
        """Drops/deletes an existing model."""

        for mixin in self.post_processing_mixins:
            mixin.delete_model(model)

        super().delete_model(model)

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Alters a table/model."""

        super(SchemaEditor, self).alter_db_table(
            model, old_db_table, new_db_table
        )

        for mixin in self.post_processing_mixins:
            mixin.alter_db_table(model, old_db_table, new_db_table)

    def add_field(self, model, field):
        """Adds a new field to an exisiting model."""

        super(SchemaEditor, self).add_field(model, field)

        for mixin in self.post_processing_mixins:
            mixin.add_field(model, field)

    def remove_field(self, model, field):
        """Removes a field from an existing model."""

        for mixin in self.post_processing_mixins:
            mixin.remove_field(model, field)

        super(SchemaEditor, self).remove_field(model, field)

    def alter_field(self, model, old_field, new_field, strict=False):
        """Alters an existing field on an existing model."""

        super(SchemaEditor, self).alter_field(
            model, old_field, new_field, strict
        )

        for mixin in self.post_processing_mixins:
            mixin.alter_field(model, old_field, new_field, strict)
