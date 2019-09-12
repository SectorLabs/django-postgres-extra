from . import base_impl
from .side_effects import (
    HStoreRequiredSchemaEditorSideEffect,
    HStoreUniqueSchemaEditorSideEffect,
)


class SchemaEditor(base_impl.schema_editor()):
    """Custom schema editor, see mixins for implementation."""

    side_effects = [
        HStoreUniqueSchemaEditorSideEffect(),
        HStoreRequiredSchemaEditorSideEffect(),
    ]

    def __init__(self, connection, collect_sql=False, atomic=True):
        super(SchemaEditor, self).__init__(connection, collect_sql, atomic)

        self.base = super(SchemaEditor, self)

        for side_effect in self.side_effects:
            side_effect.execute = self.execute
            side_effect.quote_name = self.quote_name

    def create_model(self, model):
        """Creates a new model."""

        super().create_model(model)

        for side_effect in self.side_effects:
            side_effect.create_model(model)

    def delete_model(self, model):
        """Drops/deletes an existing model."""

        for side_effect in self.side_effects:
            side_effect.delete_model(model)

        super().delete_model(model)

    def alter_db_table(self, model, old_db_table, new_db_table):
        """Alters a table/model."""

        super(SchemaEditor, self).alter_db_table(
            model, old_db_table, new_db_table
        )

        for side_effect in self.side_effects:
            side_effect.alter_db_table(model, old_db_table, new_db_table)

    def add_field(self, model, field):
        """Adds a new field to an exisiting model."""

        super(SchemaEditor, self).add_field(model, field)

        for side_effect in self.side_effects:
            side_effect.add_field(model, field)

    def remove_field(self, model, field):
        """Removes a field from an existing model."""

        for side_effect in self.side_effects:
            side_effect.remove_field(model, field)

        super(SchemaEditor, self).remove_field(model, field)

    def alter_field(self, model, old_field, new_field, strict=False):
        """Alters an existing field on an existing model."""

        super(SchemaEditor, self).alter_field(
            model, old_field, new_field, strict
        )

        for side_effect in self.side_effects:
            side_effect.alter_field(model, old_field, new_field, strict)
