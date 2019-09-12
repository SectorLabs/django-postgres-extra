import copy

from django.core.exceptions import FieldDoesNotExist, ImproperlyConfigured

from psqlextra.partitioning import PartitioningMethod

from . import base_impl
from .side_effects import (
    HStoreRequiredSchemaEditorSideEffect,
    HStoreUniqueSchemaEditorSideEffect,
)


class SchemaEditor(base_impl.schema_editor()):
    """Custom schema editor, see mixins for implementation."""

    sql_partition_by = " PARTITION BY %s (%s)"
    sql_create_partition = (
        "CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)"
    )

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

        self.deferred_sql = []

    def create_model(self, model):
        """Creates a new model."""

        super().create_model(model)

        for side_effect in self.side_effects:
            side_effect.create_model(model)

    def create_partitioned_model(self, model):
        """Creates a new partitioned model."""

        partitioning_method, partitioning_key = self._partitioning_properties_for_model(
            model
        )

        # get the sql statement that django creates for normal
        # table creations..
        sql, params = self._extract_sql(self.create_model, model)

        partitioning_key_sql = ", ".join(
            self.quote_name(field_name) for field_name in partitioning_key
        )

        # create a composite key that includes the partitioning key
        sql = sql.replace(" PRIMARY KEY", "")
        sql = sql[:-1] + ", PRIMARY KEY (%s, %s))" % (
            self.quote_name(model._meta.pk.name),
            partitioning_key_sql,
        )

        # extend the standard CREATE TABLE statement with
        # 'PARTITION BY ...'
        sql += self.sql_partition_by % (
            partitioning_method.upper(),
            partitioning_key_sql,
        )

        self.execute(sql, params)

    def create_model_partition(self, model, name, from_values, to_values):
        """Creates a new partition for the specified partitioned model.

        Arguments:
            model:
                Partitioned model to create a partition for.

            name:
                Name to give to the new partition.

            from_values:
                From values.

            to_values:
                To values.
        """

        # asserts the model is a model set up for partitioning
        self._partitioning_properties_for_model(model)

        # partition name depends on the model name. in other words
        # partition names are namespaced to the table/model names
        partition_name = "%s_%s" % (model._meta.db_table, name)

        sql = self.sql_create_partition % (
            self.quote_name(partition_name),
            self.quote_name(model._meta.db_table),
            "%s",
            "%s",
        )

        self.execute(sql, (from_values, to_values))

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

    def _extract_sql(self, method, *args):
        """Calls the specified method with the specified arguments
        and intercepts the SQL statement it WOULD execute.

        We use this to figure out the exact SQL statement
        Django would execute. We can then make a small modification
        and execute it ourselves."""

        original_execute_func = copy.deepcopy(self.execute)

        intercepted_args = []

        def _intercept(*args):
            intercepted_args.extend(args)

        self.execute = _intercept

        method(*args)

        self.execute = original_execute_func
        return intercepted_args

    @staticmethod
    def _partitioning_properties_for_model(model):
        """Gets the partitioning options for the specified model.

        Raises:
            ImproperlyConfigured:
                When the specified model is not set up
                for partitioning.
        """

        partitioning_method = getattr(model, "partitioning_method", None)
        partitioning_key = getattr(model, "partitioning_key", None)

        if not partitioning_method or not partitioning_key:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Set the `partitioning_method` and `partitioning_key` attributes."
                )
                % model.__name__
            )

        if partitioning_method not in PartitioningMethod:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " '%s' is not a member of the PartitioningMethod enum."
                )
                % (model.__name__, partitioning_method)
            )

        if not isinstance(partitioning_key, list):
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Partitioning key should be a list (of field names or values,"
                    " depending on the partitioning method)."
                )
                % model.__name__
            )

        try:
            for field_name in partitioning_key:
                model._meta.get_field(field_name)
        except FieldDoesNotExist:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Field in partitioning key '%s' is not a valid field on"
                    " the model."
                )
                % (model.__name__, partitioning_key)
            )

        return partitioning_method, partitioning_key
