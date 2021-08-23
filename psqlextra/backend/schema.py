from typing import Any, List, Optional
from unittest import mock

from django.core.exceptions import (
    FieldDoesNotExist,
    ImproperlyConfigured,
    SuspiciousOperation,
)
from django.db import transaction
from django.db.models import Field, Model

from psqlextra.type_assertions import is_sql_with_params
from psqlextra.types import PostgresPartitioningMethod

from . import base_impl
from .introspection import PostgresIntrospection
from .side_effects import (
    HStoreRequiredSchemaEditorSideEffect,
    HStoreUniqueSchemaEditorSideEffect,
)


class PostgresSchemaEditor(base_impl.schema_editor()):
    """Schema editor that adds extra methods for PostgreSQL specific features
    and hooks into existing implementations to add side effects specific to
    PostgreSQL."""

    sql_create_view = "CREATE VIEW %s AS (%s)"
    sql_replace_view = "CREATE OR REPLACE VIEW %s AS (%s)"
    sql_drop_view = "DROP VIEW IF EXISTS %s"
    sql_create_materialized_view = (
        "CREATE MATERIALIZED VIEW %s AS (%s) WITH DATA"
    )
    sql_drop_materialized_view = "DROP MATERIALIZED VIEW %s"
    sql_refresh_materialized_view = "REFRESH MATERIALIZED VIEW %s"
    sql_refresh_materialized_view_concurrently = (
        "REFRESH MATERIALIZED VIEW CONCURRENTLY %s"
    )
    sql_partition_by = " PARTITION BY %s (%s)"
    sql_add_default_partition = "CREATE TABLE %s PARTITION OF %s DEFAULT"
    sql_add_hash_partition = "CREATE TABLE %s PARTITION OF %s FOR VALUES WITH (MODULUS %s, REMAINDER %s)"
    sql_add_range_partition = (
        "CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)"
    )
    sql_add_list_partition = (
        "CREATE TABLE %s PARTITION OF %s FOR VALUES IN (%s)"
    )
    sql_delete_partition = "DROP TABLE %s"
    sql_table_comment = "COMMENT ON TABLE %s IS %s"

    side_effects = [
        HStoreUniqueSchemaEditorSideEffect(),
        HStoreRequiredSchemaEditorSideEffect(),
    ]

    def __init__(self, connection, collect_sql=False, atomic=True):
        super().__init__(connection, collect_sql, atomic)

        for side_effect in self.side_effects:
            side_effect.execute = self.execute
            side_effect.quote_name = self.quote_name

        self.deferred_sql = []
        self.introspection = PostgresIntrospection(self.connection)

    def create_model(self, model: Model) -> None:
        """Creates a new model."""

        super().create_model(model)

        for side_effect in self.side_effects:
            side_effect.create_model(model)

    def delete_model(self, model: Model) -> None:
        """Drops/deletes an existing model."""

        for side_effect in self.side_effects:
            side_effect.delete_model(model)

        super().delete_model(model)

    def refresh_materialized_view_model(
        self, model: Model, concurrently: bool = False
    ) -> None:
        """Refreshes a materialized view."""

        sql_template = (
            self.sql_refresh_materialized_view_concurrently
            if concurrently
            else self.sql_refresh_materialized_view
        )

        sql = sql_template % self.quote_name(model._meta.db_table)
        self.execute(sql)

    def create_view_model(self, model: Model) -> None:
        """Creates a new view model."""

        self._create_view_model(self.sql_create_view, model)

    def replace_view_model(self, model: Model) -> None:
        """Replaces a view model with a newer version.

        This is used to alter the backing query of a view.
        """

        self._create_view_model(self.sql_replace_view, model)

    def delete_view_model(self, model: Model) -> None:
        """Deletes a view model."""

        sql = self.sql_drop_view % self.quote_name(model._meta.db_table)
        self.execute(sql)

    def create_materialized_view_model(self, model: Model) -> None:
        """Creates a new materialized view model."""

        self._create_view_model(self.sql_create_materialized_view, model)

    def replace_materialized_view_model(self, model: Model) -> None:
        """Replaces a materialized view with a newer version.

        This is used to alter the backing query of a materialized view.

        Replacing a materialized view is a lot trickier than a normal view.
        For normal views we can use `CREATE OR REPLACE VIEW`, but for
        materialized views, we have to create the new view, copy all
        indexes and constraints and drop the old one.

        This operation is atomic as it runs in a transaction.
        """

        with self.connection.cursor() as cursor:
            constraints = self.introspection.get_constraints(
                cursor, model._meta.db_table
            )

        with transaction.atomic():
            self.delete_materialized_view_model(model)
            self.create_materialized_view_model(model)

            for constraint_name, constraint_options in constraints.items():
                if not constraint_options["definition"]:
                    raise SuspiciousOperation(
                        "Table %s has a constraint '%s' that no definition could be generated for",
                        (model._meta.db_tabel, constraint_name),
                    )

                self.execute(constraint_options["definition"])

    def delete_materialized_view_model(self, model: Model) -> None:
        """Deletes a materialized view model."""

        sql = self.sql_drop_materialized_view % self.quote_name(
            model._meta.db_table
        )
        self.execute(sql)

    def create_partitioned_model(self, model: Model) -> None:
        """Creates a new partitioned model."""

        meta = self._partitioning_properties_for_model(model)

        # get the sql statement that django creates for normal
        # table creations..
        sql, params = self._extract_sql(self.create_model, model)

        partitioning_key_sql = ", ".join(
            self.quote_name(field_name) for field_name in meta.key
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
            meta.method.upper(),
            partitioning_key_sql,
        )

        self.execute(sql, params)

    def delete_partitioned_model(self, model: Model) -> None:
        """Drops the specified partitioned model."""

        return self.delete_model(model)

    def add_range_partition(
        self,
        model: Model,
        name: str,
        from_values: Any,
        to_values: Any,
        comment: Optional[str] = None,
    ) -> None:
        """Creates a new range partition for the specified partitioned model.

        Arguments:
            model:
                Partitioned model to create a partition for.

            name:
                Name to give to the new partition.
                Final name will be "{table_name}_{partition_name}"

            from_values:
                Start of the partitioning key range of
                values that need to be stored in this
                partition.

            to_values:
                End of the partitioning key range of
                values that need to be stored in this
                partition.

            comment:
                Optionally, a comment to add on this
                partition table.
        """

        # asserts the model is a model set up for partitioning
        self._partitioning_properties_for_model(model)

        table_name = self.create_partition_table_name(model, name)

        sql = self.sql_add_range_partition % (
            self.quote_name(table_name),
            self.quote_name(model._meta.db_table),
            "%s",
            "%s",
        )

        with transaction.atomic():
            self.execute(sql, (from_values, to_values))

            if comment:
                self.set_comment_on_table(table_name, comment)

    def add_list_partition(
        self,
        model: Model,
        name: str,
        values: List[Any],
        comment: Optional[str] = None,
    ) -> None:
        """Creates a new list partition for the specified partitioned model.

        Arguments:
            model:
                Partitioned model to create a partition for.

            name:
                Name to give to the new partition.
                Final name will be "{table_name}_{partition_name}"

            values:
                Partition key values that should be
                stored in this partition.

            comment:
                Optionally, a comment to add on this
                partition table.
        """

        # asserts the model is a model set up for partitioning
        self._partitioning_properties_for_model(model)

        table_name = self.create_partition_table_name(model, name)

        sql = self.sql_add_list_partition % (
            self.quote_name(table_name),
            self.quote_name(model._meta.db_table),
            ",".join(["%s" for _ in range(len(values))]),
        )

        with transaction.atomic():
            self.execute(sql, values)

            if comment:
                self.set_comment_on_table(table_name, comment)

    def add_hash_partition(
        self,
        model: Model,
        name: str,
        modulus: int,
        remainder: int,
        comment: Optional[str] = None,
    ) -> None:
        """Creates a new hash partition for the specified partitioned model.

        Arguments:
            model:
                Partitioned model to create a partition for.

            name:
                Name to give to the new partition.
                Final name will be "{table_name}_{partition_name}"

            modulus:
                Integer value by which the key is divided.

            remainder:
                The remainder of the hash value when divided by modulus.

            comment:
                Optionally, a comment to add on this partition table.
        """

        # asserts the model is a model set up for partitioning
        self._partitioning_properties_for_model(model)

        table_name = self.create_partition_table_name(model, name)

        sql = self.sql_add_hash_partition % (
            self.quote_name(table_name),
            self.quote_name(model._meta.db_table),
            "%s",
            "%s",
        )

        with transaction.atomic():
            self.execute(sql, (modulus, remainder))

            if comment:
                self.set_comment_on_table(table_name, comment)

    def add_default_partition(
        self, model: Model, name: str, comment: Optional[str] = None
    ) -> None:
        """Creates a new default partition for the specified partitioned model.

        A default partition is a partition where rows are routed to when
        no more specific partition is a match.

        Arguments:
            model:
                Partitioned model to create a partition for.

            name:
                Name to give to the new partition.
                Final name will be "{table_name}_{partition_name}"

            comment:
                Optionally, a comment to add on this
                partition table.
        """

        # asserts the model is a model set up for partitioning
        self._partitioning_properties_for_model(model)

        table_name = self.create_partition_table_name(model, name)

        sql = self.sql_add_default_partition % (
            self.quote_name(table_name),
            self.quote_name(model._meta.db_table),
        )

        with transaction.atomic():
            self.execute(sql)

            if comment:
                self.set_comment_on_table(table_name, comment)

    def delete_partition(self, model: Model, name: str) -> None:
        """Deletes the partition with the specified name."""

        sql = self.sql_delete_partition % self.quote_name(
            self.create_partition_table_name(model, name)
        )
        self.execute(sql)

    def alter_db_table(
        self, model: Model, old_db_table: str, new_db_table: str
    ) -> None:
        """Alters a table/model."""

        super().alter_db_table(model, old_db_table, new_db_table)

        for side_effect in self.side_effects:
            side_effect.alter_db_table(model, old_db_table, new_db_table)

    def add_field(self, model: Model, field: Field) -> None:
        """Adds a new field to an exisiting model."""

        super().add_field(model, field)

        for side_effect in self.side_effects:
            side_effect.add_field(model, field)

    def remove_field(self, model: Model, field: Field) -> None:
        """Removes a field from an existing model."""

        for side_effect in self.side_effects:
            side_effect.remove_field(model, field)

        super().remove_field(model, field)

    def alter_field(
        self,
        model: Model,
        old_field: Field,
        new_field: Field,
        strict: bool = False,
    ) -> None:
        """Alters an existing field on an existing model."""

        super().alter_field(model, old_field, new_field, strict)

        for side_effect in self.side_effects:
            side_effect.alter_field(model, old_field, new_field, strict)

    def set_comment_on_table(self, table_name: str, comment: str) -> None:
        """Sets the comment on the specified table."""

        sql = self.sql_table_comment % (self.quote_name(table_name), "%s")
        self.execute(sql, (comment,))

    def _create_view_model(self, sql: str, model: Model) -> None:
        """Creates a new view model using the specified SQL query."""

        meta = self._view_properties_for_model(model)

        with self.connection.cursor() as cursor:
            view_sql = cursor.mogrify(*meta.query).decode("utf-8")

        self.execute(sql % (self.quote_name(model._meta.db_table), view_sql))

    def _extract_sql(self, method, *args):
        """Calls the specified method with the specified arguments and
        intercepts the SQL statement it WOULD execute.

        We use this to figure out the exact SQL statement Django would
        execute. We can then make a small modification and execute it
        ourselves.
        """

        with mock.patch.object(self, "execute") as execute:
            method(*args)

            return tuple(execute.mock_calls[0])[1]

    @staticmethod
    def _view_properties_for_model(model: Model):
        """Gets the view options for the specified model.

        Raises:
            ImproperlyConfigured:
                When the specified model is not set up
                as a view.
        """

        meta = getattr(model, "_view_meta", None)
        if not meta:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be a view."
                    " Create the `ViewMeta` class as a child of '%s'."
                )
                % (model.__name__, model.__name__)
            )

        if not is_sql_with_params(meta.query):
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be a view."
                    " Set the `query` and `key` attribute on the"
                    " `ViewMeta` class as a child of '%s'"
                )
                % (model.__name__, model.__name__)
            )

        return meta

    @staticmethod
    def _partitioning_properties_for_model(model: Model):
        """Gets the partitioning options for the specified model.

        Raises:
            ImproperlyConfigured:
                When the specified model is not set up
                for partitioning.
        """

        meta = getattr(model, "_partitioning_meta", None)
        if not meta:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Create the `PartitioningMeta` class as a child of '%s'."
                )
                % (model.__name__, model.__name__)
            )

        if not meta.method or not meta.key:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Set the `method` and `key` attributes on the"
                    " `PartitioningMeta` class as a child of '%s'"
                )
                % (model.__name__, model.__name__)
            )

        if meta.method not in PostgresPartitioningMethod:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " '%s' is not a member of the PostgresPartitioningMethod enum."
                )
                % (model.__name__, meta.method)
            )

        if not isinstance(meta.key, list):
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Partitioning key should be a list (of field names or values,"
                    " depending on the partitioning method)."
                )
                % model.__name__
            )

        try:
            for field_name in meta.key:
                model._meta.get_field(field_name)
        except FieldDoesNotExist:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Field '%s' in partitioning key %s is not a valid field on"
                    " '%s'."
                )
                % (model.__name__, field_name, meta.key, model.__name__)
            )

        return meta

    def create_partition_table_name(self, model: Model, name: str) -> str:
        return "%s_%s" % (model._meta.db_table.lower(), name.lower())
