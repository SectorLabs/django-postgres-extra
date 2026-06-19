from typing import TYPE_CHECKING, Any, List, Optional, Type, cast
from unittest import mock

import django

from django.core.exceptions import (
    FieldDoesNotExist,
    ImproperlyConfigured,
    SuspiciousOperation,
)
from django.db import transaction
from django.db.backends.ddl_references import Statement
from django.db.backends.postgresql.schema import (  # type: ignore[import]
    DatabaseSchemaEditor,
)
from django.db.models import Field, Model

from psqlextra.settings import (
    postgres_prepend_local_search_path,
    postgres_reset_local_search_path,
)
from psqlextra.type_assertions import is_sql_with_params
from psqlextra.types import PostgresPartitioningMethod

from . import base_impl
from .introspection import PostgresIntrospection
from .side_effects import (
    HStoreRequiredSchemaEditorSideEffect,
    HStoreUniqueSchemaEditorSideEffect,
)

if TYPE_CHECKING:

    class SchemaEditor(DatabaseSchemaEditor):
        pass

else:
    SchemaEditor = base_impl.schema_editor()


class PostgresSchemaEditor(SchemaEditor):
    """Schema editor that adds extra methods for PostgreSQL specific features
    and hooks into existing implementations to add side effects specific to
    PostgreSQL."""

    sql_add_pk = "ALTER TABLE %s ADD PRIMARY KEY (%s)"

    sql_create_fk_not_valid = f"{SchemaEditor.sql_create_fk} NOT VALID"
    sql_validate_fk = "ALTER TABLE %s VALIDATE CONSTRAINT %s"

    sql_create_sequence_with_owner = "CREATE SEQUENCE %s OWNED BY %s.%s"

    sql_alter_table_storage_setting = "ALTER TABLE %s SET (%s = %s)"
    sql_reset_table_storage_setting = "ALTER TABLE %s RESET (%s)"

    sql_alter_table_schema = "ALTER TABLE %s SET SCHEMA %s"
    sql_create_schema = "CREATE SCHEMA %s"
    sql_delete_schema = "DROP SCHEMA %s"
    sql_delete_schema_cascade = "DROP SCHEMA %s CASCADE"

    sql_create_view = "CREATE VIEW %s AS (%s)"
    sql_replace_view = "CREATE OR REPLACE VIEW %s AS (%s)"
    sql_drop_view = "DROP VIEW IF EXISTS %s"
    sql_create_materialized_view_with_data = (
        "CREATE MATERIALIZED VIEW %s AS (%s) WITH DATA"
    )
    sql_create_materialized_view_without_data = (
        "CREATE MATERIALIZED VIEW %s AS (%s) WITH NO DATA"
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

    side_effects: List[DatabaseSchemaEditor] = [
        cast(DatabaseSchemaEditor, HStoreUniqueSchemaEditorSideEffect()),
        cast(DatabaseSchemaEditor, HStoreRequiredSchemaEditorSideEffect()),
    ]

    def __init__(self, connection, collect_sql=False, atomic=True):
        super().__init__(connection, collect_sql, atomic)

        for side_effect in self.side_effects:
            side_effect.execute = self.execute
            side_effect.quote_name = self.quote_name

        self.deferred_sql = []
        self.introspection = PostgresIntrospection(self.connection)

    def create_schema(self, name: str) -> None:
        """Creates a Postgres schema."""

        self.execute(self.sql_create_schema % self.quote_name(name))

    def delete_schema(self, name: str, cascade: bool) -> None:
        """Drops a Postgres schema."""

        sql = (
            self.sql_delete_schema
            if not cascade
            else self.sql_delete_schema_cascade
        )
        self.execute(sql % self.quote_name(name))

    def create_model(self, model: Type[Model]) -> None:
        """Creates a new model."""

        super().create_model(model)

        for side_effect in self.side_effects:
            side_effect.create_model(model)

    def delete_model(self, model: Type[Model]) -> None:
        """Drops/deletes an existing model."""

        for side_effect in self.side_effects:
            side_effect.delete_model(model)

        super().delete_model(model)

    def clone_model_structure_to_schema(
        self, model: Type[Model], *, schema_name: str
    ) -> None:
        """Creates a clone of the columns for the specified model in a separate
        schema.

        The table will have exactly the same name as the model table
        in the default schema. It will have none of the constraints,
        foreign keys and indexes.

        Use this to create a temporary clone of a model table to
        replace the original model table later on. The lack of
        indices and constraints allows for greater write speeds.

        The original model table will be unaffected.

        Arguments:
            model:
                Model to clone the table of into the
                specified schema.

            schema_name:
                Name of the schema to create the cloned
                table in.
        """

        table_name = model._meta.db_table
        quoted_table_name = self.quote_name(model._meta.db_table)
        quoted_schema_name = self.quote_name(schema_name)

        quoted_table_fqn = f"{quoted_schema_name}.{quoted_table_name}"

        self.execute(
            self.sql_create_table
            % {
                "table": quoted_table_fqn,
                "definition": f"LIKE {quoted_table_name} INCLUDING ALL EXCLUDING CONSTRAINTS EXCLUDING INDEXES",
            }
        )

        # Copy sequences
        #
        # Django 4.0 and older do not use IDENTITY so Postgres does
        # not copy the sequences into the new table. We do it manually.
        if django.VERSION < (4, 1):
            with self.connection.cursor() as cursor:
                sequences = self.introspection.get_sequences(cursor, table_name)

            for sequence in sequences:
                if sequence["table"] != table_name:
                    continue

                quoted_sequence_name = self.quote_name(sequence["name"])
                quoted_sequence_fqn = (
                    f"{quoted_schema_name}.{quoted_sequence_name}"
                )
                quoted_column_name = self.quote_name(sequence["column"])

                self.execute(
                    self.sql_create_sequence_with_owner
                    % (
                        quoted_sequence_fqn,
                        quoted_table_fqn,
                        quoted_column_name,
                    )
                )

                self.execute(
                    self.sql_alter_column
                    % {
                        "table": quoted_table_fqn,
                        "changes": self.sql_alter_column_default
                        % {
                            "column": quoted_column_name,
                            "default": "nextval('%s')" % quoted_sequence_fqn,
                        },
                    }
                )

        # Copy storage settings
        #
        # Postgres only copies column-level storage options, not
        # the table-level storage options.
        with self.connection.cursor() as cursor:
            storage_settings = self.introspection.get_storage_settings(
                cursor, model._meta.db_table
            )

        for setting_name, setting_value in storage_settings.items():
            self.alter_table_storage_setting(
                quoted_table_fqn, setting_name, setting_value
            )

    def clone_model_constraints_and_indexes_to_schema(
        self, model: Type[Model], *, schema_name: str
    ) -> None:
        """Adds the constraints, foreign keys and indexes to a model table that
        was cloned into a separate table without them by
        `clone_model_structure_to_schema`.

        Arguments:
            model:
                Model for which the cloned table was created.

            schema_name:
                Name of the schema in which the cloned table
                resides.
        """

        with postgres_prepend_local_search_path(
            [schema_name], using=self.connection.alias
        ):
            for constraint in model._meta.constraints:
                self.add_constraint(model, constraint)  # type: ignore[attr-defined]

            for index in model._meta.indexes:
                self.add_index(model, index)

            if model._meta.unique_together:
                self.alter_unique_together(
                    model, tuple(), model._meta.unique_together
                )

            if django.VERSION < (5, 1):
                if model._meta.index_together:
                    self.alter_index_together(
                        model, tuple(), model._meta.index_together
                    )

            for field in model._meta.local_concrete_fields:  # type: ignore[attr-defined]
                # Django creates primary keys later added to the model with
                # a custom name. We want the name as it was created originally.
                if field.primary_key:
                    with postgres_reset_local_search_path(
                        using=self.connection.alias
                    ):
                        [primary_key_name] = self._constraint_names(  # type: ignore[attr-defined]
                            model, primary_key=True
                        )

                    self.execute(
                        self.sql_create_pk
                        % {
                            "table": self.quote_name(model._meta.db_table),
                            "name": self.quote_name(primary_key_name),
                            "columns": self.quote_name(
                                field.db_column or field.attname
                            ),
                        }
                    )
                    continue

                # Django creates foreign keys in a single statement which acquires
                # a AccessExclusiveLock on the referenced table. We want to avoid
                # that and created the FK as NOT VALID. We can run VALIDATE in
                # a separate transaction later to validate the entries without
                # acquiring a AccessExclusiveLock.
                if field.remote_field:
                    with postgres_reset_local_search_path(
                        using=self.connection.alias
                    ):
                        [fk_name] = self._constraint_names(  # type: ignore[attr-defined]
                            model, [field.column], foreign_key=True
                        )

                    sql = Statement(
                        self.sql_create_fk_not_valid,
                        table=self.quote_name(model._meta.db_table),
                        name=self.quote_name(fk_name),
                        column=self.quote_name(field.column),
                        to_table=self.quote_name(
                            field.target_field.model._meta.db_table
                        ),
                        to_column=self.quote_name(field.target_field.column),
                        deferrable=self.connection.ops.deferrable_sql(),
                    )

                    self.execute(sql)

                # It's hard to alter a field's check because it is defined
                # by the field class, not the field instance. Handle this
                # manually.
                field_check = field.db_parameters(self.connection).get("check")
                if field_check:
                    with postgres_reset_local_search_path(
                        using=self.connection.alias
                    ):
                        [field_check_name] = self._constraint_names(  # type: ignore[attr-defined]
                            model,
                            [field.column],
                            check=True,
                            exclude={
                                constraint.name
                                for constraint in model._meta.constraints
                            },
                        )

                    self.execute(
                        self._create_check_sql(  # type: ignore[attr-defined]
                            model, field_check_name, field_check
                        )
                    )

                # Clone the field and alter its state to math our current
                # table definition. This will cause Django see the missing
                # indices and create them.
                if field.remote_field:
                    # We add the foreign key constraint ourselves with NOT VALID,
                    # hence, we specify `db_constraint=False` on both old/new.
                    # Django won't touch the foreign key constraint.
                    old_field = self._clone_model_field(
                        field, db_index=False, unique=False, db_constraint=False
                    )
                    new_field = self._clone_model_field(
                        field, db_constraint=False
                    )
                    self.alter_field(model, old_field, new_field)
                else:
                    old_field = self._clone_model_field(
                        field, db_index=False, unique=False
                    )
                    new_field = self._clone_model_field(field)
                    self.alter_field(model, old_field, new_field)

    def clone_model_foreign_keys_to_schema(
        self, model: Type[Model], schema_name: str
    ) -> None:
        """Validates the foreign keys in the cloned model table created by
        `clone_model_structure_to_schema` and
        `clone_model_constraints_and_indexes_to_schema`.

        Do NOT run this in the same transaction as the
        foreign keys were added to the table. It WILL
        acquire a long-lived AccessExclusiveLock.

        Arguments:
            model:
                Model for which the cloned table was created.

            schema_name:
                Name of the schema in which the cloned table
                resides.
        """

        constraint_names = self._constraint_names(model, foreign_key=True)  # type: ignore[attr-defined]

        with postgres_prepend_local_search_path(
            [schema_name], using=self.connection.alias
        ):
            for fk_name in constraint_names:
                self.execute(
                    self.sql_validate_fk
                    % (
                        self.quote_name(model._meta.db_table),
                        self.quote_name(fk_name),
                    )
                )

    def alter_table_storage_setting(
        self, table_name: str, name: str, value: str
    ) -> None:
        """Alters a storage setting for a table.

        See: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS

        Arguments:
            table_name:
                Name of the table to alter the setting for.

            name:
                Name of the setting to alter.

            value:
                Value to alter the setting to.

                Note that this is always a string, even if it looks
                like a number or a boolean. That's how Postgres
                stores storage settings internally.
        """

        self.execute(
            self.sql_alter_table_storage_setting
            % (self.quote_name(table_name), name, value)
        )

    def alter_model_storage_setting(
        self, model: Type[Model], name: str, value: str
    ) -> None:
        """Alters a storage setting for the model's table.

        See: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS

        Arguments:
            model:
                Model of which to alter the table
                setting.

            name:
                Name of the setting to alter.

            value:
                Value to alter the setting to.

                Note that this is always a string, even if it looks
                like a number or a boolean. That's how Postgres
                stores storage settings internally.
        """

        self.alter_table_storage_setting(model._meta.db_table, name, value)

    def reset_table_storage_setting(self, table_name: str, name: str) -> None:
        """Resets a table's storage setting to the database or server default.

        See: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS

        Arguments:
            table_name:
                Name of the table to reset the setting for.

            name:
                Name of the setting to reset.
        """

        self.execute(
            self.sql_reset_table_storage_setting
            % (self.quote_name(table_name), name)
        )

    def reset_model_storage_setting(
        self, model: Type[Model], name: str
    ) -> None:
        """Resets a model's table storage setting to the database or server
        default.

        See: https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS

        Arguments:
            table_name:
            model:
                Model for which to reset the table setting for.

            name:
                Name of the setting to reset.
        """

        self.reset_table_storage_setting(model._meta.db_table, name)

    def alter_table_schema(self, table_name: str, schema_name: str) -> None:
        """Moves the specified table into the specified schema.

        WARNING: Moving models into a different schema than the default
        will break querying the model.

        Arguments:
            table_name:
                Name of the table to move into the specified schema.

            schema_name:
                Name of the schema to move the table to.
        """

        self.execute(
            self.sql_alter_table_schema
            % (self.quote_name(table_name), self.quote_name(schema_name))
        )

    def alter_model_schema(self, model: Type[Model], schema_name: str) -> None:
        """Moves the specified model's table into the specified schema.

        WARNING: Moving models into a different schema than the default
        will break querying the model.

        Arguments:
            model:
               Model of which to move the table.

            schema_name:
                Name of the schema to move the model's table to.
        """

        self.execute(
            self.sql_alter_table_schema
            % (
                self.quote_name(model._meta.db_table),
                self.quote_name(schema_name),
            )
        )

    def refresh_materialized_view_model(
        self, model: Type[Model], concurrently: bool = False
    ) -> None:
        """Refreshes a materialized view."""

        sql_template = (
            self.sql_refresh_materialized_view_concurrently
            if concurrently
            else self.sql_refresh_materialized_view
        )

        sql = sql_template % self.quote_name(model._meta.db_table)
        self.execute(sql)

    def create_view_model(self, model: Type[Model]) -> None:
        """Creates a new view model."""

        self._create_view_model(self.sql_create_view, model)

    def replace_view_model(self, model: Type[Model]) -> None:
        """Replaces a view model with a newer version.

        This is used to alter the backing query of a view.
        """

        self._create_view_model(self.sql_replace_view, model)

    def delete_view_model(self, model: Type[Model]) -> None:
        """Deletes a view model."""

        sql = self.sql_drop_view % self.quote_name(model._meta.db_table)
        self.execute(sql)

    def create_materialized_view_model(
        self, model: Type[Model], *, with_data: bool = True
    ) -> None:
        """Creates a new materialized view model."""

        if with_data:
            self._create_view_model(
                self.sql_create_materialized_view_with_data, model
            )
        else:
            self._create_view_model(
                self.sql_create_materialized_view_without_data, model
            )

    def replace_materialized_view_model(self, model: Type[Model]) -> None:
        """Replaces a materialized view with a newer version.

        This is used to alter the backing query of a materialized view.

        Replacing a materialized view is a lot trickier than a normal
        view. For normal views we can use `CREATE OR REPLACE VIEW`, but
        for materialized views, we have to create the new view, copy all
        indexes and constraints and drop the old one.

        This operation is atomic as it runs in a transaction.
        """

        with self.connection.cursor() as cursor:
            constraints = self.introspection.get_constraints(
                cursor, model._meta.db_table
            )

        with transaction.atomic(using=self.connection.alias):
            self.delete_materialized_view_model(model)
            self.create_materialized_view_model(model)

            for constraint_name, constraint_options in constraints.items():
                if not constraint_options["definition"]:
                    raise SuspiciousOperation(
                        "Table %s has a constraint '%s' that no definition could be generated for",
                        (model._meta.db_table, constraint_name),
                    )

                self.execute(constraint_options["definition"])

    def delete_materialized_view_model(self, model: Type[Model]) -> None:
        """Deletes a materialized view model."""

        sql = self.sql_drop_materialized_view % self.quote_name(
            model._meta.db_table
        )
        self.execute(sql)

    def create_partitioned_model(self, model: Type[Model]) -> None:
        """Creates a new partitioned model."""

        meta = self._partitioning_properties_for_model(model)

        # get the sql statement that django creates for normal
        # table creations..
        sql, params = self._extract_sql(self.create_model, model)

        partitioning_key_sql = ", ".join(
            self.quote_name(field_name) for field_name in meta.key
        )

        pk_field = model._meta.pk
        has_composite_pk = self._is_composite_primary_key(pk_field)

        # create a composite key that includes the partitioning key
        # if the user didn't already define one
        if not has_composite_pk:
            inline_pk_sql = self._create_primary_key_inline_sql(model, pk_field)
            inline_tablespace_sql = (
                self._create_primary_key_inline_tablespace_sql(model, pk_field)
            )

            sql = sql.replace(inline_pk_sql, "")

            if (
                not self._is_virtual_primary_key(pk_field)
                and pk_field
                and pk_field.name not in meta.key
            ):
                last_brace_idx = sql.rfind(")")
                sql = (
                    sql[:last_brace_idx]
                    + f", PRIMARY KEY (%s, %s){inline_tablespace_sql}"
                    % (
                        self.quote_name(pk_field.name),
                        partitioning_key_sql,
                    )
                    + sql[last_brace_idx:]
                )
            else:
                last_brace_idx = sql.rfind(")")
                sql = (
                    sql[:last_brace_idx]
                    + f", PRIMARY KEY (%s){inline_tablespace_sql}"
                    % (partitioning_key_sql,)
                    + sql[last_brace_idx:]
                )

        # extend the standard CREATE TABLE statement with
        # 'PARTITION BY ...'
        last_brace_idx = sql.rfind(")") + 1
        sql = (
            sql[:last_brace_idx]
            + self.sql_partition_by
            % (
                meta.method.upper(),
                partitioning_key_sql,
            )
            + sql[last_brace_idx:]
        )

        self.execute(sql, params)

    def delete_partitioned_model(self, model: Type[Model]) -> None:
        """Drops the specified partitioned model."""

        return self.delete_model(model)

    def add_range_partition(
        self,
        model: Type[Model],
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

        with transaction.atomic(using=self.connection.alias):
            self.execute(sql, (from_values, to_values))

            if comment:
                self.set_comment_on_table(table_name, comment)

    def add_list_partition(
        self,
        model: Type[Model],
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
        meta = self._partitioning_properties_for_model(model)

        table_name = self.create_partition_table_name(model, name)

        sql = self.sql_add_list_partition % (
            self.quote_name(table_name),
            self.quote_name(model._meta.db_table),
            ",".join(["%s" for _ in range(len(values))]),
        )

        if getattr(meta, "sub_key", None) and len(meta.sub_key) > 0:
            sub_partitioning_key_sql = ", ".join(
                self.quote_name(field_name) for field_name in meta.sub_key
            )

            last_brace_idx = sql.rfind(")") + 1
            sql = (
                sql[:last_brace_idx]
                + self.sql_partition_by
                % (
                    meta.sub_method.upper(),
                    sub_partitioning_key_sql,
                )
                + sql[last_brace_idx:]
            )

        with transaction.atomic(using=self.connection.alias):
            self.execute(sql, values)

            if comment:
                self.set_comment_on_table(table_name, comment)

    def add_hash_partition(
        self,
        model: Type[Model],
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

        with transaction.atomic(using=self.connection.alias):
            self.execute(sql, (modulus, remainder))

            if comment:
                self.set_comment_on_table(table_name, comment)

    def add_default_partition(
        self, model: Type[Model], name: str, comment: Optional[str] = None
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

        with transaction.atomic(using=self.connection.alias):
            self.execute(sql)

            if comment:
                self.set_comment_on_table(table_name, comment)

    def delete_partition(self, model: Type[Model], name: str) -> None:
        """Deletes the partition with the specified name."""

        sql = self.sql_delete_partition % self.quote_name(
            self.create_partition_table_name(model, name)
        )
        self.execute(sql)

    def alter_db_table(
        self, model: Type[Model], old_db_table: str, new_db_table: str
    ) -> None:
        """Alters a table/model."""

        super().alter_db_table(model, old_db_table, new_db_table)

        for side_effect in self.side_effects:
            side_effect.alter_db_table(model, old_db_table, new_db_table)

    def add_field(self, model: Type[Model], field: Field) -> None:
        """Adds a new field to an exisiting model."""

        super().add_field(model, field)

        for side_effect in self.side_effects:
            side_effect.add_field(model, field)

    def remove_field(self, model: Type[Model], field: Field) -> None:
        """Removes a field from an existing model."""

        for side_effect in self.side_effects:
            side_effect.remove_field(model, field)

        super().remove_field(model, field)

    def alter_field(
        self,
        model: Type[Model],
        old_field: Field,
        new_field: Field,
        strict: bool = False,
    ) -> None:
        """Alters an existing field on an existing model."""

        super().alter_field(model, old_field, new_field, strict)

        for side_effect in self.side_effects:
            side_effect.alter_field(model, old_field, new_field, strict)

    def vacuum_table(
        self,
        table_name: str,
        columns: List[str] = [],
        *,
        full: bool = False,
        freeze: bool = False,
        verbose: bool = False,
        analyze: bool = False,
        disable_page_skipping: bool = False,
        skip_locked: bool = False,
        index_cleanup: bool = False,
        truncate: bool = False,
        parallel: Optional[int] = None,
    ) -> None:
        """Runs the VACUUM statement on the specified table with the specified
        options.

        Arguments:
            table_name:
                Name of the table to run VACUUM on.

            columns:
                Optionally, a list of columns to vacuum. If not
                specified, all columns are vacuumed.
        """

        if self.connection.in_atomic_block:
            raise SuspiciousOperation("Vacuum cannot be done in a transaction")

        options = []
        if full:
            options.append("FULL")
        if freeze:
            options.append("FREEZE")
        if verbose:
            options.append("VERBOSE")
        if analyze:
            options.append("ANALYZE")
        if disable_page_skipping:
            options.append("DISABLE_PAGE_SKIPPING")
        if skip_locked:
            options.append("SKIP_LOCKED")
        if index_cleanup:
            options.append("INDEX_CLEANUP")
        if truncate:
            options.append("TRUNCATE")
        if parallel is not None:
            options.append(f"PARALLEL {parallel}")

        sql = "VACUUM"

        if options:
            options_sql = ", ".join(options)
            sql += f" ({options_sql})"

        sql += f" {self.quote_name(table_name)}"

        if columns:
            columns_sql = ", ".join(
                [self.quote_name(column) for column in columns]
            )
            sql += f" ({columns_sql})"

        self.execute(sql)

    def vacuum_model(
        self, model: Type[Model], fields: List[Field] = [], **kwargs
    ) -> None:
        """Runs the VACUUM statement on the table of the specified model with
        the specified options.

        Arguments:
            table_name:
            model:
                Model of which to run VACUUM the table.

            fields:
                Optionally, a list of fields to vacuum. If not
                specified, all fields are vacuumed.
        """

        columns = [
            field.column
            for field in fields
            if getattr(field, "concrete", False) and field.column
        ]
        self.vacuum_table(model._meta.db_table, columns, **kwargs)

    def set_comment_on_table(self, table_name: str, comment: str) -> None:
        """Sets the comment on the specified table."""

        sql = self.sql_table_comment % (self.quote_name(table_name), "%s")
        self.execute(sql, (comment,))

    def _create_view_model(self, sql: str, model: Type[Model]) -> None:
        """Creates a new view model using the specified SQL query."""

        meta = self._view_properties_for_model(model)

        with self.connection.cursor() as cursor:
            view_sql = cursor.mogrify(*meta.query)
            if isinstance(view_sql, bytes):
                view_sql = view_sql.decode("utf-8")

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
    def _view_properties_for_model(model: Type[Model]):
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
    def _partitioning_properties_for_model(model: Type[Model]):
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

        if not isinstance(meta.key, (list, tuple)):
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " Partitioning key should be a list (of field names or values,"
                    " depending on the partitioning method)."
                )
                % model.__name__
            )
        if meta.sub_method and len(meta.sub_key)==0:
            raise ImproperlyConfigured(
                (
                    "Model '%s' is not properly configured to be partitioned."
                    " 'sub_method' is specified '%s', but no 'sub_key' is defined."
                )
                % (model.__name__, meta.sub_method)
            )
        
        try:
            for field_name in meta.key + meta.sub_key:
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

    def create_partition_table_name(self, model: Type[Model], name: str) -> str:
        return "%s_%s" % (model._meta.db_table.lower(), name.lower())

    def _create_primary_key_inline_sql(
        self, model: Type[Model], pk_field: Optional[Field]
    ) -> str:
        pk_field = model._meta.pk
        if not pk_field:
            return ""

        tablespace_sql = self._create_primary_key_inline_tablespace_sql(
            model, pk_field
        )

        if self._is_virtual_primary_key(pk_field):
            return ""

        pk_sql = " PRIMARY KEY" if pk_field else ""
        if tablespace_sql:
            pk_sql += tablespace_sql

        return pk_sql

    def _create_primary_key_inline_tablespace_sql(
        self, model: Type[Model], pk_field: Optional[Field]
    ) -> str:
        tablespace = (pk_field.db_tablespace if pk_field else None) or model._meta.db_tablespace  # type: ignore [attr-defined]
        return (
            " " + self.connection.ops.tablespace_sql(tablespace, inline=True)
            if tablespace
            else ""
        )

    def _is_composite_primary_key(self, field: Optional[Field]) -> bool:
        """Checks whether the specified field is a composite primary key.

        This needs to be wrapped because composite primary keys are only
        natively supported in Django 5.2 and newer.
        """

        if not field:
            return False

        try:
            from django.db.models.fields.composite import CompositePrimaryKey

            return isinstance(field, CompositePrimaryKey)
        except ImportError:
            return False

    def _is_virtual_primary_key(self, field: Optional[Field]) -> bool:
        """Gets whether the declared primary key is a virtual field that
        doesn't construct any real column in the DB.

        It is pseudo-standard to have virtual fields by creating
        a field with no DB type. CompositePrimaryKey in Django
        5.2 and newer use this. Some third-party packages use
        the same technique.

        ManyToManyFields were the first to actually use this.
        """

        if not field:
            return True

        pk_db_params = field.db_parameters(connection=self.connection)
        pk_db_type = pk_db_params["type"] if pk_db_params else None
        return not bool(pk_db_type)

    def _clone_model_field(self, field: Field, **overrides) -> Field:
        """Clones the specified model field and overrides its kwargs with the
        specified overrides.

        The cloned field will not be contributed to the model.
        """

        _, _, field_args, field_kwargs = field.deconstruct()

        cloned_field_args = field_args[:]
        cloned_field_kwargs = {**field_kwargs, **overrides}

        cloned_field = field.__class__(
            *cloned_field_args, **cloned_field_kwargs
        )
        cloned_field.model = field.model
        cloned_field.set_attributes_from_name(field.name)

        if cloned_field.remote_field and field.remote_field:
            cloned_field.remote_field.model = field.remote_field.model
            cloned_field.set_attributes_from_rel()  # type: ignore[attr-defined]

        return cloned_field
