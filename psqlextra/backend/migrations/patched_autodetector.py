from contextlib import contextmanager
from unittest import mock

import django

from django.db.migrations import (
    AddField,
    AlterField,
    CreateModel,
    DeleteModel,
    RemoveField,
    RenameField,
)
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.operations.base import Operation

from psqlextra.models import (
    PostgresMaterializedViewModel,
    PostgresPartitionedModel,
    PostgresViewModel,
)
from psqlextra.types import PostgresPartitioningMethod

from . import operations
from .state import (
    PostgresMaterializedViewModelState,
    PostgresPartitionedModelState,
    PostgresViewModelState,
)

# original `MigrationAutodetector.add_operation`
# function, saved here so the patched version can
# call the original
add_operation = MigrationAutodetector.add_operation


class AddOperationHandler:
    """Handler for when operations are being added to a new migration.

    This is where we intercept operations such as
    :see:CreateModel to replace it with our own.
    """

    def __init__(self, autodetector, app_label, args, kwargs):
        self.autodetector = autodetector
        self.app_label = app_label
        self.args = args
        self.kwargs = kwargs

    def add(self, operation):
        """Adds the specified operation to the list of operations to execute in
        the migration."""

        return add_operation(
            self.autodetector,
            self.app_label,
            operation,
            *self.args,
            **self.kwargs,
        )

    def add_field(self, operation: AddField):
        """Adds the specified :see:AddField operation to the list of operations
        to execute in the migration."""

        return self._transform_view_field_operations(operation)

    def remove_field(self, operation: RemoveField):
        """Adds the specified :see:RemoveField operation to the list of
        operations to execute in the migration."""

        return self._transform_view_field_operations(operation)

    def alter_field(self, operation: AlterField):
        """Adds the specified :see:AlterField operation to the list of
        operations to execute in the migration."""

        return self._transform_view_field_operations(operation)

    def rename_field(self, operation: RenameField):
        """Adds the specified :see:RenameField operation to the list of
        operations to execute in the migration."""

        return self._transform_view_field_operations(operation)

    def _transform_view_field_operations(self, operation: Operation):
        """Transforms operations on fields on a (materialized) view into state
        only operations.

        One cannot add/remove/delete fields on a (materialized) view,
        however, we do want Django's migration system to keep track of
        these kind of changes to the model. The :see:ApplyState
        operation just tells Django the operation was applied without
        actually applying it.
        """

        if django.VERSION >= (4, 0):
            model_identifier = (self.app_label, operation.model_name.lower())
            model_state = (
                self.autodetector.to_state.models.get(model_identifier)
                or self.autodetector.from_state.models[model_identifier]
            )

            if isinstance(model_state, PostgresViewModelState):
                return self.add(
                    operations.ApplyState(state_operation=operation)
                )
        else:
            model = self.autodetector.new_apps.get_model(
                self.app_label, operation.model_name
            )

            if issubclass(model, PostgresViewModel):
                return self.add(
                    operations.ApplyState(state_operation=operation)
                )

        return self.add(operation)

    def add_create_model(self, operation: CreateModel):
        """Adds the specified :see:CreateModel operation to the list of
        operations to execute in the migration."""

        if django.VERSION >= (4, 0):
            model_state = self.autodetector.to_state.models[
                self.app_label, operation.name.lower()
            ]

            if isinstance(model_state, PostgresPartitionedModelState):
                return self.add_create_partitioned_model(operation)
            elif isinstance(model_state, PostgresMaterializedViewModelState):
                return self.add_create_materialized_view_model(operation)
            elif isinstance(model_state, PostgresViewModelState):
                return self.add_create_view_model(operation)
        else:
            model = self.autodetector.new_apps.get_model(
                self.app_label, operation.name
            )

            if issubclass(model, PostgresPartitionedModel):
                return self.add_create_partitioned_model(operation)
            elif issubclass(model, PostgresMaterializedViewModel):
                return self.add_create_materialized_view_model(operation)
            elif issubclass(model, PostgresViewModel):
                return self.add_create_view_model(operation)

        return self.add(operation)

    def add_delete_model(self, operation: DeleteModel):
        """Adds the specified :see:Deletemodel operation to the list of
        operations to execute in the migration."""

        if django.VERSION >= (4, 0):
            model_state = self.autodetector.from_state.models[
                self.app_label, operation.name.lower()
            ]

            if isinstance(model_state, PostgresPartitionedModelState):
                return self.add_delete_partitioned_model(operation)
            elif isinstance(model_state, PostgresMaterializedViewModelState):
                return self.add_delete_materialized_view_model(operation)
            elif isinstance(model_state, PostgresViewModelState):
                return self.add_delete_view_model(operation)
        else:
            model = self.autodetector.old_apps.get_model(
                self.app_label, operation.name
            )

            if issubclass(model, PostgresPartitionedModel):
                return self.add_delete_partitioned_model(operation)
            elif issubclass(model, PostgresMaterializedViewModel):
                return self.add_delete_materialized_view_model(operation)
            elif issubclass(model, PostgresViewModel):
                return self.add_delete_view_model(operation)

        return self.add(operation)

    def add_create_partitioned_model(self, operation: CreateModel):
        """Adds a :see:PostgresCreatePartitionedModel operation to the list of
        operations to execute in the migration."""

        if django.VERSION >= (4, 0):
            model_state = self.autodetector.to_state.models[
                self.app_label, operation.name.lower()
            ]
            partitioning_options = model_state.partitioning_options
        else:
            model = self.autodetector.new_apps.get_model(
                self.app_label, operation.name
            )
            partitioning_options = model._partitioning_meta.original_attrs

        _, args, kwargs = operation.deconstruct()

        if partitioning_options["method"] != PostgresPartitioningMethod.HASH:
            self.add(
                operations.PostgresAddDefaultPartition(
                    model_name=operation.name, name="default"
                )
            )

        self.add(
            operations.PostgresCreatePartitionedModel(
                *args, **kwargs, partitioning_options=partitioning_options
            )
        )

    def add_delete_partitioned_model(self, operation: DeleteModel):
        """Adds a :see:PostgresDeletePartitionedModel operation to the list of
        operations to execute in the migration."""

        _, args, kwargs = operation.deconstruct()
        return self.add(
            operations.PostgresDeletePartitionedModel(*args, **kwargs)
        )

    def add_create_view_model(self, operation: CreateModel):
        """Adds a :see:PostgresCreateViewModel operation to the list of
        operations to execute in the migration."""

        if django.VERSION >= (4, 0):
            model_state = self.autodetector.to_state.models[
                self.app_label, operation.name.lower()
            ]
            view_options = model_state.view_options
        else:
            model = self.autodetector.new_apps.get_model(
                self.app_label, operation.name
            )
            view_options = model._view_meta.original_attrs

        _, args, kwargs = operation.deconstruct()

        self.add(
            operations.PostgresCreateViewModel(
                *args, **kwargs, view_options=view_options
            )
        )

    def add_delete_view_model(self, operation: DeleteModel):
        """Adds a :see:PostgresDeleteViewModel operation to the list of
        operations to execute in the migration."""

        _, args, kwargs = operation.deconstruct()
        return self.add(operations.PostgresDeleteViewModel(*args, **kwargs))

    def add_create_materialized_view_model(self, operation: CreateModel):
        """Adds a :see:PostgresCreateMaterializedViewModel operation to the
        list of operations to execute in the migration."""

        if django.VERSION >= (4, 0):
            model_state = self.autodetector.to_state.models[
                self.app_label, operation.name.lower()
            ]
            view_options = model_state.view_options
        else:
            model = self.autodetector.new_apps.get_model(
                self.app_label, operation.name
            )
            view_options = model._view_meta.original_attrs

        _, args, kwargs = operation.deconstruct()

        self.add(
            operations.PostgresCreateMaterializedViewModel(
                *args, **kwargs, view_options=view_options
            )
        )

    def add_delete_materialized_view_model(self, operation: DeleteModel):
        """Adds a :see:PostgresDeleteMaterializedViewModel operation to the
        list of operations to execute in the migration."""

        _, args, kwargs = operation.deconstruct()
        return self.add(
            operations.PostgresDeleteMaterializedViewModel(*args, **kwargs)
        )


@contextmanager
def patched_autodetector():
    """Patches the standard Django :seee:MigrationAutodetector for the duration
    of the context.

    The patch intercepts the `add_operation` function to
    customize how new operations are added.

    We have to do this because there is no way in Django
    to extend the auto detector otherwise.
    """

    autodetector_module_path = "django.db.migrations.autodetector"
    autodetector_class_path = (
        f"{autodetector_module_path}.MigrationAutodetector"
    )
    add_operation_path = f"{autodetector_class_path}.add_operation"

    def _patched(autodetector, app_label, operation, *args, **kwargs):
        handler = AddOperationHandler(autodetector, app_label, args, kwargs)

        if isinstance(operation, CreateModel):
            return handler.add_create_model(operation)

        if isinstance(operation, DeleteModel):
            return handler.add_delete_model(operation)

        if isinstance(operation, AddField):
            return handler.add_field(operation)

        if isinstance(operation, RemoveField):
            return handler.remove_field(operation)

        if isinstance(operation, AlterField):
            return handler.alter_field(operation)

        if isinstance(operation, RenameField):
            return handler.rename_field(operation)

        return handler.add(operation)

    with mock.patch(add_operation_path, new=_patched):
        yield
