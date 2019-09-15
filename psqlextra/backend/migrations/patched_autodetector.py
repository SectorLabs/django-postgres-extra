from contextlib import contextmanager
from unittest import mock

from django.db.migrations import CreateModel, DeleteModel
from django.db.migrations.autodetector import MigrationAutodetector

from psqlextra.models import PostgresPartitionedModel

from . import operations

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

    def add_create_model(self, operation: CreateModel):
        """Adds the specified :see:CreateModel operation to the list of
        operations to execute in the migration."""

        model = self.autodetector.new_apps.get_model(
            self.app_label, operation.name
        )
        if not issubclass(model, PostgresPartitionedModel):
            return self.add(operation)

        partitioning_options = model._partitioning_meta.original_attrs
        _, args, kwargs = operation.deconstruct()

        self.add(
            operations.PostgresAddDefaultPartition(
                model_name=model.__name__, name="default"
            )
        )

        self.add(
            operations.PostgresCreatePartitionedModel(
                *args, **kwargs, partitioning_options=partitioning_options
            )
        )

    def add_delete_model(self, operation: CreateModel):
        """Adds the specified :see:Deletemodel operation to the list of
        operations to execute in the migration."""

        model = self.autodetector.old_apps.get_model(
            self.app_label, operation.name
        )
        if not issubclass(model, PostgresPartitionedModel):
            return self.add(operation)

        _, args, kwargs = operation.deconstruct()

        return self.add(
            operations.PostgresDeletePartitionedModel(*args, **kwargs)
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

        return handler.add(operation)

    with mock.patch(add_operation_path, new=_patched):
        yield
