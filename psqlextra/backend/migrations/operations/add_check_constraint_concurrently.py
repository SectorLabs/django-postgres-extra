from django.db.migrations.operations import AddConstraint
from django.db.models import CheckConstraint


class PostgresAddCheckConstraintConcurrently(AddConstraint):
    """Adds a new :see:CheckConstraint without acquiring a Access Exclusive
    table lock while the check is being validated for existing rows.

    This MUST be run in a non-atomic migration.

    This works by first adding the constraint with
    NOT VALID. This causes the constraint to only
    apply to new rows. This does acquire a Access
    Exclusive lock, but it's short-lived as no
    checking of existing rows has to be performed.

    Next, the constraint is validated for all existing
    rows. This requires only row-level locks.

    If this operation is run a a transaction (atomic
    migration), it will be no better then using
    the standard :see:AddConstraint operation.
    """

    atomic = False

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if not isinstance(self.constraint, CheckConstraint):
            raise TypeError(
                "PostgresAddCheckConstraintConcurrently can only be used with check constraints."
            )

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        model = to_state.apps.get_model(app_label, self.model_name)
        if not self.allow_migrate_model(schema_editor.connection.alias, model):
            return

        schema_editor.add_constraint_not_valid(model, self.constraint)
        schema_editor.validate_constraint(model, self.constraint)

    def describe(self) -> str:
        return "Create constraint %s concurrently on model %s" % (
            self.constraint.name,
            self.model_name,
        )
